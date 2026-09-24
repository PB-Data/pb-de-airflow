"""Membangun dlt source dari config, khusus SQL Server lewat ODBC.

Tidak ada satu pun kredensial di file ini. Semuanya dibaca dari environment
variable dengan awalan yang ditulis di config (`source.env_prefix`), sehingga
menambah database kedua cukup dengan menambah set env var baru.
"""

from __future__ import annotations

import logging
import os
from typing import Sequence

import sqlalchemy as sa
from dlt.sources.sql_database import sql_database

from pb_dwh.config.models import ConfigError, SourceConfig
from pb_dwh.sources.types import build_table_adapter, build_type_adapter
from pb_dwh.state.incremental import build_incremental

logger = logging.getLogger(__name__)

DEFAULT_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


def _env(prefix: str, nama: str, wajib: bool = True, cadangan: str | None = None) -> str:
    kunci = f"{prefix}_{nama}"
    nilai = os.environ.get(kunci)
    if nilai:
        return nilai
    if wajib:
        raise ConfigError(
            f"Environment variable {kunci} belum di-set. "
            f"Isi di docker/.env (lihat .env.example), lalu `docker compose up -d`."
        )
    return cadangan or ""


def build_connection_url(cfg: SourceConfig) -> sa.engine.URL:
    """Rakit URL SQLAlchemy untuk mssql+pyodbc.

    Dipakai `sa.engine.URL.create`, bukan f-string, supaya password yang
    mengandung karakter seperti @ : / ? # ter-escape dengan benar. Ini sumber
    bug klasik yang gejalanya "login failed" padahal passwordnya benar.
    """
    prefix = cfg.env_prefix

    driver = _env(prefix, "DRIVER", wajib=False, cadangan=DEFAULT_ODBC_DRIVER)

    # Driver 18 memaksa Encrypt=yes secara default. Cloud SQL memakai sertifikat
    # yang tidak ada di trust store container, jadi TrustServerCertificate=yes
    # diperlukan kecuali server CA-nya sengaja dipasang.
    query = {
        "driver": driver,
        "Encrypt": _env(prefix, "ENCRYPT", wajib=False, cadangan="yes"),
        "TrustServerCertificate": _env(
            prefix, "TRUST_SERVER_CERTIFICATE", wajib=False, cadangan="yes"
        ),
        "LoginTimeout": _env(prefix, "LOGIN_TIMEOUT", wajib=False, cadangan="30"),
    }

    return sa.engine.URL.create(
        drivername="mssql+pyodbc",
        username=_env(prefix, "USER"),
        password=_env(prefix, "PASSWORD"),
        host=_env(prefix, "HOST"),
        port=int(_env(prefix, "PORT", wajib=False, cadangan="1433")),
        # Nama database dari YAML menang; env var hanya cadangan untuk config
        # yang tidak menuliskannya.
        database=cfg.database or _env(prefix, "DATABASE"),
        query=query,
    )


def build_source(cfg: SourceConfig, table_names: Sequence[str] | None = None):
    """Bangun dlt source berisi tabel-tabel dari config.

    `table_names` dipakai untuk menjalankan sebagian tabel saja (testing lokal).
    Kalau None, semua tabel di config ikut.

    Dua keputusan penting di sini:

    * `defer_table_reflect=True` -- reflection baru terjadi saat task jalan,
      bukan saat DAG di-parse. Tanpa ini, DAG processor akan membuka koneksi ke
      SQL Server setiap 30 detik, untuk 100 tabel.
    * `table_names` diisi eksplisit, bukan `.with_resources()` -- dlt hanya
      me-reflect tabel yang disebut, bukan seluruh isi schema.
    """
    terpilih = cfg.tables
    if table_names is not None:
        diminta = list(table_names)
        tidak_dikenal = [n for n in diminta if n not in cfg.table_names]
        if tidak_dikenal:
            raise ConfigError(
                f"Tabel tidak ada di {cfg.source_file}: {', '.join(tidak_dikenal)}. "
                f"Yang terdaftar: {', '.join(cfg.table_names)}"
            )
        terpilih = tuple(t for t in cfg.tables if t.name in set(diminta))

    if not terpilih:
        raise ConfigError(f"Tidak ada tabel yang dipilih untuk source '{cfg.name}'.")

    # chunk_size dan reflection_level ada per tabel di config, tapi
    # `sql_database()` menerimanya sekali untuk seluruh source. Dipakai nilai
    # yang paling konservatif dari tabel-tabel yang ikut, supaya tabel paling
    # berat yang menentukan, bukan tabel paling ringan.
    chunk_size = min(t.chunk_size for t in terpilih)
    reflection_level = _reflection_paling_detail(terpilih)

    exclude_map = {t.name: t.exclude_columns for t in terpilih if t.exclude_columns}

    source = sql_database(
        # dlt menolak objek sa.URL ("ConnectionStringCredentials cannot parse
        # the configuration value"), jadi dikirim sebagai string. Hasil render
        # tetap membawa escape password dari URL.create.
        credentials=build_connection_url(cfg).render_as_string(hide_password=False),
        schema=cfg.schema,
        table_names=[t.name for t in terpilih],
        chunk_size=chunk_size,
        backend=cfg.backend,
        reflection_level=reflection_level,
        defer_table_reflect=True,
        type_adapter_callback=build_type_adapter(),
        table_adapter_callback=build_table_adapter(exclude_map),
        include_views=False,
    )

    for tabel in terpilih:
        resource = source.resources[tabel.name]

        hints: dict = {"write_disposition": tabel.write_disposition}

        if tabel.write_disposition == "merge":
            hints["primary_key"] = list(tabel.primary_key)

        incremental = build_incremental(tabel)
        if incremental is not None:
            hints["incremental"] = incremental

        if tabel.column_overrides:
            # Catatan: dengan defer_table_reflect=True, schema diputuskan saat
            # eksekusi dan bisa menimpa hint kolom di sini. Kalau override tidak
            # menempel, naikkan reflection_level tabel itu ke full_with_precision.
            hints["columns"] = {
                nama: dict(hint) for nama, hint in tabel.column_overrides.items()
            }

        resource.apply_hints(**hints)

    logger.info(
        "Source '%s' siap: %d tabel, schema %s, backend %s, reflection_level %s",
        cfg.name,
        len(terpilih),
        cfg.schema,
        cfg.backend,
        reflection_level,
    )
    return source


def _reflection_paling_detail(tables) -> str:
    """Ambil reflection_level paling rinci di antara tabel yang ikut.

    Level yang lebih rinci aman untuk tabel yang minta level lebih rendah;
    kebalikannya tidak, karena tabel yang butuh presisi akan kehilangan hint.
    """
    urutan = {"minimal": 0, "full": 1, "full_with_precision": 2}
    terpilih = max(tables, key=lambda t: urutan[t.reflection_level])
    return terpilih.reflection_level
