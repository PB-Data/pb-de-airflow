"""Baca file YAML di config/ menjadi objek SourceConfig yang sudah tervalidasi.

Aturan penggabungan setelan, dari yang paling lemah ke paling kuat:

    config/settings.yml  ->  defaults: di file source  ->  setelan per tabel

Artinya: nilai yang ditulis di tabel selalu menang. Kalau sebuah nilai tidak
ditulis di mana pun, dipakai default dari `settings.yml`.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

import yaml

from pb_dwh.config.models import (
    ConfigError,
    SourceConfig,
    TableConfig,
    as_column_tuple,
    fail,
    normalise_initial_value,
    validate_source,
)

# Kunci yang boleh muncul di bawah sebuah tabel. Apa pun di luar ini hampir
# pasti salah ketik, dan salah ketik yang didiamkan berarti setelan yang
# dikira aktif padahal tidak.
KUNCI_TABEL_DIKENAL = frozenset(
    {
        "name",
        "primary_key",
        "cursor",
        "cursor_type",
        "lookback_days",
        "lookback",
        "initial_value",
        "write_disposition",
        "chunk_size",
        "reflection_level",
        "exclude_columns",
        "column_overrides",
    }
)

KUNCI_SOURCE_DIKENAL = frozenset(
    {
        "name",
        "env_prefix",
        "database",
        "schema",
        "bq_dataset",
        "schedule",
        "start_date",
        "pool",
        "task_timeout_hours",
        "retries",
    }
)


def config_dir() -> Path:
    """Folder config.

    Di dalam container di-set lewat PB_DWH_CONFIG_DIR. Di laptop, dihitung
    relatif terhadap lokasi file ini supaya jalan tanpa setup apa pun.
    """
    dari_env = os.environ.get("PB_DWH_CONFIG_DIR")
    if dari_env:
        return Path(dari_env)
    return Path(__file__).resolve().parents[3] / "config"


def _baca_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"File config tidak ditemukan: {path}")
    try:
        isi = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"[{path.name}] YAML tidak bisa dibaca: {exc}") from exc
    if isi is None:
        raise ConfigError(f"[{path.name}] file kosong")
    if not isinstance(isi, dict):
        raise ConfigError(f"[{path.name}] isi file harus mapping, bukan {type(isi).__name__}")
    return isi


def _muat_settings() -> dict[str, Any]:
    path = config_dir() / "settings.yml"
    if not path.exists():
        return {"defaults": {}, "pipeline": {}, "airflow": {}}
    isi = _baca_yaml(path)
    return {
        "defaults": isi.get("defaults") or {},
        "pipeline": isi.get("pipeline") or {},
        "airflow": isi.get("airflow") or {},
    }


def _sebagai_tanggal(nilai: Any, source_file: str) -> date:
    """start_date dari YAML bisa berupa objek date atau string ISO."""
    if nilai is None:
        return date(2026, 1, 1)
    if isinstance(nilai, datetime):
        return nilai.date()
    if isinstance(nilai, date):
        return nilai
    if isinstance(nilai, str):
        try:
            return date.fromisoformat(nilai)
        except ValueError:
            pass
    raise ConfigError(
        f"[{source_file}] source.start_date '{nilai}' bukan tanggal yang sah. Contoh: 2026-01-01"
    )


def _bangun_tabel(
    mentah: Mapping[str, Any], defaults: Mapping[str, Any], source_file: str
) -> TableConfig:
    if not isinstance(mentah, dict):
        fail(source_file, None, f"setiap entri di 'tables:' harus mapping, bukan {type(mentah).__name__}")

    nama = mentah.get("name")
    if not isinstance(nama, str) or not nama.strip():
        fail(source_file, None, f"ada entri tabel tanpa 'name' yang sah: {mentah!r}")

    salah_ketik = sorted(set(mentah) - KUNCI_TABEL_DIKENAL)
    if salah_ketik:
        # Kasus yang paling sering: 'schema' ditulis per tabel.
        if "schema" in salah_ketik:
            fail(
                source_file,
                nama,
                "'schema' tidak boleh ditulis per tabel. Satu file config hanya untuk satu "
                "schema (diatur di source.schema). Untuk schema lain, buat file config baru.",
            )
        fail(
            source_file,
            nama,
            f"kunci tidak dikenal: {', '.join(salah_ketik)}. "
            f"Yang boleh dipakai: {', '.join(sorted(KUNCI_TABEL_DIKENAL))}",
        )

    def ambil(kunci: str, cadangan: Any = None) -> Any:
        if kunci in mentah:
            return mentah[kunci]
        if kunci in defaults:
            return defaults[kunci]
        return cadangan

    cursor = ambil("cursor")
    cursor_type = ambil("cursor_type", "datetime")
    write_disposition = ambil("write_disposition", "merge")

    def ambil_lookback(kunci: str, untuk_cursor_type: str) -> int:
        """Warisi lookback hanya kalau memang masuk akal untuk tabel ini.

        `settings.yml` menetapkan lookback_days untuk semua tabel. Kalau
        diwariskan mentah-mentah, tabel tanpa cursor dan tabel dengan cursor
        integer akan ikut menerimanya, lalu ditolak validasi -- padahal yang
        salah bukan orangnya, melainkan pewarisannya.

        Nilai yang ditulis langsung di tabel tetap dipakai apa adanya, supaya
        salah tulis yang sebenarnya tetap ketahuan.
        """
        if kunci in mentah:
            return int(mentah[kunci] or 0)
        # Tabel append tidak mewarisi lookback: append + lookback = duplikat,
        # dan yang menulis tabelnya tidak pernah meminta lookback itu.
        if cursor and cursor_type == untuk_cursor_type and write_disposition != "append":
            return int(defaults.get(kunci, 0) or 0)
        return 0

    tabel = TableConfig(
        name=nama,
        primary_key=as_column_tuple(ambil("primary_key"), source_file, nama, "primary_key"),
        cursor=cursor,
        cursor_type=cursor_type,
        lookback_days=ambil_lookback("lookback_days", "datetime"),
        lookback=ambil_lookback("lookback", "integer"),
        initial_value=normalise_initial_value(
            ambil("initial_value"), cursor_type, nama, source_file
        ),
        write_disposition=write_disposition,
        chunk_size=int(ambil("chunk_size", 50_000)),
        reflection_level=ambil("reflection_level", "minimal"),
        exclude_columns=as_column_tuple(
            ambil("exclude_columns"), source_file, nama, "exclude_columns"
        ),
        column_overrides=dict(ambil("column_overrides") or {}),
    )
    return tabel


def load_source_config(path: str | Path) -> SourceConfig:
    """Baca satu file source dan pastikan isinya masuk akal."""
    path = Path(path)
    isi = _baca_yaml(path)
    settings = _muat_settings()

    blok_source = isi.get("source")
    if not isinstance(blok_source, dict):
        raise ConfigError(f"[{path.name}] wajib ada blok 'source:' berisi name, env_prefix, schema, bq_dataset")

    salah_ketik = sorted(set(blok_source) - KUNCI_SOURCE_DIKENAL)
    if salah_ketik:
        fail(path.name, None, f"blok 'source:' punya kunci tidak dikenal: {', '.join(salah_ketik)}")

    # settings.yml kalah dari defaults: di file source.
    defaults = {**settings["defaults"], **(isi.get("defaults") or {})}
    pipeline = settings["pipeline"]

    tabel_mentah = isi.get("tables")
    if not isinstance(tabel_mentah, list):
        fail(path.name, None, "wajib ada 'tables:' berupa list")

    tabel = tuple(_bangun_tabel(t, defaults, path.name) for t in tabel_mentah)

    # Setelan Airflow: settings.yml kalah dari blok source:.
    airflow = {**settings["airflow"], **{
        k: v for k, v in blok_source.items()
        if k in {"schedule", "start_date", "pool", "task_timeout_hours", "retries"}
    }}

    source = SourceConfig(
        name=blok_source.get("name", ""),
        env_prefix=blok_source.get("env_prefix", ""),
        database=str(blok_source.get("database") or ""),
        schema=blok_source.get("schema", ""),
        bq_dataset=blok_source.get("bq_dataset", ""),
        tables=tabel,
        backend=pipeline.get("backend", "pyarrow"),
        loader_file_format=pipeline.get("loader_file_format", "parquet"),
        raw_prefix=str(pipeline.get("raw_prefix") or "").strip("/"),
        source_file=path.name,
        schedule=str(airflow.get("schedule", "0 1 * * *")),
        start_date=_sebagai_tanggal(airflow.get("start_date"), path.name),
        pool=str(airflow.get("pool", "dlt_ingest")),
        task_timeout_hours=int(airflow.get("task_timeout_hours", 4)),
        retries=int(airflow.get("retries", 1)),
    )

    validate_source(source)
    return source


def load_all_source_configs(directory: str | Path | None = None) -> list[SourceConfig]:
    """Baca semua file source, diurutkan supaya urutan DAG stabil.

    Dipanggil oleh DAG processor setiap kali DAG di-parse, jadi sengaja tidak
    menyentuh jaringan maupun database.
    """
    folder = Path(directory) if directory else config_dir() / "sources"
    if not folder.exists():
        raise ConfigError(f"Folder config source tidak ditemukan: {folder}")

    berkas = sorted(p for p in folder.glob("*.y*ml") if p.is_file())
    if not berkas:
        raise ConfigError(f"Tidak ada file config di {folder}. Tambahkan minimal satu file .yml")

    konfigurasi = [load_source_config(p) for p in berkas]

    nama = [c.name for c in konfigurasi]
    duplikat = sorted({n for n in nama if nama.count(n) > 1})
    if duplikat:
        raise ConfigError(
            f"source.name dipakai lebih dari satu file: {', '.join(duplikat)}. "
            "Nama ini jadi dag_id, jadi harus unik."
        )

    return konfigurasi
