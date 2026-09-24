"""Menjalankan ingestion bronze: dua tahap per tabel.

    ke_gcs       SQL Server -> Parquet di GCS (dlt)
    ke_bigquery  Parquet di GCS -> tabel bronze (BigQuery load job)

Fungsi di sini dipakai dua pihak dengan jalur kode yang sama persis:

* DAG Airflow, lewat `dags/_factory.py` (satu task per tahap per tabel)
* CLI untuk testing lokal, lewat `python -m pb_dwh.cli`

Disengaja sama supaya apa yang berhasil di laptop berperilaku sama di VM.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from typing import Any, Sequence

from pb_dwh.config.models import SourceConfig, periksa_dukungan_alur
from pb_dwh.sinks.bigquery import load_ke_bigquery
from pb_dwh.sinks.gcs import build_raw_pipeline, file_hasil, raw_bucket_url, wajib_env
from pb_dwh.sources.mssql import build_connection_url, build_source

logger = logging.getLogger(__name__)


def periksa_siap(cfg: SourceConfig) -> None:
    """Semua yang bisa dicek tanpa jaringan: dukungan alur dan env var.

    Dipanggil saat DAG di-parse, supaya config atau env yang kurang langsung
    muncul sebagai DAG RUSAK, bukan task yang baru gagal jam 01:00.
    """
    periksa_dukungan_alur(cfg)
    build_connection_url(cfg)
    raw_bucket_url(cfg)
    wajib_env("GCP_PROJECT_ID")


def ke_gcs(
    cfg: SourceConfig,
    table_name: str,
    pipelines_dir: str | None = None,
    dev_mode: bool = False,
) -> dict[str, Any]:
    """Tahap 1: extract satu tabel ke Parquet di GCS.

    Mengembalikan daftar file yang ditulis run ini; di Airflow nilai ini
    menjadi XCom yang dibaca tahap 2.
    """
    source = build_source(cfg, table_names=[table_name])

    # Folder kerja dlt dibuat baru per task dan dihapus sesudahnya. State
    # (termasuk nilai cursor incremental) dipulihkan dlt dari GCS, jadi tidak
    # ada yang hilang. Harus di disk data (DLT_DATA_DIR), bukan boot disk.
    folder_sementara = None
    if pipelines_dir is None:
        folder_sementara = tempfile.mkdtemp(
            prefix=f"{cfg.name}_{table_name}_", dir=os.environ.get("DLT_DATA_DIR") or None
        )
        pipelines_dir = folder_sementara

    try:
        pipeline = build_raw_pipeline(cfg, pipelines_dir=pipelines_dir, dev_mode=dev_mode)
        info = pipeline.run(source, loader_file_format=cfg.loader_file_format)
        logger.info("%s", info)
        files = file_hasil(info, table_name)
        baris = _jumlah_baris(pipeline, table_name)
    finally:
        if folder_sementara:
            shutil.rmtree(folder_sementara, ignore_errors=True)

    logger.info("%s.%s: %s baris, %d file di GCS", cfg.name, table_name, baris, len(files))
    return {
        "table": table_name,
        "rows": baris,
        "load_ids": list(info.loads_ids),
        "files": files,
    }


def ke_bigquery(
    cfg: SourceConfig,
    table_name: str,
    hasil_gcs: dict[str, Any] | None,
    dev_mode: bool = False,
) -> dict[str, Any]:
    """Tahap 2: muat file hasil tahap 1 ke tabel bronze."""
    if not hasil_gcs:
        raise RuntimeError(
            f"Tidak ada hasil tahap ke_gcs untuk tabel {table_name}. Tahap BigQuery hanya "
            "memuat file yang ditulis run yang sama, jadi jalankan ulang ke_gcs dulu."
        )
    return load_ke_bigquery(cfg, table_name, hasil_gcs.get("files") or [], dev_mode=dev_mode)


def run_source(
    cfg: SourceConfig,
    table_names: Sequence[str] | None = None,
    pipelines_dir: str | None = None,
    dev_mode: bool = False,
) -> list[dict[str, Any]]:
    """Dua tahap untuk seluruh (atau sebagian) tabel, berurutan. Dipakai CLI."""
    periksa_dukungan_alur(cfg)
    daftar = list(table_names) if table_names else cfg.table_names
    logger.info("Mulai ingestion '%s': %d tabel", cfg.name, len(daftar))

    ringkasan = []
    for nama in daftar:
        hasil_gcs = ke_gcs(cfg, nama, pipelines_dir=pipelines_dir, dev_mode=dev_mode)
        hasil_bq = ke_bigquery(cfg, nama, hasil_gcs, dev_mode=dev_mode)
        ringkasan.append({"gcs": hasil_gcs, "bigquery": hasil_bq})
    return ringkasan


def _jumlah_baris(pipeline: Any, table_name: str) -> int | None:
    """Jumlah baris yang dinormalisasi untuk tabel ini; None kalau tidak tersedia."""
    try:
        return int(pipeline.last_trace.last_normalize_info.row_counts.get(table_name, 0))
    except Exception:  # noqa: BLE001 - hanya untuk log, jangan gagalkan task
        return None
