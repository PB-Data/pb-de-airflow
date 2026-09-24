"""Pipeline dlt dengan tujuan GCS: lapisan raw, tahap pertama alur bronze.

Alur lengkapnya dua task per tabel:

    ke_gcs  SQL Server -> Arrow -> Parquet di gs://<bucket>/<source>/<tabel>/<tanggal>/
    ke_bq   BigQuery load job dari file-file itu -> tabel bronze (lihat bigquery.py)

Di GCS semua run disimpan: dlt selalu menulis dengan `append`, dan setiap run
masuk ke folder tanggalnya sendiri. `write_disposition` dari config (replace /
append) baru berlaku di tahap BigQuery. Jadi GCS berisi riwayat snapshot harian,
sedangkan tabel bronze berisi keadaan terkini.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Iterable

import dlt

from pb_dwh.config.models import ConfigError, SourceConfig

logger = logging.getLogger(__name__)

# {curr_date} = tanggal (UTC) saat paket load dibuat. {load_id} membuat setiap
# run punya nama file sendiri, jadi task BigQuery bisa memuat tepat file run itu.
LAYOUT_RAW = "{table_name}/{curr_date}/{load_id}.{file_id}.{ext}"


def wajib_env(nama: str) -> str:
    nilai = os.environ.get(nama)
    if not nilai:
        raise ConfigError(
            f"Environment variable {nama} belum di-set. "
            "Isi di docker/.env (lihat .env.example), lalu `docker compose up -d`."
        )
    return nilai


def raw_bucket_url(cfg: SourceConfig) -> str:
    """gs://<bucket>[/<raw_prefix>]; dataset dlt (= nama source) jadi folder di bawahnya."""
    bucket = wajib_env("GCS_STAGING_BUCKET").replace("gs://", "").strip("/")
    return f"gs://{bucket}/{cfg.raw_prefix}" if cfg.raw_prefix else f"gs://{bucket}"


def _selaraskan_env_dlt() -> None:
    """Petakan GCP_PROJECT_ID ke nama yang dicari dlt untuk tujuan filesystem.

    Kredensial tetap dari service account VM (ADC); yang diisi hanya project.
    setdefault: nilai versi dlt yang ditulis eksplisit tetap menang.
    """
    os.environ.setdefault(
        "DESTINATION__FILESYSTEM__CREDENTIALS__PROJECT_ID", wajib_env("GCP_PROJECT_ID")
    )


def build_raw_pipeline(
    cfg: SourceConfig,
    pipelines_dir: str | None = None,
    dev_mode: bool = False,
) -> dlt.Pipeline:
    """Pipeline dlt SQL Server -> Parquet di GCS.

    `dev_mode=True` membuat dlt menambahkan sufiks waktu pada nama dataset,
    jadi percobaan lokal menulis ke folder GCS terpisah dari folder raw asli.
    """
    _selaraskan_env_dlt()
    destination = dlt.destinations.filesystem(
        bucket_url=raw_bucket_url(cfg),
        layout=LAYOUT_RAW,
    )
    pipeline = dlt.pipeline(
        pipeline_name=cfg.pipeline_name,
        destination=destination,
        dataset_name=cfg.name,
        pipelines_dir=pipelines_dir,
        dev_mode=dev_mode,
        progress="log",
    )
    logger.info(
        "Pipeline '%s' -> %s/%s/", cfg.pipeline_name, raw_bucket_url(cfg), pipeline.dataset_name
    )
    return pipeline


def file_hasil(load_info: Any, table_name: str) -> list[str]:
    """URL gs:// file Parquet milik `table_name` yang ditulis run ini.

    Diambil dari metrik load dlt (LoadJobMetrics.remote_url), bukan dengan
    menebak path dari tanggal: {curr_date} bisa berbeda dengan tanggal task
    BigQuery dijalankan, misalnya run yang melewati tengah malam UTC.
    """
    hasil: list[str] = []
    for langkah in _semua_langkah(getattr(load_info, "metrics", {}) or {}):
        for job in (langkah.get("job_metrics") or {}).values():
            url = getattr(job, "remote_url", None)
            if (
                getattr(job, "table_name", None) == table_name
                and url
                and url.endswith(".parquet")
            ):
                hasil.append(url)
    return sorted(set(hasil))


def _semua_langkah(metrics: Any) -> Iterable[dict]:
    """metrics = {load_id: [metrik langkah, ...]}."""
    for daftar in metrics.values():
        yield from daftar
