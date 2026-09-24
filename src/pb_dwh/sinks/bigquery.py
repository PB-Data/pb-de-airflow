"""Pipeline dlt dengan tujuan BigQuery dan staging Parquet di GCS.

Alur datanya: SQL Server -> Arrow -> Parquet di GCS -> BigQuery load job.

Yang penting: karena ada `staging`, dlt memuat data lewat BigQuery load job dari
file Parquet di GCS. Bukan streaming insert. Itu jauh lebih murah, tidak kena
kuota streaming, dan hasilnya langsung bisa di-query tanpa menunggu buffer.
"""

from __future__ import annotations

import logging
import os

import dlt

from pb_dwh.config.models import ConfigError, SourceConfig

logger = logging.getLogger(__name__)

DEFAULT_BQ_LOCATION = "asia-southeast2"


def _wajib(nama: str) -> str:
    nilai = os.environ.get(nama)
    if not nilai:
        raise ConfigError(
            f"Environment variable {nama} belum di-set. "
            "Isi di docker/.env (lihat .env.example), lalu `docker compose up -d`."
        )
    return nilai


def _selaraskan_env_dlt() -> None:
    """Petakan env var yang mudah dibaca ke nama yang dimengerti dlt.

    Tim menulis GCP_PROJECT_ID di .env. dlt mencarinya sebagai
    DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID. Dipetakan di sini supaya
    .env tetap enak dibaca dan tidak ada nama panjang berlapis di mana-mana.

    Dipakai setdefault, jadi kalau seseorang memang menulis nama versi dlt
    secara eksplisit, nilai itu yang menang.
    """
    os.environ.setdefault(
        "DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID", _wajib("GCP_PROJECT_ID")
    )


def staging_bucket_url(cfg: SourceConfig) -> str:
    bucket = _wajib("GCS_STAGING_BUCKET").replace("gs://", "").strip("/")
    return f"gs://{bucket}/{cfg.staging_prefix}/{cfg.name}"


def build_pipeline(
    cfg: SourceConfig,
    pipelines_dir: str | None = None,
    dev_mode: bool = False,
) -> dlt.Pipeline:
    """Rakit pipeline dlt untuk satu source.

    Kredensial GCP tidak pernah disebut di sini. Di VM, container memakai
    service account VM lewat metadata server (ADC), jadi tidak ada file JSON
    key di mana pun. Di laptop, dipakai `gcloud auth application-default login`.

    `dev_mode=True` menambahkan sufiks pada nama dataset sehingga percobaan
    lokal tidak menimpa dataset bronze yang asli.
    """
    _selaraskan_env_dlt()

    destination = dlt.destinations.bigquery(
        location=os.environ.get("BQ_LOCATION", DEFAULT_BQ_LOCATION),
    )
    staging = dlt.destinations.filesystem(bucket_url=staging_bucket_url(cfg))

    pipeline = dlt.pipeline(
        pipeline_name=cfg.pipeline_name,
        destination=destination,
        staging=staging,
        dataset_name=cfg.bq_dataset,
        pipelines_dir=pipelines_dir,
        dev_mode=dev_mode,
        progress="log",
    )

    logger.info(
        "Pipeline '%s' -> dataset %s, staging %s",
        cfg.pipeline_name,
        cfg.bq_dataset,
        staging_bucket_url(cfg),
    )
    return pipeline
