"""Tahap kedua alur bronze: muat Parquet dari GCS ke BigQuery dengan load job.

Bukan dlt: tabel bronze dibuat manual dari sql/ddl/*.sql, dan tahap ini hanya
mengisinya. Akibatnya:

* Tabel yang belum dibuat -> gagal (CREATE_NEVER), dengan pesan yang menunjuk
  ke file DDL. Tidak ada tabel yang muncul sendiri dengan tipe tebakan.
* Kolom baru di SQL Server -> gagal sampai DDL-nya ditambah. Disengaja: skema
  bronze berubah hanya lewat DDL yang di-commit.
* `replace` memakai WRITE_TRUNCATE_DATA, yang menimpa data tapi mempertahankan
  skema, PRIMARY KEY, CLUSTER BY, dan deskripsi dari DDL. WRITE_TRUNCATE biasa
  justru mengganti skema dengan skema file dan membuang constraint.

Kredensial GCP tidak disebut di sini: di VM dipakai service account VM (ADC),
di laptop `gcloud auth application-default login`.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Sequence

from pb_dwh.config.models import ConfigError, SourceConfig, TableConfig
from pb_dwh.sinks.gcs import wajib_env

logger = logging.getLogger(__name__)

DEFAULT_BQ_LOCATION = "asia-southeast2"


def nama_dataset(cfg: SourceConfig, dev_mode: bool = False) -> str:
    return f"{cfg.bq_dataset}_dev" if dev_mode else cfg.bq_dataset


def buat_load_config(tabel: TableConfig, dev_mode: bool = False):
    """LoadJobConfig untuk satu tabel. Dipisah supaya bisa diuji tanpa GCP."""
    from google.cloud import bigquery

    if tabel.write_disposition == "replace":
        # Dataset dev dibuat otomatis tanpa DDL, jadi skemanya boleh ikut file.
        mode = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if dev_mode
            else bigquery.WriteDisposition.WRITE_TRUNCATE_DATA
        )
    elif tabel.write_disposition == "append":
        mode = bigquery.WriteDisposition.WRITE_APPEND
    else:
        raise ConfigError(
            f"write_disposition '{tabel.write_disposition}' belum didukung tahap BigQuery "
            f"(tabel {tabel.name})."
        )

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=mode,
        create_disposition=(
            bigquery.CreateDisposition.CREATE_IF_NEEDED
            if dev_mode
            else bigquery.CreateDisposition.CREATE_NEVER
        ),
    )


def load_ke_bigquery(
    cfg: SourceConfig,
    table_name: str,
    files: Sequence[str],
    dev_mode: bool = False,
) -> dict[str, Any]:
    """Muat `files` (gs://...parquet) ke tabel bronze. Mengembalikan ringkasan."""
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    tabel = cfg.table(table_name)
    project = wajib_env("GCP_PROJECT_ID")
    lokasi = os.environ.get("BQ_LOCATION") or DEFAULT_BQ_LOCATION
    dataset = nama_dataset(cfg, dev_mode)
    table_id = f"{project}.{dataset}.{table_name}"
    client = bigquery.Client(project=project, location=lokasi)

    if dev_mode:
        ds = bigquery.Dataset(f"{project}.{dataset}")
        ds.location = lokasi
        client.create_dataset(ds, exists_ok=True)

    if not files:
        # Tidak ada baris di source. Untuk replace artinya tabel harus kosong;
        # untuk append artinya tidak ada yang ditambahkan.
        if tabel.write_disposition == "replace" and not dev_mode:
            client.query(f"TRUNCATE TABLE `{table_id}`").result()
            logger.info("%s: source kosong, tabel dikosongkan (replace).", table_id)
            return {"table": table_id, "rows": 0, "files": 0}
        logger.info("%s: tidak ada file baru, tidak ada yang dimuat.", table_id)
        return {"table": table_id, "rows": 0, "files": 0}

    job_config = buat_load_config(tabel, dev_mode)
    logger.info(
        "Load %d file ke %s (%s)", len(files), table_id, job_config.write_disposition
    )
    try:
        job = client.load_table_from_uri(list(files), table_id, job_config=job_config)
        job.result()
    except NotFound as exc:
        raise ConfigError(
            f"Tabel atau dataset {table_id} tidak ditemukan. Tabel bronze dibuat manual: "
            f"jalankan DDL-nya dulu (sql/ddl/{cfg.bq_dataset}.sql). Detail: {exc.message}"
        ) from exc

    logger.info("Selesai: %s baris dimuat ke %s (job %s)", job.output_rows, table_id, job.job_id)
    return {
        "table": table_id,
        "rows": job.output_rows,
        "files": len(files),
        "job_id": job.job_id,
    }
