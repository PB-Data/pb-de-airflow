"""Transform bronze -> silver dengan BigQuery, satu task per file SQL.

Setiap file `.sql` di `sql/silver/` menjadi satu task. Urutannya mengikuti
urutan nama file, dan task berjalan berurutan. Jadi untuk mengatur urutan,
cukup ubah angka di depan nama file:

    sql/silver/10_dim_product.sql
    sql/silver/20_fct_sales_order.sql

Tidak ada kode Python yang perlu diubah saat menambah transform baru.

CATATAN: query transform yang sebenarnya belum diberikan, jadi folder ini baru
berisi satu file contoh. Ganti isinya dengan query asli; strukturnya sudah siap.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import DAG

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = Path(os.environ.get("PB_DWH_SQL_DIR", REPO_ROOT / "sql")) / "silver"

BQ_LOCATION = os.environ.get("BQ_LOCATION", "asia-southeast2")
GCP_CONN_ID = os.environ.get("PB_DWH_GCP_CONN_ID", "google_cloud_default")
TIMEZONE = os.environ.get("PB_DWH_TIMEZONE", "Asia/Jakarta")

berkas_sql = sorted(SQL_DIR.glob("*.sql")) if SQL_DIR.exists() else []

if not berkas_sql:
    logger.warning("Tidak ada file .sql di %s; DAG silver dibuat tanpa task.", SQL_DIR)

with DAG(
    dag_id="silver_transform",
    # Dijadwalkan setelah ingestion bronze selesai. Ini pemisahan berbasis
    # waktu, bukan dependency sungguhan: kalau bronze telat, silver tetap
    # jalan dengan data kemarin. Kalau itu jadi masalah, naikkan ke Asset
    # Airflow 3 atau pasang ExternalTaskSensor.
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    # Supaya "sql/silver/xxx.sql" di bawah dibaca sebagai file, bukan sebagai
    # query literal. BigQueryInsertJobOperator mengenali akhiran .sql.
    template_searchpath=[str(REPO_ROOT)],
    tags=["silver", "bigquery"],
    doc_md=__doc__,
) as dag:

    sebelumnya = None

    for path in berkas_sql:
        task = BigQueryInsertJobOperator(
            task_id=path.stem,
            gcp_conn_id=GCP_CONN_ID,
            location=BQ_LOCATION,
            configuration={
                "query": {
                    "query": f"sql/silver/{path.name}",
                    "useLegacySql": False,
                }
            },
            # Dipakai di dalam SQL sebagai {{ params.nama }}, sehingga nama
            # dataset tidak perlu ditulis keras di setiap file query.
            params={
                "project": os.environ.get("GCP_PROJECT_ID", ""),
                "bronze": os.environ.get("BQ_DATASET_BRONZE", ""),
                "silver": os.environ.get("BQ_DATASET_SILVER", ""),
            },
        )

        # Berurutan, bukan paralel: transform silver biasanya saling
        # bergantung, dan urutan yang bisa ditebak lebih mudah ditelusuri
        # daripada slot BigQuery yang habis dipakai sendiri.
        if sebelumnya is not None:
            sebelumnya >> task
        sebelumnya = task
