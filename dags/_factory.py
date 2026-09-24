"""Pabrik DAG ingestion bronze.

File ini sengaja jadi satu-satunya tempat yang tahu soal Airflow. Semua logika
pipeline ada di `src/pb_dwh/`, sehingga bisa diuji tanpa Airflow terpasang.

Untuk menambah tabel atau source baru, TIDAK ada yang perlu diubah di sini.
Cukup tambah atau edit file di `config/sources/`.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum

# Airflow 3: DAG dan TaskGroup ada di Task SDK, PythonOperator pindah ke
# provider `standard`. Jalur lama (airflow.models.dag, airflow.operators.python)
# masih jalan tapi sudah deprecated. Yang penting: PipelineTasksGroup dari dlt
# mewarisi airflow.sdk.TaskGroup saat Airflow 3, jadi DAG-nya harus dari
# airflow.sdk juga supaya tidak bercampur dua generasi API.
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from dlt.helpers.airflow_helper import DEFAULT_RETRY_BACKOFF, PipelineTasksGroup

from pb_dwh.config.models import SourceConfig
from pb_dwh.sinks.bigquery import build_pipeline
from pb_dwh.sources.mssql import build_source

logger = logging.getLogger(__name__)

# Folder kerja dlt selama task jalan. Kalau dibiarkan kosong, dlt memakai temp
# folder sistem, yang di VM ini berarti boot disk 20 GB -- dan 100 tabel Parquet
# akan memenuhinya di run pertama. Harus menunjuk ke disk data.
DLT_LOCAL_DATA_FOLDER = os.environ.get("DLT_DATA_DIR", "/opt/dlt")

# Jadwal di config ditulis dalam waktu lokal ini, bukan UTC.
TIMEZONE = os.environ.get("PB_DWH_TIMEZONE", "Asia/Jakarta")


def build_ingestion_dag(cfg: SourceConfig) -> DAG:
    """Bangun satu DAG berisi satu task per tabel."""
    with DAG(
        dag_id=cfg.dag_id,
        schedule=cfg.schedule,
        # Airflow butuh datetime ber-timezone; config menyimpannya sebagai
        # tanggal polos supaya YAML-nya tetap enak dibaca.
        start_date=pendulum.datetime(
            cfg.start_date.year, cfg.start_date.month, cfg.start_date.day, tz=TIMEZONE
        ),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "data-eng",
            "retries": cfg.retries,
            "retry_delay": timedelta(minutes=5),
            "execution_timeout": timedelta(hours=cfg.task_timeout_hours),
        },
        tags=["bronze", "dlt", "mssql", cfg.name],
        doc_md=_dokumentasi(cfg),
    ) as dag:

        tasks = PipelineTasksGroup(
            pipeline_name=cfg.pipeline_name,
            # use_data_folder=True hanya relevan di Cloud Composer (bucket
            # gcsfuse). Di VM sendiri, disk lokal jauh lebih cepat.
            use_data_folder=False,
            local_data_folder=DLT_LOCAL_DATA_FOLDER,
            # Arahkan log dlt ke log task Airflow supaya semuanya ada di satu
            # tempat saat menelusuri masalah.
            use_task_logger=True,
            log_progress_period=60,
            buffer_max_items=1000,
            # Retry hanya pada tahap `load`. Mengulang `extract` berarti
            # membaca ulang jutaan baris dari SQL Server; jarang itu yang
            # dibutuhkan, dan mahal.
            retry_policy=DEFAULT_RETRY_BACKOFF,
            retry_pipeline_steps=("load",),
            wipe_local_data=True,
        )

        pipeline = build_pipeline(cfg)
        source = build_source(cfg)

        # decompose="serialize" memecah source jadi satu task per tabel yang
        # jalan berurutan dalam satu pipeline state. Berurutan itu disengaja:
        # dua tabel besar yang di-extract bersamaan adalah cara tercepat
        # menghabiskan RAM 20 GB.
        #
        # Ini bisa karena `table_names` diisi eksplisit dan
        # `defer_table_reflect=True`, sehingga dlt tahu nama resource-nya tanpa
        # perlu menyentuh database saat DAG di-parse.
        tasks.add_run(
            pipeline,
            source,
            decompose="serialize",
            loader_file_format=cfg.loader_file_format,
            trigger_rule="all_done",
            pool=cfg.pool,
        )

    return dag


def build_broken_dag(source_file: str, error: Exception) -> DAG:
    """DAG pengganti saat sebuah config gagal dibaca.

    Sengaja dibuat DAG yang terlihat dan langsung gagal, bukan DAG yang hilang
    diam-diam. Config yang salah lalu DAG-nya lenyap dari UI adalah cara paling
    halus untuk membuat data basi tanpa ada yang sadar.
    """
    dag_id = f"bronze_ingest_RUSAK_{source_file.replace('.', '_')}"
    pesan = f"Config {source_file} tidak bisa dibaca:\n\n{error}"

    with DAG(
        dag_id=dag_id,
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
        catchup=False,
        tags=["bronze", "config-error"],
        doc_md=f"```\n{pesan}\n```",
    ) as dag:

        def _gagal() -> None:
            raise RuntimeError(pesan)

        PythonOperator(task_id="config_tidak_sah", python_callable=_gagal)

    logger.error("Config %s ditolak: %s", source_file, error)
    return dag


def _dokumentasi(cfg: SourceConfig) -> str:
    """Ringkasan yang tampil di tab Docs pada UI Airflow."""
    baris = [
        f"### Ingestion bronze: `{cfg.name}`",
        "",
        f"- Config: `config/sources/{cfg.source_file}`",
        f"- Database source: `{cfg.database or cfg.env_prefix + '_DATABASE (env)'}`",
        f"- Schema source: `{cfg.schema}`",
        f"- Dataset tujuan: `{cfg.bq_dataset}`",
        f"- Jumlah tabel: {len(cfg.tables)}",
        "",
        "| Tabel | Disposition | Cursor | Lookback | Primary key |",
        "|---|---|---|---|---|",
    ]
    for t in cfg.tables:
        if not t.cursor:
            lookback = "-"
        elif t.cursor_type == "datetime":
            lookback = f"{t.lookback_days} hari"
        else:
            lookback = f"{t.lookback} id"
        baris.append(
            f"| `{t.name}` | {t.write_disposition} | {t.cursor or '-'} | "
            f"{lookback} | {', '.join(t.primary_key) or '-'} |"
        )
    baris += [
        "",
        "Untuk menambah tabel, edit file config di atas lalu `git pull` di VM. ",
        "Tidak ada kode Python yang perlu diubah.",
    ]
    return "\n".join(baris)
