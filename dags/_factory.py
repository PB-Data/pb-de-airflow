"""Pabrik DAG ingestion bronze.

File ini sengaja jadi satu-satunya tempat yang tahu soal Airflow. Semua logika
pipeline ada di `src/pb_dwh/`, sehingga bisa diuji tanpa Airflow terpasang.

Untuk menambah tabel atau source baru, TIDAK ada yang perlu diubah di sini.
Cukup tambah atau edit file di `config/sources/`.

Bentuk DAG: satu grup per tabel, berisi dua task.

    <tabel>.ke_gcs  SQL Server -> Parquet di GCS (dlt)          pool dlt_ingest
         |  XCom: daftar file yang ditulis run ini
    <tabel>.ke_bq   Parquet -> tabel bronze (BigQuery load job)

Task ke_gcs antar-tabel dirantai berurutan: dua tabel besar yang diekstrak
bersamaan adalah cara tercepat menghabiskan RAM 20 GB. Task ke_bq ringan (yang
bekerja BigQuery), jadi boleh jalan bersamaan dengan ekstraksi tabel berikutnya.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import timedelta
from typing import Any

import pendulum

# Airflow 3: DAG dan TaskGroup ada di Task SDK, PythonOperator pindah ke
# provider `standard`. Jalur lama (airflow.models.dag, airflow.operators.python)
# masih jalan tapi sudah deprecated.
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from pb_dwh.bronze import runner
from pb_dwh.config.loader import config_dir, load_source_config
from pb_dwh.config.models import SourceConfig

logger = logging.getLogger(__name__)

# Jadwal di config ditulis dalam waktu lokal ini, bukan UTC.
TIMEZONE = os.environ.get("PB_DWH_TIMEZONE", "Asia/Jakarta")


def build_ingestion_dag(cfg: SourceConfig) -> DAG:
    """Bangun satu DAG: dua task (ke_gcs, ke_bq) per tabel."""
    # Config yang belum didukung atau env var yang kurang ketahuan di sini,
    # saat parse, dan berakhir sebagai DAG RUSAK -- bukan task yang baru gagal
    # jam 01:00. Tidak menyentuh jaringan.
    runner.periksa_siap(cfg)

    # auto_register=False: DAG di dalam blok `with` biasanya terdaftar otomatis,
    # termasuk kalau bloknya keluar karena error. Hasilnya DAG 0 task yang tetap
    # terjadwal dan "sukses" tiap run tanpa memuat apa pun -- dua kali terjadi di
    # VM. Tanpa auto-register, hanya DAG yang dikembalikan fungsi ini (lalu
    # dipasang ke globals() oleh bronze_ingest.py) yang dibaca Airflow.
    with DAG(
        auto_register=False,
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
        gcs_sebelumnya = None
        for tabel in cfg.tables:
            grup = _id_task(tabel.name)
            with TaskGroup(group_id=grup):
                ke_gcs = PythonOperator(
                    task_id="ke_gcs",
                    python_callable=_task_ke_gcs,
                    op_kwargs={"source_file": cfg.source_file, "table": tabel.name},
                    pool=cfg.pool,
                    # Tabel sebelumnya gagal tidak boleh menahan tabel ini.
                    trigger_rule="all_done",
                )
                ke_bq = PythonOperator(
                    task_id="ke_bq",
                    python_callable=_task_ke_bq,
                    op_kwargs={
                        "source_file": cfg.source_file,
                        "table": tabel.name,
                        "task_gcs": f"{grup}.ke_gcs",
                    },
                )
                ke_gcs >> ke_bq
            if gcs_sebelumnya is not None:
                gcs_sebelumnya >> ke_gcs
            gcs_sebelumnya = ke_gcs

    return dag


def _id_task(nama_tabel: str) -> str:
    """Nama tabel SQL Server boleh mengandung karakter yang ditolak Airflow."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", nama_tabel)


def _muat_cfg(source_file: str) -> SourceConfig:
    # Config dibaca ulang saat task jalan, bukan dibawa dari saat parse:
    # yang dikirim ke task cukup nama file dan nama tabel.
    return load_source_config(config_dir() / "sources" / source_file)


def _task_ke_gcs(source_file: str, table: str) -> dict[str, Any]:
    return runner.ke_gcs(_muat_cfg(source_file), table)


def _task_ke_bq(source_file: str, table: str, task_gcs: str, **context: Any) -> dict[str, Any]:
    hasil_gcs = context["ti"].xcom_pull(task_ids=task_gcs)
    return runner.ke_bigquery(_muat_cfg(source_file), table, hasil_gcs)


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
        f"- Raw (semua run disimpan): `gs://<GCS_STAGING_BUCKET>/"
        f"{cfg.raw_prefix + '/' if cfg.raw_prefix else ''}{cfg.name}/<tabel>/<tanggal>/`",
        f"- Dataset tujuan: `{cfg.bq_dataset}` (tabel dibuat manual: "
        f"`sql/ddl/{cfg.bq_dataset}.sql`)",
        f"- Jumlah tabel: {len(cfg.tables)}, masing-masing `<tabel>.ke_gcs` -> `<tabel>.ke_bq`",
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
        "Untuk menambah tabel: tambah CREATE TABLE di file DDL dan jalankan, tambah",
        "entri di file config, lalu `scripts/deploy.sh`. Tidak ada kode Python yang",
        "perlu diubah.",
    ]
    return "\n".join(baris)
