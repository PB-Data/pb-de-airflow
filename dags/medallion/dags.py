"""Bangun DAG Airflow dari YAML: bronze__<source> dan transform__<domain>."""

from datetime import timedelta

import pendulum
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import DAG, Asset

from medallion.config import DAGS_DIR, load_platform, load_source, load_transform

DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=5)}


def _start_date(platform: dict):
    # catchup mati (default Airflow 3): start_date hanya batas bawah, bukan jadwal backfill.
    return pendulum.datetime(2026, 9, 1, tz=platform["runtime"]["timezone"])


def bronze_asset(platform: dict, dataset: str, table: str) -> Asset:
    return Asset(f"bigquery://{platform['gcp']['bq_project']}/{dataset}/{table}")


def _ingest(dags_dir, job, conn):
    """Jalan di venv dlt. Hanya source fungsi ini yang disalin ke subprocess: import di dalam."""
    import sys

    sys.path.insert(0, dags_dir)
    from medallion.ingest import mssql_engine, run

    run(job, mssql_engine(conn, job["database"]))


def bronze_dag(source_name: str) -> DAG:
    platform = load_platform()
    src = load_source(source_name)
    # Dirender saat task jalan. Password tampil *** di UI & log: Airflow me-mask secret Connection.
    conn = {f: "{{ conn['%s'].%s }}" % (src["conn_id"], f) for f in ("host", "port", "login", "password")}

    with DAG(
        dag_id=f"bronze__{source_name}",
        schedule=src["schedule"],
        start_date=_start_date(platform),
        default_args=DEFAULT_ARGS,
        tags=["bronze", source_name],
    ) as dag:
        for t in src["tables"]:
            job = {
                **t,
                "database": src["database"],
                "schema": src["schema"],
                "dataset": src["dataset"],
                "file_format": src["file_format"],
                "pipeline_name": f"{source_name}__{t['table']}",
                "bq_project": platform["gcp"]["bq_project"],
                "bq_location": platform["gcp"]["bq_location"],
                "bucket_url": platform["lake"]["bucket_url"],
            }
            ExternalPythonOperator(
                task_id=t["table"],
                python=platform["runtime"]["dlt_python"],
                python_callable=_ingest,
                op_kwargs={"dags_dir": str(DAGS_DIR), "job": job, "conn": conn},
                expect_airflow=False,
                # 2 run (mis. backfill) jangan memakai state dlt tabel yang sama bersamaan
                max_active_tis_per_dag=1,
                execution_timeout=timedelta(hours=4),
                outlets=[bronze_asset(platform, src["dataset"], t["table"])],
            )
    return dag


def transform_dag(domain: str) -> DAG:
    platform = load_platform()
    tr = load_transform(domain)
    gcp = platform["gcp"]
    bq = {"project_id": gcp["bq_project"], "location": gcp["bq_location"]}

    schedule = tr.get("schedule")
    if isinstance(schedule, list):  # nama source bronze -> jalan setelah semua tabelnya ter-load
        schedule = [
            bronze_asset(platform, src["dataset"], t["table"])
            for src in map(load_source, schedule)
            for t in src["tables"]
        ]

    with DAG(
        dag_id=f"transform__{domain}",
        schedule=schedule,
        start_date=_start_date(platform),
        default_args=DEFAULT_ARGS,
        template_searchpath=[str(DAGS_DIR)],
        tags=["transform", domain],
    ) as dag:
        layers = [layer for layer in ("silver", "gold") if any(m.startswith(f"{layer}/") for m in tr["models"])]
        ensure_datasets = BigQueryInsertJobOperator(
            task_id="ensure_datasets",
            configuration={"query": {"useLegacySql": False, "query": "\n".join(
                f"CREATE SCHEMA IF NOT EXISTS {layer}_{domain} OPTIONS (location = '{gcp['bq_location']}');"
                for layer in layers
            )}},
            **bq,
        )
        previous = [ensure_datasets]
        for layer in layers:
            tasks = [
                BigQueryInsertJobOperator(
                    task_id=f"{layer}__{model.rsplit('/', 1)[1].removesuffix('.sql')}",
                    # path .sql relatif ke dags/: isinya dibaca & dirender Jinja oleh Airflow
                    configuration={"query": {"query": f"sql/{model}", "useLegacySql": False}},
                    **bq,
                )
                for model in tr["models"]
                if model.startswith(f"{layer}/")
            ]
            for upstream in previous:  # semua silver selesai dulu, baru gold
                upstream >> tasks
            previous = tasks
    return dag
