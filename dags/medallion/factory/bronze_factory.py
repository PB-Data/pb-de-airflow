"""Generate DAG ``bronze__<source>`` dari ``configs/sources/<source>.yml``.

* 1 task per tabel, dijalankan dengan ExternalPythonOperator di venv dlt (/opt/dlt-venv)
  -> dependency dlt tidak bentrok dengan Airflow/provider.
* Kredensial DB diambil dari Airflow Connection lewat template ``{{ conn[...] }}``
  (password otomatis di-mask di log & Rendered Template).
* Tiap task meng-emit Asset ``bigquery://<project>/<dataset>/<table>`` -> dipakai DAG silver/gold.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import DAG, Asset, Param

try:  # Airflow >= 3.2 (diekspor lewat airflow.sdk)
    from airflow.sdk import CronDataIntervalTimetable
except ImportError:  # Airflow 3.0 / 3.1
    from airflow.timetables.interval import CronDataIntervalTimetable

from medallion.catalog import bronze_asset_name
from medallion.factory.common import default_args
from medallion.config import SKIP_EXIT_CODE, Platform, Source, Table, bq_uri, load_platform, load_source


def _bronze_entrypoint(dags_dir, source_name, table_name, run_ctx, db_conn):
    """Jalan di dalam venv dlt.

    JANGAN pakai variabel/impor global di fungsi ini: ExternalPythonOperator hanya
    menyalin *source code* fungsi ini ke proses Python lain.
    """
    import sys

    if dags_dir not in sys.path:
        sys.path.insert(0, dags_dir)
    from medallion.bronze.runner import run_from_airflow

    return run_from_airflow(source_name, table_name, run_ctx, db_conn)


def _conn_template(conn_id: str) -> dict[str, str]:
    # conn['id'] (bukan conn.id) supaya conn_id yang mengandung '-' tetap aman di Jinja
    return {
        field: "{{ conn['%s'].%s }}" % (conn_id, field)
        for field in ("host", "port", "login", "password", "schema", "extra")
    }


RUN_CTX_TEMPLATE = {
    "dag_id": "{{ dag.dag_id }}",
    "run_id": "{{ run_id }}",
    "run_type": "{{ dag_run.run_type }}",
    "data_interval_start": "{{ data_interval_start }}",
    "data_interval_end": "{{ data_interval_end }}",
    "backfill_start": "{{ params.backfill_start }}",
    "backfill_end": "{{ params.backfill_end }}",
    "only_tables": "{{ params.tables | tojson }}",
    "try_number": "{{ ti.try_number }}",
}


def _table_doc(source: Source, table: Table, platform: Platform) -> str:
    rows = [
        ("Tabel sumber", f"`{table.table}`"),
        ("Tabel BigQuery", f"`{platform.bq_project}.{source.bronze_dataset}.{table.target_table}`"),
        ("File GCS", f"`{platform.lake_bucket_url}/{source.bronze_dataset}/{table.target_table}/`"),
        ("Load", f"{table.load} / {table.write_disposition}"),
        ("Cursor", f"`{table.cursor}` (lookback {table.lookback_hours} jam)" if table.cursor else "-"),
        ("Primary key", ", ".join(table.primary_key) or "-"),
    ]
    body = "\n".join(f"| {k} | {v} |" for k, v in rows)
    return f"{table.description}\n\n| | |\n|---|---|\n{body}"


def build_bronze_dag(source_name: str, dags_dir: str | Path | None = None) -> DAG:
    platform = load_platform()
    source = load_source(source_name)
    dags_dir = str(Path(dags_dir or Path(__file__).resolve().parents[2]))
    tz = pendulum.timezone(platform.timezone)

    schedule = CronDataIntervalTimetable(source.schedule, timezone=platform.timezone) if source.schedule else None

    dag = DAG(
        dag_id=source.dag_id,
        description=source.description or f"Bronze ingestion {source.name} -> GCS -> BigQuery (dlt)",
        # CronDataIntervalTimetable (bukan cron string biasa): di Airflow 3 cron string default
        # jadi CronTriggerTimetable yang tidak punya data interval -> backfill per tanggal jadi kosong.
        schedule=schedule,
        start_date=pendulum.datetime(source.start_date.year, source.start_date.month, source.start_date.day, tz=tz),
        catchup=False,
        max_active_runs=1,  # wajib 1: state incremental dlt per tabel tidak boleh dipakai 2 run sekaligus
        max_active_tasks=source.max_active_tasks,  # batasi query paralel ke DB sumber
        default_args=default_args(platform, source.owner),
        tags=["bronze", "dlt", source.type, source.name, *source.tags],
        params={
            "backfill_start": Param(
                None, type=["null", "string"], format="date", title="Backfill start (opsional)",
                description="Isi bersama backfill_end untuk load ulang window tertentu (jam sumber, WIB). "
                            "Kosongkan untuk incremental normal. State incremental tidak berubah.",
            ),
            "backfill_end": Param(
                None, type=["null", "string"], format="date", title="Backfill end (inklusif)",
            ),
            "tables": Param(
                [], type="array", items={"type": "string"}, title="Hanya tabel tertentu (opsional)",
                examples=[t.table for t in source.tables],
                description="Kosong = semua tabel. Tabel lain akan di-skip.",
            ),
        },
        doc_md=(
            f"### Bronze `{source.name}`\n{source.description}\n\n"
            f"- Connection Airflow: `{source.airflow_conn_id}`\n"
            f"- Dataset BigQuery: `{platform.bq_project}.{source.bronze_dataset}` ({platform.bq_location})\n"
            f"- Data lake: `{platform.lake_bucket_url}/{source.bronze_dataset}/`\n"
            f"- Config: `dags/configs/sources/{source.name}.yml`\n"
        ),
    )

    with dag:
        for table in source.tables:
            ExternalPythonOperator(
                task_id=table.task_id,
                python=platform.dlt_python,
                python_callable=_bronze_entrypoint,
                op_kwargs={
                    "dags_dir": dags_dir,
                    "source_name": source.name,
                    "table_name": table.table,
                    "run_ctx": dict(RUN_CTX_TEMPLATE),
                    "db_conn": _conn_template(source.airflow_conn_id),
                },
                expect_airflow=False,
                skip_on_exit_code=SKIP_EXIT_CODE,
                # max 1 instance per tabel di SEMUA run (termasuk `airflow backfill create` yang
                # mengabaikan max_active_runs DAG) -> state dlt tabel tidak pernah dipakai paralel
                max_active_tis_per_dag=1,
                **({"pool": source.pool} if source.pool else {}),
                outlets=[
                    Asset(
                        name=bronze_asset_name(table),
                        uri=bq_uri(platform.bq_project, source.bronze_dataset, table.target_table),
                        group="bronze",
                    )
                ],
                execution_timeout=timedelta(minutes=table.timeout_minutes),
                doc_md=_table_doc(source, table, platform),
            )
    return dag
