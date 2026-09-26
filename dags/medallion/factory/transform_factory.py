"""Generate DAG silver/gold dari ``configs/transforms/<key>.yml`` + file SQL di ``dags/sql``.

* 1 task BigQueryInsertJobOperator per file SQL (model).
* Dependency antar model dibaca otomatis dari ``ref('...')`` di SQL (seperti dbt).
* ``schedule: assets`` -> DAG jalan otomatis setelah semua tabel bronze/model upstream
  (dari ``source(...)`` / ``ref(...)`` ke DAG lain) ter-update.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import DAG, Asset, Param

try:  # Airflow >= 3.2
    from airflow.sdk import CronTriggerTimetable
except ImportError:  # Airflow 3.0 / 3.1
    from airflow.timetables.trigger import CronTriggerTimetable

from medallion.catalog import Catalog
from medallion.factory.common import default_args
from medallion.config import ConfigError, label_value, load_transform, sql_dir


def build_transform_dag(key: str) -> DAG:
    catalog = Catalog.load(strict=False)
    tdag = load_transform(key)
    if tdag.dag_id not in catalog.transforms:
        # misal model duplikat dengan DAG lain -> tampilkan error aslinya
        raise ConfigError(catalog.errors.get(f"transforms/{tdag.dag_id}") or f"Transform {key} tidak valid")
    catalog.validate([tdag.dag_id])
    platform = catalog.platform
    tz = pendulum.timezone(platform.timezone)

    if tdag.schedule == "assets":
        upstream = catalog.external_assets(tdag)
        if not upstream:
            raise ConfigError(f"transforms/{key}.yml: schedule 'assets' tapi tidak ada source()/ref() ke luar DAG")
        schedule = [Asset(name=name, uri=uri) for name, uri in upstream]
    elif tdag.schedule in ("none", "manual"):
        schedule = None
    else:
        schedule = CronTriggerTimetable(tdag.schedule, timezone=platform.timezone)

    dag = DAG(
        dag_id=tdag.dag_id,
        description=tdag.description or "Transform silver/gold di BigQuery",
        schedule=schedule,
        start_date=pendulum.datetime(tdag.start_date.year, tdag.start_date.month, tdag.start_date.day, tz=tz),
        catchup=False,
        max_active_runs=1,
        max_active_tasks=tdag.max_active_tasks,
        default_args=default_args(platform, tdag.owner),
        tags=["transform", *sorted({m.layer for m in tdag.models}), *tdag.tags],
        params={
            "full_refresh": Param(
                False, type="boolean", title="Full refresh",
                description="True = rebuild semua model incremental dari seluruh histori bronze.",
            ),
            "lookback_days": Param(
                tdag.lookback_days, type="integer", minimum=1, title="Lookback (hari)",
                description="Model incremental hanya membaca partisi bronze N hari terakhir.",
            ),
        },
        user_defined_macros=catalog.jinja_macros(),
        template_searchpath=[str(sql_dir())],
        doc_md=(
            f"### {tdag.dag_id}\n{tdag.description}\n\n"
            f"- Config: `dags/configs/transforms/{key}.yml`\n"
            f"- Trigger: {'Assets (' + str(len(schedule)) + ' upstream)' if isinstance(schedule, list) else tdag.schedule}\n"
        ),
    )

    with dag:
        # dataset silver/gold dibuat kalau belum ada (dataset bronze dibuat otomatis oleh dlt)
        ensure_sql = "\n".join(
            f"CREATE SCHEMA IF NOT EXISTS `{platform.bq_project}.{ds}` OPTIONS (location = '{platform.bq_location}');"
            for ds in sorted({m.dataset for m in tdag.models})
        )
        ensure_datasets = BigQueryInsertJobOperator(
            task_id="ensure_datasets",
            gcp_conn_id=platform.gcp_conn_id,
            project_id=platform.bq_project,
            location=platform.bq_location,
            configuration={
                "query": {"query": ensure_sql, "useLegacySql": False},
                "labels": {"layer": "meta", "pipeline": label_value(tdag.dag_id)},
            },
        )
        tasks = {}
        for model in tdag.models:
            relation = catalog.model_relation(model)
            labels = {
                "layer": model.layer,
                "model": label_value(model.name),
                "pipeline": label_value(tdag.dag_id),
                **dict(model.labels),
            }
            tasks[model.id] = BigQueryInsertJobOperator(
                task_id=model.task_id,
                gcp_conn_id=platform.gcp_conn_id,
                project_id=platform.bq_project,
                location=platform.bq_location,
                configuration={
                    "query": {"query": Catalog.model_query_template(model), "useLegacySql": False},
                    "labels": labels,
                },
                outlets=[Asset(name=model.id, uri=relation.uri, group=model.layer)],
                execution_timeout=timedelta(minutes=model.timeout_minutes),
                doc_md=f"{model.description}\n\nSQL: `dags/sql/{model.sql_file}` -> `{relation.fqn}`",
            )
        edges = catalog.internal_edges(tdag)
        for upstream_id, downstream_id in edges:
            tasks[upstream_id] >> tasks[downstream_id]
        has_upstream = {down for _, down in edges}
        for model_id, task in tasks.items():
            if model_id not in has_upstream:
                ensure_datasets >> task
    return dag
