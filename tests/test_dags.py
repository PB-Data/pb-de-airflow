"""Test import DAG dengan Airflow 3 (dijalankan di environment yang ada Airflow-nya)."""

import pytest

pytest.importorskip("airflow")

from conftest import DAGS_DIR  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    import inspect

    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # Airflow 3.0 / 3.1
        from airflow.models.dagbag import DagBag

    kwargs = {"dag_folder": str(DAGS_DIR)}
    if "include_examples" in inspect.signature(DagBag.__init__).parameters:
        kwargs["include_examples"] = False
    return DagBag(**kwargs)


def test_tidak_ada_import_error(dagbag):
    assert dagbag.import_errors == {}


def test_bronze_dag(dagbag):
    dag = dagbag.dags["bronze__sqlserver_main"]
    assert {t.task_id for t in dag.tasks} == {"dbo__customer", "dbo__sales_order_header", "dbo__sales_order_detail", "dbo__product"}
    assert dag.max_active_runs == 1
    task = dag.get_task("dbo__customer")
    assert type(task).__name__ == "ExternalPythonOperator"
    assert [a.uri for a in task.outlets] == ["bigquery://smi-dwh/bronze_sqlserver_main/dbo__customer"]
    assert type(dag.timetable).__name__ == "CronDataIntervalTimetable"
    assert task.max_active_tis_per_dag == 1  # aman dari backfill paralel


def test_transform_dag(dagbag):
    dag = dagbag.dags["transform__sales"]
    gold = dag.get_task("gold__daily_sales")
    assert gold.upstream_task_ids == {"silver__sales_order", "silver__sales_order_line", "silver__product"}
    assert "ensure_datasets" in dag.get_task("silver__customer").upstream_task_ids
    assert type(dag.timetable).__name__ == "AssetTriggeredTimetable"
