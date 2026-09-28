"""Parse DAG dengan Airflow. Di laptop Windows Airflow tidak bisa di-import -> di-skip;
gerbang sebenarnya: scripts/deploy.sh menjalankan `medallion.validate --dagbag` di container VM."""

import pytest

pytest.importorskip("airflow.sdk")

from medallion.validate import dagbag_errors  # noqa: E402


def test_dag_tanpa_import_error():
    assert dagbag_errors() == []
