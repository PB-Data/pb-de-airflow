"""Airflow DAG silver/gold untuk configs/transforms/sales.yml (SQL di dags/sql/silver|gold/sales/).

Domain baru: copy file ini, ganti nama file & argumen build_transform_dag().
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:  # Airflow 3.0 (CLI `dags test`) belum otomatis menambahkan folder DAG
    sys.path.append(_HERE)

from medallion.factory.transform_factory import build_transform_dag  # noqa: E402

dag = build_transform_dag("sales")
