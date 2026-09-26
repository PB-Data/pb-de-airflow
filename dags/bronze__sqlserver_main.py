"""Airflow DAG bronze untuk configs/sources/sqlserver_main.yml.

Nambah/ubah TABEL cukup edit YAML-nya. File ini hanya 1 per source (sengaja dipisah
per file: kalau YAML satu source error, DAG source lain tetap jalan).
Source baru: copy file ini, ganti nama file & argumen build_bronze_dag().
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:  # Airflow 3.0 (CLI `dags test`) belum otomatis menambahkan folder DAG
    sys.path.append(_HERE)

from medallion.factory.bronze_factory import build_bronze_dag  # noqa: E402

dag = build_bronze_dag("sqlserver_main")
