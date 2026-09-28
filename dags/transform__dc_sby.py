"""Airflow DAG silver/gold untuk configs/transforms/dc_sby.yml (SQL di dags/sql/{silver,gold}/dc_sby/).

Domain baru: copy file ini, ganti nama file dan argumen di baris terakhir.
(Kata "Airflow" dan "DAG" di docstring ini wajib: tanpa itu Airflow melewati file ini.)
"""

from medallion.dags import transform_dag

dag = transform_dag("dc_sby")
