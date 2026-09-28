"""Airflow DAG bronze untuk configs/sources/dc_sby.yml.

Tambah tabel: edit YAML-nya saja. Source baru: copy file ini, ganti nama file dan argumen di baris terakhir.
(Kata "Airflow" dan "DAG" di docstring ini wajib: tanpa itu Airflow melewati file ini.)
"""

from medallion.dags import bronze_dag

dag = bronze_dag("dc_sby")
