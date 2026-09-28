"""Bronze: 1 tabel SQL Server -> file di GCS (data lake) -> BigQuery, pakai dlt.

Jalan di venv dlt (/opt/dlt-venv), BUKAN di environment Airflow: jangan import airflow di sini.
"""

import os
from datetime import date, datetime, timezone

# Setelan dlt yang sama untuk semua tabel.
DLT_ENV = {
    "RUNTIME__DLTHUB_TELEMETRY": "false",
    # Default dlt menghapus file lama tabel di GCS sebelum load. Data lake: file disimpan.
    "DESTINATION__TRUNCATE_TABLES_ON_STAGING_DESTINATION_BEFORE_LOAD": "false",
    # Backend pyarrow tidak menambah _dlt_load_id kalau tidak diminta.
    "NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID": "true",
    # File kerja yang sudah ter-load dihapus dari /opt/dlt (tidak menumpuk di disk VM).
    "LOAD__DELETE_COMPLETED_JOBS": "true",
    # Nama tabel & kolom BigQuery = nama asli SQL Server (AIMHeader.nomorAIM), bukan snake_case.
    # JANGAN diubah setelah ada tabel ter-load: ganti naming = drop & load ulang semua tabel.
    "SCHEMA__NAMING": "sql_cs_v1",
}
LAKE_LAYOUT = "{table_name}/{YYYY}/{MM}/{DD}/{load_id}.{file_id}.{ext}"


def mssql_engine(conn: dict, database: str):
    """Engine SQLAlchemy dari field Airflow Connection (host, port, login, password)."""
    import sqlalchemy as sa

    port = conn.get("port")
    url = sa.URL.create(
        "mssql+pyodbc",
        username=conn["login"],
        password=conn["password"],
        host=conn["host"],
        port=int(port) if port not in (None, "", "None") else 1433,
        database=database,
        # Sertifikat Cloud SQL ditandatangani CA per-instance, bukan CA publik.
        query={"driver": "ODBC Driver 18 for SQL Server", "Encrypt": "yes", "TrustServerCertificate": "yes"},
    )
    return sa.create_engine(url, pool_pre_ping=True)


def run(job: dict, credentials, destination=None) -> None:
    """Ingest 1 tabel. `job` dirakit medallion.dags.bronze_dag; `destination` hanya diganti di test."""
    for key, value in DLT_ENV.items():
        os.environ.setdefault(key, value)

    import dlt
    import pyarrow as pa
    from dlt.destinations.adapters import bigquery_adapter
    from dlt.sources.sql_database import sql_table

    incremental = None
    if job.get("cursor"):
        initial = job.get("initial_value")
        incremental = dlt.sources.incremental(
            job["cursor"],
            # "2024-01-01" (str) atau 2024-01-01 (date YAML) -> datetime naive, sama dengan datetime SQL Server
            initial_value=datetime.fromisoformat(str(initial)) if isinstance(initial, (str, date)) else initial,
            # lag dlt dalam detik untuk cursor datetime
            lag=job.get("lookback_hours", 0) * 3600 or None,
        )

    resource = sql_table(
        credentials=credentials,
        schema=job["schema"],
        table=job["table"],
        backend="pyarrow",
        chunk_size=job.get("chunk_size", 50000),
        excluded_columns=job.get("excluded_columns"),
        incremental=incremental,
        # incremental = append (duplikat dari lookback di-dedup di silver); tanpa cursor = snapshot penuh
        write_disposition="append" if incremental else "replace",
    )

    ingested_at = pa.scalar(datetime.now(timezone.utc), type=pa.timestamp("us", tz="UTC"))
    resource.add_map(lambda t: t.append_column("_ingested_at", pa.repeat(ingested_at, t.num_rows)))
    bigquery_adapter(resource, partition="_ingested_at")

    pipeline = dlt.pipeline(
        pipeline_name=job["pipeline_name"],  # 1 pipeline per tabel: state incremental terpisah
        destination=destination
        or dlt.destinations.bigquery(project_id=job["bq_project"], location=job["bq_location"]),
        staging=dlt.destinations.filesystem(bucket_url=job["bucket_url"], layout=LAKE_LAYOUT),
        dataset_name=job["dataset"],
    )
    # Sisa run yang gagal setelah extract: run(data) hanya me-load paket lama lalu keluar TANPA
    # extract data baru. Selesaikan dulu, baru extract.
    if pipeline.has_pending_data:
        print("Ada load package tertunda dari run sebelumnya, diselesaikan dulu:")
        print(pipeline.run())
    print(pipeline.run(resource, loader_file_format=job["file_format"]))
    print(pipeline.last_trace.last_normalize_info)
