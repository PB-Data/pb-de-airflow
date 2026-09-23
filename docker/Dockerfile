# syntax=docker/dockerfile:1
# Airflow 3.1.6 + ODBC Driver 18 for SQL Server + dlt
#
# Build:  docker compose build
# Semua versi di-pin. Dependency Airflow dikunci lewat constraints resmi
# supaya provider tidak saling tarik-menarik versi.

FROM apache/airflow:3.1.6-python3.12

# ---------------------------------------------------------------------------
# 1. System packages: ODBC Driver 18 for SQL Server + unixODBC
#    Prosedur resmi Microsoft untuk Debian 12 (bookworm).
# ---------------------------------------------------------------------------
USER root

RUN set -eux; \
    apt-get -o DPkg::Lock::Timeout=300 update; \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        ca-certificates \
        apt-transport-https; \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        -o /etc/apt/trusted.gpg.d/microsoft.asc; \
    curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
        -o /etc/apt/sources.list.d/mssql-release.list; \
    apt-get -o DPkg::Lock::Timeout=300 update; \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        unixodbc \
        unixodbc-dev; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

# Verifikasi driver benar-benar terdaftar. Kalau baris ini gagal,
# build berhenti di sini alih-alih gagal misterius saat runtime.
RUN odbcinst -q -d -n "ODBC Driver 18 for SQL Server"

USER airflow

# ---------------------------------------------------------------------------
# 2. Provider Airflow — dikunci constraints resmi 3.1.6
# ---------------------------------------------------------------------------
ARG AIRFLOW_VERSION=3.1.6
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" \
        apache-airflow-providers-google \
        apache-airflow-providers-microsoft-mssql \
        apache-airflow-providers-common-sql

# ---------------------------------------------------------------------------
# 3. dlt + driver database
#    Sengaja TANPA constraints: dlt bukan bagian dari ekosistem Airflow.
#    `pip check` di bawah yang menjaga tidak ada konflik tersisa.
#
#    CATATAN: paket `dlt` (Apache 2.0) saja. JANGAN `dlthub` (berbayar).
# ---------------------------------------------------------------------------
RUN pip install --no-cache-dir \
        "dlt[bigquery,filesystem,parquet]==1.30.0" \
        "pyodbc==5.2.0"

RUN pip check
