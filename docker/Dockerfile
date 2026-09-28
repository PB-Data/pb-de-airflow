# syntax=docker/dockerfile:1
# Airflow 3.3.2 + ODBC Driver 18 for SQL Server + venv dlt terpisah
#
# Build:  docker compose build
# Tag -python3.12 WAJIB: requirements-dlt.txt di-lock untuk Python 3.12.
# Provider google, standard, smtp sudah dibawa image resmi; tidak dipasang ulang.

FROM apache/airflow:3.3.2-python3.12

USER root

# ---------------------------------------------------------------------------
# 1. ODBC Driver 18 for SQL Server (dipakai pyodbc di venv dlt)
#    Repo Microsoft dipilih sesuai versi Debian image (12 = bookworm, 13 = trixie).
#    curl 404 = versi Debian itu belum didukung Microsoft.
# ---------------------------------------------------------------------------
RUN set -eux; \
    . /etc/os-release; \
    curl -fsSL -o /tmp/packages-microsoft-prod.deb \
      "https://packages.microsoft.com/config/debian/${VERSION_ID%%.*}/packages-microsoft-prod.deb"; \
    dpkg -i /tmp/packages-microsoft-prod.deb; \
    rm /tmp/packages-microsoft-prod.deb; \
    apt-get -o DPkg::Lock::Timeout=300 update; \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc; \
    apt-get autoremove -yqq --purge; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

# Verifikasi driver benar-benar terdaftar. Kalau baris ini gagal,
# build berhenti di sini alih-alih gagal misterius saat runtime.
RUN odbcinst -q -d -n "ODBC Driver 18 for SQL Server"

# ---------------------------------------------------------------------------
# 2. venv dlt TERPISAH dari environment Airflow (hindari konflik dependency)
#    DAG pb-dwh memanggilnya lewat ExternalPythonOperator
#    (runtime.dlt_python di pb-dwh dags/configs/platform.yml).
#    Image resmi set PIP_USER=true -> dimatikan untuk pip di dalam venv.
#    CATATAN: paket `dlt` (Apache 2.0) saja. JANGAN `dlthub` (berbayar).
# ---------------------------------------------------------------------------
COPY requirements-dlt.txt /tmp/requirements-dlt.txt
RUN set -eux; \
    python -m venv /opt/dlt-venv; \
    PIP_USER=false /opt/dlt-venv/bin/pip install --no-cache-dir --upgrade pip; \
    PIP_USER=false /opt/dlt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dlt.txt; \
    chmod -R a+rX /opt/dlt-venv; \
    /opt/dlt-venv/bin/python -c "import dlt, pyodbc; assert 'ODBC Driver 18 for SQL Server' in pyodbc.drivers(); print('dlt', dlt.__version__)"

USER airflow
