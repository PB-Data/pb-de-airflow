# =====================================================================
# Image Airflow 3 + Microsoft ODBC Driver 18 (SQL Server) + venv dlt terpisah
#
# Build (di VM, dari root repo):
#   docker build -t smi-airflow:3.1.6-dlt -f docker/Dockerfile docker/
# Lalu di .env docker-compose:  AIRFLOW_IMAGE_NAME=smi-airflow:3.1.6-dlt
#
# PENTING: samakan AIRFLOW_IMAGE dengan versi Airflow yang sekarang jalan di VM
#          (cek: docker compose exec airflow-scheduler airflow version)
#          dan tetap pakai tag -python3.12: requirements-dlt.txt di-lock untuk Python 3.12
# =====================================================================
ARG AIRFLOW_IMAGE=apache/airflow:3.1.6-python3.12
FROM ${AIRFLOW_IMAGE}

USER root

# --- Microsoft ODBC Driver 18 for SQL Server (dipakai pyodbc di venv dlt) ---
# Repo Microsoft dipilih otomatis sesuai versi Debian image (12 = bookworm, 13 = trixie).
# Kalau build gagal di langkah curl (404) = versi Debian itu belum didukung Microsoft.
RUN set -eux; \
    . /etc/os-release; \
    curl -fsSL -o /tmp/packages-microsoft-prod.deb \
      "https://packages.microsoft.com/config/debian/${VERSION_ID%%.*}/packages-microsoft-prod.deb"; \
    dpkg -i /tmp/packages-microsoft-prod.deb; \
    rm /tmp/packages-microsoft-prod.deb; \
    apt-get update; \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc; \
    apt-get autoremove -yqq --purge; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

# --- venv dlt TERPISAH dari environment Airflow (hindari konflik dependency) ---
# Image resmi Airflow set PIP_USER=true -> harus dimatikan untuk pip di dalam venv.
COPY requirements-dlt.txt /tmp/requirements-dlt.txt
RUN set -eux; \
    python -m venv /opt/dlt-venv; \
    PIP_USER=false /opt/dlt-venv/bin/pip install --no-cache-dir --upgrade pip; \
    PIP_USER=false /opt/dlt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dlt.txt; \
    chmod -R a+rX /opt/dlt-venv; \
    /opt/dlt-venv/bin/python -c "import dlt, pyodbc, sqlalchemy; print('dlt', dlt.__version__, 'odbc drivers', pyodbc.drivers())"

USER airflow

# --- (opsional) package tambahan untuk environment Airflow utama ---
# Airflow di-pin ke versi image supaya pip tidak meng-upgrade/downgrade Airflow.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt
