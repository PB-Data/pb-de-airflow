#!/usr/bin/env bash
# =====================================================================
# Deploy manual laptop -> VM Airflow (Docker Compose). Untuk Mac / Linux / WSL / Git Bash.
# (Windows PowerShell: pakai scripts/deploy.ps1)
#
#   cp scripts/deploy.env.example scripts/deploy.env    # isi sekali
#   ./scripts/deploy.sh           # sync dags/ (tanpa restart; Airflow re-parse otomatis)
# Image & docker-compose Airflow dikelola repo pb-airflow.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f scripts/deploy.env ]] && source scripts/deploy.env
: "${VM_NAME:?isi VM_NAME di scripts/deploy.env}"
: "${VM_ZONE:?isi VM_ZONE di scripts/deploy.env}"
: "${VM_PROJECT:?isi VM_PROJECT di scripts/deploy.env}"
DAGS_DIR="${DAGS_DIR:-/data/airflow/dags}"
DOCKER="${DOCKER:-docker}"
FLAGS=(--zone "$VM_ZONE" --project "$VM_PROJECT")
[[ "${USE_IAP:-false}" == "true" ]] && FLAGS+=(--tunnel-through-iap)

PY=python3; command -v python3 >/dev/null || PY=python
echo ">> 1/3 Validasi config & SQL (lokal)"
(cd dags && "$PY" -m medallion.validate)

echo ">> 2/3 Paketkan & upload ke $VM_NAME"
# folder sementara RELATIF di dalam repo: path absolut Windows (C:\...) bisa disalahartikan
# sebagai nama instance oleh `gcloud compute scp` di Git Bash
TMP=".deploy_tmp"; rm -rf "$TMP"; mkdir -p "$TMP"; trap 'rm -rf "$TMP"' EXIT
tar --exclude='__pycache__' --exclude='.local' --exclude='.dlt' -czf "$TMP/medallion_dags.tgz" -C dags .
gcloud compute scp "$TMP/medallion_dags.tgz" scripts/remote_install.sh "$VM_NAME:/tmp/" "${FLAGS[@]}"

echo ">> 3/3 Pasang di VM"
# shellcheck disable=SC2086  # DOCKER sengaja di-split ("sudo docker")
gcloud compute ssh "$VM_NAME" "${FLAGS[@]}" \
  --command "bash /tmp/remote_install.sh '$DAGS_DIR' $DOCKER"
echo "Deploy selesai."
