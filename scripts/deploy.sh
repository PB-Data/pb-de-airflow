#!/usr/bin/env bash
# =====================================================================
# Deploy manual laptop -> VM Airflow (Docker Compose). Untuk Mac / Linux / WSL / Git Bash.
# (Windows PowerShell: pakai scripts/deploy.ps1)
#
#   cp scripts/deploy.env.example scripts/deploy.env    # isi sekali
#   ./scripts/deploy.sh           # sync dags/ (tanpa restart; Airflow re-parse otomatis)
#   ./scripts/deploy.sh --image   # + build ulang image (kalau docker/Dockerfile / requirements berubah)
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f scripts/deploy.env ]] && source scripts/deploy.env
: "${VM_NAME:?isi VM_NAME di scripts/deploy.env}"
: "${VM_ZONE:?isi VM_ZONE di scripts/deploy.env}"
: "${VM_PROJECT:?isi VM_PROJECT di scripts/deploy.env}"
REMOTE_DIR="${REMOTE_DIR:-airflow}"
DOCKER="${DOCKER:-docker}"
IMAGE="${IMAGE:-smi-airflow:3.1.6-dlt}"
FLAGS=(--zone "$VM_ZONE" --project "$VM_PROJECT")
[[ "${USE_IAP:-false}" == "true" ]] && FLAGS+=(--tunnel-through-iap)
WITH_IMAGE=false; [[ "${1:-}" == "--image" ]] && WITH_IMAGE=true

PY=python3; command -v python3 >/dev/null || PY=python
echo ">> 1/3 Validasi config & SQL (lokal)"
(cd dags && "$PY" -m medallion.validate)

echo ">> 2/3 Paketkan & upload ke $VM_NAME"
# folder sementara RELATIF di dalam repo: path absolut Windows (C:\...) bisa disalahartikan
# sebagai nama instance oleh `gcloud compute scp` di Git Bash
TMP=".deploy_tmp"; rm -rf "$TMP"; mkdir -p "$TMP"; trap 'rm -rf "$TMP"' EXIT
tar --exclude='__pycache__' --exclude='.local' --exclude='.dlt' -czf "$TMP/medallion_dags.tgz" -C dags .
FILES=("$TMP/medallion_dags.tgz" "scripts/remote_install.sh")
if $WITH_IMAGE; then tar -czf "$TMP/medallion_docker.tgz" -C docker . && FILES+=("$TMP/medallion_docker.tgz"); fi
gcloud compute scp "${FILES[@]}" "$VM_NAME:/tmp/" "${FLAGS[@]}"

echo ">> 3/3 Pasang di VM"
# shellcheck disable=SC2086  # DOCKER sengaja di-split ("sudo docker")
gcloud compute ssh "$VM_NAME" "${FLAGS[@]}" \
  --command "bash /tmp/remote_install.sh '$REMOTE_DIR' $WITH_IMAGE '$IMAGE' $DOCKER"
echo "Deploy selesai."
