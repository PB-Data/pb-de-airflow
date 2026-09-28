#!/usr/bin/env bash
# Deploy manual laptop -> VM Airflow: salin dags/ ke folder DAG di VM. Jalankan dari Git Bash.
#
#   cp scripts/deploy.env.example scripts/deploy.env   # isi sekali
#   scripts/deploy.sh
#
# Yang dikirim: file di dags/ yang dikenal git, termasuk perubahan yang belum di-commit.
# File baru harus `git add` dulu; file yang di-gitignore (cache, .dlt) tidak pernah ikut.
# Di VM, DAG baru di-parse dulu di container sekali-pakai (image scheduler yang sedang jalan).
# Kalau gagal, folder DAG Airflow tidak disentuh.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f scripts/deploy.env ] && . scripts/deploy.env
: "${VM_NAME:?isi VM_NAME di scripts/deploy.env}"
: "${VM_ZONE:?isi VM_ZONE di scripts/deploy.env}"
: "${VM_PROJECT:?isi VM_PROJECT di scripts/deploy.env}"
DAGS_DIR=${DAGS_DIR:-/data/airflow/dags}   # volume DAG di docker-compose pb-airflow
FLAGS=(--zone "$VM_ZONE" --project "$VM_PROJECT")
[ "${USE_IAP:-false}" = true ] && FLAGS+=(--tunnel-through-iap)

BELUM=$(git ls-files --others --deleted --exclude-standard dags)
[ -z "$BELUM" ] || { printf 'File di dags/ belum di-git add / git rm:\n%s\n' "$BELUM" >&2; exit 1; }

echo ">> 1/3 Validasi lokal"
PY=$(ls "$PWD"/.venv/Scripts/python.exe "$PWD"/.venv/bin/python 2>/dev/null | head -n1 || true)
(cd dags && "${PY:-python3}" -m medallion.validate)

echo ">> 2/3 Upload ke $VM_NAME"
# Folder sementara RELATIF: path absolut Windows (C:\...) dibaca `gcloud compute scp` sebagai nama VM.
TMP=.deploy_tmp
rm -rf "$TMP" && mkdir "$TMP" && trap 'rm -rf "$TMP"' EXIT
(cd dags && git ls-files -z | tar --null -T - -czf "../$TMP/pb-dwh-dags.tgz")
cat > "$TMP/pb-dwh-install.sh" <<'REMOTE'
set -euo pipefail
DAGS_DIR=$1
STAGE=/tmp/pb-dwh-dags
if docker info >/dev/null 2>&1; then D=docker; else D="sudo docker"; fi
command -v rsync >/dev/null || { echo "rsync belum ada di VM: sudo apt-get install -y rsync" >&2; exit 1; }
[ -d "$DAGS_DIR" ] || { echo "$DAGS_DIR tidak ada. Setup VM dulu (README pb-airflow)." >&2; exit 1; }
IMAGE=$($D ps --filter label=com.docker.compose.service=airflow-scheduler --format '{{.Image}}' | head -n1)
[ -n "$IMAGE" ] || { echo "Container airflow-scheduler tidak jalan (pb-airflow: docker compose up -d)." >&2; exit 1; }

rm -rf "$STAGE" && mkdir -p "$STAGE"
tar -xzf /tmp/pb-dwh-dags.tgz -C "$STAGE" && chmod -R a+rX "$STAGE"
echo ">> Parse DAG baru dengan $IMAGE (container sekali-pakai, Airflow yang jalan belum disentuh)"
$D run --rm --entrypoint python -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$STAGE:/opt/airflow/dags:ro" -w /opt/airflow/dags "$IMAGE" -m medallion.validate --dagbag

echo ">> Salin ke $DAGS_DIR"
# Isi folder diganti IN PLACE. Jangan hapus/ganti folder-nya: bind mount container menempel ke folder ini.
sudo rsync -a --delete --exclude=__pycache__ --chown="$(stat -c %u:%g "$DAGS_DIR")" "$STAGE/" "$DAGS_DIR/"
rm -rf "$STAGE" /tmp/pb-dwh-dags.tgz /tmp/pb-dwh-install.sh
echo "OK. DAG processor membaca perubahan dalam ~1 menit."
REMOTE
gcloud compute scp "$TMP/pb-dwh-dags.tgz" "$TMP/pb-dwh-install.sh" "$VM_NAME:/tmp/" "${FLAGS[@]}"

echo ">> 3/3 Pasang di VM"
gcloud compute ssh "$VM_NAME" "${FLAGS[@]}" --command "bash /tmp/pb-dwh-install.sh $DAGS_DIR"
