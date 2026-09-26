#!/usr/bin/env bash
# Dijalankan DI VM oleh deploy.sh / deploy.ps1 (jangan dijalankan manual di laptop).
# Argumen: <REMOTE_DIR> <WITH_IMAGE:true|false> <IMAGE> <DOCKER_CMD...>
#
# Alur aman: extract ke folder staging -> (build image) -> validasi DAG di container sekali-pakai
#            -> kalau lolos baru sync ke folder dags/ yang dipakai Airflow.
set -euo pipefail
REMOTE_DIR="$1"; WITH_IMAGE="$2"; IMAGE="$3"; shift 3
DOCKER=("$@"); [[ ${#DOCKER[@]} -eq 0 ]] && DOCKER=(docker)

cd ~ && cd "$REMOTE_DIR"
command -v rsync >/dev/null || { echo "rsync belum ada di VM: sudo apt-get install -y rsync"; exit 1; }

rm -rf .deploy/dags && mkdir -p .deploy/dags dags dlt_data
tar -xzf /tmp/medallion_dags.tgz -C .deploy/dags && rm -f /tmp/medallion_dags.tgz

if [[ "$WITH_IMAGE" == "true" ]]; then
  echo ">> Build image $IMAGE"
  rm -rf .deploy/docker && mkdir -p .deploy/docker
  tar -xzf /tmp/medallion_docker.tgz -C .deploy/docker && rm -f /tmp/medallion_docker.tgz
  "${DOCKER[@]}" build -t "$IMAGE" -f .deploy/docker/Dockerfile .deploy/docker
fi

echo ">> Validasi DAG baru (container sekali-pakai, belum menyentuh Airflow yang jalan)"
"${DOCKER[@]}" run --rm --entrypoint python -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$PWD/.deploy/dags:/opt/airflow/dags:ro" -w /opt/airflow/dags \
  "$IMAGE" -m medallion.validate --dagbag

echo ">> Sync ke dags/"
# Update isi folder IN PLACE. Jangan hapus/ganti folder dags-nya: bind mount container
# menempel ke folder lama, jadi container tidak akan melihat folder pengganti.
rsync -a --delete --exclude='__pycache__' .deploy/dags/ dags/
echo "   $(find dags -name '*.py' | wc -l) file .py, $(find dags/configs -name '*.yml' | wc -l) config, $(find dags/sql -name '*.sql' | wc -l) SQL"

if [[ "$WITH_IMAGE" == "true" ]]; then
  grep -q "^AIRFLOW_IMAGE_NAME=$IMAGE" .env 2>/dev/null || echo "!! Pastikan .env berisi AIRFLOW_IMAGE_NAME=$IMAGE"
  "${DOCKER[@]}" compose up -d
fi
echo "OK. DAG processor akan membaca perubahan dalam ~1 menit."
