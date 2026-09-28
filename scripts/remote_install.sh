#!/usr/bin/env bash
# Dijalankan DI VM oleh deploy.sh / deploy.ps1 (jangan dijalankan manual di laptop).
# Argumen: <DAGS_DIR> <DOCKER_CMD...>
#
# Alur aman: extract ke folder staging -> validasi DAG di container sekali-pakai
#            -> kalau lolos baru sync ke DAGS_DIR yang dipakai Airflow.
# Image & docker-compose Airflow dikelola repo pb-airflow; script ini tidak menyentuhnya.
set -euo pipefail
DAGS_DIR="$1"; shift
DOCKER=("$@"); [[ ${#DOCKER[@]} -eq 0 ]] && DOCKER=(docker)

command -v rsync >/dev/null || { echo "rsync belum ada di VM: sudo apt-get install -y rsync"; exit 1; }
[[ -d "$DAGS_DIR" ]] || { echo "$DAGS_DIR tidak ada. Setup VM dulu lewat repo pb-airflow."; exit 1; }

# Validasi pakai image yang SEDANG dipakai scheduler: Airflow & venv dlt-nya sama persis.
IMAGE=$("${DOCKER[@]}" ps --filter label=com.docker.compose.service=airflow-scheduler --format '{{.Image}}' | head -n1)
[[ -n "$IMAGE" ]] || { echo "Container airflow-scheduler tidak jalan (pb-airflow: docker compose up -d)."; exit 1; }

STAGE=~/.pb-dwh-deploy/dags
rm -rf "$STAGE" && mkdir -p "$STAGE"
tar -xzf /tmp/medallion_dags.tgz -C "$STAGE" && rm -f /tmp/medallion_dags.tgz

echo ">> Validasi DAG baru dengan $IMAGE (container sekali-pakai, belum menyentuh Airflow yang jalan)"
"${DOCKER[@]}" run --rm --entrypoint python -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$STAGE:/opt/airflow/dags:ro" -w /opt/airflow/dags \
  "$IMAGE" -m medallion.validate --dagbag

echo ">> Sync ke $DAGS_DIR"
# Update isi folder IN PLACE. Jangan hapus/ganti folder dags-nya: bind mount container
# menempel ke folder lama, jadi container tidak akan melihat folder pengganti.
# Folder milik AIRFLOW_UID (pb-airflow) -> pakai sudo kalau user SSH tidak bisa menulis.
if [[ -w "$DAGS_DIR" ]]; then
  rsync -a --delete --exclude='__pycache__' "$STAGE/" "$DAGS_DIR/"
else
  sudo rsync -a --delete --exclude='__pycache__' --chown="$(stat -c %u:%g "$DAGS_DIR")" "$STAGE/" "$DAGS_DIR/"
fi
echo "   $(find "$DAGS_DIR" -name '*.py' | wc -l) file .py, $(find "$DAGS_DIR/configs" -name '*.yml' | wc -l) config, $(find "$DAGS_DIR/sql" -name '*.sql' | wc -l) SQL"
echo "OK. DAG processor akan membaca perubahan dalam ~1 menit."
