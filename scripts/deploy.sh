#!/usr/bin/env bash
# Sinkronkan VM Airflow dengan branch yang sudah di-push ke GitHub.
#
#   scripts/deploy.sh                 deploy branch aktif
#   scripts/deploy.sh --push          git push dulu, lalu deploy
#   scripts/deploy.sh --branch main   deploy branch lain
#   scripts/deploy.sh --rebuild       sekalian docker compose up -d --build
#   scripts/deploy.sh --dry-run       cek lokal saja, VM tidak disentuh
#
# Yang sampai ke VM hanya commit yang sudah ada di GitHub, bukan file di
# laptop: VM menarik sendiri lewat git. Jadi isi VM selalu bisa ditelusuri ke
# satu commit, dan dua orang yang deploy tidak saling menimpa file.
#
# docker/.env (rahasia) TIDAK ikut. Isinya diatur langsung di VM.
#
# Butuh git, gcloud yang sudah login ke project, dan hak SSH ke VM. Saat
# pertama kali, gcloud membuat SSH key dan menambahkannya ke metadata VM.
#
# Setelan bisa ditimpa lewat env var:
#   PB_DWH_VM        (airflow-prod)
#   PB_DWH_ZONE      (asia-southeast2-c)
#   PB_DWH_PROJECT   (smi-dwh)
#   PB_DWH_REPO_DIR  (kosong = dicari dari stack Compose yang sedang jalan)
#   PB_DWH_VM_USER   (kosong = user default gcloud)

set -euo pipefail

VM=${PB_DWH_VM:-airflow-prod}
ZONE=${PB_DWH_ZONE:-asia-southeast2-c}
PROJECT=${PB_DWH_PROJECT:-smi-dwh}
REPO_DIR=${PB_DWH_REPO_DIR:-}
VM_USER=${PB_DWH_VM_USER:-}

BRANCH=""
PUSH=0
REBUILD=0
DRY_RUN=0

die() { printf '\nGAGAL: %s\n' "$*" >&2; exit 1; }
say() { printf '\n== %s\n' "$*"; }
usage() { sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --branch)
      [ $# -ge 2 ] || die "--branch butuh nama branch"
      BRANCH=$2; shift 2 ;;
    --push) PUSH=1; shift ;;
    --rebuild) REBUILD=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "opsi tidak dikenal: $1" ;;
  esac
done

ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || die "jalankan dari dalam repo pb-dwh"
cd "$ROOT"

CURRENT=$(git rev-parse --abbrev-ref HEAD)
if [ -z "$BRANCH" ]; then
  [ "$CURRENT" != HEAD ] || die "sedang detached HEAD; pakai --branch <nama>"
  BRANCH=$CURRENT
fi
case "$BRANCH" in
  ''|*[!A-Za-z0-9._/-]*) die "nama branch tidak wajar: '$BRANCH'" ;;
esac

# ---------------------------------------------------------------------------
# 1. Cek lokal: yang akan di-deploy harus sama dengan yang sudah dites.
# ---------------------------------------------------------------------------

say "Cek lokal: $BRANCH"
if [ "$BRANCH" = "$CURRENT" ]; then
  git diff --quiet && git diff --cached --quiet \
    || die "ada perubahan yang belum di-commit. VM hanya menerima commit, jadi commit dulu (atau git stash)."

  BELUM=$(git ls-files --others --exclude-standard -- config dags src sql .dlt docker)
  [ -z "$BELUM" ] || die "file baru ini belum di-commit, jadi tidak akan sampai ke VM:
$BELUM"

  PY=""
  for kandidat in .venv/Scripts/python.exe .venv/bin/python; do
    if [ -x "$kandidat" ]; then PY=$kandidat; break; fi
  done
  if [ -n "$PY" ]; then
    "$PY" -m pytest -q || die "test gagal; perbaiki dulu sebelum deploy."
    PYTHONPATH=src "$PY" -m pb_dwh.cli validate || die "config tidak sah; perbaiki dulu sebelum deploy."
  else
    echo "Tidak ada .venv, jadi pytest dan validate dilewati. Config tetap divalidasi di VM."
  fi
else
  echo "Branch $BRANCH bukan branch aktif ($CURRENT): cek working tree dan test dilewati."
fi

# ---------------------------------------------------------------------------
# 2. Pastikan commit-nya sudah ada di GitHub.
# ---------------------------------------------------------------------------

say "Cek GitHub: origin/$BRANCH"
git fetch --quiet origin "+refs/heads/$BRANCH:refs/remotes/origin/$BRANCH" 2>/dev/null || true

# Perbandingan dengan branch lokal hanya berarti untuk branch aktif, karena
# hanya itu yang baru saja dites. Branch lain di-deploy apa adanya dari GitHub.
AHEAD=0
BEHIND=0
if ! git rev-parse --verify --quiet "refs/remotes/origin/$BRANCH" >/dev/null; then
  AHEAD=belum-ada
elif [ "$BRANCH" = "$CURRENT" ]; then
  read -r BEHIND AHEAD < <(git rev-list --left-right --count "origin/$BRANCH...$BRANCH")
fi

if [ "$AHEAD" != 0 ]; then
  if [ "$AHEAD" = belum-ada ]; then
    alasan="branch $BRANCH belum ada di GitHub"
  else
    alasan="$AHEAD commit lokal belum di-push"
  fi
  if [ "$PUSH" != 1 ]; then
    die "$alasan. Jalankan ulang dengan --push, atau git push dulu."
  fi
  [ "$BRANCH" = "$CURRENT" ] || die "--push hanya untuk branch aktif ($CURRENT)."
  if [ "$DRY_RUN" = 1 ]; then
    echo "$alasan; akan di-push (dilewati karena --dry-run)."
  else
    echo "$alasan; push sekarang."
    git push -u origin "$BRANCH"
  fi
fi
[ "$BEHIND" = 0 ] \
  || die "branch lokal ketinggalan $BEHIND commit dari origin/$BRANCH. git pull dulu supaya yang dites sama dengan yang di-deploy."

TARGET=$(git rev-parse --short "origin/$BRANCH" 2>/dev/null || git rev-parse --short HEAD)
echo "Akan di-deploy: $BRANCH @ $TARGET"

# ---------------------------------------------------------------------------
# 3. Script yang dijalankan di VM.
# ---------------------------------------------------------------------------

REMOTE=$(cat <<'REMOTE'
set -euo pipefail
say() { printf '\n== [VM] %s\n' "$*"; }
die() { printf '\nGAGAL [VM]: %s\n' "$*" >&2; exit 1; }

if docker info >/dev/null 2>&1; then D="docker"; else D="sudo docker"; fi

# Cari stack Compose yang sedang jalan dan pakai persis itu: salah tebak
# nama project atau file compose berarti menyalakan stack Airflow kedua.
# Lokasi repo juga diambil dari sini (compose ada di <repo>/docker/), supaya
# tidak bergantung pada tempat repo di-clone.
say "Docker Compose"
INFO=$($D ps --filter "label=com.docker.compose.service=airflow-scheduler" \
  --format '{{.Label "com.docker.compose.project"}}|{{.Label "com.docker.compose.project.working_dir"}}|{{.Label "com.docker.compose.project.config_files"}}' \
  | head -n 1)
COMPOSE_ARGS=()
if [ -n "$INFO" ]; then
  PROJ=${INFO%%|*}
  SISA=${INFO#*|}
  WD=${SISA%%|*}
  CONFIG_FILES=${SISA#*|}
  COMPOSE_ARGS+=(-p "$PROJ")
  IFS=, read -r -a DAFTAR_CONFIG <<< "$CONFIG_FILES"
  for berkas in "${DAFTAR_CONFIG[@]}"; do
    COMPOSE_ARGS+=(-f "$berkas")
  done
  echo "Project '$PROJ', compose: $CONFIG_FILES"
  REPO=${REPO:-$(dirname "$WD")}
else
  [ -n "$REPO" ] || die "stack Airflow tidak sedang jalan, jadi lokasi repo tidak bisa ditebak. Set PB_DWH_REPO_DIR."
  WD="$REPO/docker"
  echo "Container airflow-scheduler tidak sedang jalan; memakai $WD."
fi
# Membaca daftar container cukup butuh akses ke socket docker, tapi compose
# juga membaca docker/.env sebagai user yang menjalankannya -- dan file itu
# biasanya hanya bisa dibaca pemilik repo.
if [ "$D" = docker ] && [ -e "$WD/.env" ] && [ ! -r "$WD/.env" ]; then
  D="sudo docker"
fi
c() { (cd "$WD" && $D compose "${COMPOSE_ARGS[@]}" "$@"); }

[ -d "$REPO/.git" ] || die "repo git tidak ditemukan di $REPO. Set PB_DWH_REPO_DIR ke lokasi repo di VM."

# git dijalankan sebagai pemilik repo: SSH key / kredensial GitHub ada di akun
# itu, dan git menolak repo milik user lain ("dubious ownership"). Kalau
# pemiliknya UID tanpa akun di VM (mis. 50000 milik container), git jalan
# sebagai root dan kepemilikan dikembalikan sesudahnya.
UID_PEMILIK=$(stat -c %u "$REPO")
PEMILIK=$(stat -c %u:%g "$REPO")
NAMA_PEMILIK=$(getent passwd "$UID_PEMILIK" | cut -d: -f1 || true)
if [ "$UID_PEMILIK" = "$(id -u)" ]; then
  g() { git -C "$REPO" "$@"; }
elif [ -n "$NAMA_PEMILIK" ]; then
  g() { sudo -u "$NAMA_PEMILIK" -H git -C "$REPO" "$@"; }
else
  g() { sudo -H git -c safe.directory="$REPO" -C "$REPO" "$@"; }
  trap 'sudo chown -R "$PEMILIK" "$REPO"' EXIT
fi

say "Repo $REPO (pemilik ${NAMA_PEMILIK:-$PEMILIK})"
DIUBAH=$(g status --porcelain --untracked-files=no)
[ -z "$DIUBAH" ] || die "ada file yang diubah langsung di VM; deploy dibatalkan supaya tidak tertimpa:
$DIUBAH"

LAMA=$(g rev-parse HEAD)
BRANCH_LAMA=$(g rev-parse --abbrev-ref HEAD)
g fetch --quiet origin "+refs/heads/$BRANCH:refs/remotes/origin/$BRANCH" \
  || die "VM tidak bisa mengambil $BRANCH dari GitHub. Cek akses repo (SSH key / token) milik ${NAMA_PEMILIK:-pemilik repo}."
if g show-ref --verify --quiet "refs/heads/$BRANCH"; then
  g checkout --quiet "$BRANCH"
else
  g checkout --quiet -b "$BRANCH" "origin/$BRANCH"
fi
g merge --ff-only --quiet "origin/$BRANCH" \
  || die "branch $BRANCH di VM punya commit yang tidak ada di GitHub; rapikan manual di $REPO"
BARU=$(g rev-parse HEAD)

if [ "$LAMA" = "$BARU" ]; then
  echo "Sudah paling baru: $BRANCH @ ${BARU:0:7}"
else
  echo "$BRANCH_LAMA @ ${LAMA:0:7}  ->  $BRANCH @ ${BARU:0:7}"
  g --no-pager log --oneline --no-decorate "$LAMA..$BARU" | head -n 20
  g --no-pager diff --stat "$LAMA" "$BARU" | tail -n 25
fi
BERUBAH=$(g diff --name-only "$LAMA" "$BARU")
BALIK="untuk kembali ke versi sebelumnya: git -C $REPO checkout ${LAMA:0:7}"

if [ "$REBUILD" = 1 ]; then
  say "docker compose up -d --build"
  c up -d --build
elif printf '%s\n' "$BERUBAH" | grep -qxE 'docker/(Dockerfile|docker-compose\.yml)'; then
  # Container yang jalan masih memakai compose/image lama, jadi validasi di
  # dalamnya tidak berarti apa-apa. Berhenti di sini dengan instruksi jelas.
  die "docker/Dockerfile atau docker-compose.yml ikut berubah. Kode sudah ditarik, tapi container masih versi lama. Jalankan ulang: scripts/deploy.sh --rebuild"
else
  echo "Image dan compose tidak berubah; container tidak perlu di-restart."
fi

say "Validasi config di container"
c exec -T airflow-scheduler python -m pb_dwh.cli validate </dev/null \
  || die "config ditolak di VM (lihat pesan di atas). $BALIK"

say "Parse ulang DAG"
c exec -T airflow-scheduler airflow dags reserialize </dev/null >/dev/null 2>&1 \
  || echo "(reserialize dilewati; DAG processor tetap mem-parse ulang sendiri dalam beberapa puluh detik)"
c exec -T airflow-scheduler airflow dags list-import-errors </dev/null || true
RUSAK=$(c exec -T airflow-scheduler airflow dags list -o plain </dev/null 2>/dev/null \
  | grep -o 'bronze_ingest_RUSAK[A-Za-z0-9_]*' | sort -u || true)

say "Selesai: VM di $BRANCH @ ${BARU:0:7}"
if [ -n "$RUSAK" ]; then
  echo "PERHATIAN: ada DAG rusak, buka tab Docs-nya di Airflow UI:"
  echo "$RUSAK"
  exit 1
fi
REMOTE
)

# Script dikirim sebagai base64 satu baris: gcloud di Windows memotong argumen
# multi-baris dan mengacak tanda kutip.
PAYLOAD=$(printf 'BRANCH=%q\nREPO=%q\nREBUILD=%q\n%s\n' "$BRANCH" "$REPO_DIR" "$REBUILD" "$REMOTE")
B64=$(printf '%s' "$PAYLOAD" | base64 | tr -d '\n')
CMD="f=\$(mktemp); echo $B64 | base64 -d > \$f; bash \$f; rc=\$?; rm -f \$f; exit \$rc"

HOST=${VM_USER:+$VM_USER@}$VM

if [ "$DRY_RUN" = 1 ]; then
  say "Dry run"
  echo "Repo di VM: ${REPO_DIR:-otomatis, dari stack Compose yang sedang jalan}"
  echo "Akan menjalankan: gcloud compute ssh $HOST --zone $ZONE --project $PROJECT --command <script ${#CMD} karakter>"
  echo "VM tidak disentuh."
  exit 0
fi

say "Deploy ke $VM ($ZONE)"
# Semua path (REPO_DIR dll.) ada di dalam base64, bukan di argumen: Git Bash di
# Windows mengubah argumen yang diawali "/" menjadi path Windows sebelum sampai
# ke gcloud. Konversi itu jangan dimatikan dengan MSYS_NO_PATHCONV -- wrapper
# gcloud di Git Bash justru bergantung padanya.
gcloud compute ssh "$HOST" --zone "$ZONE" --project "$PROJECT" --command "$CMD"
