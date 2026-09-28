# pb-airflow — Runtime Airflow di VM

Repo ini hanya berisi **setup Airflow**: image, docker-compose, dan library
tambahan yang dibutuhkan task (ODBC Driver 18, venv dlt). DAG, config pipeline,
dan SQL ada di repo **pb-dwh**, yang men-sync DAG-nya sendiri ke
`/data/airflow/dags`.

| Repo | Isi | Deploy |
|---|---|---|
| pb-airflow (ini) | Image, compose, `.env`, library | `scripts/deploy.sh`: VM `git pull`, `--rebuild` kalau image berubah |
| pb-dwh | DAG, config YAML, SQL | `scripts/deploy.sh` di pb-dwh: rsync ke `/data/airflow/dags` |

Airflow 3.3.2, LocalExecutor, metadata DB Postgres, semuanya container di satu
VM Compute Engine.

---

## Isi repo

```
docker/
  Dockerfile              Airflow 3.3.2 + ODBC Driver 18 + venv dlt (/opt/dlt-venv)
  docker-compose.yml      LocalExecutor + Postgres, volume ke /data
  .env.example            kunci & login UI; salin ke docker/.env di VM
  requirements-dlt.in     dependency venv dlt (yang diedit)
  requirements-dlt.txt    lock hasil compile (jangan diedit manual)
scripts/deploy.sh         sinkronkan VM dengan branch di GitHub
```

Kenapa image custom: image resmi `apache/airflow` tidak membawa ODBC Driver 18
(paket apt), dan dlt bentrok dengan dependency yang dikunci Airflow. Jadi dlt
dipasang di venv terpisah `/opt/dlt-venv`, dipanggil DAG pb-dwh lewat
`ExternalPythonOperator`.

## Layout di VM

| Path | Isi |
|---|---|
| `/data/airflow/repo` | clone repo ini; compose dijalankan dari `docker/` |
| `/data/airflow/dags` | DAG dari pb-dwh |
| `/data/airflow/{logs,config,plugins}` | milik Airflow |
| `/data/dlt` | state & file kerja dlt (`DLT_DATA_DIR`) |

Semuanya di disk data `/data`, bukan boot disk.

---

## Setup pertama

```bash
sudo git clone <url-repo> /data/airflow/repo

# Folder harus milik AIRFLOW_UID (default 50000). airflow-init hanya membuat
# folder yang belum ada, tidak mengubah pemiliknya.
sudo mkdir -p /data/airflow/{dags,logs,config,plugins} /data/dlt
sudo chown -R 50000:0 /data/airflow /data/dlt

cd /data/airflow/repo/docker
sudo cp .env.example .env      # isi semua nilainya, ikuti komentar di file
sudo docker compose up -d --build
```

Setelah Airflow jalan:

1. Deploy DAG dari repo pb-dwh (`./scripts/deploy.sh` di pb-dwh).
2. Buat Airflow Connection ke database sumber, ikuti README pb-dwh.
3. Unpause DAG (DAG baru selalu ter-pause: `DAGS_ARE_PAUSED_AT_CREATION`).

Simpan salinan `AIRFLOW__CORE__FERNET_KEY` di luar VM. Kalau hilang, semua
Connection yang tersimpan tidak bisa didekripsi.

## Deploy perubahan

Dari laptop (Git Bash di Windows, atau terminal biasa):

```bash
scripts/deploy.sh --dry-run   # cek saja: working tree bersih, sudah di-push
scripts/deploy.sh             # VM git pull ke branch aktif
scripts/deploy.sh --push      # git push dulu, lalu deploy
scripts/deploy.sh --rebuild   # + docker compose up -d --build
```

Yang sampai ke VM hanya commit yang sudah ada di GitHub. Script menolak kalau
ada perubahan lokal yang belum di-commit, ada file yang diubah langsung di VM,
atau branch di VM punya commit yang tidak ada di GitHub. `docker/.env` tidak
pernah ikut; isinya diatur langsung di VM.

`--rebuild` wajib kalau `Dockerfile`, `docker-compose.yml`, atau
`requirements-dlt.txt` berubah. Script memberi peringatan kalau lupa. Setelah
deploy, script mem-parse ulang DAG dan menampilkan import error, karena image
baru bisa membuat DAG pb-dwh gagal di-import.

## Mengubah versi

**dlt atau driver database**

1. Edit `docker/requirements-dlt.in`.
2. Compile ulang lock:
   `uv pip compile docker/requirements-dlt.in --python-version 3.12 -o docker/requirements-dlt.txt`
3. Samakan versinya di `requirements-dev.txt` repo pb-dwh.
4. Commit, lalu `scripts/deploy.sh --rebuild`.

Hanya paket `dlt` (Apache 2.0). Jangan pasang `dlthub`, itu produk berbayar
yang berbeda.

**Airflow**

Ganti tag `FROM` di `Dockerfile` dan `image:` di `docker-compose.yml`. Tag
`-python3.12` wajib dipertahankan karena `requirements-dlt.txt` di-lock untuk
Python 3.12. Cek dulu DAG pb-dwh di versi baru sebelum deploy.

---

## Troubleshoot

### `ValueError: The value api_auth/jwt_secret must be set!`

Hampir selalu penulisan `docker/.env`:

- tulis polos: `KEY=abc123`
- **jangan** pakai tanda kutip: `KEY="abc123"`
- **jangan** ada spasi di sekitar `=`
- **jangan** ada karakter `#` di dalam nilai (dianggap awal komentar)

### `Can't open lib 'ODBC Driver 18 for SQL Server'`

Container masih memakai image lama.

```bash
sudo docker compose build --no-cache && sudo docker compose up -d
sudo docker compose run --rm airflow-cli odbcinst -q -d
```

### Task mati tanpa traceback (OOM)

Dengan LocalExecutor, task jalan sebagai subprocess di container scheduler.
`mem_limit: 10g` scheduler dihitung untuk 4 slot sekaligus
(`AIRFLOW__CORE__PARALLELISM: '4'`). Cek:

```bash
sudo docker inspect --format '{{.State.OOMKilled}}' $(sudo docker compose ps -q airflow-scheduler)
```

Yang dicoba berurutan: kecilkan `chunk_size` tabelnya di config pb-dwh, buang
kolom berat (`excluded_columns`), lalu kecilkan `PARALLELISM` di compose.

### Disk penuh

```bash
df -h /data
du -sh /data/airflow/logs /data/dlt
```

Log Airflow tidak dibersihkan otomatis. Pasang cron di VM:

```bash
0 3 * * * find /data/airflow/logs -type f -mtime +30 -delete
0 3 * * * find /data/airflow/logs -type d -empty -delete
```

`/data/dlt` berisi folder pipeline dlt per tabel (state + paket load). File yang
sudah ter-load dihapus otomatis. State incremental juga tersimpan di BigQuery
(`_dlt_pipeline_state`) dan dipulihkan dlt kalau folder lokalnya hilang, jadi
kalau terpaksa, folder ini boleh dikosongkan saat tidak ada DAG yang jalan.
