# pb-dwh: SQL Server → GCS → BigQuery (medallion)

DAG, config, dan SQL untuk data warehouse. Airflow-nya (image, compose, `.env`) ada di repo
**pb-airflow**. Isi folder `dags/` di repo ini disalin ke `/data/airflow/dags` di VM.

```
SQL Server (Cloud SQL, project lain)
   │  DAG bronze__<source>: 1 task per tabel, dlt di venv /opt/dlt-venv
   ▼
GCS data lake   gs://<bucket>/<dataset>/<Tabel>/YYYY/MM/DD/<load_id>.<file_id>.parquet   (file lama disimpan)
   │  BigQuery load job (oleh dlt)
   ▼
BigQuery bronze <dataset>.<Tabel>   + _ingested_at (partisi harian), _dlt_load_id
   │  Asset event: DAG transform__<domain> jalan setelah SEMUA tabel bronze-nya ter-load
   ▼
silver_<domain>.*  →  gold_<domain>.*   (SQL biasa, BigQueryInsertJobOperator)
```

## Struktur

```
dags/                         ← SELURUH isi folder ini disalin ke VM; jangan taruh file lain di sini
  bronze__dc_sby.py           ← 1 file per source (2 baris)
  transform__dc_sby.py        ← 1 file per domain (2 baris)
  configs/platform.yml        ← project BigQuery, region, bucket lake, timezone
  configs/sources/*.yml       ← daftar tabel bronze per source
  configs/transforms/*.yml    ← daftar model silver/gold per domain
  sql/{silver,gold}/<domain>/<nama>.sql
  medallion/                  ← framework: config.py, dags.py (Airflow), ingest.py (venv dlt), validate.py
infra/                        ← setup GCP (IAM lintas project, bucket, IP) & login read-only SQL Server
scripts/deploy.sh             ← salin dags/ dari laptop ke VM
tests/
```

## Setup sekali

1. **GCP**: isi variabel di `infra/gcp_setup.sh`, jalankan sebagai admin (Cloud Shell). Isinya: IAM SA
   VM ke project BigQuery, bucket lake, IP VM statis + authorized networks Cloud SQL, cek scope VM.
2. **SQL Server**: buat login read-only `dlt_reader` dengan `infra/sqlserver_readonly_login.sql`.
3. **Airflow Connection** ke instance SQL Server, di VM (atau lewat UI: Admin → Connections).
   Database tidak diisi di sini, tapi di YAML source (`database:`), jadi satu Connection cukup untuk
   semua database di instance yang sama:
   ```bash
   cd /data/airflow/repo/docker
   sudo docker compose exec airflow-scheduler airflow connections add mssql_pb \
     --conn-type generic --conn-host <IP publik Cloud SQL> --conn-port 1433 \
     --conn-login dlt_reader --conn-password '<password>'
   ```
   Password tersimpan terenkripsi (Fernet) dan tampil `***` di UI & log.
4. **Deploy**: `cp scripts/deploy.env.example scripts/deploy.env`, sesuaikan, lalu `scripts/deploy.sh`.
5. **Unpause** `bronze__dc_sby` dan `transform__dc_sby` di UI (DAG baru selalu ter-pause).

## Kerja harian

| Mau | Caranya |
|---|---|
| Tambah tabel | tambah item di `tables` pada `configs/sources/<source>.yml` |
| Tambah source | YAML baru di `configs/sources/`, copy `dags/bronze__dc_sby.py` dan ganti argumennya |
| Tambah model | `sql/<silver\|gold>/<domain>/<nama>.sql` yang menulis ke `<silver\|gold>_<domain>.<nama>`, daftarkan di `configs/transforms/<domain>.yml` |
| Tambah domain | YAML baru di `configs/transforms/`, copy `dags/transform__dc_sby.py` dan ganti argumennya |

Key per tabel:

| Key | Arti |
|---|---|
| `table` | nama tabel di SQL Server, sekaligus nama tabel di BigQuery (wajib) |
| `cursor` | kolom datetime last-modified. Ada = incremental (append). Tidak ada = full refresh (replace) tiap run |
| `initial_value` | incremental: run pertama ambil data sejak nilai ini (default: semua data) |
| `lookback_hours` | incremental: baca ulang N jam terakhir supaya data yang telat tidak terlewat |
| `chunk_size` | baris per batch (default 50000). Kecilkan kalau task kena OOM |
| `excluded_columns` | kolom yang tidak diambil (kolom besar / tipe aneh) |

SQL silver/gold adalah SQL BigQuery biasa, bisa di-copy ke konsol BigQuery. Nama dataset tanpa project
(job jalan di project BigQuery). Semua silver jalan dulu, baru gold.

## Dev lokal (Windows + Git Bash)

```bash
uv venv --python 3.12 && uv pip install -r requirements-dev.txt
source .venv/Scripts/activate
(cd dags && python -m medallion.validate)   # config, file DAG, SQL
pytest                                      # ingest end-to-end sqlite -> lake lokal
```

Airflow tidak bisa di-import di Windows, jadi `tests/test_dags.py` di-skip di laptop.
`scripts/deploy.sh` mem-parse semua DAG di container VM sebelum menyalin, jadi DAG yang rusak
tidak pernah sampai ke Airflow.

## Perlu diketahui

- **Jam WIB tercatat sebagai UTC.** Kolom `datetime` SQL Server (jam dinding WIB, tanpa timezone) masuk
  bronze sebagai `TIMESTAMP` berlabel UTC. Jam WIB-nya: `DATETIME(kolom, 'UTC')`.
- **File lake tidak pernah dihapus** (default dlt justru menghapus file lama per tabel sebelum load).
  Tiap run menambah file, termasuk snapshot tabel full refresh. Biaya storage: pasang lifecycle bucket
  kalau sudah besar.
- **Incremental** memakai state dlt per tabel (tersimpan juga di BigQuery `_dlt_pipeline_state`). Kolom
  cursor harus NOT NULL dan ter-index; baris ber-cursor NULL tidak pernah terambil. Hard delete di
  sumber tidak tertangkap, pakai full refresh untuk tabel yang bisa dihapus barisnya. Bronze incremental
  berisi duplikat dari lookback, silver yang men-dedup (lihat `sql/silver/dc_sby/aim_header.sql`).
- **Nama tabel & kolom** di BigQuery sama persis dengan SQL Server (naming dlt `sql_cs_v1` di
  `medallion/ingest.py`). Jangan diubah setelah ada tabel ter-load.
- **Dataset Datastream lama** (`pb_bronze_dc_sby_dbo`, dibaca view di `pb_original_data_source`) jangan
  dipakai sebagai `dataset` bronze: tabelnya akan tertimpa.
- **Run gagal** di tengah load diselesaikan otomatis oleh run berikutnya sebelum extract data baru.

## Kontrak dengan pb-airflow

| Nilai | pb-airflow | pb-dwh |
|---|---|---|
| Versi dlt & driver | `docker/requirements-dlt.in` | `requirements-dev.txt` |
| Python venv dlt | `/opt/dlt-venv` di `docker/Dockerfile` | `runtime.dlt_python` di `dags/configs/platform.yml` |
| Folder DAG di VM | volume `/data/airflow/dags` di compose | `DAGS_DIR` di `scripts/deploy.sh` (bisa ditimpa di `deploy.env`) |

Ubah satu sisi = ubah sisi lainnya.
