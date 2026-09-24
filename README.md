# pb-dwh — Ingestion SQL Server ke BigQuery

Pipeline harian yang menarik data dari Cloud SQL for SQL Server ke BigQuery
layer bronze, lalu mentransformasinya ke silver.

Alur datanya:

```
SQL Server  --(dlt, PyArrow)-->  Parquet di GCS  --(BigQuery load job)-->  bronze
                                                                             |
                                                        (BigQueryInsertJobOperator)
                                                                             v
                                                                           silver
```

Dijadwalkan oleh Apache Airflow 3.1.6 yang berjalan di VM GCP dengan Docker
Compose.

**Prinsip utama: menambah tabel tidak perlu menyentuh kode Python.** Semua
diatur dari file YAML di `config/sources/`.

---

## Daftar isi

- [Struktur project](#struktur-project)
- [Menambah tabel baru](#menambah-tabel-baru)
- [Pilihan setelan tabel](#pilihan-setelan-tabel)
- [Menjalankan di laptop untuk testing](#menjalankan-di-laptop-untuk-testing)
- [Setup ODBC Driver 18](#setup-odbc-driver-18)
- [Deploy ke VM](#deploy-ke-vm)
- [Menambah transform silver](#menambah-transform-silver)
- [Troubleshoot](#troubleshoot)
- [Catatan teknis](#catatan-teknis)

---

## Struktur project

```
config/
  settings.yml              default global untuk semua source
  sources/*.yml             satu file = satu database = satu DAG   <-- SERING DIEDIT
sql/
  silver/*.sql              query transform, satu file = satu task <-- SERING DIEDIT
dags/
  bronze_ingest.py          baca config, bikin DAG
  _factory.py               isi DAG-nya
  silver_transform.py       DAG transform
src/pb_dwh/
  config/                   baca & validasi YAML
  sources/mssql.py          bangun dlt source dari config
  sources/types.py          penyesuaian tipe SQL Server
  state/incremental.py      cursor & lookback
  sinks/bigquery.py         pipeline dlt -> GCS -> BigQuery
  bronze/runner.py          perekat; dipakai DAG maupun CLI
  cli.py                    perkakas testing lokal
docker/                     Dockerfile, compose, contoh .env
tests/                      test config, tanpa perlu DB
.dlt/config.toml            setelan memori & rotasi file dlt
```

Yang sehari-hari diedit hanya dua baris pertama. Sisanya jarang berubah.

---

## Menambah tabel baru

Buka file source yang sesuai di `config/sources/`, lalu tambahkan satu entri di
bawah `tables:`.

```yaml
  - name: nama_tabel_di_sql_server
    primary_key: [kolom_id]
    cursor: modified_at
    lookback_days: 2
```

Lalu periksa dan deploy:

```bash
python -m pb_dwh.cli validate          # pastikan config sah
git add config/ && git commit -m "Tambah tabel nama_tabel"
git push
```

Di VM:

```bash
cd /data/airflow/repo && git pull
```

Airflow memungut perubahannya sendiri dalam waktu kurang dari satu menit. Tidak
perlu restart apa pun.

### Bagaimana memilih setelannya

| Kondisi tabel di SQL Server | Setelan yang dipakai |
|---|---|
| Ada kolom waktu yang ikut berubah saat baris di-update | `cursor: <kolom>` + `lookback_days` |
| Hanya bertambah, tidak pernah di-update (log, transaksi) | `cursor: <kolom_id>` + `cursor_type: integer` + `lookback` |
| Kecil (< 100 ribu baris) dan barisnya bisa dihapus | `write_disposition: replace`, tanpa cursor |
| Tidak ada kolom waktu maupun id urut | `write_disposition: replace`, tanpa cursor |

Kalau ragu, mulai dari `replace`. Itu selalu benar, hanya lebih boros. Pindah ke
incremental setelah tahu kolom mana yang bisa dipercaya.

### Kenapa butuh `lookback_days`

Jam di server aplikasi dan jam di database tidak selalu sama persis, dan ada
transaksi yang commit-nya telat beberapa detik setelah `modified_at` diisi.
Kalau kita hanya mengambil baris yang lebih baru dari nilai terakhir, baris
seperti itu terlewat selamanya tanpa ada error apa pun.

`lookback_days: 2` membuat pipeline selalu mundur 2 hari. Baris yang sama
memang terambil berulang kali — dan itu tidak masalah, karena
`write_disposition: merge` dengan `primary_key` akan menimpanya, bukan
menduplikasinya.

---

## Pilihan setelan tabel

| Kunci | Wajib? | Arti |
|---|---|---|
| `name` | ya | Nama tabel di SQL Server |
| `primary_key` | ya kalau `merge` | Kolom kunci. Boleh satu (`id`) atau gabungan (`[a, b]`) |
| `cursor` | tidak | Kolom penanda perubahan |
| `cursor_type` | tidak | `datetime` (default) atau `integer` |
| `lookback_days` | tidak | Mundur berapa hari; hanya untuk `cursor_type: datetime` |
| `lookback` | tidak | Mundur berapa nilai cursor; hanya untuk `cursor_type: integer` |
| `initial_value` | tidak | Titik mulai saat run pertama. Berguna agar tidak menarik data 10 tahun ke belakang |
| `write_disposition` | tidak | `merge` (default), `replace`, atau `append` |
| `exclude_columns` | tidak | Kolom yang tidak ikut, mis. `varbinary(max)` |
| `column_overrides` | tidak | Paksa tipe kolom tertentu |
| `chunk_size` | tidak | Baris per batch; kecilkan untuk tabel sangat lebar |
| `reflection_level` | tidak | `minimal` (default), `full`, `full_with_precision` |

Config divalidasi sebelum dipakai. Kombinasi yang berbahaya ditolak dengan
pesan yang menyebut nama file dan tabelnya, misalnya `merge` tanpa
`primary_key` — yang kalau dibiarkan akan menumpuk duplikat setiap hari tanpa
satu pun pesan error.

---

## Menjalankan di laptop untuk testing

### Sekali saja: siapkan environment

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
```

Butuh Python 3.10 atau lebih baru, plus ODBC Driver 18 (lihat bagian
berikutnya).

### Sekali saja: kredensial

```bash
cp docker/.env.example .env.local
# isi MSSQL_*, GCP_PROJECT_ID, GCS_STAGING_BUCKET, BQ_*

gcloud auth application-default login    # untuk akses BigQuery & GCS
```

Muat isinya ke shell:

```bash
set -a && source .env.local && set +a
```

### Perintah sehari-hari

```bash
# 1. Periksa semua config (tidak menyentuh database sama sekali)
python -m pb_dwh.cli validate

# 2. Lihat daftar tabel beserta setelannya
python -m pb_dwh.cli list --source erp_mssql

# 3. Uji koneksi ODBC dan cek semua tabel di config memang ada
python -m pb_dwh.cli check-connection --source erp_mssql

# 4. Jalankan SATU tabel ke dataset percobaan
python -m pb_dwh.cli run --source erp_mssql --table sales_order --dev

# 5. Jalankan semua tabel (hati-hati, ini berat)
python -m pb_dwh.cli run --source erp_mssql --dev
```

**Selalu pakai `--dev` saat testing.** Tanpa flag itu, data ditulis ke dataset
bronze yang asli. Dengan `--dev`, dlt menambahkan sufiks pada nama dataset
sehingga percobaan tidak menimpa apa pun.

Jalankan test:

```bash
pytest
```

Test hanya memeriksa pembacaan dan validasi config, jadi cepat dan tidak butuh
database.

---

## Setup ODBC Driver 18

### Di VM (sudah otomatis)

Sudah terpasang di dalam image lewat `docker/Dockerfile`. Build akan berhenti
kalau drivernya tidak terdaftar, jadi tidak bisa lolos diam-diam.

### Di laptop Windows

Unduh dan pasang **Microsoft ODBC Driver 18 for SQL Server** dari situs
Microsoft. Verifikasi:

```powershell
Get-OdbcDriver -Name "ODBC Driver 18*"
```

### Di laptop macOS

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18
```

### Di laptop Linux (Debian/Ubuntu)

```bash
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
  | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc > /dev/null
curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
```

Verifikasi di mana pun:

```bash
odbcinst -q -d -n "ODBC Driver 18 for SQL Server"
```

### Catatan enkripsi

Driver 18 memaksa `Encrypt=yes` secara default, dan sertifikat Cloud SQL tidak
ada di trust store container. Karena itu koneksi memakai
`TrustServerCertificate=yes`. Kalau nanti CA Cloud SQL dipasang ke container,
set `MSSQL_TRUST_SERVER_CERTIFICATE=no` di `.env`.

---

## Deploy ke VM

Repo di-clone ke `/data/airflow/repo` dan di-mount ke `/opt/pb-dwh` di dalam
container. Isi repo-nya langsung dibaca Airflow, jadi deploy = `git pull`.

Pertama kali:

```bash
sudo git clone <url-repo> /data/airflow/repo
sudo chown -R 50000:0 /data/airflow/repo
cd /opt/pb-dwh/docker    # atau lokasi compose di VM
docker compose up -d
```

Selanjutnya, untuk setiap perubahan config, SQL, atau kode Python:

```bash
cd /data/airflow/repo && git pull
```

Rebuild image **hanya** perlu kalau versi paket di `docker/Dockerfile` berubah:

```bash
docker compose build && docker compose up -d
```

---

## Menambah transform silver

Buat file baru di `sql/silver/`, diberi awalan angka untuk menentukan urutan:

```
sql/silver/10_dim_product.sql
sql/silver/20_fct_sales_order.sql
```

Satu file menjadi satu task, dijalankan berurutan sesuai nama file. Tidak ada
kode Python yang perlu diubah.

Variabel yang tersedia di dalam file SQL:

| Variabel | Isi |
|---|---|
| `{{ params.project }}` | `GCP_PROJECT_ID` |
| `{{ params.bronze }}` | `BQ_DATASET_BRONZE` |
| `{{ params.silver }}` | `BQ_DATASET_SILVER` |
| `{{ ds }}` | Tanggal logical date, `YYYY-MM-DD` |

Lihat `sql/silver/10_contoh_dim_product.sql` sebagai template. **File itu masih
contoh** dan perlu diganti dengan query yang sebenarnya.

---

## Troubleshoot

### Semua log dlt muncul sebagai ERROR di UI Airflow

Ini bug kosmetik yang sudah diketahui di dlt dengan Airflow 3
([dlt#4462](https://github.com/dlt-hub/dlt/issues/4462)): supervisor Airflow 3
menandai semua keluaran stderr sebagai ERROR, sehingga log INFO dari dlt ikut
merah. **Task yang benar-benar gagal ditandai merah di grid view**, bukan di
warna teks log. Selama task-nya hijau, abaikan warna lognya.

### `Can't open lib 'ODBC Driver 18 for SQL Server'`

Driver belum terpasang di tempat kode dijalankan.

- Di VM: `docker compose build --no-cache` lalu cek
  `docker compose run --rm airflow-cli odbcinst -q -d`
- Di laptop: lihat [Setup ODBC Driver 18](#setup-odbc-driver-18)

### `Login failed for user` padahal password benar

Hampir selalu karena karakter khusus di password. Kode ini merakit connection
string lewat `sqlalchemy.engine.URL.create`, yang sudah meng-escape dengan
benar — jadi kalau tetap gagal, periksa cara penulisan di `.env`:

- tulis polos: `MSSQL_PASSWORD=abc123`
- **jangan** pakai tanda kutip: `MSSQL_PASSWORD="abc123"`
- **jangan** ada spasi di sekitar `=`
- **jangan** ada karakter `#` di dalam nilai (dianggap awal komentar)

### Task ingestion dimatikan karena kehabisan memori

Gejalanya: task mati tanpa traceback, `docker compose logs airflow-scheduler`
menunjukkan OOM kill.

Urutan yang dicoba:

1. Kecilkan `chunk_size` tabel itu di config (mis. `50000` → `10000`)
2. Kecilkan `file_max_bytes` di `.dlt/config.toml` (128 MB → 64 MB)
3. Buang kolom berat dengan `exclude_columns`
4. Kalau masih juga, kecilkan `AIRFLOW__CORE__PARALLELISM` di compose

Pastikan `DLT_DATA_DIR` menunjuk ke `/opt/dlt` (disk data), bukan temp folder
sistem. Kalau tidak, file kerja dlt menumpuk di boot disk 20 GB dan penuh di
run pertama.

### Disk penuh

```bash
df -h /data
du -sh /data/airflow/logs /data/dlt
```

Log Airflow tidak dibersihkan otomatis. Pasang cron:

```bash
0 3 * * * find /data/airflow/logs -type f -mtime +30 -delete
0 3 * * * find /data/airflow/logs -type d -empty -delete
```

File kerja dlt dihapus otomatis setelah task selesai (`wipe_local_data=True`).
Kalau ada sisa, biasanya dari task yang dimatikan paksa — aman dihapus manual
saat tidak ada DAG yang jalan.

### DAG muncul bernama `bronze_ingest_RUSAK_...`

Config YAML-nya tidak bisa dibaca. Buka tab **Docs** pada DAG itu; pesan
lengkapnya ada di sana. Ini disengaja: config rusak membuat DAG yang gagal
dengan jelas, bukan DAG yang hilang diam-diam.

Periksa di laptop sebelum push:

```bash
python -m pb_dwh.cli validate
```

### Kolom decimal mendarat sebagai BIGNUMERIC

Disengaja. BigQuery `NUMERIC` hanya menampung presisi 38 dengan skala 9. Kolom
yang skalanya lebih dari 9 otomatis jadi `BIGNUMERIC` agar tidak ada digit yang
hilang. Kalau layer silver cukup dengan `NUMERIC`, lakukan `CAST` di query
silver, bukan di bronze — bronze menyimpan apa adanya.

### Kolom `datetime2` kehilangan digit terakhir

`datetime2(7)` di SQL Server berpresisi 100 nanodetik, sedangkan BigQuery
`TIMESTAMP` hanya mikrodetik. Digit terakhir memang terpotong. Ini diterima apa
adanya, bukan bug. Kalau presisi penuh benar-benar dibutuhkan, simpan kolom itu
sebagai `STRING` lewat `column_overrides`.

### `column_overrides` tidak berpengaruh

Karena `defer_table_reflect=True`, schema diputuskan saat task berjalan dan bisa
menimpa hint kolom. Naikkan `reflection_level` tabel itu menjadi
`full_with_precision`.

### Baris yang dihapus di SQL Server masih ada di BigQuery

`merge` hanya menangani baris baru dan baris yang berubah; penghapusan tidak
terdeteksi. Untuk tabel yang barisnya bisa dihapus, pakai
`write_disposition: replace`. Untuk tabel besar yang butuh deteksi penghapusan,
itu memerlukan CDC dan belum dibangun di sini.

---

## Catatan teknis

Beberapa keputusan yang tidak terlihat jelas dari kode, dan alasannya.

**`table_names` diisi eksplisit, bukan `.with_resources()`.** dlt hanya akan
me-reflect tabel yang disebut, bukan seluruh isi schema. Untuk database dengan
ratusan tabel, ini perbedaan antara reflection beberapa detik dan beberapa
menit.

**`defer_table_reflect=True`.** Reflection baru terjadi saat task berjalan.
Tanpa ini, DAG processor akan membuka koneksi ke SQL Server setiap kali DAG
di-parse — untuk 100 tabel, setiap 30 detik, selamanya.

**`decompose="serialize"`.** Satu task per tabel, dijalankan berurutan.
Berurutan itu disengaja: dua tabel besar yang diekstrak bersamaan adalah cara
tercepat menghabiskan RAM 20 GB. Paralelisme diatur antar-DAG lewat pool
`dlt_ingest`, bukan di dalam satu DAG.

**`backend="pyarrow"`, bukan `connectorx`.** Jalur Arrow → Parquet membuat tahap
normalize dlt hampir tanpa biaya, dan itu yang paling menentukan hemat memori.

**Ada `staging`, jadi BigQuery memakai load job.** Bukan streaming insert. Lebih
murah, tidak kena kuota streaming, dan data langsung bisa di-query.

**`DLT_PROJECT_DIR` dan `DLT_DATA_DIR` sengaja berbeda.** Yang pertama menunjuk
ke repo (tempat `.dlt/config.toml`), yang kedua ke disk data (tempat file kerja
ditulis). Kalau keduanya disamakan, `.dlt/config.toml` tidak terbaca dan semua
setelan rotasi file diam-diam tidak berlaku.

**Hanya paket `dlt` (Apache 2.0).** Jangan pasang `dlthub`, itu produk berbayar
yang berbeda.

### Menyamakan versi

Versi paket ditulis di dua tempat yang belum disatukan:

- `requirements.txt` — dipakai untuk menjalankan di laptop
- `docker/Dockerfile` — dipakai untuk membangun image VM

Kalau menaikkan versi, ubah keduanya. Menyatukannya (Dockerfile membaca
`requirements.txt`) adalah perbaikan yang layak dilakukan nanti.
