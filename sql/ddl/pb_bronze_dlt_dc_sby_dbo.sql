-- Dataset & tabel bronze untuk config/sources/dc_sby.yml.
--
-- Dibuat MANUAL, sekali, sebelum DAG bronze_ingest_dc_sby dijalankan pertama
-- kali. Jalankan di BigQuery console dengan Query settings -> Location =
-- asia-southeast2, oleh orang yang punya hak membuat dataset. SA Airflow
-- (airflow-pb-dwh) sengaja tidak punya hak itu, dan SA-nya harus sudah dibuat
-- sebelum GRANT di bawah dijalankan.
--
-- Tabel diisi task <tabel>.ke_bq: BigQuery load job dari Parquet yang ditulis
-- dlt ke GCS. Load job memakai CREATE_NEVER, jadi tabel yang belum ada di sini
-- membuat task gagal -- tidak ada tabel yang muncul sendiri.
--
-- Syarat supaya tabel cocok dengan Parquet-nya. Kalau beda, load job gagal:
--   * Nama kolom persis seperti di SQL Server (naming sql_cs_v1).
--   * Kolom datetime SQL Server -> TIMESTAMP, BUKAN DATETIME seperti bronze
--     Datastream. Parquet tulisan dlt menyimpan waktu sebagai timestamp UTC.
--     Nilainya jam WIB apa adanya tapi berlabel UTC, jadi di silver pakai
--     DATETIME(kolom), JANGAN DATETIME(kolom, 'Asia/Jakarta') -- menggeser 7 jam.
--   * Semua kolom NULLABLE.
--   * Kolom baru di SQL Server membuat load gagal sampai ditambahkan di sini
--     (ALTER TABLE ... ADD COLUMN). Disengaja: skema bronze hanya berubah lewat
--     file ini.
--   * Tanpa datastream_metadata (itu kolom milik Datastream).
--
-- replace memakai WRITE_TRUNCATE_DATA: data ditimpa, tapi PRIMARY KEY dan
-- CLUSTER BY di bawah tetap utuh.

CREATE SCHEMA IF NOT EXISTS `smi-dwh.pb_bronze_dlt_dc_sby_dbo`
OPTIONS (
  location = 'asia-southeast2',
  description = 'Bronze DC Surabaya (PB_DC_SBY.dbo), dimuat dlt dari Airflow. DDL: sql/ddl/pb_bronze_dlt_dc_sby_dbo.sql'
);

GRANT `roles/bigquery.dataEditor`
ON SCHEMA `smi-dwh.pb_bronze_dlt_dc_sby_dbo`
TO "serviceAccount:airflow-pb-dwh@smi-dwh.iam.gserviceaccount.com";

-- AIMHeader: full refresh tiap run (write_disposition: replace). Riwayat
-- snapshot hariannya ada di GCS: gs://<bucket>/dc_sby/AIMHeader/<tanggal>/.
CREATE TABLE IF NOT EXISTS `smi-dwh.pb_bronze_dlt_dc_sby_dbo.AIMHeader`
(
  IdDC INT64,
  IdVendor INT64,
  nomorPO INT64,
  nomorLPB INT64,
  nomorAIM INT64,
  UserID STRING,
  tglApprove TIMESTAMP,
  UserApprove STRING,
  Catatan STRING,
  TglInvoiceSupplier TIMESTAMP,
  NoInvoiceSupplier STRING,
  UserCreate STRING,
  TanggalCreate TIMESTAMP,
  NomorInvoiceMatching INT64,
  StatusProses INT64,
  TglUploadSAP TIMESTAMP,
  PRIMARY KEY (nomorAIM, nomorPO, IdDC, nomorLPB, IdVendor) NOT ENFORCED
)
CLUSTER BY nomorAIM, nomorPO, IdDC, nomorLPB;
