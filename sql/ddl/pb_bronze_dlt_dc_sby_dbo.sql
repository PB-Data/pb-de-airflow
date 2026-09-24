-- Dataset & tabel bronze untuk config/sources/dc_sby.yml.
--
-- Dibuat MANUAL, sekali, sebelum DAG bronze_ingest_dc_sby dijalankan pertama
-- kali. Jalankan di BigQuery console dengan Query settings -> Location =
-- asia-southeast2, oleh orang yang punya hak membuat dataset. SA Airflow
-- (airflow-pb-dwh) sengaja tidak punya hak itu, dan SA-nya harus sudah dibuat
-- sebelum GRANT di bawah dijalankan.
--
-- Syarat supaya tabel cocok dengan yang dikirim dlt. Kalau beda, load job gagal:
--   * Nama kolom persis seperti di SQL Server (naming sql_cs_v1).
--   * Kolom datetime SQL Server -> TIMESTAMP, BUKAN DATETIME seperti bronze
--     Datastream. dlt 1.30 selalu menulis TIMESTAMP ke BigQuery. Nilainya jam
--     WIB apa adanya tapi berlabel UTC, jadi di silver pakai DATETIME(kolom),
--     JANGAN DATETIME(kolom, 'Asia/Jakarta') -- itu menggeser 7 jam.
--   * Semua kolom NULLABLE. dlt tidak pernah mengubah tipe atau mode kolom yang
--     sudah ada; kolom baru di SQL Server ditambahkan dlt lewat ADD COLUMN.
--   * Tanpa datastream_metadata (itu kolom milik Datastream).
--
-- dlt tetap membuat sendiri tiga tabel internalnya di dataset ini
-- (_dlt_loads, _dlt_version, _dlt_pipeline_state), jadi SA butuh
-- bigquery.dataEditor di dataset, bukan cuma hak tulis data.

CREATE SCHEMA IF NOT EXISTS `smi-dwh.pb_bronze_dlt_dc_sby_dbo`
OPTIONS (
  location = 'asia-southeast2',
  description = 'Bronze DC Surabaya (PB_DC_SBY.dbo), dimuat dlt dari Airflow. DDL: sql/ddl/pb_bronze_dlt_dc_sby_dbo.sql'
);

GRANT `roles/bigquery.dataEditor`
ON SCHEMA `smi-dwh.pb_bronze_dlt_dc_sby_dbo`
TO "serviceAccount:airflow-pb-dwh@smi-dwh.iam.gserviceaccount.com";

-- AIMHeader: full refresh tiap run (write_disposition: replace). dlt memakai
-- TRUNCATE lalu insert, jadi PRIMARY KEY dan CLUSTER BY di bawah tetap terjaga.
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
