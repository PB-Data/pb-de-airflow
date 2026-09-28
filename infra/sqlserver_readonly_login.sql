-- =====================================================================
-- Login read-only khusus ingestion (jalankan sebagai user admin Cloud SQL, mis. "sqlserver")
-- via SSMS / Azure Data Studio / sqlcmd.
-- JANGAN pakai `gcloud sql users create` untuk ini: user dari gcloud/console otomatis
-- dapat role CustomerDbRootRole (terlalu tinggi untuk ingestion).
-- =====================================================================
USE [master];
GO
CREATE LOGIN [dlt_reader] WITH PASSWORD = N'<GANTI_DENGAN_PASSWORD_KUAT>', CHECK_POLICY = ON;
GO

-- Ulangi blok ini untuk setiap database yang di-ingest
USE [<NamaDatabase>];
GO
CREATE USER [dlt_reader] FOR LOGIN [dlt_reader];
ALTER ROLE [db_datareader] ADD MEMBER [dlt_reader];
GO

-- Cek:
-- EXECUTE AS LOGIN = 'dlt_reader'; SELECT TOP 1 * FROM dbo.AIMHeader; REVERT;

-- Catatan performa (diskusikan dengan DBA):
-- * Pastikan kolom cursor incremental (mis. ModifiedDate) ter-index, karena query dlt:
--     SELECT ... FROM dbo.X WHERE ModifiedDate >= @last_value
-- * Extract tabel besar di jam kerja bisa bikin blocking dengan transaksi aplikasi kalau
--   READ_COMMITTED_SNAPSHOT = OFF. Opsi: jadwal di luar jam sibuk, atau aktifkan RCSI
--   (ALTER DATABASE [<NamaDatabase>] SET READ_COMMITTED_SNAPSHOT ON) setelah dievaluasi DBA.
