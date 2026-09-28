-- silver_dc_sby.aim_header: 1 baris per PK dari bronze AIMHeader.
--
-- Bronze AIMHeader di-replace tiap run, jadi silver cukup dibangun ulang.
-- Untuk tabel incremental (append), pola yang sama tetap benar: QUALIFY mengambil
-- versi terbaru per PK dari semua load.
--
-- Datetime SQL Server (jam dinding WIB, tanpa timezone) tersimpan di bronze sebagai
-- TIMESTAMP berlabel UTC. Jam WIB-nya: DATETIME(kolom, 'UTC').

CREATE OR REPLACE TABLE silver_dc_sby.aim_header AS
SELECT * EXCEPT (_dlt_load_id)
FROM pb_bronze_dlt_dc_sby_dbo.AIMHeader
WHERE TRUE  -- QUALIFY butuh WHERE/GROUP BY/HAVING
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY nomorAIM, nomorPO, IdDC, nomorLPB, IdVendor
  ORDER BY _ingested_at DESC
) = 1;
