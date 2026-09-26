-- =====================================================================
-- silver.product : master produk
--
-- Pola FULL REBUILD: bronze-nya load "full" (replace tiap run) -> silver cukup dibangun ulang.
-- Cocok untuk tabel master kecil. Histori snapshot harian tetap ada di data lake GCS.
-- =====================================================================

CREATE OR REPLACE TABLE {{ this }}
CLUSTER BY product_id
AS
SELECT
  product_id,
  UPPER(TRIM(product_code))           AS product_code,
  TRIM(product_name)                  AS product_name,
  COALESCE(NULLIF(TRIM(category), ''), 'UNKNOWN') AS category,
  CAST(price AS NUMERIC)              AS price,
  COALESCE(is_active, FALSE)          AS is_active,
  _ingested_at
FROM {{ source('sqlserver_main', 'dbo.Product') }}
WHERE TRUE  -- BigQuery: QUALIFY butuh WHERE/GROUP BY/HAVING
-- jaga-jaga kalau ada duplikat PK di sumber
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _ingested_at DESC) = 1;
