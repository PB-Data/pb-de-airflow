-- CONTOH / TEMPLATE. Ganti isinya dengan query transform yang asli.
--
-- Yang tersedia otomatis saat query ini dijalankan:
--   {{ params.project }}  -> GCP_PROJECT_ID
--   {{ params.bronze }}   -> BQ_DATASET_BRONZE
--   {{ params.silver }}   -> BQ_DATASET_SILVER
--   {{ ds }}              -> tanggal logical date, format YYYY-MM-DD
--
-- Pola yang dipakai di bawah adalah CREATE OR REPLACE TABLE: idempoten, jadi
-- aman diulang berapa kali pun. Untuk tabel besar, ganti ke MERGE agar tidak
-- menulis ulang seluruh tabel setiap hari.

CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.silver }}.dim_product`
PARTITION BY DATE(updated_at)
CLUSTER BY product_id
AS
SELECT
    product_id,
    product_code,
    product_name,
    -- Kolom decimal presisi tinggi mendarat sebagai BIGNUMERIC di bronze.
    -- CAST di sini kalau layer silver cukup dengan NUMERIC.
    CAST(standard_cost AS NUMERIC) AS standard_cost,
    modified_at                    AS updated_at,
    CURRENT_TIMESTAMP()            AS _silver_loaded_at
FROM `{{ params.project }}.{{ params.bronze }}.master_product`
WHERE product_id IS NOT NULL
