-- =====================================================================
-- silver.customer : 1 baris per customer (versi terbaru), dedup dari bronze append-only
--
-- Pola INCREMENTAL MERGE (copy pola ini untuk tabel incremental lain):
--   * run pertama (tabel belum ada) / param full_refresh=true -> build dari SELURUH histori bronze
--   * run berikutnya -> hanya baca partisi bronze sejak load terakhir yang sudah masuk tabel ini
--     (dikurangi `lookback_days` sebagai pengaman), lalu MERGE
--
-- Catatan kolom bronze (dibuat dlt):
--   * nama snake_case: CustomerID -> customer_id, ModifiedDate -> modified_date
--   * datetime SQL Server tersimpan sebagai TIMESTAMP "UTC" padahal isinya jam WIB
--     -> pakai as_local_datetime() (DATETIME WIB) atau as_utc_timestamp() (TIMESTAMP benar)
--   * _ingested_at = waktu load (partisi bronze), _dlt_load_id = id load dlt
-- =====================================================================

DECLARE is_full BOOL DEFAULT (
  {{ params.full_refresh }}
  OR NOT EXISTS (
    SELECT 1 FROM `{{ this.project }}.{{ this.dataset }}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = '{{ this.table }}'
  )
);
DECLARE since TIMESTAMP DEFAULT TIMESTAMP '1970-01-01';

IF NOT is_full THEN
  -- mulai dari progres tabel INI (MAX _ingested_at) dikurangi lookback, bukan dari "sekarang":
  -- kalau DAG sempat berhenti beberapa hari, run berikutnya otomatis mengejar semua load bronze.
  SET since = (
    SELECT TIMESTAMP_SUB(COALESCE(MAX(_ingested_at), TIMESTAMP '1970-01-01'), INTERVAL {{ params.lookback_days }} DAY)
    FROM {{ this }}
  );
END IF;

CREATE TEMP TABLE src AS
SELECT * EXCEPT (_rn)
FROM (
  SELECT
    customer_id,
    TRIM(customer_name)                        AS customer_name,
    NULLIF(TRIM(phone), '')                    AS phone,
    INITCAP(TRIM(city))                        AS city,
    {{ as_local_datetime('created_date') }}    AS created_at,    -- DATETIME (WIB)
    {{ as_local_datetime('modified_date') }}   AS modified_at,   -- DATETIME (WIB)
    _ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY modified_date DESC, _ingested_at DESC, _dlt_load_id DESC
    ) AS _rn
  FROM {{ source('sqlserver_main', 'dbo.Customer') }}
  WHERE _ingested_at >= since                  -- partition pruning bronze
)
WHERE _rn = 1;

IF is_full THEN
  CREATE OR REPLACE TABLE {{ this }}
  CLUSTER BY customer_id
  AS SELECT * FROM src;
ELSE
  MERGE {{ this }} T
  USING src S
  ON T.customer_id = S.customer_id
  -- guard: jangan timpa versi yang lebih baru (mis. saat ada backfill bronze data lama)
  WHEN MATCHED AND (T.modified_at IS NULL OR S.modified_at >= T.modified_at) THEN UPDATE SET
    customer_name = S.customer_name,
    phone         = S.phone,
    city          = S.city,
    created_at    = S.created_at,
    modified_at   = S.modified_at,
    _ingested_at  = S._ingested_at
  WHEN NOT MATCHED THEN INSERT ROW;
END IF;
