-- =====================================================================
-- silver.sales_order : header transaksi, 1 baris per sales_order_id (versi terbaru)
-- Pola INCREMENTAL MERGE (sama seperti silver/sales/customer.sql) + partisi per tanggal order
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
    sales_order_id,
    TRIM(order_number)                        AS order_number,
    DATE({{ as_local_datetime('order_date') }}) AS order_date,     -- tanggal WIB
    {{ as_local_datetime('order_date') }}     AS order_at,         -- DATETIME WIB
    customer_id,
    UPPER(TRIM(store_code))                   AS store_code,
    UPPER(TRIM(status))                       AS status,
    CAST(total_amount AS NUMERIC)             AS total_amount,
    {{ as_local_datetime('modified_date') }}  AS modified_at,
    _ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY sales_order_id
      ORDER BY modified_date DESC, _ingested_at DESC, _dlt_load_id DESC
    ) AS _rn
  FROM {{ source('sqlserver_main', 'dbo.SalesOrderHeader') }}
  WHERE _ingested_at >= since
)
WHERE _rn = 1;

IF is_full THEN
  CREATE OR REPLACE TABLE {{ this }}
  PARTITION BY order_date
  CLUSTER BY store_code, sales_order_id
  AS SELECT * FROM src;
ELSE
  MERGE {{ this }} T
  USING src S
  ON T.sales_order_id = S.sales_order_id
  WHEN MATCHED AND (T.modified_at IS NULL OR S.modified_at >= T.modified_at) THEN UPDATE SET
    order_number = S.order_number,
    order_date   = S.order_date,
    order_at     = S.order_at,
    customer_id  = S.customer_id,
    store_code   = S.store_code,
    status       = S.status,
    total_amount = S.total_amount,
    modified_at  = S.modified_at,
    _ingested_at = S._ingested_at
  WHEN NOT MATCHED THEN INSERT ROW;
END IF;
