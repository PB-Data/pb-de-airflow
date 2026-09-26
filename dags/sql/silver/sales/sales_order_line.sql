-- =====================================================================
-- silver.sales_order_line : detail transaksi (PK komposit: sales_order_id + sales_order_detail_id)
-- Pola INCREMENTAL MERGE dengan composite key
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
    sales_order_detail_id,
    product_id,
    CAST(qty AS NUMERIC)                      AS qty,
    CAST(unit_price AS NUMERIC)               AS unit_price,
    CAST(COALESCE(discount, 0) AS NUMERIC)    AS discount,
    CAST(line_total AS NUMERIC)               AS line_total,
    {{ as_local_datetime('modified_date') }}  AS modified_at,
    _ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY sales_order_id, sales_order_detail_id
      ORDER BY modified_date DESC, _ingested_at DESC, _dlt_load_id DESC
    ) AS _rn
  FROM {{ source('sqlserver_main', 'dbo.SalesOrderDetail') }}
  WHERE _ingested_at >= since
)
WHERE _rn = 1;

IF is_full THEN
  CREATE OR REPLACE TABLE {{ this }}
  CLUSTER BY sales_order_id, product_id
  AS SELECT * FROM src;
ELSE
  MERGE {{ this }} T
  USING src S
  ON  T.sales_order_id        = S.sales_order_id
  AND T.sales_order_detail_id = S.sales_order_detail_id
  WHEN MATCHED AND (T.modified_at IS NULL OR S.modified_at >= T.modified_at) THEN UPDATE SET
    product_id   = S.product_id,
    qty          = S.qty,
    unit_price   = S.unit_price,
    discount     = S.discount,
    line_total   = S.line_total,
    modified_at  = S.modified_at,
    _ingested_at = S._ingested_at
  WHEN NOT MATCHED THEN INSERT ROW;
END IF;
