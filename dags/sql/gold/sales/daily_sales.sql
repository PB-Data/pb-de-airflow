-- =====================================================================
-- gold.daily_sales : omzet harian per toko & kategori produk
--
-- Pola INCREMENTAL PER PARTISI TANGGAL:
--   * run pertama / full_refresh -> build semua tanggal
--   * run berikutnya -> cari tanggal order paling awal yang berubah di silver sejak build terakhir
--     (_ingested_at > _built_at terakhir), hitung ulang dari tanggal itu, lalu ganti partisinya
--     secara atomik (MERGE ... ON FALSE = delete + insert dalam 1 statement)
-- =====================================================================

DECLARE is_full BOOL DEFAULT (
  {{ params.full_refresh }}
  OR NOT EXISTS (
    SELECT 1 FROM `{{ this.project }}.{{ this.dataset }}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = '{{ this.table }}'
  )
);
DECLARE since DATE DEFAULT DATE '1900-01-01';
DECLARE last_built TIMESTAMP;

IF NOT is_full THEN
  SET last_built = (SELECT MAX(_built_at) FROM {{ this }});
  -- tanggal order paling awal yang berubah di silver sejak build gold terakhir
  -- (termasuk order lama yang baru di-update / baru masuk), minimal `lookback_days` terakhir
  SET since = (
    SELECT MIN(d) FROM (
      SELECT o.order_date AS d
      FROM {{ ref('silver.sales_order') }} AS o
      WHERE o._ingested_at > COALESCE(last_built, TIMESTAMP '1970-01-01')
      UNION ALL
      SELECT o.order_date
      FROM {{ ref('silver.sales_order_line') }} AS l
      JOIN {{ ref('silver.sales_order') }} AS o ON o.sales_order_id = l.sales_order_id
      WHERE l._ingested_at > COALESCE(last_built, TIMESTAMP '1970-01-01')
    )
  );
  SET since = LEAST(
    COALESCE(since, CURRENT_DATE('{{ tz }}')),
    DATE_SUB(CURRENT_DATE('{{ tz }}'), INTERVAL {{ params.lookback_days }} DAY)
  );
END IF;

CREATE TEMP TABLE agg AS
SELECT
  o.order_date,
  o.store_code,
  COALESCE(p.category, 'UNKNOWN')     AS category,
  COUNT(DISTINCT o.sales_order_id)    AS order_count,
  SUM(l.qty)                          AS qty,
  SUM(l.line_total)                   AS revenue,
  SUM(l.discount)                     AS discount,
  CURRENT_TIMESTAMP()                 AS _built_at
FROM {{ ref('silver.sales_order') }} AS o
JOIN {{ ref('silver.sales_order_line') }} AS l
  ON l.sales_order_id = o.sales_order_id
LEFT JOIN {{ ref('silver.product') }} AS p
  ON p.product_id = l.product_id
WHERE o.order_date >= since
  AND o.status <> 'CANCELLED'
GROUP BY 1, 2, 3;

IF is_full THEN
  CREATE OR REPLACE TABLE {{ this }}
  PARTITION BY order_date
  CLUSTER BY store_code, category
  AS SELECT * FROM agg;
ELSE
  MERGE {{ this }} T
  USING agg S
  ON FALSE
  WHEN NOT MATCHED BY SOURCE AND T.order_date >= since THEN DELETE
  WHEN NOT MATCHED THEN INSERT ROW;
END IF;
