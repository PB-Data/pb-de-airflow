-- gold_dc_sby.aim_per_status: jumlah AIM per DC dan status proses (contoh model gold).

CREATE OR REPLACE TABLE gold_dc_sby.aim_per_status AS
SELECT
  IdDC,
  StatusProses,
  COUNT(*) AS jumlah_aim,
  MAX(_ingested_at) AS data_per
FROM silver_dc_sby.aim_header
GROUP BY IdDC, StatusProses;
