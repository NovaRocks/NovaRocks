-- @order_sensitive=true
-- @tags=iceberg_dml,aggregate
-- Test Objective:
-- 1. Validate GROUP BY aggregate semantics on BIGINT columns after Iceberg INSERT.
-- 2. Validate COUNT(*) vs COUNT(col) behavior with NULL values.
-- 3. Validate SUM aggregate on positive and negative values.
DROP TABLE IF EXISTS ${case_db}.t_metrics;
CREATE TABLE ${case_db}.t_metrics (
  grp STRING,
  v BIGINT
);
INSERT INTO ${case_db}.t_metrics VALUES
  ('A', 1),
  ('A', 2),
  ('A', NULL),
  ('B', 5),
  ('B', -1),
  ('B', NULL);
SELECT grp, COUNT(*) AS cnt_all, COUNT(v) AS cnt_v, SUM(v) AS sum_v
FROM ${case_db}.t_metrics
GROUP BY grp
ORDER BY grp;
