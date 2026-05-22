-- @order_sensitive=true
-- @tags=iceberg_dml,datetime,invalid_literal
-- Test Objective:
-- 1. Validate invalid temporal literal rows can be sanitized to NULL before DATETIME sink writes.
-- 2. Validate valid temporal literals are still persisted correctly in the same batch.
DROP TABLE IF EXISTS ${case_db}.t_datetime_literal_src;
DROP TABLE IF EXISTS ${case_db}.t_datetime_literal_sink;
CREATE TABLE ${case_db}.t_datetime_literal_src (
  id BIGINT,
  raw_dt STRING
);
CREATE TABLE ${case_db}.t_datetime_literal_sink (
  id BIGINT,
  dt DATETIME
);
INSERT INTO ${case_db}.t_datetime_literal_src VALUES
  (1, '2024-02-29 12:34:56'),
  (2, '2024-02-30 00:00:00'),
  (3, 'not-a-datetime'),
  (4, NULL);
INSERT INTO ${case_db}.t_datetime_literal_sink
SELECT
  id,
  CASE
    WHEN raw_dt IS NULL THEN NULL
    WHEN raw_dt = '2024-02-29 12:34:56' THEN CAST('2024-02-29 12:34:56' AS DATETIME)
    ELSE NULL
  END AS dt
FROM ${case_db}.t_datetime_literal_src;
SELECT
  id,
  dt,
  YEAR(dt) AS y
FROM ${case_db}.t_datetime_literal_sink
ORDER BY id;
