-- @order_sensitive=true
-- @tags=iceberg_dml,datetime
-- Test Objective:
-- 1. Validate DATE/DATETIME values are persisted correctly through INSERT-SELECT into Iceberg sink.
-- 2. Cover leap-day and epoch-style values together with NULL temporal fields.
DROP TABLE IF EXISTS ${case_db}.t_temporal_insert_src;
DROP TABLE IF EXISTS ${case_db}.t_temporal_insert_sink;
CREATE TABLE ${case_db}.t_temporal_insert_src (
  id BIGINT,
  d DATE,
  dt DATETIME
);
CREATE TABLE ${case_db}.t_temporal_insert_sink (
  id BIGINT,
  d DATE,
  dt DATETIME
);
INSERT INTO ${case_db}.t_temporal_insert_src VALUES
  (1, '1970-01-01', '1970-01-01 00:00:00'),
  (2, '2024-02-29', '2024-02-29 23:59:59'),
  (3, NULL, NULL);
INSERT INTO ${case_db}.t_temporal_insert_sink
SELECT id, d, dt
FROM ${case_db}.t_temporal_insert_src;
SELECT id, d, dt
FROM ${case_db}.t_temporal_insert_sink
ORDER BY id;
