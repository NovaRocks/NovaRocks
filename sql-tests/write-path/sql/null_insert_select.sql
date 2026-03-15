-- @order_sensitive=true
-- @tags=write_path,dml,null
-- Test Objective:
-- 1. Validate NULL values are preserved across INSERT-SELECT into typed sink columns.
-- 2. Validate mixed NULL/non-NULL rows across INT/STRING/DECIMAL/DATETIME columns.
-- Test Flow:
-- 1. Create/reset mixed-type source and sink tables.
-- 2. Insert deterministic rows with different NULL patterns into source.
-- 3. Insert-select into sink and verify ordered output row-by-row.
DROP TABLE IF EXISTS ${case_db}.t_null_insert_src;
DROP TABLE IF EXISTS ${case_db}.t_null_insert_sink;
CREATE TABLE ${case_db}.t_null_insert_src (
  id BIGINT,
  c_int BIGINT,
  c_str STRING,
  c_dec DECIMAL(9, 2),
  c_dt DATETIME
);
CREATE TABLE ${case_db}.t_null_insert_sink (
  id INT,
  c_int INT,
  c_str STRING,
  c_dec DECIMAL(9, 2),
  c_dt DATETIME
);
INSERT INTO ${case_db}.t_null_insert_src VALUES
  (1, NULL, NULL, NULL, NULL),
  (2, 20, 'ok', 12.30, '2024-01-02 03:04:05'),
  (3, NULL, 'tail', NULL, '2024-06-01 00:00:00');
INSERT INTO ${case_db}.t_null_insert_sink
SELECT id, c_int, c_str, c_dec, c_dt
FROM ${case_db}.t_null_insert_src;
SELECT id, c_int, c_str, c_dec, c_dt
FROM ${case_db}.t_null_insert_sink
ORDER BY id;
