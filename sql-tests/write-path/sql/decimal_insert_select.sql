-- @order_sensitive=true
-- @tags=write_path,dml,decimal
-- Test Objective:
-- 1. Validate INSERT-SELECT writing DECIMAL values from a wider DECIMAL source into a narrower sink schema.
-- 2. Validate NULL propagation for DECIMAL columns through Iceberg table sink.
-- Test Flow:
-- 1. Create/reset DECIMAL source and sink tables in write-path database.
-- 2. Insert deterministic rows (positive/negative/NULL) into source table.
-- 3. Insert into sink by SELECT from source and verify ordered read-back.
DROP TABLE IF EXISTS ${case_db}.t_decimal_insert_src;
DROP TABLE IF EXISTS ${case_db}.t_decimal_insert_sink;
CREATE TABLE ${case_db}.t_decimal_insert_src (
  id BIGINT,
  v DECIMAL(20, 6)
);
CREATE TABLE ${case_db}.t_decimal_insert_sink (
  id INT,
  v DECIMAL(10, 3)
);
INSERT INTO ${case_db}.t_decimal_insert_src VALUES
  (1, 123.456000),
  (2, -99.125000),
  (3, NULL);
INSERT INTO ${case_db}.t_decimal_insert_sink
SELECT id, v
FROM ${case_db}.t_decimal_insert_src;
SELECT id, v
FROM ${case_db}.t_decimal_insert_sink
ORDER BY id;
