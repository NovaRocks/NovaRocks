-- @order_sensitive=true
-- @tags=write_path,managed_lake,shared_data
-- Test Objective:
-- 1. Validate TRUNCATE TABLE on a standalone managed-lake table clears rows
--    but preserves the table for subsequent writes.
-- 2. Validate DROP TABLE removes the managed-lake table from the catalog.
-- 3. Validate managed-lake CREATE TABLE rejects invalid key definitions.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_managed_lake_lifecycle;
CREATE TABLE ${case_db}.t_managed_lake_lifecycle (
  k1 INT,
  v1 STRING
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 2;
INSERT INTO ${case_db}.t_managed_lake_lifecycle VALUES
  (1, 'a'),
  (2, 'b');

-- query 2
SELECT k1, v1
FROM ${case_db}.t_managed_lake_lifecycle
ORDER BY k1;

-- query 3
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_managed_lake_lifecycle;

-- query 4
SELECT count(*)
FROM ${case_db}.t_managed_lake_lifecycle;

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t_managed_lake_lifecycle VALUES (10, 'z');

-- query 6
SELECT k1, v1
FROM ${case_db}.t_managed_lake_lifecycle
ORDER BY k1;

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.t_managed_lake_lifecycle;

-- query 8
-- @expect_error=Unknown table
SELECT * FROM ${case_db}.t_managed_lake_lifecycle;

-- query 9
-- @expect_error=key columns are missing from table schema
CREATE TABLE ${case_db}.t_managed_missing_key (
  v INT,
  k INT
)
DUPLICATE KEY(missing)
DISTRIBUTED BY HASH(v) BUCKETS 2;

-- query 10
-- @expect_error=leading column prefix
CREATE TABLE ${case_db}.t_managed_non_prefix_key (
  v INT,
  k INT
)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(v) BUCKETS 2;
