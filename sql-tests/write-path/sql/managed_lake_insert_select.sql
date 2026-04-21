-- @order_sensitive=true
-- @tags=write_path,managed_lake,shared_data
-- Test Objective:
-- 1. Validate CREATE / INSERT / SELECT on a standalone managed-lake table.
-- 2. Confirm that a hash-distributed table with multiple buckets round-trips
--    multi-row inserts including a NULL value.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_managed_lake_insert_select;
CREATE TABLE ${case_db}.t_managed_lake_insert_select (
  k1 INT,
  v1 STRING
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 2;
INSERT INTO ${case_db}.t_managed_lake_insert_select VALUES
  (1, 'a'),
  (2, 'b'),
  (3, NULL);

-- query 2
SELECT k1, v1
FROM ${case_db}.t_managed_lake_insert_select
ORDER BY k1;
