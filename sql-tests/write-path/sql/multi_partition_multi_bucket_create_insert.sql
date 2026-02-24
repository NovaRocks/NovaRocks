-- @order_sensitive=true
-- Test Objective:
-- 1. Validate internal table DDL with multi-partition and multi-bucket clauses.
-- 2. Validate insert path can commit a dirty partition even when some buckets have no rows.
-- Test Flow:
-- 1. Switch to internal catalog and create a range-partitioned hash-bucketed table.
-- 2. Insert deterministic rows that only hit one partition and one hash key.
-- 3. Query ordered rows for deterministic assertion.
SET catalog default_catalog;
CREATE DATABASE IF NOT EXISTS sql_tests_write_path_internal;
DROP TABLE IF EXISTS sql_tests_write_path_internal.t_multi_partition_multi_bucket;
CREATE TABLE sql_tests_write_path_internal.t_multi_partition_multi_bucket (
  p BIGINT,
  k BIGINT,
  v BIGINT
)
DUPLICATE KEY(p, k)
PARTITION BY RANGE (p) (
  PARTITION p_lt_10 VALUES LESS THAN ("10"),
  PARTITION p_lt_20 VALUES LESS THAN ("20")
)
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO sql_tests_write_path_internal.t_multi_partition_multi_bucket VALUES
  (1, 1, 10),
  (2, 1, 20);
SELECT p, k, v
FROM sql_tests_write_path_internal.t_multi_partition_multi_bucket
ORDER BY p, k, v;
