-- @order_sensitive=true
-- @tags=iceberg_dml,partition
-- Test Objective:
-- 1. Validate Iceberg identity-partitioned table accepts multi-row INSERT spanning partitions.
-- 2. Validate read-back returns the inserted rows.
DROP TABLE IF EXISTS ${case_db}.t_partitioned_insert;
CREATE TABLE ${case_db}.t_partitioned_insert (
  p BIGINT,
  k BIGINT,
  v BIGINT
)
PARTITION BY identity(p);
INSERT INTO ${case_db}.t_partitioned_insert VALUES
  (1, 1, 10),
  (2, 1, 20);
SELECT p, k, v
FROM ${case_db}.t_partitioned_insert
ORDER BY p, k, v;
