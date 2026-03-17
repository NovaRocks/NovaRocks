-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_online_optimize_table_expr_partition)
-- Test Objective:
-- 1. Verify optimize table works on expression-partitioned table (PARTITION BY date_trunc).
-- 2. Verify data integrity and SHOW CREATE TABLE after optimize.
-- @sequential=true

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t(k int, k1 date) PARTITION BY date_trunc('day', k1)
DISTRIBUTED BY HASH(k) BUCKETS 10
    PROPERTIES("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t VALUES(1, '2020-06-01'),(2, '2020-07-01'),(3, '2020-08-01');

-- query 3
-- @result_contains=BUCKETS 10
USE ${case_db};
SHOW CREATE TABLE t;

-- query 4
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY HASH(k);

-- query 5
-- After auto-bucket, no explicit BUCKETS clause
-- @result_not_contains=BUCKETS
-- @result_contains=date_trunc
USE ${case_db};
SHOW CREATE TABLE t;

-- query 6
USE ${case_db};
SELECT * FROM t ORDER BY k;
