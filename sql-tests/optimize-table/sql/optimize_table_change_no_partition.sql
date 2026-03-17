-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_change_no_partition_distribution)
-- Test Objective:
-- 1. Verify changing bucket count on a non-partitioned table.
-- 2. Verify changing to RANDOM distribution.
-- 3. Verify data integrity and SHOW CREATE TABLE after each change.
-- @sequential=true

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t(k int) DISTRIBUTED BY HASH(k) BUCKETS 10
    PROPERTIES("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t VALUES(1),(2),(3);

-- query 3
USE ${case_db};
SELECT * FROM t ORDER BY k;

-- query 4
-- Change to 4 buckets
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY HASH(k) BUCKETS 4;

-- query 5
-- @result_contains=BUCKETS 4
USE ${case_db};
SHOW CREATE TABLE t;

-- query 6
USE ${case_db};
SELECT * FROM t ORDER BY k;

-- query 7
-- Change to RANDOM distribution
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY RANDOM;

-- query 8
-- @result_contains=DISTRIBUTED BY RANDOM
USE ${case_db};
SHOW CREATE TABLE t;

-- query 9
USE ${case_db};
SELECT * FROM t ORDER BY k;
