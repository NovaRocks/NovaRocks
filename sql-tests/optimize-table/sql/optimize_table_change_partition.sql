-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_change_partition_distribution)
-- Test Objective:
-- 1. Verify changing bucket count on a partitioned table.
-- 2. Verify changing to auto-bucket (no explicit BUCKETS clause).
-- 3. Verify changing distribution column.
-- 4. Verify changing to RANDOM distribution.
-- 5. Verify data integrity after each change.
-- @sequential=true

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t(k int, k1 date) PARTITION BY RANGE(k1)(
    PARTITION p202006 VALUES LESS THAN ("2020-07-01"),
    PARTITION p202007 VALUES LESS THAN ("2020-08-01"),
    PARTITION p202008 VALUES LESS THAN ("2020-09-01")
) DISTRIBUTED BY HASH(k) BUCKETS 10
    PROPERTIES("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t VALUES(1, '2020-06-01'),(2, '2020-07-01'),(3, '2020-08-01');

-- query 3
-- Change to 4 buckets
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY HASH(k) BUCKETS 4;

-- query 4
-- @result_contains=BUCKETS 4
USE ${case_db};
SHOW CREATE TABLE t;

-- query 5
USE ${case_db};
SELECT * FROM t ORDER BY k;

-- query 6
-- Change to auto-bucket
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY HASH(k);

-- query 7
-- After auto-bucket, no explicit BUCKETS clause in SHOW CREATE TABLE
-- @result_not_contains=BUCKETS
USE ${case_db};
SHOW CREATE TABLE t;

-- query 8
USE ${case_db};
SELECT * FROM t ORDER BY k;

-- query 9
-- Change distribution column to k1
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY HASH(k1) BUCKETS 4;

-- query 10
-- @result_contains=HASH(`k1`)
USE ${case_db};
SHOW CREATE TABLE t;

-- query 11
USE ${case_db};
SELECT * FROM t ORDER BY k;

-- query 12
-- Change to RANDOM distribution
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY RANDOM;

-- query 13
-- @result_contains=DISTRIBUTED BY RANDOM
USE ${case_db};
SHOW CREATE TABLE t;

-- query 14
USE ${case_db};
SELECT * FROM t ORDER BY k;
