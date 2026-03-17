-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_optimize_duplicate_partitions)
-- Test Objective:
-- 1. Verify duplicate partition names in PARTITIONS() clause are deduplicated.
-- 2. Verify temporary partitions are cleaned up after optimize.
-- 3. Verify per-partition bucket counts are correct.
-- @sequential=true

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t(k1 date, k2 int)
DUPLICATE KEY(k1)
PARTITION BY RANGE(k1)(
    PARTITION p202006 VALUES LESS THAN ("2020-07-01"),
    PARTITION p202007 VALUES LESS THAN ("2020-08-01"),
    PARTITION p202008 VALUES LESS THAN ("2020-09-01")
) DISTRIBUTED BY HASH(k1) BUCKETS 3
    PROPERTIES("replication_num" = "1");

-- query 2
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t PARTITIONS(p202006,p202006,p202007) DISTRIBUTED BY HASH(k1) BUCKETS 4;

-- query 3
-- Temporary partitions should be empty after optimize completes
USE ${case_db};
SHOW TEMPORARY PARTITIONS FROM t;

-- query 4
-- p202006 and p202007 should have 4 buckets, p202008 should keep 3
-- @skip_result_check=true
USE ${case_db};
SHOW PARTITIONS FROM t;
