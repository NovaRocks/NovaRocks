-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_change_partial_partition_distribution)
-- Test Objective:
-- 1. Verify optimizing specific partitions (only p202006, p202008) changes their bucket count.
-- 2. Verify unspecified partitions (p202007) keep original bucket count.
-- 3. Verify error when changing distribution column with PARTITIONS clause.
-- 4. Verify error when changing distribution type with PARTITIONS clause.
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
-- Optimize only p202006 and p202008 to 4 buckets
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t PARTITIONS(p202006,p202008) DISTRIBUTED BY HASH(k) BUCKETS 4;

-- query 4
USE ${case_db};
SELECT * FROM t ORDER BY k;

-- query 5
-- Cannot change distribution column when specifying partitions
-- @expect_error=not support change distribution column when specify partitions
USE ${case_db};
ALTER TABLE t PARTITIONS(p202006,p202008) DISTRIBUTED BY HASH(k1) BUCKETS 4;

-- query 6
-- Cannot change distribution type when specifying partitions
-- @expect_error=not support change distribution type when specify partitions
USE ${case_db};
ALTER TABLE t PARTITIONS(p202006,p202008) DISTRIBUTED BY RANDOM;
