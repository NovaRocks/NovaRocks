-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_drop_all_temporary_partitions)
-- Test Objective:
-- 1. Verify ADD TEMPORARY PARTITION works.
-- 2. Verify SHOW TEMPORARY PARTITIONS FROM lists temporary partitions.
-- 3. Verify DROP ALL TEMPORARY PARTITIONS removes all temporary partitions.

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
ALTER TABLE t ADD TEMPORARY PARTITION tp202008 VALUES LESS THAN ("2020-09-01");

-- query 3
-- @skip_result_check=true
USE ${case_db};
ALTER TABLE t ADD TEMPORARY PARTITION tp202009 VALUES LESS THAN ("2020-10-01");

-- query 4
-- @result_contains=tp202008
-- @result_contains=tp202009
USE ${case_db};
SHOW TEMPORARY PARTITIONS FROM t;

-- query 5
-- @skip_result_check=true
USE ${case_db};
ALTER TABLE t DROP ALL TEMPORARY PARTITIONS;

-- query 6
-- After dropping all temporary partitions, result should be empty (header only)
USE ${case_db};
SHOW TEMPORARY PARTITIONS FROM t;
