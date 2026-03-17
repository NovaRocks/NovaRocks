-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_disable_random)
-- Test Objective:
-- 1. Verify ALTER TABLE DISTRIBUTED BY RANDOM on a RANDOM-distributed table is rejected.

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t(k int) DISTRIBUTED BY RANDOM BUCKETS 5
    PROPERTIES("replication_num" = "1");

-- query 2
-- @expect_error=Random distribution table already supports automatic scaling and does not require optimization
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY RANDOM BUCKETS 10;
