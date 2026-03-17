-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_cancel_optimize)
-- Test Objective:
-- 1. Verify CANCEL ALTER TABLE OPTIMIZE works.

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t(k int) DISTRIBUTED BY HASH(k) BUCKETS 10
    PROPERTIES("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY HASH(k);

-- query 3
-- @skip_result_check=true
USE ${case_db};
CANCEL ALTER TABLE OPTIMIZE FROM t;

-- query 4
-- @result_contains=CANCELLED
USE ${case_db};
SHOW ALTER TABLE OPTIMIZE;
