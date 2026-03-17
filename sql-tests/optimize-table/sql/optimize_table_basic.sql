-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_optimize_table)
-- Test Objective:
-- 1. Verify basic optimize table: change distribution, data preserved.
-- 2. Verify SHOW ALTER TABLE OPTIMIZE reports FINISHED.
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
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t DISTRIBUTED BY HASH(k);

-- query 5
USE ${case_db};
SELECT * FROM t ORDER BY k;
