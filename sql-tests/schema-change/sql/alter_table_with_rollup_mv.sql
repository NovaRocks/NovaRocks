-- Test Objective:
-- 1. Validate that dropping columns used by rollup materialized views is not allowed.
-- 2. Verify non-existent column drop is caught.
-- 3. Verify table structure and data remain intact.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_table_with_mv

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_rollup_mv_${uuid0} FORCE;
CREATE DATABASE sc_alter_rollup_mv_${uuid0};
USE sc_alter_rollup_mv_${uuid0};
CREATE TABLE t0(k0 BIGINT, k1 DATETIME, v0 INT, v1 VARCHAR(100))
 distributed by hash(k0)
 order by (v0)
 properties('replication_num'='1');
INSERT INTO t0 VALUES(0, '2024-01-01 00:00:00', 10, '100');
CREATE MATERIALIZED VIEW test_mv1 AS SELECT k0, v1 FROM t0 WHERE v0 > 10;

-- query 2
-- Wait for first rollup MV to finish.
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE sc_alter_rollup_mv_${uuid0};
SHOW ALTER TABLE ROLLUP FROM sc_alter_rollup_mv_${uuid0} ORDER BY CreateTime DESC LIMIT 1;

-- query 3
-- Wait for FE to fully settle after rollup, then create second MV.
-- @retry_count=30
-- @retry_interval_ms=2000
-- @skip_result_check=true
USE sc_alter_rollup_mv_${uuid0};
CREATE MATERIALIZED VIEW test_mv2 AS SELECT k0, v1, k1, v0 FROM t0 WHERE v0 > 10 and k1 = '2024-01-01 00:00:00';

-- query 4
-- Give FE time to register the second rollup job, then wait for both to finish.
-- @retry_count=60
-- @retry_interval_ms=1000
-- @skip_result_check=true
-- @result_not_contains=PENDING
-- @result_not_contains=RUNNING
-- @result_not_contains=WAITING_TXN
USE sc_alter_rollup_mv_${uuid0};
SET @a = sleep(3);
SHOW ALTER TABLE ROLLUP FROM sc_alter_rollup_mv_${uuid0};

-- query 5
-- Cannot drop column k1 used by rollup MV.
-- Retry because the table may still be transitioning after rollup completion.
-- @retry_count=30
-- @retry_interval_ms=2000
-- @expect_error=the column is used in the related rollup
USE sc_alter_rollup_mv_${uuid0};
ALTER TABLE t0 DROP COLUMN k1;

-- query 6
-- Cannot drop column v0 used by rollup MV.
-- @expect_error=the column is used in the related rollup
USE sc_alter_rollup_mv_${uuid0};
ALTER TABLE t0 DROP COLUMN v0;

-- query 7
-- Multi-column drop fails if any column is used by rollup.
-- @expect_error=the column is used in the related rollup
USE sc_alter_rollup_mv_${uuid0};
ALTER TABLE t0 DROP COLUMN v1, DROP COLUMN v0;

-- query 8
-- Non-existent column drop fails.
-- @expect_error=Column does not exists
USE sc_alter_rollup_mv_${uuid0};
ALTER TABLE t0 DROP COLUMN v1, DROP COLUMN v100;

-- query 9
-- Verify table structure is intact.
USE sc_alter_rollup_mv_${uuid0};
DESC t0;

-- query 10
-- @order_sensitive=true
USE sc_alter_rollup_mv_${uuid0};
SELECT * from t0 order by k0;

-- query 11
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_rollup_mv_${uuid0} FORCE;
