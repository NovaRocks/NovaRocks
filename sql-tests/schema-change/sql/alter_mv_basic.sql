-- Test Objective:
-- 1. Validate basic async MV creation with bitmap index.
-- 2. Verify MV refresh and query correctness.
-- Migrated from: dev/test/sql/test_alter_mv/T/test_alter_mv (first part)

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_mv_basic_${uuid0} FORCE;
CREATE DATABASE sc_alter_mv_basic_${uuid0};
USE sc_alter_mv_basic_${uuid0};
CREATE TABLE t0(c0 INT, c1 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num'='1');
INSERT INTO t0 VALUES(1,1),(2,2),(1,3),(2,4);
CREATE MATERIALIZED VIEW mv1 REFRESH IMMEDIATE MANUAL AS SELECT c0 AS k, count(c1) as cnt FROM t0 GROUP BY c0;
CREATE INDEX index_cnt ON mv1(cnt) USING BITMAP COMMENT 'index1';

-- query 2
-- Wait for the bitmap index creation to finish.
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE sc_alter_mv_basic_${uuid0};
SHOW ALTER TABLE COLUMN FROM sc_alter_mv_basic_${uuid0} ORDER BY CreateTime DESC LIMIT 1;

-- query 3
-- Wait for table state to transition back to NORMAL after index creation.
-- @retry_count=30
-- @retry_interval_ms=2000
-- @skip_result_check=true
USE sc_alter_mv_basic_${uuid0};
REFRESH MATERIALIZED VIEW mv1 FORCE WITH SYNC MODE;

-- query 4
-- @order_sensitive=true
USE sc_alter_mv_basic_${uuid0};
SELECT k, cnt FROM mv1 ORDER BY k;

-- query 5
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_mv_basic_${uuid0} FORCE;
