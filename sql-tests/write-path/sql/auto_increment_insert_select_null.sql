-- @order_sensitive=true
-- @tags=write_path,auto_increment,primary_key,insert_select,null
-- Test Objective:
-- 1. Validate INSERT...SELECT from a table with NULL values in auto_increment column:
--    NULL rows are rejected (auto_increment columns are NOT NULL), but the counter
--    still advances for the allocated-then-rejected IDs.
-- 2. Validate INSERT...SELECT with miss_auto_increment_column:
--    existing rows keep their auto_increment values, new rows get fresh IDs.
-- 3. Counter advancement: rejected NULL rows still consume counter values.

-- query 1
-- @skip_result_check=true
SET catalog default_catalog;
DROP DATABASE IF EXISTS sql_tests_auto_increment_isn FORCE;
CREATE DATABASE sql_tests_auto_increment_isn;
USE sql_tests_auto_increment_isn;
ADMIN SET FRONTEND CONFIG ('auto_increment_cache_size' = '0');
ADMIN SET FRONTEND CONFIG ('empty_load_as_error' = 'false');
CREATE TABLE t1 (id BIGINT NOT NULL, idx BIGINT AUTO_INCREMENT)
  PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1
  PROPERTIES ('replication_num' = '1', 'replicated_storage' = 'true');
CREATE TABLE t2 (id BIGINT NOT NULL, idx BIGINT NULL)
  PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1
  PROPERTIES ('replication_num' = '1', 'replicated_storage' = 'true');
INSERT INTO t2 VALUES (1, NULL), (2, NULL);

-- query 2
-- INSERT...SELECT with NULL idx rows: all rejected (max_filter_ratio=1). t1 stays empty.
-- But counter advances by 2 (IDs allocated then discarded).
-- Run 3 more similar INSERTs to advance counter further (matches dev/test coverage).
-- @skip_result_check=true
USE sql_tests_auto_increment_isn;
INSERT INTO t1 properties ('max_filter_ratio' = '1') SELECT * FROM t2;
INSERT INTO t1 (id, idx) properties ('max_filter_ratio' = '1') SELECT * FROM t2;
INSERT INTO t1 (id, idx) properties ('max_filter_ratio' = '1') SELECT id, idx FROM t2;
INSERT INTO t1 properties ('max_filter_ratio' = '1') SELECT 1, NULL;

-- query 3
-- Add non-NULL rows to t2, recreate t1.
-- @skip_result_check=true
USE sql_tests_auto_increment_isn;
INSERT INTO t2 VALUES (10, 1), (20, 2);
DROP TABLE t1;
CREATE TABLE t1 (id BIGINT NOT NULL, idx BIGINT AUTO_INCREMENT)
  PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 1
  PROPERTIES ('replication_num' = '1', 'replicated_storage' = 'true');

-- query 4
-- INSERT...SELECT: NULL rows rejected (counter+2), non-NULL rows inserted.
USE sql_tests_auto_increment_isn;
INSERT INTO t1 (id, idx) properties ('max_filter_ratio' = '1') SELECT * FROM t2;
SELECT * FROM t1 ORDER BY id;

-- query 5
-- Same INSERT again: NULL rows rejected (counter+2), non-NULL rows upserted.
USE sql_tests_auto_increment_isn;
INSERT INTO t1 (id, idx) properties ('max_filter_ratio' = '1') SELECT id, idx FROM t2;
SELECT * FROM t1 ORDER BY id;

-- query 6
-- miss_auto_increment_column: new rows (id=1,2) get fresh IDs from counter (now at 4).
-- Existing rows (id=10,20) keep their idx values.
USE sql_tests_auto_increment_isn;
INSERT INTO t1 (id) SELECT id FROM t2;
SELECT * FROM t1 ORDER BY id;

-- query 7
-- @skip_result_check=true
USE sql_tests_auto_increment_isn;
ADMIN SET FRONTEND CONFIG ('empty_load_as_error' = 'true');
DROP DATABASE IF EXISTS sql_tests_auto_increment_isn FORCE;
