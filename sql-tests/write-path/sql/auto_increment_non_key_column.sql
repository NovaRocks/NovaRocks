-- @order_sensitive=true
-- @tags=write_path,auto_increment,primary_key
-- Test Objective:
-- 1. Validate AUTO_INCREMENT on a non-key column generates sequential values.
-- 2. Validate explicit values for auto_increment non-key column work correctly.
-- 3. Validate UPDATE on non-auto-increment columns preserves auto_increment value.

-- query 1
-- @skip_result_check=true
SET catalog default_catalog;
DROP DATABASE IF EXISTS sql_tests_auto_increment_nk FORCE;
CREATE DATABASE sql_tests_auto_increment_nk;
USE sql_tests_auto_increment_nk;
ADMIN SET FRONTEND CONFIG ('auto_increment_cache_size' = '0');
CREATE TABLE t (
  id BIGINT NOT NULL,
  name BIGINT NOT NULL,
  job1 BIGINT NOT NULL AUTO_INCREMENT,
  job2 BIGINT NOT NULL
) PRIMARY KEY (id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1', 'replicated_storage' = 'true');

-- query 2
-- Insert with omitted auto_increment non-key column: should get job1=1.
USE sql_tests_auto_increment_nk;
INSERT INTO t (id,name,job2) VALUES (1,1,1);
SELECT * FROM t ORDER BY name;

-- query 3
-- Second row: should get job1=2.
USE sql_tests_auto_increment_nk;
INSERT INTO t (id,name,job2) VALUES (2,2,2);
SELECT * FROM t ORDER BY name;

-- query 4
-- Explicit value job1=100.
USE sql_tests_auto_increment_nk;
INSERT INTO t (id,name,job1,job2) VALUES (5,5,100,5);
SELECT * FROM t ORDER BY name;

-- query 5
-- DEFAULT auto_increment: should get job1=3.
USE sql_tests_auto_increment_nk;
INSERT INTO t (id,name,job1,job2) VALUES (0,0,DEFAULT,0);
SELECT * FROM t ORDER BY name;

-- query 6
-- UPDATE non-auto-increment column: auto_increment should be preserved.
USE sql_tests_auto_increment_nk;
UPDATE t SET job2 = 99 WHERE id = 0 AND name = 0;
SELECT * FROM t ORDER BY name;

-- query 7
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_auto_increment_nk FORCE;
