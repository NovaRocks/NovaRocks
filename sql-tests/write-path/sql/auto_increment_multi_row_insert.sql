-- @order_sensitive=true
-- @tags=write_path,auto_increment,primary_key
-- Test Objective:
-- 1. Validate multi-row INSERT VALUES assigns sequential auto_increment IDs
--    in VALUES clause order (deterministic).
-- 2. Validate mixed DEFAULT and explicit values in the same INSERT.
-- 3. Validate multi-row INSERT on non-key auto_increment column.

-- query 1
-- @skip_result_check=true
SET catalog default_catalog;
DROP DATABASE IF EXISTS sql_tests_auto_increment_mr FORCE;
CREATE DATABASE sql_tests_auto_increment_mr;
USE sql_tests_auto_increment_mr;
ADMIN SET FRONTEND CONFIG ('auto_increment_cache_size' = '0');
CREATE TABLE t (
  id BIGINT NOT NULL AUTO_INCREMENT,
  name BIGINT NOT NULL,
  job1 BIGINT NOT NULL,
  job2 BIGINT NOT NULL
) PRIMARY KEY (id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1', 'replicated_storage' = 'true');

-- query 2
-- Multi-row INSERT: both rows get sequential IDs (1, 2).
USE sql_tests_auto_increment_mr;
INSERT INTO t (id,name,job1,job2) VALUES (DEFAULT,1,1,1),(DEFAULT,2,2,2);
SELECT * FROM t ORDER BY name;

-- query 3
-- Second multi-row INSERT: IDs continue (3, 4).
USE sql_tests_auto_increment_mr;
INSERT INTO t (id,name,job1,job2) VALUES (DEFAULT,3,3,3),(DEFAULT,4,4,4);
SELECT * FROM t ORDER BY name;

-- query 4
-- Mixed explicit and DEFAULT in same INSERT.
USE sql_tests_auto_increment_mr;
INSERT INTO t (id,name,job1,job2) VALUES (100,5,5,5);
INSERT INTO t (id,name,job1,job2) VALUES (101,6,6,6),(DEFAULT,7,7,7);
SELECT * FROM t ORDER BY name;

-- query 5
-- Non-key auto_increment column with multi-row insert.
USE sql_tests_auto_increment_mr;
CREATE TABLE t2 (
  id BIGINT NOT NULL,
  name BIGINT NOT NULL,
  job1 BIGINT NOT NULL AUTO_INCREMENT,
  job2 BIGINT NOT NULL
) PRIMARY KEY (id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1', 'replicated_storage' = 'true');
INSERT INTO t2 (id,name,job2) VALUES (1,1,1),(2,2,2);
SELECT * FROM t2 ORDER BY name;

-- query 6
-- Explicit and DEFAULT mixed on non-key auto_increment.
USE sql_tests_auto_increment_mr;
INSERT INTO t2 (id,name,job1,job2) VALUES (5,5,100,5);
INSERT INTO t2 (id,name,job1,job2) VALUES (6,6,101,6),(7,7,DEFAULT,7);
SELECT * FROM t2 ORDER BY name;

-- query 7
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_auto_increment_mr FORCE;
