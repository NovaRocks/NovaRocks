-- @order_sensitive=true
-- @tags=write_path,auto_increment,primary_key
-- Test Objective:
-- 1. Validate AUTO_INCREMENT column generates sequential IDs for DEFAULT values.
-- 2. Validate explicit values are accepted alongside DEFAULT.
-- 3. Validate UPDATE preserves auto_increment value and does not re-allocate.
-- 4. Validate ALTER TABLE AUTO_INCREMENT = N resets the counter.
-- Uses single-row INSERTs to avoid FE per-row chunk ordering non-determinism.

-- query 1
-- @skip_result_check=true
SET catalog default_catalog;
DROP DATABASE IF EXISTS sql_tests_auto_increment_iu FORCE;
CREATE DATABASE sql_tests_auto_increment_iu;
USE sql_tests_auto_increment_iu;
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
-- Single-row INSERT with DEFAULT: should get id=1.
USE sql_tests_auto_increment_iu;
INSERT INTO t (id,name,job1,job2) VALUES (DEFAULT,0,0,0);
SELECT * FROM t ORDER BY name;

-- query 3
-- Second row: should get id=2.
USE sql_tests_auto_increment_iu;
INSERT INTO t (id,name,job1,job2) VALUES (DEFAULT,1,1,1);
SELECT * FROM t ORDER BY name;

-- query 4
-- UPDATE preserves id=1, only changes job2.
USE sql_tests_auto_increment_iu;
UPDATE t SET job2 = 99 WHERE id = 1 AND name = 0;
SELECT * FROM t ORDER BY name;

-- query 5
-- Explicit id=100.
USE sql_tests_auto_increment_iu;
INSERT INTO t (id,name,job1,job2) VALUES (100,5,5,5);
SELECT * FROM t ORDER BY name;

-- query 6
-- Next DEFAULT should get id=3 (counter continues from last allocation, not from max).
USE sql_tests_auto_increment_iu;
INSERT INTO t (id,name,job1,job2) VALUES (DEFAULT,7,7,7);
SELECT * FROM t ORDER BY name;

-- query 7
-- ALTER TABLE AUTO_INCREMENT = 300 resets the counter.
USE sql_tests_auto_increment_iu;
CREATE TABLE t_alter (
  k BIGINT NOT NULL,
  v1 BIGINT AUTO_INCREMENT
) PRIMARY KEY (k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ('replicated_storage' = 'true', 'replication_num' = '1');
INSERT INTO t_alter VALUES (1, 1);
INSERT INTO t_alter VALUES (2, 2);
ALTER TABLE t_alter AUTO_INCREMENT = 300;
INSERT INTO t_alter VALUES (3, DEFAULT);
SELECT * FROM t_alter ORDER BY k;

-- query 8
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_auto_increment_iu FORCE;
