-- @order_sensitive=true
-- @tags=write_path,auto_increment,primary_key,null
-- Test Objective:
-- 1. Validate DEFAULT for auto_increment column inserts successfully.
-- 2. Validate explicit NULL for auto_increment column is rejected by FE.
-- 3. Validate UPDATE SET auto_increment = NULL is rejected by FE.

-- query 1
-- @skip_result_check=true
SET catalog default_catalog;
DROP DATABASE IF EXISTS sql_tests_auto_increment_null FORCE;
CREATE DATABASE sql_tests_auto_increment_null;
USE sql_tests_auto_increment_null;
ADMIN SET FRONTEND CONFIG ('auto_increment_cache_size' = '0');
CREATE TABLE t (
  id BIGINT NOT NULL,
  name BIGINT NOT NULL AUTO_INCREMENT,
  job1 BIGINT NULL,
  job2 BIGINT NULL
) PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1', 'replicated_storage' = 'true');

-- query 2
-- DEFAULT for auto_increment column: should succeed, name gets auto-generated value.
USE sql_tests_auto_increment_null;
INSERT INTO t (id, name, job1, job2) VALUES (1,DEFAULT,NULL,2);
SELECT * FROM t;

-- query 3
-- Explicit NULL for auto_increment column: FE should reject.
-- @expect_error=NULL
USE sql_tests_auto_increment_null;
INSERT INTO t (id, name, job1, job2) VALUES (1,NULL,NULL,2);

-- query 4
-- Explicit NULL in full VALUES: FE should reject.
-- @expect_error=NULL
USE sql_tests_auto_increment_null;
INSERT INTO t VALUES (1,NULL,NULL,2);

-- query 5
-- UPDATE SET name = NULL: FE should reject.
-- @expect_error=NULL
USE sql_tests_auto_increment_null;
UPDATE t SET name = NULL WHERE id = 1;

-- query 6
-- Verify the original row is unchanged after all rejected operations.
USE sql_tests_auto_increment_null;
SELECT * FROM t;

-- query 7
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_auto_increment_null FORCE;
