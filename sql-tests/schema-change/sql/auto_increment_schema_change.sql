-- @tags=schema_change,auto_increment,primary_key
-- Test Objective:
-- 1. Validate ADD COLUMN with AUTO_INCREMENT is rejected (FE restriction).
-- 2. Validate MODIFY COLUMN to remove AUTO_INCREMENT is rejected.

-- query 1
-- @skip_result_check=true
SET catalog default_catalog;
DROP DATABASE IF EXISTS sql_tests_auto_increment_sc FORCE;
CREATE DATABASE sql_tests_auto_increment_sc;
USE sql_tests_auto_increment_sc;
CREATE TABLE t (
  id BIGINT NOT NULL,
  name BIGINT NOT NULL,
  job1 BIGINT NOT NULL,
  job2 BIGINT NOT NULL AUTO_INCREMENT
) PRIMARY KEY (id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1', 'replicated_storage' = 'true');

-- query 2
-- ADD COLUMN with AUTO_INCREMENT should fail.
-- @expect_error=can not be AUTO_INCREMENT when ADD COLUMN
USE sql_tests_auto_increment_sc;
ALTER TABLE t ADD COLUMN newcol BIGINT AUTO_INCREMENT;

-- query 3
-- MODIFY COLUMN to remove AUTO_INCREMENT should fail.
-- @expect_error=Can't not modify a AUTO_INCREMENT column
USE sql_tests_auto_increment_sc;
ALTER TABLE t MODIFY COLUMN job2 BIGINT;

-- query 4
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_auto_increment_sc FORCE;
