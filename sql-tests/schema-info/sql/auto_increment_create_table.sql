-- @tags=schema_info,auto_increment,ddl,primary_key
-- Test Objective:
-- 1. Validate normal CREATE TABLE with AUTO_INCREMENT on various column positions.
-- 2. Validate abnormal CREATE TABLE with AUTO_INCREMENT is rejected for invalid cases:
--    NULL column, DEFAULT value, non-BIGINT type, multiple AUTO_INCREMENT columns.

-- query 1
-- @skip_result_check=true
SET catalog default_catalog;
DROP DATABASE IF EXISTS sql_tests_auto_increment_ddl FORCE;
CREATE DATABASE sql_tests_auto_increment_ddl;
USE sql_tests_auto_increment_ddl;

-- query 2
-- Normal: AUTO_INCREMENT on non-key value column.
-- @skip_result_check=true
USE sql_tests_auto_increment_ddl;
CREATE TABLE t1 (id BIGINT NOT NULL, name BIGINT NOT NULL, job1 BIGINT AUTO_INCREMENT, job2 BIGINT NOT NULL) PRIMARY KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES('replication_num'='1', 'replicated_storage'='true');
DROP TABLE t1;

-- query 3
-- Normal: AUTO_INCREMENT on first key column.
-- @skip_result_check=true
USE sql_tests_auto_increment_ddl;
CREATE TABLE t2 (id BIGINT NOT NULL AUTO_INCREMENT, name BIGINT NOT NULL, job1 BIGINT NOT NULL, job2 BIGINT NOT NULL) PRIMARY KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES('replication_num'='1', 'replicated_storage'='true');
DROP TABLE t2;

-- query 4
-- Abnormal: NULL column with AUTO_INCREMENT should fail.
-- @expect_error=AUTO_INCREMENT
USE sql_tests_auto_increment_ddl;
CREATE TABLE t_bad1 (id BIGINT NULL AUTO_INCREMENT, name BIGINT NOT NULL) PRIMARY KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES('replication_num'='1', 'replicated_storage'='true');

-- query 5
-- Abnormal: AUTO_INCREMENT with DEFAULT value should fail (syntax error).
-- @expect_error=Unexpected input
USE sql_tests_auto_increment_ddl;
CREATE TABLE t_bad2 (id BIGINT NOT NULL AUTO_INCREMENT DEFAULT "100", name BIGINT NOT NULL) PRIMARY KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES('replication_num'='1', 'replicated_storage'='true');

-- query 6
-- Abnormal: AUTO_INCREMENT on INT (not BIGINT) should fail.
-- @expect_error=AUTO_INCREMENT
USE sql_tests_auto_increment_ddl;
CREATE TABLE t_bad3 (id INT NOT NULL AUTO_INCREMENT, name BIGINT NOT NULL) PRIMARY KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES('replication_num'='1', 'replicated_storage'='true');

-- query 7
-- Abnormal: Multiple AUTO_INCREMENT columns should fail.
-- @expect_error=AUTO_INCREMENT
USE sql_tests_auto_increment_ddl;
CREATE TABLE t_bad4 (id BIGINT NOT NULL AUTO_INCREMENT, name BIGINT NOT NULL AUTO_INCREMENT) PRIMARY KEY (id, name) DISTRIBUTED BY HASH(id) BUCKETS 7 PROPERTIES('replication_num'='1', 'replicated_storage'='true');

-- query 8
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_auto_increment_ddl FORCE;
