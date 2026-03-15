-- @order_sensitive=true
-- @tags=write_path,auto_increment,partial_update,primary_key
-- Test Objective:
-- Migrated from dev/test test_auto_increment_partial_update_only and
-- test_auto_increment_partial_update_column_upsert.
-- Original tests use stream load; this uses SQL INSERT partial update
-- which exercises the same miss_auto_increment_column lake writer path.
-- 1. Validate partial update (only non-auto-increment columns) preserves
--    existing auto_increment values for existing rows.
-- 2. Validate new rows get fresh auto_increment IDs.
-- 3. Validate repeated partial updates don't change auto_increment values.
-- 4. Validate full-row INSERT overwrites auto_increment with explicit values.

-- query 1
-- @skip_result_check=true
SET catalog default_catalog;
DROP DATABASE IF EXISTS sql_tests_auto_increment_cpu FORCE;
CREATE DATABASE sql_tests_auto_increment_cpu;
USE sql_tests_auto_increment_cpu;
ADMIN SET FRONTEND CONFIG ('auto_increment_cache_size' = '0');

-- Case A: partial update on (k1,k2,k3), auto_increment k4 is missing.
CREATE TABLE t_pu (
  k1 BIGINT NOT NULL,
  k2 BIGINT NOT NULL,
  k3 BIGINT NOT NULL,
  k4 BIGINT AUTO_INCREMENT
) PRIMARY KEY (k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ('replication_num' = '1', 'compression' = 'LZ4');

-- query 2
-- Initial insert: k4 gets auto_increment value 1.
USE sql_tests_auto_increment_cpu;
INSERT INTO t_pu VALUES (1, 2, 3, DEFAULT);
SELECT * FROM t_pu;

-- query 3
-- Partial update (k1,k2,k3): k1=1 exists (keeps k4=1), k1=2 is new (gets k4=2).
USE sql_tests_auto_increment_cpu;
INSERT INTO t_pu (k1, k2, k3) VALUES (1,20,30),(2,40,50);
SELECT * FROM t_pu ORDER BY k1;

-- query 4
-- Repeated partial update: existing rows keep k4 values.
USE sql_tests_auto_increment_cpu;
INSERT INTO t_pu (k1, k2, k3) VALUES (1,20,30),(2,40,50);
SELECT * FROM t_pu ORDER BY k1;

-- query 5
-- Case B: column-mode partial update on (k, v2), auto_increment v1 missing.
USE sql_tests_auto_increment_cpu;
CREATE TABLE t_cu (
  k BIGINT NOT NULL,
  v1 BIGINT AUTO_INCREMENT,
  v2 BIGINT,
  v3 BIGINT
) PRIMARY KEY (k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES ('replicated_storage' = 'true', 'replication_num' = '1');
INSERT INTO t_cu VALUES (1, DEFAULT, 3, 4);
SELECT * FROM t_cu;

-- query 6
-- Partial update (k, v2): k=1 keeps v1=1, k=2 is new (gets v1=2). v3 keeps old value for k=1.
USE sql_tests_auto_increment_cpu;
INSERT INTO t_cu (k, v2) VALUES (1,20),(2,40);
SELECT * FROM t_cu ORDER BY k;

-- query 7
-- Full-row INSERT overwrites everything with explicit values.
USE sql_tests_auto_increment_cpu;
INSERT INTO t_cu VALUES (1, 300, 20, 30), (2, 301, 40, 50);
SELECT * FROM t_cu ORDER BY k;

-- query 8
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_auto_increment_cpu FORCE;
