-- @order_sensitive=true
-- @tags=write_path,auto_increment,partial_update,primary_key
-- Test Objective:
-- 1. Validate partial update (INSERT with only PK columns) correctly allocates
--    auto_increment IDs for new rows via the lake writer (miss_auto_increment_column path).
-- 2. Validate that repeated upserts to existing keys preserve the original auto_increment value.
-- 3. Validate new keys in subsequent inserts get fresh sequential IDs.

-- query 1
-- @skip_result_check=true
SET catalog default_catalog;
DROP DATABASE IF EXISTS sql_tests_auto_increment_pu FORCE;
CREATE DATABASE sql_tests_auto_increment_pu;
USE sql_tests_auto_increment_pu;
ADMIN SET FRONTEND CONFIG ('auto_increment_cache_size' = '0');
CREATE TABLE t (
  k STRING NOT NULL,
  v1 BIGINT AUTO_INCREMENT,
  created DATETIME NULL DEFAULT CURRENT_TIMESTAMP
) PRIMARY KEY (k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES (
  'replication_num' = '1',
  'enable_persistent_index' = 'true',
  'replicated_storage' = 'true'
);

-- query 2
-- First insert: k='a' is new, gets v1=1.
USE sql_tests_auto_increment_pu;
INSERT INTO t (k) VALUES ('a');
SELECT k, v1 FROM t ORDER BY k;

-- query 3
-- Upsert k='a' (existing, keeps v1=1), k='b' is new (gets v1=2).
USE sql_tests_auto_increment_pu;
INSERT INTO t (k) VALUES ('a'),('b');
SELECT k, v1 FROM t ORDER BY k;

-- query 4
-- Upsert k='a','b' (existing, keep), k='c' is new (gets v1=3).
USE sql_tests_auto_increment_pu;
INSERT INTO t (k) VALUES ('a'),('b'),('c');
SELECT k, v1 FROM t ORDER BY k;

-- query 5
-- All existing upserts, then k='d' is new (gets v1=4).
USE sql_tests_auto_increment_pu;
INSERT INTO t (k) VALUES ('a'),('b'),('c'),('d');
SELECT k, v1 FROM t ORDER BY k;

-- query 6
-- Repeated upserts then k='e' is new (gets v1=5).
USE sql_tests_auto_increment_pu;
INSERT INTO t (k) VALUES ('a'),('b'),('c'),('d');
INSERT INTO t (k) VALUES ('a'),('b'),('c'),('d');
INSERT INTO t (k) VALUES ('a'),('b'),('c'),('d');
INSERT INTO t (k) VALUES ('a'),('b'),('c'),('d');
INSERT INTO t (k) VALUES ('a'),('b'),('e');
SELECT k, v1 FROM t ORDER BY k;

-- query 7
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_auto_increment_pu FORCE;
