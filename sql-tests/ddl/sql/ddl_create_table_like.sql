-- Migrated from dev/test/sql/test_ddl/T/create_table_like
-- Test Objective:
-- 1. Validate CREATE TABLE ... LIKE copies schema from a simple DUPLICATE KEY table.
-- 2. Validate CREATE TABLE ... LIKE copies schema from a complex PRIMARY KEY table with indexes and partitioning.
-- 3. Validate INSERT into the LIKE'd table works and count is correct.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t1;
DROP TABLE IF EXISTS ${case_db}.t2;
CREATE TABLE ${case_db}.t1 (id1 int, id2 int, id3 double)
  DUPLICATE KEY(id1)
  COMMENT "c1"
  DISTRIBUTED BY HASH(id1) BUCKETS 1
  PROPERTIES('replication_num' = '1');
CREATE TABLE ${case_db}.t2 LIKE ${case_db}.t1;

-- query 2
-- SHOW CREATE TABLE t2: should mirror t1's schema with COMMENT "c1"
-- @result_contains=DUPLICATE KEY(`id1`)
-- @result_contains=COMMENT "c1"
-- @result_contains=`id1` int
-- @result_contains=`id2` int
-- @result_contains=`id3` double
SHOW CREATE TABLE ${case_db}.t2;

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.test_pk_tbl1;
DROP TABLE IF EXISTS ${case_db}.test_pk_tbl2;
CREATE TABLE ${case_db}.test_pk_tbl1 (
  `k1` DATE,
  `k2` DATETIME,
  `k3` VARCHAR(20),
  `k4` VARCHAR(20),
  `k5` BOOLEAN,
  `v1` TINYINT NULL,
  `v2` SMALLINT NULL,
  `v3` INT NULL,
  `v4` BIGINT NOT NULL,
  INDEX init_bitmap_index (k2) USING BITMAP
) ENGINE=OLAP
PRIMARY KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
COMMENT "OLAP"
PARTITION BY RANGE (k1) (
    START ("1970-01-01") END ("2030-01-01") EVERY (INTERVAL 30 YEAR)
)
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 50
ORDER BY(`k2`, `v4`)
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "v2",
    "enable_persistent_index" = "true",
    "bloom_filter_columns" = "k1,k2"
);
CREATE TABLE ${case_db}.test_pk_tbl2 LIKE ${case_db}.test_pk_tbl1;
INSERT INTO ${case_db}.test_pk_tbl2 VALUES
  ('2023-09-07', '2023-09-07 13:35:00', 'a', 'a', true, 1, 1, 1, 10),
  ('2023-09-06', '2023-09-07 13:35:00', 'a', 'a', true, 1, 1, 1, 10);

-- query 4
-- test_pk_tbl2 is a LIKE copy; same partitioning and distribution; 2 rows inserted
SELECT count(1) FROM ${case_db}.test_pk_tbl2;
