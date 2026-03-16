-- Migrated from dev/test/sql/test_ddl/T/alter_table
-- Test Objective:
-- 1. Validate ALTER TABLE COMMENT updates the table comment.
-- 2. Validate SHOW CREATE TABLE reflects updated comment.
-- 3. Validate ALTER TABLE ORDER BY on PRIMARY KEY table (sort key reorder).
-- 4. Validate ALTER TABLE ADD COLUMN on PRIMARY KEY table.
-- 5. Validate ALTER TABLE MODIFY COLUMN on PRIMARY KEY table.
-- Each schema change is followed by a wait for the ALTER job to finish,
-- plus a brief sleep to allow the table lock to fully clear.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t1;
CREATE TABLE ${case_db}.t1 (id1 int) DUPLICATE KEY(id1) COMMENT "c1"
  DISTRIBUTED BY HASH(id1) BUCKETS 1
  PROPERTIES('replication_num' = '1');
ALTER TABLE ${case_db}.t1 COMMENT="c2";

-- query 2
-- SHOW CREATE TABLE should reflect the updated comment "c2"
-- @result_contains=COMMENT "c2"
SHOW CREATE TABLE ${case_db}.t1;

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.test_pk_tbl1;
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
INSERT INTO ${case_db}.test_pk_tbl1 VALUES
  ('2023-09-07', '2023-09-07 13:35:00', 'a', 'a', true, 1, 1, 1, 10),
  ('2023-09-06', '2023-09-07 13:35:00', 'a', 'a', true, 1, 1, 1, 10);

-- query 4
SELECT count(1) FROM ${case_db}.test_pk_tbl1;

-- query 5
-- @skip_result_check=true
ALTER TABLE ${case_db}.test_pk_tbl1 ORDER BY (k3);

-- query 6
-- Wait for ORDER BY schema change to finish
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
SHOW ALTER TABLE COLUMN FROM ${case_db} ORDER BY JobId DESC LIMIT 1;

-- query 7
-- Extra sleep to allow the table lock to fully clear after ALTER finishes
-- @skip_result_check=true
SELECT sleep(10);

-- query 8
SELECT count(1) FROM ${case_db}.test_pk_tbl1;

-- query 9
-- @skip_result_check=true
ALTER TABLE ${case_db}.test_pk_tbl1 ORDER BY (k3,v1,v3,k2);

-- query 10
-- Wait for ORDER BY schema change to finish
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
SHOW ALTER TABLE COLUMN FROM ${case_db} ORDER BY JobId DESC LIMIT 1;

-- query 11
-- Extra sleep to allow the table lock to fully clear after ALTER finishes
-- @skip_result_check=true
SELECT sleep(10);

-- query 12
SELECT count(1) FROM ${case_db}.test_pk_tbl1;

-- query 13
-- @skip_result_check=true
ALTER TABLE ${case_db}.test_pk_tbl1 ADD COLUMN v5 bitmap AFTER v4;

-- query 14
-- Wait for ADD COLUMN to finish
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
SHOW ALTER TABLE COLUMN FROM ${case_db} ORDER BY JobId DESC LIMIT 1;

-- query 15
-- Extra sleep to allow the table lock to fully clear after ALTER finishes
-- @skip_result_check=true
SELECT sleep(10);

-- query 16
SELECT count(1) FROM ${case_db}.test_pk_tbl1;

-- query 17
-- @skip_result_check=true
ALTER TABLE ${case_db}.test_pk_tbl1 ADD COLUMN (v6 bitmap NOT NULL, v7 bitmap);

-- query 18
-- Wait for ADD COLUMN (multi) to finish
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
SHOW ALTER TABLE COLUMN FROM ${case_db} ORDER BY JobId DESC LIMIT 1;

-- query 19
-- Extra sleep to allow the table lock to fully clear after ALTER finishes
-- @skip_result_check=true
SELECT sleep(10);

-- query 20
SELECT count(1) FROM ${case_db}.test_pk_tbl1;

-- query 21
-- @skip_result_check=true
ALTER TABLE ${case_db}.test_pk_tbl1 MODIFY COLUMN v6 bitmap NULL;

-- query 22
-- Wait for MODIFY COLUMN to finish
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
SHOW ALTER TABLE COLUMN FROM ${case_db} ORDER BY JobId DESC LIMIT 1;

-- query 23
-- Extra sleep to allow the table lock to fully clear after ALTER finishes
-- @skip_result_check=true
SELECT sleep(10);

-- query 24
SELECT count(1) FROM ${case_db}.test_pk_tbl1;
