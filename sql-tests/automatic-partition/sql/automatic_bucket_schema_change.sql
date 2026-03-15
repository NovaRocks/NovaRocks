-- @order_sensitive=true
-- Test Objective:
-- 1. Validate schema change (ADD COLUMN, ORDER BY) on auto-bucket expression-partitioned table.
-- 2. Validate sync materialized view on auto-bucket expression-partitioned table.
-- Migrated from: dev/test/sql/test_automatic_bucket/T/test_automatic_partition

-- query 1
-- Setup: create expression-partitioned table with bucket_size=1 and insert data
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_sc(k date, v int) DUPLICATE KEY(k) PARTITION BY DATE_TRUNC('DAY', k)
PROPERTIES ("replication_num" = "1", "bucket_size" = "1");
INSERT INTO ${case_db}.t_sc VALUES('2021-01-01', 1);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-03', 1);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-05', 1);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-01', 1);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-01', 2);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-03', 2);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-05', 2);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-01', 2);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-01', 3);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-03', 3);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-05', 3);
INSERT INTO ${case_db}.t_sc VALUES('2021-01-01', 3);

-- query 2
-- Verify initial data: 12 rows
SELECT * FROM ${case_db}.t_sc ORDER BY k, v;

-- query 3
-- ALTER TABLE ADD COLUMN, then wait for schema change to finish
-- @skip_result_check=true
-- @wait_alter_column=t_sc
ALTER TABLE ${case_db}.t_sc ADD COLUMN c bigint;

-- query 4
-- Verify new column has NULL values
SELECT k, v, c FROM ${case_db}.t_sc ORDER BY k, v;

-- query 5
-- ALTER TABLE ORDER BY to reorder sort key
-- @skip_result_check=true
-- @wait_alter_column=t_sc
ALTER TABLE ${case_db}.t_sc ORDER BY (k, c, v);

-- query 6
-- Verify data after ORDER BY (column display order depends on fast_schema_evolution)
SELECT k, v, c FROM ${case_db}.t_sc ORDER BY k, v;

-- query 7
-- Setup MV test: create table and insert data
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_mv(k date, v int, v1 int) DUPLICATE KEY(k) PARTITION BY DATE_TRUNC('DAY', k)
PROPERTIES ("replication_num" = "1", "bucket_size" = "1");
INSERT INTO ${case_db}.t_mv VALUES('2021-01-01', 1, 1);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-03', 1, 1);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-05', 1, 1);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-01', 1, 1);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-01', 2, 2);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-03', 2, 2);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-05', 2, 2);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-01', 2, 2);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-01', 3, 3);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-03', 3, 3);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-05', 3, 3);
INSERT INTO ${case_db}.t_mv VALUES('2021-01-01', 3, 3);

-- query 8
-- Verify initial data
SELECT * FROM ${case_db}.t_mv ORDER BY k, v, v1;

-- query 9
-- Create sync materialized view, then wait for rollup to finish
-- @skip_result_check=true
-- @wait_alter_rollup=t_mv
USE ${case_db};
CREATE MATERIALIZED VIEW mv AS SELECT k, v1 FROM t_mv;

-- query 10
-- Query using the MV columns
SELECT k, v1 FROM ${case_db}.t_mv ORDER BY k, v1;
