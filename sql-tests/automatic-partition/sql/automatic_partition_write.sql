-- @order_sensitive=true
-- Test Objective:
-- 1. Validate pipeline INSERT...SELECT creating automatic partitions progressively.
-- 2. Validate TRUNCATE on auto-partitioned table and re-insert.
-- 3. Validate auto-increment column with auto partition.
-- 4. Validate multi-fragment cross-table INSERT with auto partition.
-- Migrated from: dev/test/sql/test_automatic_partition/T/test_automatic_partition

-- query 1
-- Pipeline: create table and insert initial data
-- @skip_result_check=true
CREATE TABLE ${case_db}.ss(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.ss VALUES('2023-02-14', 2),('2033-03-01', 2);

-- query 2
-- After initial insert: 2 rows
SELECT COUNT(*) FROM ${case_db}.ss;

-- query 3
-- Insert from select with add_months (+1 month), creating new partitions
-- @skip_result_check=true
INSERT INTO ${case_db}.ss SELECT date(add_months(event_day, 1)), pv FROM ${case_db}.ss;

-- query 4
SELECT COUNT(*) FROM ${case_db}.ss;

-- query 5
-- Insert from select with add_months (+6 months)
-- @skip_result_check=true
INSERT INTO ${case_db}.ss SELECT date(add_months(event_day, 6)), pv FROM ${case_db}.ss;

-- query 6
SELECT COUNT(*) FROM ${case_db}.ss;

-- query 7
-- Insert from select with add_months (+12 months)
-- @skip_result_check=true
INSERT INTO ${case_db}.ss SELECT date(add_months(event_day, 12)), pv FROM ${case_db}.ss;

-- query 8
SELECT COUNT(*) FROM ${case_db}.ss;

-- query 9
-- Insert full self-copy
-- @skip_result_check=true
INSERT INTO ${case_db}.ss SELECT * FROM ${case_db}.ss;

-- query 10
SELECT COUNT(*) FROM ${case_db}.ss;

-- query 11
-- Truncate: create table, insert, verify
CREATE TABLE ${case_db}.ss_trunc(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.ss_trunc VALUES('2000-01-01', 2);
SELECT * FROM ${case_db}.ss_trunc ORDER BY event_day;

-- query 12
-- Truncate the table
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.ss_trunc;

-- query 13
-- Re-insert into a different partition after truncate
INSERT INTO ${case_db}.ss_trunc VALUES('2002-01-01', 2);
SELECT * FROM ${case_db}.ss_trunc ORDER BY event_day;

-- query 14
-- Auto-increment column with auto partition by day
CREATE TABLE ${case_db}.t_autoinc(
  id DATE NOT NULL,
  job1 BIGINT NOT NULL AUTO_INCREMENT)
PRIMARY KEY (id)
PARTITION BY date_trunc('day', id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.t_autoinc VALUES('2021-01-01', 1);
SELECT COUNT(*) FROM ${case_db}.t_autoinc;

-- query 15
-- @skip_result_check=true
INSERT INTO ${case_db}.t_autoinc VALUES('2021-01-03', default);

-- query 16
SELECT COUNT(*) FROM ${case_db}.t_autoinc;

-- query 17
-- @skip_result_check=true
INSERT INTO ${case_db}.t_autoinc VALUES('2021-01-05', default);

-- query 18
SELECT COUNT(*) FROM ${case_db}.t_autoinc;

-- query 19
-- Multi-fragment: create source table with known data across multiple partitions
-- @skip_result_check=true
CREATE TABLE ${case_db}.ss_src(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.ss_src VALUES
  ('2023-02-14', 2),('2023-03-14', 2),('2023-04-14', 2),
  ('2023-05-14', 2),('2023-06-14', 2),('2023-07-14', 2),
  ('2033-03-01', 2),('2033-04-01', 2),('2033-05-01', 2),
  ('2033-06-01', 2),('2033-07-01', 2),('2033-08-01', 2);

-- query 20
SELECT COUNT(*) FROM ${case_db}.ss_src;

-- query 21
-- Cross-table insert into new auto-partitioned table
CREATE TABLE ${case_db}.dt_dest(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.dt_dest SELECT * FROM ${case_db}.ss_src;
SELECT COUNT(*) FROM ${case_db}.dt_dest;
