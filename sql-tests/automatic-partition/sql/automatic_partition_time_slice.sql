-- @order_sensitive=true
-- Test Objective:
-- 1. Validate automatic partition by time_slice with various intervals (4 month, 4 day, 2 year).
-- 2. Verify data correctness and partition creation.
-- 3. Validate time_slice with 10 minute interval.
-- Migrated from: dev/test/sql/test_automatic_partition/T/test_automatic_partition

-- query 1
-- time_slice(event_day, interval 4 month)
CREATE TABLE ${case_db}.ss_month(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY time_slice(event_day, interval 4 month)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.ss_month VALUES('2023-02-14 12:31:07', 2);
INSERT INTO ${case_db}.ss_month VALUES('2023-04-14 12:31:07', 2);
INSERT INTO ${case_db}.ss_month VALUES('2023-07-14 12:31:07', 2);
SELECT * FROM ${case_db}.ss_month ORDER BY event_day;

-- query 2
-- time_slice(event_day, interval 4 day)
CREATE TABLE ${case_db}.ss_day(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY time_slice(event_day, interval 4 day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.ss_day VALUES('2023-02-14 12:31:07', 2);
INSERT INTO ${case_db}.ss_day VALUES('2023-02-16 12:31:07', 2);
INSERT INTO ${case_db}.ss_day VALUES('2023-02-19 12:31:07', 2);
SELECT * FROM ${case_db}.ss_day ORDER BY event_day;

-- query 3
-- time_slice(event_day, interval 2 year)
CREATE TABLE ${case_db}.ss_year(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY time_slice(event_day, interval 2 year)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.ss_year VALUES('2023-02-14 12:31:07', 2);
INSERT INTO ${case_db}.ss_year VALUES('2024-02-16 12:31:07', 2);
INSERT INTO ${case_db}.ss_year VALUES('2026-02-19 12:31:07', 2);
SELECT * FROM ${case_db}.ss_year ORDER BY event_day;

-- query 4
-- time_slice with 10 minute interval
CREATE TABLE ${case_db}.ss_minute(k datetime)
PARTITION BY time_slice(k, interval 10 minute)
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.ss_minute VALUES(now());
SELECT COUNT(*) FROM ${case_db}.ss_minute;
