-- @order_sensitive=true
-- Test Objective:
-- 1. Validate automatic partition by date_trunc with various granularities (month/day/year).
-- 2. Cover both DATE and DATETIME column types.
-- 3. Verify data correctness after automatic partition creation.
-- 4. Validate date_trunc('minute') with DATETIME.
-- 5. Validate partition column not being the first column.
-- Migrated from: dev/test/sql/test_automatic_partition/T/test_automatic_partition

-- query 1
-- Basic auto partition by month, no explicit replication_num
CREATE TABLE ${case_db}.ss_basic(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 1;
INSERT INTO ${case_db}.ss_basic VALUES('2021-01-01', 1);
SELECT * FROM ${case_db}.ss_basic ORDER BY event_day;

-- query 2
-- Auto partition by day with DATETIME (single replica)
CREATE TABLE ${case_db}.site_access(
  event_day DATETIME NOT NULL,
  site_id INT DEFAULT '10',
  city_code VARCHAR(100),
  user_name VARCHAR(32) DEFAULT '',
  pv BIGINT DEFAULT '0')
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY date_trunc('day', event_day)
DISTRIBUTED BY HASH(event_day, site_id)
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.site_access VALUES('2015-06-02 00:00:00', 1, 1, 'a', 1);
INSERT INTO ${case_db}.site_access VALUES('2015-06-02 00:10:00', 1, 1, 'a', 1);
INSERT INTO ${case_db}.site_access VALUES('2015-06-04 00:10:00', 1, 1, 'a', 1);
SELECT * FROM ${case_db}.site_access ORDER BY event_day;

-- query 3
-- date_trunc('month') with DATE
CREATE TABLE ${case_db}.dt_month_date(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.dt_month_date VALUES('2023-02-14', 2);
INSERT INTO ${case_db}.dt_month_date VALUES('2023-03-14', 2);
SELECT * FROM ${case_db}.dt_month_date ORDER BY event_day;

-- query 4
-- date_trunc('day') with DATE
CREATE TABLE ${case_db}.dt_day_date(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('day', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.dt_day_date VALUES('2023-02-14', 2);
INSERT INTO ${case_db}.dt_day_date VALUES('2023-03-14', 2);
SELECT * FROM ${case_db}.dt_day_date ORDER BY event_day;

-- query 5
-- date_trunc('year') with DATE
CREATE TABLE ${case_db}.dt_year_date(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('year', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.dt_year_date VALUES('2023-02-14', 2);
INSERT INTO ${case_db}.dt_year_date VALUES('2024-03-14', 2);
SELECT * FROM ${case_db}.dt_year_date ORDER BY event_day;

-- query 6
-- date_trunc('week') and date_trunc('quarter') with DATE - verify creation
-- @skip_result_check=true
CREATE TABLE ${case_db}.dt_week_date(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('week', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
CREATE TABLE ${case_db}.dt_quarter_date(event_day DATE, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('quarter', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");

-- query 7
-- date_trunc('month') with DATETIME
CREATE TABLE ${case_db}.dt_month_dt(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.dt_month_dt VALUES('2023-02-14 12:31:07', 2);
INSERT INTO ${case_db}.dt_month_dt VALUES('2023-03-14 12:31:07', 2);
SELECT * FROM ${case_db}.dt_month_dt ORDER BY event_day;

-- query 8
-- date_trunc('day') with DATETIME
CREATE TABLE ${case_db}.dt_day_dt(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('day', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.dt_day_dt VALUES('2023-02-14 12:31:07', 2);
INSERT INTO ${case_db}.dt_day_dt VALUES('2023-03-14 12:31:07', 2);
SELECT * FROM ${case_db}.dt_day_dt ORDER BY event_day;

-- query 9
-- date_trunc('year') with DATETIME
CREATE TABLE ${case_db}.dt_year_dt(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('year', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.dt_year_dt VALUES('2023-02-14 12:31:07', 2);
INSERT INTO ${case_db}.dt_year_dt VALUES('2024-03-14 12:31:07', 2);
SELECT * FROM ${case_db}.dt_year_dt ORDER BY event_day;

-- query 10
-- hour/minute/second/week/quarter for DATETIME - verify creation
-- @skip_result_check=true
CREATE TABLE ${case_db}.dt_hour(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('hour', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
CREATE TABLE ${case_db}.dt_minute(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('minute', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");
CREATE TABLE ${case_db}.dt_second(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('second', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "1");

-- query 11
-- date_trunc('minute') insert and count
CREATE TABLE ${case_db}.dt_minute_data(k datetime)
PARTITION BY date_trunc('minute', k)
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.dt_minute_data VALUES(now());
SELECT COUNT(*) FROM ${case_db}.dt_minute_data;

-- query 12
-- Partition column as second column in schema
CREATE TABLE ${case_db}.p_second(c1 int null, c2 date null)
DUPLICATE KEY(c1, c2)
PARTITION BY date_trunc('day', c2)
DISTRIBUTED BY HASH(c1) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.p_second VALUES(1, '2021-01-01');
SELECT * FROM ${case_db}.p_second ORDER BY c1;

-- query 13
-- Auto partition DATETIME in new node context
CREATE TABLE ${case_db}.dt_new_node(event_day DATETIME, pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.dt_new_node VALUES('2023-02-14 12:31:07', 2);
INSERT INTO ${case_db}.dt_new_node VALUES('2023-03-14 12:31:07', 2);
SELECT * FROM ${case_db}.dt_new_node ORDER BY event_day;
