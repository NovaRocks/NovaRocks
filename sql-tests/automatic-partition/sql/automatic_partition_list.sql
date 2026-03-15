-- @order_sensitive=true
-- Test Objective:
-- 1. Validate automatic list partition with multiple columns.
-- 2. Validate list partition with various data types (bigint, datetime, date).
-- 3. Validate drop partition and re-insert behavior.
-- 4. Validate ADD PARTITION limits on auto list partition table.
-- 5. Validate automatic partition idempotent reuse.
-- 6. Validate case-sensitive list partition names with varchar primary key.
-- Migrated from: dev/test/sql/test_automatic_partition

-- query 1
-- Multi-column list auto partition (dt, province)
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_recharge(
  id bigint, user_id bigint,
  recharge_money decimal(32,2),
  province varchar(20) not null,
  dt varchar(20) not null)
DUPLICATE KEY(id)
PARTITION BY (dt, province)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_recharge VALUES(1,1,1,'hangzhou','2022-04-01');

-- query 2
SELECT * FROM ${case_db}.t_recharge ORDER BY dt, province;

-- query 3
-- Drop partition and re-insert
-- @skip_result_check=true
ALTER TABLE ${case_db}.t_recharge DROP PARTITION p20220401_hangzhou FORCE;
INSERT INTO ${case_db}.t_recharge VALUES(1,1,1,'hangzhou','2022-04-01'),(1,1,1,'beijing','2022-03-01');

-- query 4
SELECT * FROM ${case_db}.t_recharge ORDER BY dt, province;

-- query 5
-- Auto partition by bigint column
CREATE TABLE ${case_db}.t_bigint(
  id bigint not null, user_id bigint not null,
  recharge_money decimal(32,2) not null,
  province varchar(20) not null,
  dt varchar(20) not null)
DUPLICATE KEY(id)
PARTITION BY (id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_bigint VALUES(1,1,1,'hangzhou','2022-04-01');
SELECT * FROM ${case_db}.t_bigint ORDER BY id;

-- query 6
-- Auto partition by datetime column
CREATE TABLE ${case_db}.t_datetime(
  dt datetime not null, user_id bigint not null,
  recharge_money decimal(32,2) not null,
  province varchar(20) not null,
  id varchar(20) not null)
DUPLICATE KEY(dt)
PARTITION BY (dt)
DISTRIBUTED BY HASH(dt) BUCKETS 10
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_datetime VALUES('2022-04-01',1,1,'hangzhou',1);
SELECT * FROM ${case_db}.t_datetime ORDER BY dt;

-- query 7
-- Auto partition by date column
CREATE TABLE ${case_db}.t_date(
  dt date not null, user_id bigint not null,
  recharge_money decimal(32,2) not null,
  province varchar(20) not null,
  id varchar(20) not null)
DUPLICATE KEY(dt)
PARTITION BY (dt)
DISTRIBUTED BY HASH(dt) BUCKETS 10
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_date VALUES('2022-04-01',1,1,'hangzhou',1);
SELECT * FROM ${case_db}.t_date ORDER BY dt;

-- query 8
-- ADD PARTITION single value to auto list partition table: allowed
-- @skip_result_check=true
CREATE TABLE ${case_db}.list_auto(
  id bigint, user_id bigint,
  recharge_money decimal(32,2),
  province varchar(20) not null,
  dt varchar(20) not null)
DUPLICATE KEY(id)
PARTITION BY (dt)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES ("replication_num" = "1");
ALTER TABLE ${case_db}.list_auto ADD PARTITION psingle VALUES IN ("2023-04-01");

-- query 9
-- ADD PARTITION multiple values to auto list partition: should error
-- @expect_error=Automatically partitioned tables does not support multiple values
ALTER TABLE ${case_db}.list_auto ADD PARTITION pmul VALUES IN ("2022-04-01", "2022-04-02");

-- query 10
-- Normal list partition table supports multiple values in ADD PARTITION
-- @skip_result_check=true
CREATE TABLE ${case_db}.list_normal(
  id bigint, user_id bigint,
  recharge_money decimal(32,2),
  province varchar(20) not null,
  dt varchar(20) not null)
DUPLICATE KEY(id)
PARTITION BY LIST (dt)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES ("replication_num" = "1");
ALTER TABLE ${case_db}.list_normal ADD PARTITION psingle VALUES IN ("2023-04-01");
ALTER TABLE ${case_db}.list_normal ADD PARTITION pmul VALUES IN ("2022-04-01", "2022-04-02");

-- query 11
-- Automatic partition idempotent reuse: re-inserting same partition values works
CREATE TABLE ${case_db}.t_reuse(
  c1 date NOT NULL, c2 varchar(20), c3 boolean NOT NULL)
DUPLICATE KEY(c1, c2)
PARTITION BY (c1, c3)
DISTRIBUTED BY HASH(c1)
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_reuse VALUES("2025-12-01", "1", true), ("2025-12-01", "0", false);
SELECT * FROM ${case_db}.t_reuse ORDER BY c2;

-- query 12
-- Re-insert same partition values: idempotent, duplicate data expected
INSERT INTO ${case_db}.t_reuse VALUES("2025-12-01", "1", true), ("2025-12-01", "0", false);
SELECT * FROM ${case_db}.t_reuse ORDER BY c2;

-- query 13
-- Case-sensitive varchar auto list partition (PRIMARY KEY table)
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_case(
  col1 varchar(100), col2 varchar(100), col3 bigint)
PRIMARY KEY (col1)
PARTITION BY (col1)
DISTRIBUTED BY HASH(col1) BUCKETS 5
ORDER BY (col2)
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_case VALUES('a.com','val1',100),('A.com','val1',200),('A.Com','val1',300);

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.t_case VALUES('a.com','val1',100),('A.com','val1',200),('A.Com','val1',300);

-- query 15
-- @skip_result_check=true
INSERT INTO ${case_db}.t_case VALUES('a.cOm','val1',100),('A.coM','val1',200),('A.COm','val1',300);

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.t_case VALUES('a.cOM','val1',100),('A.COM','val1',200),('a.COM','val1',300);

-- query 17
-- @skip_result_check=true
INSERT INTO ${case_db}.t_case VALUES('a.com','val1',100),('A.com','val1',200),('A.Com','val1',300);

-- query 18
-- @skip_result_check=true
INSERT INTO ${case_db}.t_case VALUES('a.cOm','val1',100),('A.coM','val1',200),('A.COm','val1',300);

-- query 19
-- Split: insert new partition value separately to avoid mixing new+existing partition in one INSERT
-- @skip_result_check=true
INSERT INTO ${case_db}.t_case VALUES('b.cOm','val1',100);
INSERT INTO ${case_db}.t_case VALUES('A.coM','val1',200),('A.COm','val1',300);

-- query 20
SELECT * FROM ${case_db}.t_case ORDER BY col1, col2, col3;

-- query 21
-- Partition-pruning query: exact match
SELECT * FROM ${case_db}.t_case WHERE col1 = 'a.com' ORDER BY col1, col2, col3;

-- query 22
SELECT * FROM ${case_db}.t_case WHERE col1 = 'A.com' ORDER BY col1, col2, col3;

-- query 23
SELECT * FROM ${case_db}.t_case WHERE col1 IN ('A.com', 'a.com') ORDER BY col1, col2, col3;
