-- @order_sensitive=true
-- Test Objective:
-- 1. Validate analyze sample/full table on various column types.
-- 2. Verify column_statistics population after full analyze and empty after sample analyze.
-- 3. Verify drop stats clears column_statistics.
-- Note: This case modifies global FE config; run with -j 1.

-- query 1
-- @skip_result_check=true
admin set frontend config('enable_statistic_collect_on_first_load'='false');
CREATE TABLE ${case_db}.t1 (
    `k1`  date,
    `k2`  datetime,
    `k3`  char(20),
    `k4`  varchar(20),
    `k5`  boolean,
    `k6`  tinyint,
    `k7`  smallint,
    `k8`  int,
    `k9`  bigint,
    `k10` largeint,
    `k11` float,
    `k12` double,
    `k13` decimal(27,9))
DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3
PROPERTIES ('replication_num' = '1');
INSERT INTO t1 VALUES
    ('2020-10-22','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889),
    ('2020-10-23','2020-10-24 12:12:12','k3','k4',0,0,2,3,4,5,1.1,1.12,2.889),
    ('2020-10-24','2020-10-25 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889),
    (null,'2020-10-26 12:12:12',null,null,null,null,null,null,null,null,1,1.12,2.889),
    (null,'2020-10-27 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889);

-- query 2
-- @skip_result_check=true
drop stats t1;
analyze sample table t1;

-- query 3
select column_name, row_count from _statistics_.column_statistics where table_name = '${case_db}.t1' order by column_name;

-- query 4
-- @skip_result_check=true
drop stats t1;
analyze full table t1(k1,k2);

-- query 5
select column_name, row_count from _statistics_.column_statistics where table_name = '${case_db}.t1' order by column_name;

-- query 6
-- @skip_result_check=true
drop stats t1;
analyze full table t1(k3,k4);

-- query 7
select column_name, row_count from _statistics_.column_statistics where table_name = '${case_db}.t1' order by column_name;

-- query 8
-- @skip_result_check=true
drop stats t1;
analyze sample table t1(k3,k4);

-- query 9
select column_name, row_count from _statistics_.column_statistics where table_name = '${case_db}.t1' order by column_name;

-- query 10
-- @skip_result_check=true
drop stats t1;
analyze sample table t1;

-- query 11
select column_name, row_count from _statistics_.column_statistics where table_name = '${case_db}.t1' order by column_name;

-- query 12
-- @skip_result_check=true
drop stats t1;
analyze full table t1;

-- query 13
select column_name, row_count from _statistics_.column_statistics where table_name = '${case_db}.t1' order by column_name;

-- query 14
drop stats t1;
select column_name, row_count from _statistics_.column_statistics where table_name = '${case_db}.t1' order by column_name;

-- query 15
-- @skip_result_check=true
drop stats t1;
analyze sample table t1;

-- query 16
select column_name, row_count from _statistics_.column_statistics where table_name = '${case_db}.t1' order by column_name;

-- query 17
-- @skip_result_check=true
drop stats t1;
admin set frontend config('enable_statistic_collect_on_first_load'='true');
