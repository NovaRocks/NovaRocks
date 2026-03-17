-- Test Objective:
-- 1. Validate basic analyze table populates column_statistics.
-- 2. Verify drop stats clears column_statistics.
-- 3. Validate analyze histogram and drop histogram on specific columns.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.duplicate_table_with_null (
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
INSERT INTO duplicate_table_with_null VALUES
    ('2020-10-22','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889),
    ('2020-10-23','2020-10-24 12:12:12','k3','k4',0,0,2,3,4,5,1.1,1.12,2.889),
    ('2020-10-24','2020-10-25 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889),
    (null,'2020-10-26 12:12:12',null,null,null,null,null,null,null,null,1,1.12,2.889),
    (null,'2020-10-27 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889);

-- query 2
-- @skip_result_check=true
-- @result_contains=OK
analyze table duplicate_table_with_null;

-- query 3
select count(1) from default_catalog._statistics_.column_statistics where table_name='${case_db}.duplicate_table_with_null';

-- query 4
-- @skip_result_check=true
drop stats duplicate_table_with_null;

-- query 5
-- @skip_result_check=true
-- @result_contains=OK
analyze table duplicate_table_with_null update histogram on k1,k6;

-- query 6
select count(1) from _statistics_.histogram_statistics where table_name='${case_db}.duplicate_table_with_null';

-- query 7
-- @skip_result_check=true
analyze table duplicate_table_with_null drop histogram on k1,k6;
