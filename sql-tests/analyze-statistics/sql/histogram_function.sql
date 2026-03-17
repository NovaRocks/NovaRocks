-- Test Objective:
-- 1. Validate histogram analysis on all column types with MCV values.
-- 2. Validate histogram analysis with HLL bucket NDV mode.
-- 3. Validate histogram analysis with sample bucket NDV mode.

-- query 1
-- @skip_result_check=true
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
INSERT INTO t1 (k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13)
SELECT
    date_add('2020-01-01', s1),
    minutes_add('2020-01-01 00:00:00', s1),
    cast(s1 as string),
    cast(s1 as string),
    cast(s1 as boolean),
    cast(s1 as tinyint),
    cast(s1 as smallint),
    cast(s1 as int),
    cast(s1 as bigint),
    cast(s1 as largeint),
    cast(s1 as float),
    cast(s1 as double),
    cast(s1 as decimal(27,9))
FROM
    (SELECT * FROM TABLE(generate_series(1, 10000))  AS t(s1) ) r;
INSERT INTO t1 (k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13)
WITH
    series1 AS (SELECT * FROM TABLE(generate_series(1, 10000))  AS t(s1) ),
    mcv AS (SELECT s1 % 10 as s1 FROM series1)
SELECT
    date_add('2020-01-01', s1),
    minutes_add('2020-01-01 00:00:00', s1),
    cast(s1 as string),
    cast(s1 as string),
    cast(s1 as boolean),
    cast(s1 as tinyint),
    cast(s1 as smallint),
    cast(s1 as int),
    cast(s1 as bigint),
    cast(s1 as largeint),
    cast(s1 as float),
    cast(s1 as double),
    cast(s1 as decimal(27,9))
FROM
    mcv;

-- query 2
-- @skip_result_check=true
ANALYZE TABLE t1 UPDATE HISTOGRAM ON k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13 PROPERTIES('histogram_sample_ratio'='1.0', 'histogram_mcv_size'='10');

-- query 3
-- @skip_result_check=true
-- @result_contains=k1
-- @result_contains=k13
SELECT column_name, buckets FROM _statistics_.histogram_statistics where table_name='${case_db}.t1' order by column_name;

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
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
INSERT INTO t2 (k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13)
SELECT
    date_add('2020-01-01', s1),
    minutes_add('2020-01-01 00:00:00', s1),
    cast(s1 as string),
    cast(s1 as string),
    cast(s1 as boolean),
    cast(s1 as tinyint),
    cast(s1 as smallint),
    cast(s1 as int),
    cast(s1 as bigint),
    cast(s1 as largeint),
    cast(s1 as float),
    cast(s1 as double),
    cast(s1 as decimal(27,9))
FROM
    (SELECT * FROM TABLE(generate_series(1, 10000, 3))  AS t(s1) ) r;
INSERT INTO t2 (k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13)
SELECT
    date_add('2020-01-01', s1),
    minutes_add('2020-01-01 00:00:00', s1),
    cast(s1 as string),
    cast(s1 as string),
    cast(s1 as boolean),
    cast(s1 as tinyint),
    cast(s1 as smallint),
    cast(s1 as int),
    cast(s1 as bigint),
    cast(s1 as largeint),
    cast(s1 as float),
    cast(s1 as double),
    cast(s1 as decimal(27,9))
FROM
    (SELECT * FROM TABLE(generate_series(1, 10000, 3))  AS t(s1) ) r;

-- query 5
-- @skip_result_check=true
ANALYZE TABLE t2 UPDATE HISTOGRAM ON k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13 PROPERTIES('histogram_sample_ratio'='1.0', 'histogram_mcv_size'='0', 'histogram_collect_bucket_ndv_mode' = 'hll');

-- query 6
-- @skip_result_check=true
-- @result_contains=k1
-- @result_contains=k13
SELECT column_name, buckets FROM _statistics_.histogram_statistics where table_name='${case_db}.t2' order by column_name;

-- query 7
-- @skip_result_check=true
ANALYZE TABLE t2 UPDATE HISTOGRAM ON k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13 PROPERTIES('histogram_sample_ratio'='1.0', 'histogram_mcv_size'='0', 'histogram_collect_bucket_ndv_mode' = 'sample');

-- query 8
-- @skip_result_check=true
-- @result_contains=k1
-- @result_contains=k13
SELECT column_name, buckets FROM _statistics_.histogram_statistics where table_name='${case_db}.t2' order by column_name;
