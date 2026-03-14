-- Migrated from dev/test/sql/test_agg_function/R/test_ds_hll.sql
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_ds_hll FORCE;
CREATE DATABASE sql_tests_test_ds_hll;
USE sql_tests_test_ds_hll;

-- name: test_ds_hll
-- query 2
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
CREATE TABLE t1 (
  id BIGINT,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- query 3
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
CREATE TABLE t2 (
  `id` bigint,
  `dt` varchar(10),
  `ds_id` binary,
  `ds_province` binary,
  `ds_age` binary,
  `ds_dt` binary
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- query 4
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
CREATE TABLE t3 (
  `id` bigint NOT NULL,
  `dt` varchar(10) NOT NULL,
  `ds_id` binary,
  `ds_province` binary,
  `ds_age` binary,
  `ds_dt` binary
) ENGINE=OLAP
PRIMARY KEY(id, dt)
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- query 5
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));

-- query 6
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
INSERT INTO t1 VALUES (NULL, NULL, NULL, NULL), (NULL, 'a', 1, '2024-07-24'), (1, NULL, 1, '2024-07-24');

-- query 7
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
INSERT INTO t2 SELECT id, dt,
  ds_hll_count_distinct_state(id),
  ds_hll_count_distinct_state(province, 10),
  ds_hll_count_distinct_state(age, 20, "HLL_6"),
  ds_hll_count_distinct_state(dt, 10, "HLL_8") FROM t1;

-- query 8
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
INSERT INTO t2 VALUES (NULL, NULL, NULL, NULL, NULL, NULL), (NULL, 'a', to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64')), (1, NULL, to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'));

-- query 9
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
INSERT INTO t3 SELECT * FROM t2 WHERE id IS NOT NULL AND dt IS NOT NULL;

-- query 10
-- @expect_error=Resolved function ds_hll_count_distinct_union has no binary as argument type.
USE sql_tests_test_ds_hll;
select DS_HLL_COMBINE(id) from t1;

-- query 11
-- @expect_error=Resolved function ds_hll_count_distinct_union has no binary as argument type.
USE sql_tests_test_ds_hll;
select DS_HLL_COMBINE(dt) from t1;

-- query 12
-- @expect_error=Resolved function ds_hll_count_distinct_merge has no binary as argument type.
USE sql_tests_test_ds_hll;
select DS_HLL_ESTIMATE(id) from t1;

-- query 13
-- @expect_error=Resolved function ds_hll_count_distinct_merge has no binary as argument type.
USE sql_tests_test_ds_hll;
select DS_HLL_ESTIMATE(dt) from t1;

-- query 14
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
SELECT dt, DS_HLL_ACCUMULATE(id), DS_HLL_ACCUMULATE(province, 20),  DS_HLL_ACCUMULATE(age, 12, "HLL_6"), DS_HLL_ACCUMULATE(dt) FROM t1 GROUP BY dt ORDER BY 1 limit 3;

-- query 15
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
SELECT id, DS_HLL_ACCUMULATE(id), DS_HLL_ACCUMULATE(province),  DS_HLL_ACCUMULATE(age), DS_HLL_ACCUMULATE(dt) FROM t1 GROUP BY id ORDER BY 1 limit 3;

-- query 16
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
SELECT DS_HLL_ACCUMULATE(id), DS_HLL_ACCUMULATE(province),  DS_HLL_ACCUMULATE(age), DS_HLL_ACCUMULATE(dt) FROM t1;

-- query 17
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
SELECT DS_HLL_COMBINE(ds_id), DS_HLL_COMBINE(ds_province), DS_HLL_COMBINE(ds_age), DS_HLL_COMBINE(ds_dt) FROM t2;

-- query 18
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
SELECT dt, DS_HLL_COMBINE(ds_id), DS_HLL_COMBINE(ds_province), DS_HLL_COMBINE(ds_age), DS_HLL_COMBINE(ds_dt) FROM t2 GROUP BY dt ORDER BY 1 limit 3;

-- query 19
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
SELECT id, DS_HLL_COMBINE(ds_id), DS_HLL_COMBINE(ds_province), DS_HLL_COMBINE(ds_age), DS_HLL_COMBINE(ds_dt) FROM t2 GROUP BY id ORDER BY 1 limit 3;

-- query 20
USE sql_tests_test_ds_hll;
SELECT
  DS_HLL_ESTIMATE(ds_id) BETWEEN 950 AND 1050 AS id_ok,
  DS_HLL_ESTIMATE(ds_province) BETWEEN 900 AND 1100 AS province_ok,
  DS_HLL_ESTIMATE(ds_age) BETWEEN 90 AND 110 AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 2 AS dt_ok
FROM t2;

-- query 21
USE sql_tests_test_ds_hll;
SELECT
  IFNULL(dt, 'NULL') AS dt_key,
  CASE WHEN dt = '2024-07-24' THEN DS_HLL_ESTIMATE(ds_id) BETWEEN 950 AND 1050 ELSE DS_HLL_ESTIMATE(ds_id) = 1 END AS id_ok,
  CASE WHEN dt = '2024-07-24' THEN DS_HLL_ESTIMATE(ds_province) BETWEEN 900 AND 1100 ELSE DS_HLL_ESTIMATE(ds_province) = 1 END AS province_ok,
  CASE WHEN dt = '2024-07-24' THEN DS_HLL_ESTIMATE(ds_age) BETWEEN 90 AND 110 ELSE DS_HLL_ESTIMATE(ds_age) = 1 END AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 1 AS dt_ok
FROM t2
GROUP BY dt
ORDER BY 1
limit 3;

-- query 22
USE sql_tests_test_ds_hll;
SELECT
  IFNULL(CAST(id AS STRING), 'NULL') AS id_key,
  CASE WHEN id = 1 THEN DS_HLL_ESTIMATE(ds_id) = 2 ELSE DS_HLL_ESTIMATE(ds_id) = 1 END AS id_ok,
  CASE WHEN id = 1 THEN DS_HLL_ESTIMATE(ds_province) = 1 ELSE DS_HLL_ESTIMATE(ds_province) = 1 END AS province_ok,
  CASE WHEN id = 1 THEN DS_HLL_ESTIMATE(ds_age) = 2 ELSE DS_HLL_ESTIMATE(ds_age) = 1 END AS age_ok,
  CASE WHEN id = 1 THEN DS_HLL_ESTIMATE(ds_dt) = 2 ELSE DS_HLL_ESTIMATE(ds_dt) = 1 END AS dt_ok
FROM t2
GROUP BY id
ORDER BY 1
limit 3;

-- query 23
USE sql_tests_test_ds_hll;
SELECT
  DS_HLL_ESTIMATE(ds_id) BETWEEN 950 AND 1050 AS id_ok,
  DS_HLL_ESTIMATE(ds_province) BETWEEN 900 AND 1100 AS province_ok,
  DS_HLL_ESTIMATE(ds_age) BETWEEN 90 AND 110 AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 1 AS dt_ok
FROM t3;

-- query 24
USE sql_tests_test_ds_hll;
SELECT
  dt,
  DS_HLL_ESTIMATE(ds_id) BETWEEN 950 AND 1050 AS id_ok,
  DS_HLL_ESTIMATE(ds_province) BETWEEN 900 AND 1100 AS province_ok,
  DS_HLL_ESTIMATE(ds_age) BETWEEN 90 AND 110 AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 1 AS dt_ok
FROM t3
GROUP BY dt
ORDER BY 1
limit 3;

-- query 25
USE sql_tests_test_ds_hll;
SELECT
  CAST(id AS STRING) AS id_key,
  DS_HLL_ESTIMATE(ds_id) = 1 AS id_ok,
  CASE WHEN id = 1 THEN DS_HLL_ESTIMATE(ds_province) IN (0, 1) ELSE DS_HLL_ESTIMATE(ds_province) = 1 END AS province_ok,
  DS_HLL_ESTIMATE(ds_age) = 1 AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 1 AS dt_ok
FROM t3
GROUP BY id
ORDER BY 1
limit 3;

-- query 26
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
INSERT INTO t2 SELECT id, dt,
  ds_hll_count_distinct_state(id),
  ds_hll_count_distinct_state(province, 10),
  ds_hll_count_distinct_state(age, 20, "HLL_6"),
  ds_hll_count_distinct_state(dt, 10, "HLL_8") FROM t1;

-- query 27
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
INSERT INTO t2 VALUES (NULL, NULL, NULL, NULL, NULL, NULL), (NULL, 'a', to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64')), (1, NULL, to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'));

-- query 28
-- @skip_result_check=true
USE sql_tests_test_ds_hll;
INSERT INTO t3 SELECT * FROM t2 WHERE id IS NOT NULL AND dt IS NOT NULL;

-- query 29
USE sql_tests_test_ds_hll;
SELECT
  DS_HLL_ESTIMATE(ds_id) BETWEEN 950 AND 1050 AS id_ok,
  DS_HLL_ESTIMATE(ds_province) BETWEEN 900 AND 1100 AS province_ok,
  DS_HLL_ESTIMATE(ds_age) BETWEEN 90 AND 110 AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 2 AS dt_ok
FROM t2;

-- query 30
USE sql_tests_test_ds_hll;
SELECT
  IFNULL(dt, 'NULL') AS dt_key,
  CASE WHEN dt = '2024-07-24' THEN DS_HLL_ESTIMATE(ds_id) BETWEEN 950 AND 1050 ELSE DS_HLL_ESTIMATE(ds_id) = 1 END AS id_ok,
  CASE WHEN dt = '2024-07-24' THEN DS_HLL_ESTIMATE(ds_province) BETWEEN 900 AND 1100 ELSE DS_HLL_ESTIMATE(ds_province) = 1 END AS province_ok,
  CASE WHEN dt = '2024-07-24' THEN DS_HLL_ESTIMATE(ds_age) BETWEEN 90 AND 110 ELSE DS_HLL_ESTIMATE(ds_age) = 1 END AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 1 AS dt_ok
FROM t2
GROUP BY dt
ORDER BY 1
limit 3;

-- query 31
USE sql_tests_test_ds_hll;
SELECT
  IFNULL(CAST(id AS STRING), 'NULL') AS id_key,
  CASE WHEN id = 1 THEN DS_HLL_ESTIMATE(ds_id) = 2 ELSE DS_HLL_ESTIMATE(ds_id) = 1 END AS id_ok,
  CASE WHEN id IS NULL OR id = 1 THEN DS_HLL_ESTIMATE(ds_province) = 2 ELSE DS_HLL_ESTIMATE(ds_province) = 1 END AS province_ok,
  CASE WHEN id IS NULL OR id = 1 THEN DS_HLL_ESTIMATE(ds_age) = 2 ELSE DS_HLL_ESTIMATE(ds_age) = 1 END AS age_ok,
  CASE WHEN id IS NULL OR id = 1 THEN DS_HLL_ESTIMATE(ds_dt) = 2 ELSE DS_HLL_ESTIMATE(ds_dt) = 1 END AS dt_ok
FROM t2
GROUP BY id
ORDER BY 1
limit 3;

-- query 32
USE sql_tests_test_ds_hll;
SELECT
  DS_HLL_ESTIMATE(ds_id) BETWEEN 950 AND 1050 AS id_ok,
  DS_HLL_ESTIMATE(ds_province) BETWEEN 900 AND 1100 AS province_ok,
  DS_HLL_ESTIMATE(ds_age) BETWEEN 90 AND 110 AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 1 AS dt_ok
FROM t3;

-- query 33
USE sql_tests_test_ds_hll;
SELECT
  dt,
  DS_HLL_ESTIMATE(ds_id) BETWEEN 950 AND 1050 AS id_ok,
  DS_HLL_ESTIMATE(ds_province) BETWEEN 900 AND 1100 AS province_ok,
  DS_HLL_ESTIMATE(ds_age) BETWEEN 90 AND 110 AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 1 AS dt_ok
FROM t3
GROUP BY dt
ORDER BY 1
limit 3;

-- query 34
USE sql_tests_test_ds_hll;
SELECT
  CAST(id AS STRING) AS id_key,
  DS_HLL_ESTIMATE(ds_id) = 1 AS id_ok,
  CASE WHEN id = 1 THEN DS_HLL_ESTIMATE(ds_province) IN (0, 1) ELSE DS_HLL_ESTIMATE(ds_province) = 1 END AS province_ok,
  DS_HLL_ESTIMATE(ds_age) = 1 AS age_ok,
  DS_HLL_ESTIMATE(ds_dt) = 1 AS dt_ok
FROM t3
GROUP BY id
ORDER BY 1
limit 3;
