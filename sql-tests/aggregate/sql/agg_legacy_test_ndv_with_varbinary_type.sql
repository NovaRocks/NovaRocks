-- Migrated from dev/test/sql/test_agg_function/R/test_ndv_with_varbinary_type
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_ndv_with_varbinary_type FORCE;
CREATE DATABASE sql_tests_test_ndv_with_varbinary_type;
USE sql_tests_test_ndv_with_varbinary_type;

-- name: test_ndv_with_varbinary_type
-- query 2
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
CREATE TABLE tbinary_ndv_test (
    id INT,
    data VARBINARY,
    category INT
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
INSERT INTO tbinary_ndv_test
SELECT generate_series,
       to_binary(CONCAT('value_', CAST(generate_series AS VARCHAR)), 'utf8'),
       (generate_series - 1) % 5 + 1
FROM TABLE(GENERATE_SERIES(1, 100));

-- query 4
USE sql_tests_test_ndv_with_varbinary_type;
SELECT ndv(data) AS ndv_result FROM tbinary_ndv_test;

-- query 5
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category, ndv(data) AS ndv_result FROM tbinary_ndv_test GROUP BY category ORDER BY category;

-- query 6
USE sql_tests_test_ndv_with_varbinary_type;
SELECT approx_count_distinct(data) AS approx_count_result FROM tbinary_ndv_test;

-- query 7
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category, approx_count_distinct(data) AS approx_count_result FROM tbinary_ndv_test GROUP BY category ORDER BY category;

-- query 8
USE sql_tests_test_ndv_with_varbinary_type;
SELECT ds_hll_count_distinct(data) AS ds_hll_result FROM tbinary_ndv_test;

-- query 9
USE sql_tests_test_ndv_with_varbinary_type;
SELECT ds_hll_count_distinct(data, 10) AS ds_hll_result_with_logk FROM tbinary_ndv_test;

-- query 10
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category, ds_hll_count_distinct(data) AS ds_hll_result FROM tbinary_ndv_test GROUP BY category ORDER BY category;

-- query 11
-- @expect_error=unsupported agg function: ds_theta_count_distinct
USE sql_tests_test_ndv_with_varbinary_type;
SELECT ds_theta_count_distinct(data) AS theta_result FROM tbinary_ndv_test;

-- query 12
-- @expect_error=unsupported agg function: ds_theta_count_distinct
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category, ds_theta_count_distinct(data) AS theta_result FROM tbinary_ndv_test GROUP BY category ORDER BY category;

-- query 13
USE sql_tests_test_ndv_with_varbinary_type;
SELECT COUNT(DISTINCT data) AS distinct_count FROM tbinary_ndv_test;

-- query 14
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category, COUNT(DISTINCT data) AS distinct_count FROM tbinary_ndv_test GROUP BY category ORDER BY category;

-- query 15
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
CREATE TABLE tbinary_ndv_null_test (
    id INT,
    data VARBINARY,
    category INT
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 16
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
INSERT INTO tbinary_ndv_null_test VALUES
(1, to_binary('aaa', 'utf8'), 1),
(2, to_binary('bbb', 'utf8'), 1),
(3, NULL, 1),
(4, to_binary('aaa', 'utf8'), 2),
(5, to_binary('ccc', 'utf8'), 2),
(6, NULL, 2),
(7, to_binary('ddd', 'utf8'), 3),
(8, to_binary('eee', 'utf8'), 3),
(9, to_binary('fff', 'utf8'), 3);

-- query 17
USE sql_tests_test_ndv_with_varbinary_type;
SELECT ndv(data) AS ndv_with_null FROM tbinary_ndv_null_test;

-- query 18
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category, ndv(data) AS ndv_with_null FROM tbinary_ndv_null_test GROUP BY category ORDER BY category;

-- query 19
USE sql_tests_test_ndv_with_varbinary_type;
SELECT COUNT(DISTINCT data) AS distinct_count_with_null FROM tbinary_ndv_null_test;

-- query 20
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category, COUNT(DISTINCT data) AS distinct_count_with_null FROM tbinary_ndv_null_test GROUP BY category ORDER BY category;

-- query 21
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
CREATE TABLE tbinary_ndv_dup_test (
    id INT,
    data VARBINARY
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 22
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
INSERT INTO tbinary_ndv_dup_test VALUES
(1, to_binary('same_value', 'utf8')),
(2, to_binary('same_value', 'utf8')),
(3, to_binary('same_value', 'utf8')),
(4, to_binary('different_value', 'utf8')),
(5, to_binary('different_value', 'utf8')),
(6, NULL),
(7, NULL);

-- query 23
USE sql_tests_test_ndv_with_varbinary_type;
SELECT ndv(data) AS ndv_dup_result FROM tbinary_ndv_dup_test;

-- query 24
USE sql_tests_test_ndv_with_varbinary_type;
SELECT COUNT(DISTINCT data) AS distinct_count_dup FROM tbinary_ndv_dup_test;

-- query 25
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
CREATE TABLE tbinary_multi_distinct_test (
    id INT,
    data1 VARBINARY,
    data2 VARBINARY,
    data3 VARBINARY,
    category INT
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 26
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
INSERT INTO tbinary_multi_distinct_test VALUES
(1, to_binary('value1', 'utf8'), to_binary('val1', 'utf8'), to_binary('v1', 'utf8'), 1),
(2, to_binary('value2', 'utf8'), to_binary('val1', 'utf8'), to_binary('v1', 'utf8'), 1),
(3, to_binary('value1', 'utf8'), to_binary('val2', 'utf8'), to_binary('v2', 'utf8'), 1),
(4, to_binary('value3', 'utf8'), to_binary('val2', 'utf8'), to_binary('v1', 'utf8'), 2),
(5, to_binary('value4', 'utf8'), to_binary('val3', 'utf8'), to_binary('v3', 'utf8'), 2),
(6, to_binary('value3', 'utf8'), to_binary('val3', 'utf8'), to_binary('v2', 'utf8'), 2),
(7, to_binary('value5', 'utf8'), to_binary('val4', 'utf8'), to_binary('v4', 'utf8'), 3),
(8, to_binary('value6', 'utf8'), to_binary('val4', 'utf8'), to_binary('v4', 'utf8'), 3),
(9, to_binary('value5', 'utf8'), to_binary('val5', 'utf8'), to_binary('v5', 'utf8'), 3);

-- query 27
USE sql_tests_test_ndv_with_varbinary_type;
SELECT COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2,
       COUNT(DISTINCT data3) AS distinct_data3
FROM tbinary_multi_distinct_test;

-- query 28
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category,
       COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2,
       COUNT(DISTINCT data3) AS distinct_data3
FROM tbinary_multi_distinct_test
GROUP BY category
ORDER BY category;

-- query 29
USE sql_tests_test_ndv_with_varbinary_type;
SELECT ndv(data1) AS ndv_data1,
       approx_count_distinct(data2) AS approx_data2,
       ds_hll_count_distinct(data3) AS hll_data3
FROM tbinary_multi_distinct_test;

-- query 30
-- @expect_error=unsupported agg function: ds_theta_count_distinct
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category,
       ndv(data1) AS ndv_data1,
       approx_count_distinct(data2) AS approx_data2,
       ds_theta_count_distinct(data3) AS theta_data3
FROM tbinary_multi_distinct_test
GROUP BY category
ORDER BY category;

-- query 31
USE sql_tests_test_ndv_with_varbinary_type;
SELECT COUNT(DISTINCT data1) AS exact_count1,
       ndv(data2) AS approx_count2,
       COUNT(DISTINCT data3) AS exact_count3,
       ds_hll_count_distinct(data1) AS hll_count1
FROM tbinary_multi_distinct_test;

-- query 32
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category,
       COUNT(DISTINCT data1) AS exact_count1,
       ndv(data2) AS approx_count2,
       approx_count_distinct(data3) AS approx_count3
FROM tbinary_multi_distinct_test
GROUP BY category
ORDER BY category;

-- query 33
USE sql_tests_test_ndv_with_varbinary_type;
SELECT COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2,
       COUNT(*) AS total_rows,
       MAX(id) AS max_id
FROM tbinary_multi_distinct_test;

-- query 34
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category,
       COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2,
       COUNT(*) AS total_rows,
       MIN(id) AS min_id,
       MAX(id) AS max_id
FROM tbinary_multi_distinct_test
GROUP BY category
ORDER BY category;

-- query 35
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category,
       COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2,
       ndv(data3) AS ndv_data3,
       COUNT(*) AS total
FROM tbinary_multi_distinct_test
WHERE id > 2
GROUP BY category
HAVING COUNT(DISTINCT data1) > 1
ORDER BY category;

-- query 36
-- @expect_error=unsupported agg function: ds_theta_count_distinct
USE sql_tests_test_ndv_with_varbinary_type;
SELECT ndv(data1) AS ndv1,
       approx_count_distinct(data2) AS approx2,
       ds_hll_count_distinct(data3) AS hll3,
       ds_theta_count_distinct(data1) AS theta1
FROM tbinary_multi_distinct_test;

-- query 37
-- @expect_error=unsupported agg function: ds_theta_count_distinct
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category,
       ndv(data1) AS ndv1,
       approx_count_distinct(data2) AS approx2,
       ds_hll_count_distinct(data3) AS hll3,
       ds_theta_count_distinct(data1) AS theta1
FROM tbinary_multi_distinct_test
GROUP BY category
ORDER BY category;

-- query 38
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
CREATE TABLE tbinary_multi_null_test (
    id INT,
    data1 VARBINARY,
    data2 VARBINARY,
    category INT
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 39
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
INSERT INTO tbinary_multi_null_test VALUES
(1, to_binary('val1', 'utf8'), to_binary('valA', 'utf8'), 1),
(2, NULL, to_binary('valA', 'utf8'), 1),
(3, to_binary('val1', 'utf8'), NULL, 1),
(4, NULL, NULL, 1),
(5, to_binary('val2', 'utf8'), to_binary('valB', 'utf8'), 2),
(6, to_binary('val2', 'utf8'), NULL, 2),
(7, NULL, to_binary('valB', 'utf8'), 2);

-- query 40
USE sql_tests_test_ndv_with_varbinary_type;
SELECT COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2
FROM tbinary_multi_null_test;

-- query 41
USE sql_tests_test_ndv_with_varbinary_type;
SELECT category,
       COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2
FROM tbinary_multi_null_test
GROUP BY category
ORDER BY category;

-- query 42
USE sql_tests_test_ndv_with_varbinary_type;
SELECT COUNT(DISTINCT data1) AS exact1,
       ndv(data2) AS approx2,
       approx_count_distinct(data1) AS approx1,
       COUNT(DISTINCT data2) AS exact2
FROM tbinary_multi_null_test;

-- query 43
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
DROP TABLE IF EXISTS tbinary_ndv_test;

-- query 44
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
DROP TABLE IF EXISTS tbinary_ndv_null_test;

-- query 45
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
DROP TABLE IF EXISTS tbinary_ndv_dup_test;

-- query 46
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
DROP TABLE IF EXISTS tbinary_multi_distinct_test;

-- query 47
-- @skip_result_check=true
USE sql_tests_test_ndv_with_varbinary_type;
DROP TABLE IF EXISTS tbinary_multi_null_test;
