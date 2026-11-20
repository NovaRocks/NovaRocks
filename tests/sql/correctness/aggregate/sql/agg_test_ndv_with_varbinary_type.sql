-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Migrated from dev/test/sql/test_agg_function/R/test_ndv_with_varbinary_type
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_ndv_with_varbinary_type
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE tbinary_ndv_test (
    id INT,
    data VARBINARY,
    category INT
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
INSERT INTO tbinary_ndv_test
SELECT generate_series,
       to_binary(CONCAT('value_', CAST(generate_series AS VARCHAR)), 'utf8'),
       (generate_series - 1) % 5 + 1
FROM TABLE(GENERATE_SERIES(1, 100));

-- query 4
USE ${case_db};
SELECT ndv(data) AS ndv_result FROM tbinary_ndv_test;

-- query 5
USE ${case_db};
SELECT category, ndv(data) AS ndv_result FROM tbinary_ndv_test GROUP BY category ORDER BY category;

-- query 6
USE ${case_db};
SELECT approx_count_distinct(data) AS approx_count_result FROM tbinary_ndv_test;

-- query 7
USE ${case_db};
SELECT category, approx_count_distinct(data) AS approx_count_result FROM tbinary_ndv_test GROUP BY category ORDER BY category;

-- query 8
USE ${case_db};
SELECT COUNT(DISTINCT data) AS distinct_count FROM tbinary_ndv_test;

-- query 9
USE ${case_db};
SELECT category, COUNT(DISTINCT data) AS distinct_count FROM tbinary_ndv_test GROUP BY category ORDER BY category;

-- query 10
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE tbinary_ndv_null_test (
    id INT,
    data VARBINARY,
    category INT
) WITH (format_version = 3);

-- query 11
-- @skip_result_check=true
USE ${case_db};
INSERT INTO tbinary_ndv_null_test VALUES
ROW(1, to_binary('aaa', 'utf8'), 1),
ROW(2, to_binary('bbb', 'utf8'), 1),
ROW(3, NULL, 1),
ROW(4, to_binary('aaa', 'utf8'), 2),
ROW(5, to_binary('ccc', 'utf8'), 2),
ROW(6, NULL, 2),
ROW(7, to_binary('ddd', 'utf8'), 3),
ROW(8, to_binary('eee', 'utf8'), 3),
ROW(9, to_binary('fff', 'utf8'), 3);

-- query 12
USE ${case_db};
SELECT ndv(data) AS ndv_with_null FROM tbinary_ndv_null_test;

-- query 13
USE ${case_db};
SELECT category, ndv(data) AS ndv_with_null FROM tbinary_ndv_null_test GROUP BY category ORDER BY category;

-- query 14
USE ${case_db};
SELECT COUNT(DISTINCT data) AS distinct_count_with_null FROM tbinary_ndv_null_test;

-- query 15
USE ${case_db};
SELECT category, COUNT(DISTINCT data) AS distinct_count_with_null FROM tbinary_ndv_null_test GROUP BY category ORDER BY category;

-- query 16
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE tbinary_ndv_dup_test (
    id INT,
    data VARBINARY
) WITH (format_version = 3);

-- query 17
-- @skip_result_check=true
USE ${case_db};
INSERT INTO tbinary_ndv_dup_test VALUES
ROW(1, to_binary('same_value', 'utf8')),
ROW(2, to_binary('same_value', 'utf8')),
ROW(3, to_binary('same_value', 'utf8')),
ROW(4, to_binary('different_value', 'utf8')),
ROW(5, to_binary('different_value', 'utf8')),
ROW(6, NULL),
ROW(7, NULL);

-- query 18
USE ${case_db};
SELECT ndv(data) AS ndv_dup_result FROM tbinary_ndv_dup_test;

-- query 19
USE ${case_db};
SELECT COUNT(DISTINCT data) AS distinct_count_dup FROM tbinary_ndv_dup_test;

-- query 20
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE tbinary_multi_distinct_test (
    id INT,
    data1 VARBINARY,
    data2 VARBINARY,
    data3 VARBINARY,
    category INT
) WITH (format_version = 3);

-- query 21
-- @skip_result_check=true
USE ${case_db};
INSERT INTO tbinary_multi_distinct_test VALUES
ROW(1, to_binary('value1', 'utf8'), to_binary('val1', 'utf8'), to_binary('v1', 'utf8'), 1),
ROW(2, to_binary('value2', 'utf8'), to_binary('val1', 'utf8'), to_binary('v1', 'utf8'), 1),
ROW(3, to_binary('value1', 'utf8'), to_binary('val2', 'utf8'), to_binary('v2', 'utf8'), 1),
ROW(4, to_binary('value3', 'utf8'), to_binary('val2', 'utf8'), to_binary('v1', 'utf8'), 2),
ROW(5, to_binary('value4', 'utf8'), to_binary('val3', 'utf8'), to_binary('v3', 'utf8'), 2),
ROW(6, to_binary('value3', 'utf8'), to_binary('val3', 'utf8'), to_binary('v2', 'utf8'), 2),
ROW(7, to_binary('value5', 'utf8'), to_binary('val4', 'utf8'), to_binary('v4', 'utf8'), 3),
ROW(8, to_binary('value6', 'utf8'), to_binary('val4', 'utf8'), to_binary('v4', 'utf8'), 3),
ROW(9, to_binary('value5', 'utf8'), to_binary('val5', 'utf8'), to_binary('v5', 'utf8'), 3);

-- query 22
USE ${case_db};
SELECT COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2,
       COUNT(DISTINCT data3) AS distinct_data3
FROM tbinary_multi_distinct_test;

-- query 23
USE ${case_db};
SELECT category,
       COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2,
       COUNT(DISTINCT data3) AS distinct_data3
FROM tbinary_multi_distinct_test
GROUP BY category
ORDER BY category;

-- query 24
USE ${case_db};
SELECT ndv(data1) AS ndv_data1,
       approx_count_distinct(data2) AS approx_data2
FROM tbinary_multi_distinct_test;

-- query 25
-- @expect_error=unsupported agg function: ds_theta_count_distinct
USE ${case_db};
SELECT category,
       ndv(data1) AS ndv_data1,
       approx_count_distinct(data2) AS approx_data2
FROM tbinary_multi_distinct_test
GROUP BY category
ORDER BY category;

-- query 26
USE ${case_db};
SELECT COUNT(DISTINCT data1) AS exact_count1,
       ndv(data2) AS approx_count2,
       COUNT(DISTINCT data3) AS exact_count3
FROM tbinary_multi_distinct_test;

-- query 27
USE ${case_db};
SELECT category,
       COUNT(DISTINCT data1) AS exact_count1,
       ndv(data2) AS approx_count2,
       approx_count_distinct(data3) AS approx_count3
FROM tbinary_multi_distinct_test
GROUP BY category
ORDER BY category;

-- query 28
USE ${case_db};
SELECT COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2,
       COUNT(*) AS total_rows,
       MAX(id) AS max_id
FROM tbinary_multi_distinct_test;

-- query 29
USE ${case_db};
SELECT category,
       COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2,
       COUNT(*) AS total_rows,
       MIN(id) AS min_id,
       MAX(id) AS max_id
FROM tbinary_multi_distinct_test
GROUP BY category
ORDER BY category;

-- query 30
USE ${case_db};
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

-- query 31
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE tbinary_multi_null_test (
    id INT,
    data1 VARBINARY,
    data2 VARBINARY,
    category INT
) WITH (format_version = 3);

-- query 32
-- @skip_result_check=true
USE ${case_db};
INSERT INTO tbinary_multi_null_test VALUES
ROW(1, to_binary('val1', 'utf8'), to_binary('valA', 'utf8'), 1),
ROW(2, NULL, to_binary('valA', 'utf8'), 1),
ROW(3, to_binary('val1', 'utf8'), NULL, 1),
ROW(4, NULL, NULL, 1),
ROW(5, to_binary('val2', 'utf8'), to_binary('valB', 'utf8'), 2),
ROW(6, to_binary('val2', 'utf8'), NULL, 2),
ROW(7, NULL, to_binary('valB', 'utf8'), 2);

-- query 33
USE ${case_db};
SELECT COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2
FROM tbinary_multi_null_test;

-- query 34
USE ${case_db};
SELECT category,
       COUNT(DISTINCT data1) AS distinct_data1,
       COUNT(DISTINCT data2) AS distinct_data2
FROM tbinary_multi_null_test
GROUP BY category
ORDER BY category;

-- query 35
USE ${case_db};
SELECT COUNT(DISTINCT data1) AS exact1,
       ndv(data2) AS approx2,
       approx_count_distinct(data1) AS approx1,
       COUNT(DISTINCT data2) AS exact2
FROM tbinary_multi_null_test;

-- query 36
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS tbinary_ndv_test;

-- query 37
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS tbinary_ndv_null_test;

-- query 38
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS tbinary_ndv_dup_test;

-- query 39
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS tbinary_multi_distinct_test;

-- query 40
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS tbinary_multi_null_test;
