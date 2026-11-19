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

-- name: test_decimal256_subquery_cte
-- Tests: Scalar subqueries, EXISTS, IN, CTEs with decimal256 values

-- Create main test table
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.decimal_main_test (
    id INT,
    category VARCHAR(10),
    d50_15 DECIMAL(38,15),
    d76_20 DECIMAL(38,20)
) WITH (format_version = 3);

-- Create secondary test table for joins
CREATE TABLE ${case_db}.decimal_secondary_test (
    id INT,
    ref_category VARCHAR(10),
    threshold_d50 DECIMAL(38,15),
    threshold_d76 DECIMAL(38,20)
) WITH (format_version = 3);

-- Insert test data into main table - using Iceberg-compatible decimal values
INSERT INTO ${case_db}.decimal_main_test VALUES
ROW(1, 'A', 123456789012.123456789012345, 1234567890.12345678901234567890),
ROW(2, 'A', 987654321098.456789012345678, 9876543210.45678901234567890123),
ROW(3, 'B', 555555555555.789012345678901, 5555555555.78901234567890123456),
ROW(4, 'B', 777777777777.012345678901234, 7777777777.01234567890123456789),
ROW(5, 'C', 444444444444.567890123456789, 4444444444.56789012345678901234),
ROW(6, 'C', 888888888888.901234567890123, 8888888888.90123456789012345678),
ROW(7, 'A', 111111111111.234567890123456, 1111111111.23456789012345678901),
ROW(8, 'B', 222222222222.678901234567890, 2222222222.67890123456789012345),
ROW(9, 'C', 999999999999.345678901234567, 9999999999.34567890123456789012),
ROW(10, 'A', 666666666666.111111111111111, 6666666666.11111111111111111111);

-- Insert test data into secondary table - using Iceberg-compatible threshold values
INSERT INTO ${case_db}.decimal_secondary_test VALUES
ROW(1, 'A', 500000000000.000000000000000, 5000000000.00000000000000000000),
ROW(2, 'B', 600000000000.000000000000000, 6000000000.00000000000000000000),
ROW(3, 'C', 700000000000.000000000000000, 7000000000.00000000000000000000);

-- query 2
SELECT
    'Test1_SCALAR_SUBQUERY_SELECT' as test_name,
    id,
    category,
    d50_15,
    CAST((SELECT AVG(d50_15) FROM ${case_db}.decimal_main_test) AS DECIMAL(38,15)) as overall_avg,
    d50_15 - CAST((SELECT AVG(d50_15) FROM ${case_db}.decimal_main_test) AS DECIMAL(38,15)) as diff_from_avg
FROM ${case_db}.decimal_main_test
ORDER BY id;

-- query 3
SELECT
    'Test2_SCALAR_SUBQUERY_WHERE' as test_name,
    id,
    category,
    d50_15,
    d76_20
FROM ${case_db}.decimal_main_test
WHERE d50_15 > (SELECT AVG(d50_15) FROM ${case_db}.decimal_main_test)
ORDER BY d50_15;

-- query 4
SELECT
    'Test3_EXISTS_SUBQUERY' as test_name,
    mt.id,
    mt.category,
    mt.d50_15
FROM ${case_db}.decimal_main_test mt
WHERE EXISTS (
    SELECT 1 FROM ${case_db}.decimal_secondary_test st
    WHERE st.ref_category = mt.category
    AND mt.d50_15 > st.threshold_d50
)
ORDER BY mt.id;

-- query 5
SELECT
    'Test4_IN_SUBQUERY' as test_name,
    id,
    category,
    d76_20
FROM ${case_db}.decimal_main_test
WHERE d76_20 IN (
    SELECT d76_20 FROM ${case_db}.decimal_main_test
    WHERE category = 'A'
    AND d76_20 > 100.00000000000000000000
)
ORDER BY d76_20;

-- query 6
WITH decimal_stats AS (
    SELECT
        category,
        AVG(d50_15) as avg_d50,
        AVG(d76_20) as avg_d76,
        COUNT(*) as cnt
    FROM ${case_db}.decimal_main_test
    GROUP BY category
)
SELECT
    'Test6_BASIC_CTE' as test_name,
    ds.category,
    ds.avg_d50,
    ds.avg_d76,
    ds.cnt,
    mt.d50_15,
    mt.d50_15 - ds.avg_d50 as diff_from_cat_avg
FROM decimal_stats ds
JOIN ${case_db}.decimal_main_test mt ON ds.category = mt.category
ORDER BY ds.category, mt.d50_15;

-- query 7
WITH category_stats AS (
    SELECT
        category,
        SUM(d50_15) as total_d50,
        MAX(d76_20) as max_d76
    FROM ${case_db}.decimal_main_test
    GROUP BY category
),
threshold_data AS (
    SELECT
        ref_category,
        AVG(threshold_d50) as avg_threshold
    FROM ${case_db}.decimal_secondary_test
    GROUP BY ref_category
)
SELECT
    'Test8_MULTIPLE_CTE' as test_name,
    cs.category,
    cs.total_d50,
    cs.max_d76,
    td.avg_threshold,
    CASE
        WHEN cs.total_d50 > td.avg_threshold * 3 THEN 'HIGH'
        WHEN cs.total_d50 > td.avg_threshold THEN 'MEDIUM'
        ELSE 'LOW'
    END as performance_level
FROM category_stats cs
LEFT JOIN threshold_data td ON cs.category = td.ref_category
ORDER BY cs.category;
