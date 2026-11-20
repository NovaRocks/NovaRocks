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

-- Test Objective:
-- 1. Validate ROW_NUMBER, RANK, DENSE_RANK over decimal256 ORDER BY columns.
-- 2. Validate PARTITION BY category ranking with decimal256 order keys.
-- 3. Validate SUM OVER, AVG OVER, COUNT OVER, MAX OVER window aggregates with decimal256.
-- 4. Validate LAG and LEAD functions with decimal256 values and default fallback.
-- 5. Validate FIRST_VALUE and LAST_VALUE window functions with Iceberg-compatible decimal columns.
-- 6. Validate complex window expressions combining decimal256 arithmetic.
-- Migrated from dev/test/sql/test_decimal/T/test_decimal256_window_functions.sql

-- Create test table with Iceberg-compatible decimal types
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.decimal_window_test (
    id INT,
    category VARCHAR(10),
    d50_15 DECIMAL(38,15),    -- 35 integer digits + 15 decimal digits
    d76_20 DECIMAL(38,20),    -- 56 integer digits + 20 decimal digits
    d76_0 DECIMAL(38,0)       -- 76 integer digits + 0 decimal digits
) WITH (format_version = 3);

-- Insert test data - using Iceberg-compatible decimal values
-- Each category has multiple rows for proper window function testing
INSERT INTO ${case_db}.decimal_window_test VALUES
-- Category A (4 rows)
ROW(1, 'A', 123456789012.123456789012345, 1234567890.12345678901234567890, 12345678901234),
ROW(2, 'A', 987654321098.456789012345678, 9876543210.45678901234567890123, 98765432109876),
ROW(3, 'A', 555555555555.789012345678901, 5555555555.78901234567890123456, 55555555555555),
ROW(4, 'A', 666666666666.111111111111111, 6666666666.11111111111111111111, 66666666666666),

-- Category B (4 rows)
ROW(5, 'B', 777777777777.012345678901234, 7777777777.01234567890123456789, 77777777777777),
ROW(6, 'B', 444444444444.567890123456789, 4444444444.56789012345678901234, 44444444444444),
ROW(7, 'B', 888888888888.901234567890123, 8888888888.90123456789012345678, 88888888888888),
ROW(8, 'B', 333333333333.222222222222222, 3333333333.22222222222222222222, 33333333333333),

-- Category C (4 rows)
ROW(9, 'C', 111111111111.234567890123456, 1111111111.23456789012345678901, 11111111111111),
ROW(10, 'C', 222222222222.678901234567890, 2222222222.67890123456789012345, 22222222222222),
ROW(11, 'C', 999998888877.345678901234567, 9999988888.34567890123456789012, 99999888887777),
ROW(12, 'C', 777776666655.777777777777777, 7777766666.77777777777777777777, 77777666665555);

-- query 2
-- Test 1: Basic ranking functions
SELECT
    'Test1_RANKING_FUNCTIONS' as test_name,
    id,
    category,
    d50_15,
    ROW_NUMBER() OVER (ORDER BY d50_15) as row_num,
    RANK() OVER (ORDER BY d50_15) as rank_val,
    DENSE_RANK() OVER (ORDER BY d50_15) as dense_rank_val
FROM ${case_db}.decimal_window_test
ORDER BY d50_15;

-- query 3
-- Test 2: Partition by category ranking
SELECT
    'Test2_PARTITION_RANKING' as test_name,
    id,
    category,
    d76_20,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY d76_20) as row_num_by_cat,
    RANK() OVER (PARTITION BY category ORDER BY d76_20) as rank_by_cat
FROM ${case_db}.decimal_window_test
ORDER BY category, d76_20;

-- query 4
-- Test 3: Window aggregate functions
SELECT
    'Test3_WINDOW_AGGREGATES' as test_name,
    id,
    category,
    d50_15,
    SUM(d50_15) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_sum,
    AVG(d50_15) OVER (PARTITION BY category ORDER BY id) as running_avg_by_cat,
    COUNT(*) OVER (PARTITION BY category) as count_by_cat,
    MAX(d76_0) OVER (PARTITION BY category ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_max
FROM ${case_db}.decimal_window_test
ORDER BY id;

-- query 5
-- Test 4: LEAD and LAG functions
SELECT
    'Test4_LEAD_LAG' as test_name,
    id,
    category,
    d76_20,
    LAG(d76_20, 1) OVER (ORDER BY d76_20) as prev_val,
    LEAD(d76_20, 1) OVER (ORDER BY d76_20) as next_val,
    LAG(d76_20, 1, 0.0) OVER (PARTITION BY category ORDER BY d76_20) as prev_val_by_cat,
    LEAD(d76_20, 1, 0.0) OVER (PARTITION BY category ORDER BY d76_20) as next_val_by_cat
FROM ${case_db}.decimal_window_test
ORDER BY d76_20;

-- query 6
-- Test 5: FIRST_VALUE and LAST_VALUE
SELECT
    'Test5_FIRST_LAST_VALUE' as test_name,
    id,
    category,
    d76_0,
    FIRST_VALUE(d76_0) OVER (PARTITION BY category ORDER BY d76_0) as first_val_by_cat,
    LAST_VALUE(d76_0) OVER (PARTITION BY category ORDER BY d76_0 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_val_by_cat,
    FIRST_VALUE(d50_15) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) as first_val_window
FROM ${case_db}.decimal_window_test
ORDER BY category, d76_0;

-- query 7
-- Test 6: Complex window expressions
SELECT
    'Test6_COMPLEX_WINDOW' as test_name,
    id,
    category,
    d50_15,
    d76_20,
    SUM(d50_15 * d76_20) OVER (PARTITION BY category ORDER BY id) as running_product_sum,
    AVG(d50_15 + d76_20) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as moving_sum_avg
FROM ${case_db}.decimal_window_test
ORDER BY id;
