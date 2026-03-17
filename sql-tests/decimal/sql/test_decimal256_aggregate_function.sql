-- Test Objective:
-- 1. Validate basic aggregate functions (COUNT, SUM, AVG, MIN, MAX) on DECIMAL(40..76) columns.
-- 2. Validate GROUP BY aggregation on category and decimal columns.
-- 3. Validate DISTINCT aggregates (COUNT/SUM/AVG DISTINCT) with both planner stages 0 and 2.
-- 4. Validate SELECT DISTINCT on single and multiple decimal columns.
-- 5. Validate HAVING with DISTINCT aggregates.
-- 6. Validate edge cases: empty result set, NULL-only rows, single-row result.
-- Migrated from dev/test/sql/test_decimal/T/test_decimal256_aggregate_function

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.decimal256_agg_test (
    id INT,
    category VARCHAR(10),
    p40s10 DECIMAL(40,10),
    p50s15 DECIMAL(50,15),
    p76s20 DECIMAL(76,20),
    p76s0 DECIMAL(76,0)
) properties("replication_num"="1");

INSERT INTO ${case_db}.decimal256_agg_test VALUES
(1, 'A', 100.1234567890, 100.123456789012345, 100.12345678901234567890, 100),
(2, 'A', 100.1234567890, 100.123456789012345, 100.12345678901234567890, 100),
(3, 'A', 100.1234567890, 100.123456789012345, 100.12345678901234567890, 100),
(4, 'B', 200.5555555555, 200.555555555555555, 200.55555555555555555555, 200),
(5, 'B', 200.5555555555, 200.555555555555555, 200.55555555555555555555, 200),
(6, 'C', 0.0000000000, 0.000000000000000, 0.00000000000000000000, 0),
(7, 'C', 0.0000000000, 0.000000000000000, 0.00000000000000000000, 0),
(8, 'C', 0.0000000000, 0.000000000000000, 0.00000000000000000000, 0),
(9, 'D', -50.9876543210, -50.987654321098765, -50.98765432109876543210, -50),
(10, 'D', -50.9876543210, -50.987654321098765, -50.98765432109876543210, -50),
(11, 'E', 300.1111111111, 300.111111111111111, 300.11111111111111111111, 300),
(12, 'F', 400.2222222222, 400.222222222222222, 400.22222222222222222222, 400),
(13, 'G', 500.3333333333, 500.333333333333333, 500.33333333333333333333, 500),
(14, 'H', 999999999999999999999999999999.9999999999,
          99999999999999999999999999999999999.999999999999999,
          99999999999999999999999999999999999999999999999999999999.99999999999999999999,
          9999999999999999999999999999999999999999999999999999999999999999999999999999),
(15, 'H', 999999999999999999999999999999.9999999999,
          99999999999999999999999999999999999.999999999999999,
          99999999999999999999999999999999999999999999999999999999.99999999999999999999,
          9999999999999999999999999999999999999999999999999999999999999999999999999999),
(16, 'I', 0.0000000001, 0.000000000000001, 0.00000000000000000001, 1),
(17, 'I', 0.0000000001, 0.000000000000001, 0.00000000000000000001, 1),
(18, 'J', NULL, NULL, NULL, NULL),
(19, 'J', 600.4444444444, 600.444444444444444, 600.44444444444444444444, 600),
(20, 'K', 777.7777777777, 777.777777777777777, 777.77777777777777777777, 777),
(21, 'K', 777.7777777777, 888.888888888888888, 999.99999999999999999999, 777),
(22, 'K', 777.7777777777, 777.777777777777777, 777.77777777777777777777, 888);

-- query 2
SELECT 'Test1_COUNT_BASIC' as test_name, COUNT(*) as total_rows FROM ${case_db}.decimal256_agg_test;

-- query 3
SELECT
    'Test1_COUNT_NON_NULL' as test_name,
    COUNT(p40s10) as count_p40s10,
    COUNT(p50s15) as count_p50s15,
    COUNT(p76s20) as count_p76s20,
    COUNT(p76s0) as count_p76s0
FROM ${case_db}.decimal256_agg_test;

-- query 4
SELECT
    'Test2_SUM' as test_name,
    SUM(p40s10) as sum_p40s10,
    SUM(p50s15) as sum_p50s15,
    SUM(p76s20) as sum_p76s20,
    SUM(p76s0) as sum_p76s0
FROM ${case_db}.decimal256_agg_test;

-- query 5
SELECT
    'Test3_AVG' as test_name,
    AVG(p40s10) as avg_p40s10,
    AVG(p50s15) as avg_p50s15,
    AVG(p76s20) as avg_p76s20,
    AVG(p76s0) as avg_p76s0
FROM ${case_db}.decimal256_agg_test;

-- query 6
SELECT
    'Test4_MIN_MAX' as test_name,
    MIN(p40s10) as min_p40s10,
    MAX(p40s10) as max_p40s10,
    MIN(p50s15) as min_p50s15,
    MAX(p50s15) as max_p50s15,
    MIN(p76s20) as min_p76s20,
    MAX(p76s20) as max_p76s20,
    MIN(p76s0) as min_p76s0,
    MAX(p76s0) as max_p76s0
FROM ${case_db}.decimal256_agg_test;

-- query 7
SELECT
    'Test5_GROUP_BY_CATEGORY' as test_name,
    category,
    COUNT(*) as row_count,
    SUM(p40s10) as sum_p40s10,
    AVG(p40s10) as avg_p40s10,
    MIN(p50s15) as min_p50s15,
    MAX(p76s20) as max_p76s20
FROM ${case_db}.decimal256_agg_test
GROUP BY category
ORDER BY category;

-- query 8
SELECT
    'Test6_GROUP_BY_P40S10' as test_name,
    p40s10,
    COUNT(*) as row_count,
    SUM(p50s15) as sum_p50s15,
    AVG(p50s15) as avg_p50s15,
    MIN(p76s0) as min_p76s0,
    MAX(p76s0) as max_p76s0
FROM ${case_db}.decimal256_agg_test
WHERE p40s10 IS NOT NULL
GROUP BY p40s10
ORDER BY p40s10;

-- query 9
set new_planner_agg_stage=0;
SELECT
    'Test7_COUNT_DISTINCT_planner0' as test_name,
    COUNT(DISTINCT p40s10) as distinct_p40s10,
    COUNT(DISTINCT p50s15) as distinct_p50s15,
    COUNT(DISTINCT p76s20) as distinct_p76s20,
    COUNT(DISTINCT p76s0) as distinct_p76s0
FROM ${case_db}.decimal256_agg_test;

-- query 10
set new_planner_agg_stage=2;
SELECT
    'Test7_COUNT_DISTINCT_planner2' as test_name,
    COUNT(DISTINCT p40s10) as distinct_p40s10,
    COUNT(DISTINCT p50s15) as distinct_p50s15,
    COUNT(DISTINCT p76s20) as distinct_p76s20,
    COUNT(DISTINCT p76s0) as distinct_p76s0
FROM ${case_db}.decimal256_agg_test;
set new_planner_agg_stage=0;

-- query 11
set new_planner_agg_stage=0;
SELECT
    'Test8_SUM_DISTINCT_planner0' as test_name,
    SUM(DISTINCT p40s10) as sum_distinct_p40s10,
    SUM(DISTINCT p50s15) as sum_distinct_p50s15,
    SUM(DISTINCT p76s20) as sum_distinct_p76s20,
    SUM(DISTINCT p76s0) as sum_distinct_p76s0
FROM ${case_db}.decimal256_agg_test;

-- query 12
set new_planner_agg_stage=2;
SELECT
    'Test8_SUM_DISTINCT_planner2' as test_name,
    SUM(DISTINCT p40s10) as sum_distinct_p40s10,
    SUM(DISTINCT p50s15) as sum_distinct_p50s15,
    SUM(DISTINCT p76s20) as sum_distinct_p76s20,
    SUM(DISTINCT p76s0) as sum_distinct_p76s0
FROM ${case_db}.decimal256_agg_test;
set new_planner_agg_stage=0;

-- query 13
set new_planner_agg_stage=0;
SELECT
    'Test9_AVG_DISTINCT_planner0' as test_name,
    AVG(DISTINCT p40s10) as avg_distinct_p40s10,
    AVG(DISTINCT p50s15) as avg_distinct_p50s15,
    AVG(DISTINCT p76s20) as avg_distinct_p76s20,
    AVG(DISTINCT p76s0) as avg_distinct_p76s0
FROM ${case_db}.decimal256_agg_test;

-- query 14
set new_planner_agg_stage=2;
SELECT
    'Test9_AVG_DISTINCT_planner2' as test_name,
    AVG(DISTINCT p40s10) as avg_distinct_p40s10,
    AVG(DISTINCT p50s15) as avg_distinct_p50s15,
    AVG(DISTINCT p76s20) as avg_distinct_p76s20,
    AVG(DISTINCT p76s0) as avg_distinct_p76s0
FROM ${case_db}.decimal256_agg_test;
set new_planner_agg_stage=0;

-- query 15
set new_planner_agg_stage=0;
SELECT
    'Test10_GROUP_COUNT_DISTINCT_planner0' as test_name,
    category,
    COUNT(*) as row_count,
    COUNT(DISTINCT p40s10) as distinct_p40s10,
    COUNT(DISTINCT p76s0) as distinct_p76s0
FROM ${case_db}.decimal256_agg_test
GROUP BY category
ORDER BY category;

-- query 16
set new_planner_agg_stage=2;
SELECT
    'Test10_GROUP_COUNT_DISTINCT_planner2' as test_name,
    category,
    COUNT(*) as row_count,
    COUNT(DISTINCT p40s10) as distinct_p40s10,
    COUNT(DISTINCT p76s0) as distinct_p76s0
FROM ${case_db}.decimal256_agg_test
GROUP BY category
ORDER BY category;
set new_planner_agg_stage=0;

-- query 17
set new_planner_agg_stage=0;
SELECT
    'Test11_GROUP_SUM_DISTINCT_planner0' as test_name,
    category,
    SUM(DISTINCT p40s10) as sum_distinct_p40s10,
    SUM(DISTINCT p50s15) as sum_distinct_p50s15
FROM ${case_db}.decimal256_agg_test
GROUP BY category
ORDER BY category;

-- query 18
set new_planner_agg_stage=2;
SELECT
    'Test11_GROUP_SUM_DISTINCT_planner2' as test_name,
    category,
    SUM(DISTINCT p40s10) as sum_distinct_p40s10,
    SUM(DISTINCT p50s15) as sum_distinct_p50s15
FROM ${case_db}.decimal256_agg_test
GROUP BY category
ORDER BY category;
set new_planner_agg_stage=0;

-- query 19
set new_planner_agg_stage=0;
SELECT
    'Test12_GROUP_AVG_DISTINCT_planner0' as test_name,
    category,
    AVG(DISTINCT p40s10) as avg_distinct_p40s10,
    AVG(DISTINCT p76s0) as avg_distinct_p76s0
FROM ${case_db}.decimal256_agg_test
GROUP BY category
ORDER BY category;

-- query 20
set new_planner_agg_stage=2;
SELECT
    'Test12_GROUP_AVG_DISTINCT_planner2' as test_name,
    category,
    AVG(DISTINCT p40s10) as avg_distinct_p40s10,
    AVG(DISTINCT p76s0) as avg_distinct_p76s0
FROM ${case_db}.decimal256_agg_test
GROUP BY category
ORDER BY category;
set new_planner_agg_stage=0;

-- query 21
set new_planner_agg_stage=0;
SELECT
    'Test13_GROUP_BY_DECIMAL_DISTINCT_planner0' as test_name,
    p76s0,
    COUNT(*) as row_count,
    COUNT(DISTINCT p40s10) as distinct_p40s10,
    SUM(DISTINCT p40s10) as sum_distinct_p40s10,
    AVG(DISTINCT p40s10) as avg_distinct_p40s10
FROM ${case_db}.decimal256_agg_test
WHERE p76s0 IS NOT NULL
GROUP BY p76s0
ORDER BY p76s0;

-- query 22
set new_planner_agg_stage=2;
SELECT
    'Test13_GROUP_BY_DECIMAL_DISTINCT_planner2' as test_name,
    p76s0,
    COUNT(*) as row_count,
    COUNT(DISTINCT p40s10) as distinct_p40s10,
    SUM(DISTINCT p40s10) as sum_distinct_p40s10,
    AVG(DISTINCT p40s10) as avg_distinct_p40s10
FROM ${case_db}.decimal256_agg_test
WHERE p76s0 IS NOT NULL
GROUP BY p76s0
ORDER BY p76s0;
set new_planner_agg_stage=0;

-- query 23
set new_planner_agg_stage=0;
SELECT DISTINCT 'Test14_SELECT_DISTINCT_SINGLE_planner0' as test_name, p40s10
FROM ${case_db}.decimal256_agg_test
WHERE p40s10 IS NOT NULL
ORDER BY p40s10;

-- query 24
set new_planner_agg_stage=2;
SELECT DISTINCT 'Test14_SELECT_DISTINCT_SINGLE_planner2' as test_name, p40s10
FROM ${case_db}.decimal256_agg_test
WHERE p40s10 IS NOT NULL
ORDER BY p40s10;
set new_planner_agg_stage=0;

-- query 25
set new_planner_agg_stage=0;
SELECT DISTINCT 'Test15_SELECT_DISTINCT_MULTI_planner0' as test_name, p40s10, p50s15, p76s0
FROM ${case_db}.decimal256_agg_test
WHERE p40s10 IS NOT NULL
ORDER BY p40s10, p50s15;

-- query 26
set new_planner_agg_stage=2;
SELECT DISTINCT 'Test15_SELECT_DISTINCT_MULTI_planner2' as test_name, p40s10, p50s15, p76s0
FROM ${case_db}.decimal256_agg_test
WHERE p40s10 IS NOT NULL
ORDER BY p40s10, p50s15;
set new_planner_agg_stage=0;

-- query 27
set new_planner_agg_stage=0;
SELECT
    'Test16_COMPLEX_DISTINCT_planner0' as test_name,
    COUNT(DISTINCT p40s10) as count_distinct_p40s10,
    SUM(DISTINCT p40s10) as sum_distinct_p40s10,
    AVG(DISTINCT p40s10) as avg_distinct_p40s10,
    COUNT(DISTINCT p76s0) as count_distinct_p76s0,
    SUM(DISTINCT p76s0) as sum_distinct_p76s0,
    AVG(DISTINCT p76s0) as avg_distinct_p76s0
FROM ${case_db}.decimal256_agg_test;

-- query 28
set new_planner_agg_stage=2;
SELECT
    'Test16_COMPLEX_DISTINCT_planner2' as test_name,
    COUNT(DISTINCT p40s10) as count_distinct_p40s10,
    SUM(DISTINCT p40s10) as sum_distinct_p40s10,
    AVG(DISTINCT p40s10) as avg_distinct_p40s10,
    COUNT(DISTINCT p76s0) as count_distinct_p76s0,
    SUM(DISTINCT p76s0) as sum_distinct_p76s0,
    AVG(DISTINCT p76s0) as avg_distinct_p76s0
FROM ${case_db}.decimal256_agg_test;
set new_planner_agg_stage=0;

-- query 29
set new_planner_agg_stage=0;
SELECT
    'Test17_HAVING_DISTINCT_planner0' as test_name,
    p40s10,
    COUNT(*) as row_count,
    COUNT(DISTINCT p50s15) as distinct_p50s15,
    SUM(DISTINCT p50s15) as sum_distinct_p50s15
FROM ${case_db}.decimal256_agg_test
WHERE p40s10 IS NOT NULL
GROUP BY p40s10
HAVING COUNT(*) > 1
ORDER BY p40s10;

-- query 30
set new_planner_agg_stage=2;
SELECT
    'Test17_HAVING_DISTINCT_planner2' as test_name,
    p40s10,
    COUNT(*) as row_count,
    COUNT(DISTINCT p50s15) as distinct_p50s15,
    SUM(DISTINCT p50s15) as sum_distinct_p50s15
FROM ${case_db}.decimal256_agg_test
WHERE p40s10 IS NOT NULL
GROUP BY p40s10
HAVING COUNT(*) > 1
ORDER BY p40s10;
set new_planner_agg_stage=0;

-- query 31
SELECT
    'Test18_EMPTY_RESULT' as test_name,
    COUNT(*) as count_all,
    SUM(p40s10) as sum_p40s10,
    AVG(p40s10) as avg_p40s10,
    MIN(p50s15) as min_p50s15,
    MAX(p76s20) as max_p76s20
FROM ${case_db}.decimal256_agg_test
WHERE id > 1000;

-- query 32
SELECT
    'Test19_NULL_ONLY' as test_name,
    COUNT(*) as count_all,
    COUNT(p40s10) as count_p40s10,
    SUM(p50s15) as sum_p50s15,
    AVG(p50s15) as avg_p50s15
FROM ${case_db}.decimal256_agg_test
WHERE id = 18;

-- query 33
SELECT
    'Test20_SINGLE_ROW' as test_name,
    COUNT(*) as count_all,
    SUM(p40s10) as sum_p40s10,
    AVG(p40s10) as avg_p40s10,
    MIN(p50s15) as min_p50s15,
    MAX(p76s20) as max_p76s20
FROM ${case_db}.decimal256_agg_test
WHERE id = 11;
