-- Migrated from dev/test/sql/test_array_fn/R/test_array_min_max
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_min_max FORCE;
CREATE DATABASE sql_tests_complex_test_array_min_max;
USE sql_tests_complex_test_array_min_max;

-- name: test_array_min_max_all_type @mac @no_arrow_flight_sql
-- query 2
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
CREATE TABLE test_array_min_max (
    id INT,
    array_boolean ARRAY<BOOLEAN>,
    array_tinyint ARRAY<TINYINT>,
    array_smallint ARRAY<SMALLINT>,
    array_int ARRAY<INT>,
    array_bigint ARRAY<BIGINT>,
    array_largeint ARRAY<LARGEINT>,
    array_decimalv2 ARRAY<DECIMALV2(10, 2)>,
    array_decimal32 ARRAY<DECIMAL32(9, 2)>,
    array_decimal64 ARRAY<DECIMAL64(18, 2)>,
    array_decimal128 ARRAY<DECIMAL128(38, 10)>,
    array_float ARRAY<FLOAT>,
    array_double ARRAY<DOUBLE>,
    array_varchar ARRAY<VARCHAR(100)>,
    array_date ARRAY<DATE>,
    array_datetime ARRAY<DATETIME>
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
INSERT INTO test_array_min_max VALUES
(1, [true, false, NULL], [1, 2, NULL], [100, 200, NULL], [10, 20, NULL], [1000, 2000, NULL], [1234567890123456789, NULL, NULL],
 [12345.67, 89012.34, NULL], [123.45, 678.90, NULL], [12345678.90, 9876543.21, NULL], [1234567890.1234567890, NULL, NULL], 
 [1.23, 4.56, NULL], [123.456, 789.012, NULL], ['hello', NULL, 'starrocks'], 
 ['2025-01-01', '2025-01-02', NULL], ['2025-01-01 12:00:00', NULL, '2025-01-02 14:00:00']),
(2, NULL, [5, NULL, 3], [500, 400, NULL], [50, 40, NULL], [5000, NULL, 3000], [987654321987654321, NULL, NULL], 
 [56789.01, NULL, 45678.12], [345.67, NULL, 234.56], [56789012.34, NULL, 34567890.12], [2345678901.2345678901, NULL, NULL], 
 [2.34, NULL, 1.23], [234.567, NULL, 123.456], [NULL, 'array', 'test'], 
 [NULL, '2024-12-31', '2024-12-30'], ['2024-12-31 23:59:59', '2024-12-30 12:00:00', NULL]),
(3, [true, NULL, false], [1, 6, NULL], [150, NULL, 250], [15, NULL, 35], [1100, NULL, 2200], [2345678901234567890, NULL, NULL],
 [67890.12, NULL, 56789.01], [456.78, NULL, 345.67], [67890123.45, NULL, 56789012.34], [3456789012.3456789012, NULL, NULL],
 [1.11, NULL, 3.33], [222.333, NULL, 333.444], ['foo', 'bar', NULL], 
 ['2025-01-03', NULL, '2025-01-04'], ['2025-01-03 16:00:00', '2025-01-04 18:00:00', NULL]),
(4, [NULL, true, false], [NULL, 20, 10], [110, 220, NULL], [NULL, 12, 24], [1200, NULL, 2400], [3456789012345678901, NULL, NULL],
 [78901.23, 67890.12, NULL], [567.89, 456.78, NULL], [78901234.56, 67890123.45, NULL], [4567890123.4567890123, NULL, NULL],
 [NULL, 5.55, 4.44], [NULL, 777.888, 666.777], ['NULL', 'banana', 'apple'], 
 [NULL, '2025-01-05', '2025-01-06'], [NULL, '2025-01-06 20:00:00', '2025-01-05 18:00:00']),
(5, [false, NULL, true], [10, NULL, 30], [300, 400, NULL], [70, NULL, 90], [4000, NULL, 6000], [987654321234567890, NULL, NULL],
 [123456.78, NULL, 876543.21], [678.90, NULL, 789.01], [9876543.21, NULL, 1234567.89], [5678901234.5678901234, NULL, NULL],
 [3.21, 4.32, NULL], [111.222, NULL, 333.444], ['dog', 'cat', NULL], 
 ['2025-01-07', '2025-01-08', NULL], ['2025-01-07 10:00:00', NULL, '2025-01-08 15:00:00']),
(6, [NULL, true, true], [NULL, 40, 50], [450, 500, NULL], [80, 100, NULL], [1500, NULL, 2500], [765432198765432109, NULL, NULL],
 [34567.89, NULL, 12345.67], [123.45, NULL, 678.90], [54321.12, NULL, 12345.67], [7654321098.7654321098, NULL, NULL],
 [NULL, 6.54, 7.65], [555.666, NULL, 444.333], [NULL, 'bird', 'fish'], 
 ['2025-01-09', '2025-01-10', NULL], ['2025-01-09 12:00:00', NULL, '2025-01-10 18:00:00']),
(7, [false, false, NULL], [70, NULL, 90], [650, NULL, 750], [120, 140, NULL], [8000, NULL, 9000], [543210987654321098, NULL, NULL],
 [45678.12, NULL, 23456.78], [234.56, NULL, 456.78], [67890123.45, NULL, 34567890.12], [4321098765.4321098765, NULL, NULL],
 [7.89, 8.90, NULL], [333.222, NULL, 111.000], ['lion', NULL, 'tiger'], 
 ['2025-01-11', '2025-01-12', NULL], ['2025-01-11 20:00:00', NULL, '2025-01-12 22:00:00']),
(8, [true, NULL, false], [5, 15, NULL], [50, NULL, 150], [25, NULL, 75], [4500, NULL, 5500], [321098765432109876, NULL, NULL],
 [23456.78, NULL, 12345.67], [345.67, NULL, 456.78], [8901234.56, NULL, 7890123.45], [2109876543.2109876543, NULL, NULL],
 [5.67, NULL, 4.56], [666.555, NULL, 222.111], [NULL, 'grape', 'pear'], 
 ['2025-01-13', NULL, '2025-01-14'], ['2025-01-13 23:59:59', '2025-01-14 12:00:00', NULL]),
(9, [false, true, NULL], [25, 35, NULL], [350, NULL, 450], [100, 200, NULL], [6000, NULL, 7000], [654321098765432109, NULL, NULL],
 [67890.12, NULL, 34567.89], [456.78, NULL, 234.56], [34567890.12, NULL, 23456789.01], [8765432109.8765432109, NULL, NULL],
 [9.87, NULL, 8.76], [444.333, NULL, 555.222], ['watermelon', NULL, 'kiwi'], 
 [NULL, '2025-01-15', '2025-01-16'], ['2025-01-15 12:00:00', NULL, '2025-01-16 18:00:00']),
(10, [true, true, NULL], [50, 70, NULL], [750, 850, NULL], [300, 400, NULL], [10000, NULL, 12000], [789012345678901234, NULL, NULL],
 [78901.23, NULL, 67890.12], [567.89, NULL, 456.78], [12345678.90, NULL, 9876543.21], [1234567890.1234567890, NULL, NULL],
 [NULL, 1.11, 2.22], [777.888, 999.000, NULL], ['blueberry', 'cherry', NULL], 
 ['2025-01-17', '2025-01-18', NULL], [NULL, '2025-01-17 10:00:00', '2025-01-18 20:00:00']),
(11, [NULL], [NULL], [NULL], [NULL], [NULL], [NULL, NULL],
 [NULL], [NULL], [NULL], [NULL, NULL],
 [NULL], [NULL], [NULL], 
 [NULL], [NULL]);

-- query 4
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_boolean) AS result FROM test_array_min_max ORDER BY id;

-- query 5
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_boolean) AS result 
FROM test_array_min_max 
WHERE array_boolean IS NOT NULL ORDER BY id;

-- query 6
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_boolean) AS result 
FROM test_array_min_max 
WHERE array_length(array_boolean) > 3 ORDER BY id;

-- query 7
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_tinyint) AS result FROM test_array_min_max ORDER BY id;

-- query 8
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_tinyint) AS result 
FROM test_array_min_max 
WHERE array_tinyint IS NOT NULL ORDER BY id;

-- query 9
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_tinyint) AS result 
FROM test_array_min_max 
WHERE array_length(array_tinyint) > 3 ORDER BY id;

-- query 10
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_smallint) AS result FROM test_array_min_max ORDER BY id;

-- query 11
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_smallint) AS result 
FROM test_array_min_max 
WHERE array_smallint IS NOT NULL ORDER BY id;

-- query 12
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_smallint) AS result 
FROM test_array_min_max 
WHERE array_length(array_smallint) > 3 ORDER BY id;

-- query 13
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_int) AS result FROM test_array_min_max ORDER BY id;

-- query 14
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_int) AS result 
FROM test_array_min_max 
WHERE array_int IS NOT NULL ORDER BY id;

-- query 15
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_int) AS result 
FROM test_array_min_max 
WHERE array_length(array_int) > 3 ORDER BY id;

-- query 16
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_bigint) AS result FROM test_array_min_max ORDER BY id;

-- query 17
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_bigint) AS result 
FROM test_array_min_max 
WHERE array_bigint IS NOT NULL ORDER BY id;

-- query 18
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_bigint) AS result 
FROM test_array_min_max 
WHERE array_length(array_bigint) > 3 ORDER BY id;

-- query 19
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_largeint) AS result FROM test_array_min_max ORDER BY id;

-- query 20
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_largeint) AS result 
FROM test_array_min_max 
WHERE array_largeint IS NOT NULL ORDER BY id;

-- query 21
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_largeint) AS result 
FROM test_array_min_max 
WHERE array_length(array_largeint) > 3 ORDER BY id;

-- query 22
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_float) AS result FROM test_array_min_max ORDER BY id;

-- query 23
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_float) AS result 
FROM test_array_min_max 
WHERE array_float IS NOT NULL ORDER BY id;

-- query 24
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_float) AS result 
FROM test_array_min_max 
WHERE array_length(array_float) > 3 ORDER BY id;

-- query 25
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimal32) AS result FROM test_array_min_max ORDER BY id;

-- query 26
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimal32) AS result 
FROM test_array_min_max 
WHERE array_decimal32 IS NOT NULL ORDER BY id;

-- query 27
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimal32) AS result 
FROM test_array_min_max 
WHERE array_length(array_decimal32) > 3 ORDER BY id;

-- query 28
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimal64) AS result FROM test_array_min_max ORDER BY id;

-- query 29
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimal64) AS result 
FROM test_array_min_max 
WHERE array_decimal64 IS NOT NULL ORDER BY id;

-- query 30
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimal64) AS result 
FROM test_array_min_max 
WHERE array_length(array_decimal64) > 3 ORDER BY id;

-- query 31
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimal128) AS result FROM test_array_min_max ORDER BY id;

-- query 32
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimal128) AS result 
FROM test_array_min_max 
WHERE array_decimal128 IS NOT NULL ORDER BY id;

-- query 33
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimal128) AS result 
FROM test_array_min_max 
WHERE array_length(array_decimal128) > 3 ORDER BY id;

-- query 34
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_double) AS result FROM test_array_min_max ORDER BY id;

-- query 35
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_double) AS result 
FROM test_array_min_max 
WHERE array_double IS NOT NULL ORDER BY id;

-- query 36
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_double) AS result 
FROM test_array_min_max 
WHERE array_length(array_double) > 3 ORDER BY id;

-- query 37
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimalv2) AS result FROM test_array_min_max ORDER BY id;

-- query 38
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimalv2) AS result 
FROM test_array_min_max 
WHERE array_decimalv2 IS NOT NULL ORDER BY id;

-- query 39
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_decimalv2) AS result 
FROM test_array_min_max 
WHERE array_length(array_decimalv2) > 3 ORDER BY id;

-- query 40
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_date) AS result FROM test_array_min_max ORDER BY id;

-- query 41
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_date) AS result 
FROM test_array_min_max 
WHERE array_date IS NOT NULL ORDER BY id;

-- query 42
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_date) AS result 
FROM test_array_min_max 
WHERE array_length(array_date) > 3 ORDER BY id;

-- query 43
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_datetime) AS result FROM test_array_min_max ORDER BY id;

-- query 44
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_datetime) AS result 
FROM test_array_min_max 
WHERE array_datetime IS NOT NULL ORDER BY id;

-- query 45
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_datetime) AS result 
FROM test_array_min_max 
WHERE array_length(array_datetime) > 3 ORDER BY id;

-- query 46
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_varchar) AS result FROM test_array_min_max ORDER BY id;

-- query 47
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_varchar) AS result 
FROM test_array_min_max 
WHERE array_varchar IS NOT NULL ORDER BY id;

-- query 48
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_min(array_varchar) AS result 
FROM test_array_min_max 
WHERE array_length(array_varchar) > 3 ORDER BY id;

-- query 49
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_boolean) AS result FROM test_array_min_max ORDER BY id;

-- query 50
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_boolean) AS result 
FROM test_array_min_max 
WHERE array_boolean IS NOT NULL ORDER BY id;

-- query 51
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_boolean) AS result 
FROM test_array_min_max 
WHERE array_length(array_boolean) > 3 ORDER BY id;

-- query 52
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_tinyint) AS result FROM test_array_min_max ORDER BY id;

-- query 53
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_tinyint) AS result 
FROM test_array_min_max 
WHERE array_tinyint IS NOT NULL ORDER BY id;

-- query 54
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_tinyint) AS result 
FROM test_array_min_max 
WHERE array_length(array_tinyint) > 3 ORDER BY id;

-- query 55
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_smallint) AS result FROM test_array_min_max ORDER BY id;

-- query 56
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_smallint) AS result 
FROM test_array_min_max 
WHERE array_smallint IS NOT NULL ORDER BY id;

-- query 57
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_smallint) AS result 
FROM test_array_min_max 
WHERE array_length(array_smallint) > 3 ORDER BY id;

-- query 58
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_int) AS result FROM test_array_min_max ORDER BY id;

-- query 59
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_int) AS result 
FROM test_array_min_max 
WHERE array_int IS NOT NULL ORDER BY id;

-- query 60
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_int) AS result 
FROM test_array_min_max 
WHERE array_length(array_int) > 3 ORDER BY id;

-- query 61
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_bigint) AS result FROM test_array_min_max ORDER BY id;

-- query 62
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_bigint) AS result 
FROM test_array_min_max 
WHERE array_bigint IS NOT NULL ORDER BY id;

-- query 63
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_bigint) AS result 
FROM test_array_min_max 
WHERE array_length(array_bigint) > 3 ORDER BY id;

-- query 64
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_largeint) AS result FROM test_array_min_max ORDER BY id;

-- query 65
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_largeint) AS result 
FROM test_array_min_max 
WHERE array_largeint IS NOT NULL ORDER BY id;

-- query 66
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_largeint) AS result 
FROM test_array_min_max 
WHERE array_length(array_largeint) > 3 ORDER BY id;

-- query 67
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_float) AS result FROM test_array_min_max ORDER BY id;

-- query 68
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_float) AS result 
FROM test_array_min_max 
WHERE array_float IS NOT NULL ORDER BY id;

-- query 69
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_float) AS result 
FROM test_array_min_max 
WHERE array_length(array_float) > 3 ORDER BY id;

-- query 70
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimal32) AS result FROM test_array_min_max ORDER BY id;

-- query 71
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimal32) AS result 
FROM test_array_min_max 
WHERE array_decimal32 IS NOT NULL ORDER BY id;

-- query 72
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimal32) AS result 
FROM test_array_min_max 
WHERE array_length(array_decimal32) > 3 ORDER BY id;

-- query 73
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimal64) AS result FROM test_array_min_max ORDER BY id;

-- query 74
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimal64) AS result 
FROM test_array_min_max 
WHERE array_decimal64 IS NOT NULL ORDER BY id;

-- query 75
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimal64) AS result 
FROM test_array_min_max 
WHERE array_length(array_decimal64) > 3 ORDER BY id;

-- query 76
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimal128) AS result FROM test_array_min_max ORDER BY id;

-- query 77
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimal128) AS result 
FROM test_array_min_max 
WHERE array_decimal128 IS NOT NULL ORDER BY id;

-- query 78
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimal128) AS result 
FROM test_array_min_max 
WHERE array_length(array_decimal128) > 3 ORDER BY id;

-- query 79
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_double) AS result FROM test_array_min_max ORDER BY id;

-- query 80
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_double) AS result 
FROM test_array_min_max 
WHERE array_double IS NOT NULL ORDER BY id;

-- query 81
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_double) AS result 
FROM test_array_min_max 
WHERE array_length(array_double) > 3 ORDER BY id;

-- query 82
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimalv2) AS result FROM test_array_min_max ORDER BY id;

-- query 83
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimalv2) AS result 
FROM test_array_min_max 
WHERE array_decimalv2 IS NOT NULL ORDER BY id;

-- query 84
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_decimalv2) AS result 
FROM test_array_min_max 
WHERE array_length(array_decimalv2) > 3 ORDER BY id;

-- query 85
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_date) AS result FROM test_array_min_max ORDER BY id;

-- query 86
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_date) AS result 
FROM test_array_min_max 
WHERE array_date IS NOT NULL ORDER BY id;

-- query 87
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_date) AS result 
FROM test_array_min_max 
WHERE array_length(array_date) > 3 ORDER BY id;

-- query 88
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_datetime) AS result FROM test_array_min_max ORDER BY id;

-- query 89
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_datetime) AS result 
FROM test_array_min_max 
WHERE array_datetime IS NOT NULL ORDER BY id;

-- query 90
-- @skip_result_check=true
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_datetime) AS result 
FROM test_array_min_max 
WHERE array_length(array_datetime) > 3 ORDER BY id;

-- query 91
USE sql_tests_complex_test_array_min_max;
SELECT id, array_max(array_varchar) AS result FROM test_array_min_max ORDER BY id;

-- query 92
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_min_max FORCE;
