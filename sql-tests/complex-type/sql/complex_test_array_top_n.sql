-- Migrated from dev/test/sql/test_array_fn/R/test_array_top_n
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_top_n FORCE;
CREATE DATABASE sql_tests_complex_test_array_top_n;
USE sql_tests_complex_test_array_top_n;

-- name: test_array_top_n_all_types @no_arrow_flight_sql
-- query 2
-- @skip_result_check=true
USE sql_tests_complex_test_array_top_n;
CREATE TABLE test_array_top_n (
    id INT,
    array_int ARRAY<INT>,
    array_bigint ARRAY<BIGINT>,
    array_float ARRAY<FLOAT>,
    array_double ARRAY<DOUBLE>,
    array_decimalv2 ARRAY<DECIMALV2(10, 2)>,
    array_boolean ARRAY<BOOLEAN>,
    array_date ARRAY<DATE>,
    array_datetime ARRAY<DATETIME>,
    array_varchar ARRAY<VARCHAR(100)>
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_complex_test_array_top_n;
INSERT INTO test_array_top_n VALUES
(1, [100, 1, NULL, 5, 100, 1], [100, 1, NULL, 5, 100, 1], [100.0, 1.0, NULL, 5.0, 100.0, 1.0],
 [100.0, 1.0, NULL, 5.0, 100.0, 1.0], [100.0, 1.0, NULL, 5.0, 100.0, 1.0], [true, false, NULL, true, true, false],
 ['2023-12-31', '2023-01-01', NULL, '2023-06-15', '2023-12-31', '2023-01-01'],
 ['2023-12-31 23:59:59', '2023-01-01 00:00:01', NULL, '2023-06-15 12:00:00', '2023-12-31 23:59:59', '2023-01-01 00:00:01'],
 ['zzz', 'a', NULL, 'g', 'zzz', 'a']),
(2, [], [], [], [], [], [], [], [], []),
(3, [100], [100], [100.0], [100.0], [100.0], [true], ['2023-12-31'], ['2023-12-31 23:59:59'], ['zzz']),
(4, [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL],
 [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL], [NULL, NULL, NULL]),
(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(6, [5, 5, 5, 3, 3], [5, 5, 5, 3, 3], [5.0, 5.0, 5.0, 3.0, 3.0], [5.0, 5.0, 5.0, 3.0, 3.0],
 [5.0, 5.0, 5.0, 3.0, 3.0], [true, true, true, false, false],
 ['2023-05-05', '2023-05-05', '2023-05-05', '2023-03-03', '2023-03-03'],
 ['2023-05-05 12:00:00', '2023-05-05 12:00:00', '2023-05-05 12:00:00', '2023-03-03 10:00:00', '2023-03-03 10:00:00'],
 ['eee', 'eee', 'eee', 'ccc', 'ccc']);

-- query 4
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_int, 3) AS result FROM test_array_top_n ORDER BY id;

-- query 5
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_bigint, 3) AS result FROM test_array_top_n ORDER BY id;

-- query 6
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_float, 3) AS result FROM test_array_top_n ORDER BY id;

-- query 7
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_double, 3) AS result FROM test_array_top_n ORDER BY id;

-- query 8
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_decimalv2, 3) AS result FROM test_array_top_n ORDER BY id;

-- query 9
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_boolean, 3) AS result FROM test_array_top_n ORDER BY id;

-- query 10
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_date, 3) AS result FROM test_array_top_n ORDER BY id;

-- query 11
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_datetime, 3) AS result FROM test_array_top_n ORDER BY id;

-- query 12
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_varchar, 3) AS result FROM test_array_top_n ORDER BY id;

-- query 13
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_int, 0) AS result FROM test_array_top_n WHERE id = 1 ORDER BY id;

-- query 14
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_int, -1) AS result FROM test_array_top_n WHERE id = 1 ORDER BY id;

-- query 15
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_int, 10) AS result FROM test_array_top_n WHERE id = 1 ORDER BY id;

-- query 16
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_int, 1) AS result FROM test_array_top_n WHERE id = 1 ORDER BY id;

-- query 17
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_int, 1) AS result FROM test_array_top_n WHERE id = 4 ORDER BY id;

-- query 18
USE sql_tests_complex_test_array_top_n;
SELECT id, array_top_n(array_int, 3) AS result FROM test_array_top_n WHERE id = 6 ORDER BY id;

-- query 19
USE sql_tests_complex_test_array_top_n;
SELECT array_top_n([1, 100, 2, 5, 3], 3) AS result;

-- query 20
USE sql_tests_complex_test_array_top_n;
SELECT array_top_n([1, 100, 2, 5, 3], 3) AS result;

-- query 21
USE sql_tests_complex_test_array_top_n;
SELECT array_top_n(['hello', 'world', 'aaa', 'zzz', 'mmm'], 3) AS result;

-- query 22
USE sql_tests_complex_test_array_top_n;
SELECT array_top_n([1, NULL, 100, NULL, 5], 3) AS result;

-- query 23
USE sql_tests_complex_test_array_top_n;
SELECT array_top_n([], 3) AS result;

-- query 24
USE sql_tests_complex_test_array_top_n;
SELECT array_top_n(NULL, 3) AS result;

-- query 25
-- @skip_result_check=true
USE sql_tests_complex_test_array_top_n;
DROP TABLE test_array_top_n;

-- query 26
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_top_n FORCE;
