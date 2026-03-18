-- @tags=join,one_key
-- Test Objective:
-- Validate single-key hash join correctness across all supported data types.
-- For each type, tests INNER JOIN, FULL OUTER JOIN, and LEFT SEMI JOIN to verify
-- matched row counts, NULL key handling, and column propagation.
-- Types covered: bool, tinyint, smallint, int, bigint, largeint, float, double,
-- decimalv2, decimal32, decimal64, decimal128, date, datetime, char, varchar.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.__row_util_base;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.__row_util_base (
  k1 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT generate_series FROM TABLE(generate_series(0, 10000 - 1));

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT * FROM ${case_db}.__row_util_base;

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT * FROM ${case_db}.__row_util_base;

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT * FROM ${case_db}.__row_util_base;

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT * FROM ${case_db}.__row_util_base;

-- query 8
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT * FROM ${case_db}.__row_util_base;

-- query 9
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT * FROM ${case_db}.__row_util_base;

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT * FROM ${case_db}.__row_util_base;

-- query 11
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.__row_util;

-- query 12
-- @skip_result_check=true
CREATE TABLE ${case_db}.__row_util (
  idx bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`idx`)
DISTRIBUTED BY HASH(`idx`) BUCKETS 32
PROPERTIES ("replication_num" = "1");

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util SELECT row_number() over() AS idx FROM ${case_db}.__row_util_base;

-- query 14
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t1;

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 bigint NULL,
    c_bool boolean,
    c_bool_null boolean NULL,
    c_tinyint tinyint,
    c_tinyint_null tinyint NULL,
    c_smallint smallint,
    c_smallint_null smallint NULL,
    c_int int,
    c_int_null int NULL,
    c_bigint bigint,
    c_bigint_null bigint NULL,
    c_largeint bigint,
    c_largeint_null bigint NULL,
    c_float float,
    c_float_null float NULL,
    c_double double,
    c_double_null double NULL,
    c_decimalv2 DECIMAL,
    c_decimalv2_null DECIMAL NULL,
    c_decimal32 DECIMAL(9),
    c_decimal32_null DECIMAL(9) NULL,
    c_decimal64 DECIMAL(18),
    c_decimal64_null DECIMAL(18) NULL,
    c_decimal128 DECIMAL(38),
    c_decimal128_null DECIMAL(38) NULL,
    c_date date,
    c_date_null date NULL,
    c_datetime datetime,
    c_datetime_null datetime NULL,
    c_char char(100),
    c_char_null char(100) NULL,
    c_varchar varchar(100),
    c_varchar_null varchar(100) NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES ("replication_num" = "1");

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.t1
SELECT
    idx,
    idx % 2 = 0,
    if (idx % 7 = 0, idx % 2 = 0, null),
    idx % 128,
    if (idx % 12 = 0, idx % 128, null),
    idx % 32768,
    if (idx % 13 = 0, idx % 32768, null),
    idx / 2,
    if (idx % 14 = 0, idx, null),
    idx / 2,
    if (idx % 15 = 0, idx, null),
    idx / 2,
    if (idx % 16 = 0, idx, null),
    idx / 2,
    if (idx % 17 = 0, idx, null),
    idx / 2,
    if (idx % 18 = 0, idx, null),
    idx / 2,
    if (idx % 23 = 0, idx, null),
    idx / 2,
    if (idx % 24 = 0, idx, null),
    idx / 2,
    if (idx % 25 = 0, idx, null),
    idx / 2,
    if (idx % 26 = 0, idx, null),
    date_add('2023-01-01', idx / 2),
    if (idx % 19 = 0, date_add('2023-01-01', idx), null),
    date_add('2023-01-01 00:00:00', idx / 2),
    if (idx % 20 = 0, date_add('2023-01-01 00:00:00', idx), null),
    concat('char_', idx / 2),
    if (idx % 21 = 0, concat('char_', idx), null),
    concat('varchar_', idx / 2),
    if (idx % 22 = 0, concat('varchar_', idx), null)
FROM ${case_db}.__row_util;

-- ========== c_bool_null ==========

-- query 17
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.k1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 18
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.k1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 19
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.k1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- ========== c_tinyint_null ==========

-- query 20
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.k1), count(t1.c_tinyint_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 21
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.k1), count(t1.c_tinyint_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 22
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.k1), count(t1.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- ========== c_smallint_null ==========

-- query 23
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10000)
SELECT count(1), count(t1.k1), count(t1.c_smallint_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 24
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10000)
SELECT count(1), count(t1.k1), count(t1.c_smallint_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 25
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10000)
SELECT count(1), count(t1.k1), count(t1.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- ========== c_int (non-null) ==========

-- query 26
SELECT count(1), count(t1.k1), count(t1.c_int)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_int = t2.c_int;

-- query 27
SELECT count(1), count(t1.k1), count(t1.c_int)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_int = t2.c_int;

-- query 28
SELECT count(1), count(t1.k1), count(t1.c_int)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_int_null ==========

-- query 29
SELECT count(1), count(t1.k1), count(t1.c_int_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_int_null = t2.c_int_null;

-- query 30
SELECT count(1), count(t1.k1), count(t1.c_int_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_int_null = t2.c_int_null;

-- query 31
SELECT count(1), count(t1.k1), count(t1.c_int_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_int_null = t2.c_int_null;

-- ========== c_bigint (non-null) ==========

-- query 32
SELECT count(1), count(t1.k1), count(t1.c_bigint)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_bigint = t2.c_bigint;

-- query 33
SELECT count(1), count(t1.k1), count(t1.c_bigint)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_bigint = t2.c_bigint;

-- query 34
SELECT count(1), count(t1.k1), count(t1.c_bigint)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_bigint_null ==========

-- query 35
SELECT count(1), count(t1.k1), count(t1.c_bigint_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_bigint_null = t2.c_bigint_null;

-- query 36
SELECT count(1), count(t1.k1), count(t1.c_bigint_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_bigint_null = t2.c_bigint_null;

-- query 37
SELECT count(1), count(t1.k1), count(t1.c_bigint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_bigint_null = t2.c_bigint_null;

-- ========== c_largeint (non-null) ==========

-- query 38
SELECT count(1), count(t1.k1), count(t1.c_largeint)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_largeint = t2.c_largeint;

-- query 39
SELECT count(1), count(t1.k1), count(t1.c_largeint)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_largeint = t2.c_largeint;

-- query 40
SELECT count(1), count(t1.k1), count(t1.c_largeint)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_largeint_null ==========

-- query 41
SELECT count(1), count(t1.k1), count(t1.c_largeint_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_largeint_null = t2.c_largeint_null;

-- query 42
SELECT count(1), count(t1.k1), count(t1.c_largeint_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_largeint_null = t2.c_largeint_null;

-- query 43
SELECT count(1), count(t1.k1), count(t1.c_largeint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_largeint_null = t2.c_largeint_null;

-- ========== c_float (non-null) ==========

-- query 44
SELECT count(1), count(t1.k1), count(t1.c_float)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- query 45
SELECT count(1), count(t1.k1), count(t1.c_float)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- query 46
SELECT count(1), count(t1.k1), count(t1.c_float)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_float_null ==========

-- query 47
SELECT count(1), count(t1.k1), count(t1.c_float_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_float_null = t2.c_float_null;

-- query 48
SELECT count(1), count(t1.k1), count(t1.c_float_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_float_null = t2.c_float_null;

-- query 49
SELECT count(1), count(t1.k1), count(t1.c_float_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float_null = t2.c_float_null;

-- ========== c_double (non-null) ==========

-- query 50
SELECT count(1), count(t1.k1), count(t1.c_double)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_double = t2.c_double;

-- query 51
SELECT count(1), count(t1.k1), count(t1.c_double)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_double = t2.c_double;

-- query 52
SELECT count(1), count(t1.k1), count(t1.c_double)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_double_null ==========

-- query 53
SELECT count(1), count(t1.k1), count(t1.c_double_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_double_null = t2.c_double_null;

-- query 54
SELECT count(1), count(t1.k1), count(t1.c_double_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_double_null = t2.c_double_null;

-- query 55
SELECT count(1), count(t1.k1), count(t1.c_double_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_double_null = t2.c_double_null;

-- ========== c_decimalv2 (non-null) ==========

-- query 56
SELECT count(1), count(t1.k1), count(t1.c_decimalv2)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_decimalv2 = t2.c_decimalv2;

-- query 57
SELECT count(1), count(t1.k1), count(t1.c_decimalv2)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_decimalv2 = t2.c_decimalv2;

-- query 58
SELECT count(1), count(t1.k1), count(t1.c_decimalv2)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_decimalv2_null ==========

-- query 59
SELECT count(1), count(t1.k1), count(t1.c_decimalv2_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_decimalv2_null = t2.c_decimalv2_null;

-- query 60
SELECT count(1), count(t1.k1), count(t1.c_decimalv2_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_decimalv2_null = t2.c_decimalv2_null;

-- query 61
SELECT count(1), count(t1.k1), count(t1.c_decimalv2_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_decimalv2_null = t2.c_decimalv2_null;

-- ========== c_decimal32 (non-null) ==========

-- query 62
SELECT count(1), count(t1.k1), count(t1.c_decimal32)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_decimal32 = t2.c_decimal32;

-- query 63
SELECT count(1), count(t1.k1), count(t1.c_decimal32)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_decimal32 = t2.c_decimal32;

-- query 64
SELECT count(1), count(t1.k1), count(t1.c_decimal32)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_decimal32_null ==========

-- query 65
SELECT count(1), count(t1.k1), count(t1.c_decimal32_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_decimal32_null = t2.c_decimal32_null;

-- query 66
SELECT count(1), count(t1.k1), count(t1.c_decimal32_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_decimal32_null = t2.c_decimal32_null;

-- query 67
SELECT count(1), count(t1.k1), count(t1.c_decimal32_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_decimal32_null = t2.c_decimal32_null;

-- ========== c_decimal64 (non-null) ==========

-- query 68
SELECT count(1), count(t1.k1), count(t1.c_decimal64)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_decimal64 = t2.c_decimal64;

-- query 69
SELECT count(1), count(t1.k1), count(t1.c_decimal64)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_decimal64 = t2.c_decimal64;

-- query 70
SELECT count(1), count(t1.k1), count(t1.c_decimal64)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_decimal64_null ==========

-- query 71
SELECT count(1), count(t1.k1), count(t1.c_decimal64_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_decimal64_null = t2.c_decimal64_null;

-- query 72
SELECT count(1), count(t1.k1), count(t1.c_decimal64_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_decimal64_null = t2.c_decimal64_null;

-- query 73
SELECT count(1), count(t1.k1), count(t1.c_decimal64_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_decimal64_null = t2.c_decimal64_null;

-- ========== c_decimal128 (non-null) ==========

-- query 74
SELECT count(1), count(t1.k1), count(t1.c_decimal128)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_decimal128 = t2.c_decimal128;

-- query 75
SELECT count(1), count(t1.k1), count(t1.c_decimal128)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_decimal128 = t2.c_decimal128;

-- query 76
SELECT count(1), count(t1.k1), count(t1.c_decimal128)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_decimal128_null ==========

-- query 77
SELECT count(1), count(t1.k1), count(t1.c_decimal128_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_decimal128_null = t2.c_decimal128_null;

-- query 78
SELECT count(1), count(t1.k1), count(t1.c_decimal128_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_decimal128_null = t2.c_decimal128_null;

-- query 79
SELECT count(1), count(t1.k1), count(t1.c_decimal128_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_decimal128_null = t2.c_decimal128_null;

-- ========== c_date (non-null) ==========

-- query 80
SELECT count(1), count(t1.k1), count(t1.c_date)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_date = t2.c_date;

-- query 81
SELECT count(1), count(t1.k1), count(t1.c_date)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_date = t2.c_date;

-- query 82
SELECT count(1), count(t1.k1), count(t1.c_date)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_date_null ==========

-- query 83
SELECT count(1), count(t1.k1), count(t1.c_date_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_date_null = t2.c_date_null;

-- query 84
SELECT count(1), count(t1.k1), count(t1.c_date_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_date_null = t2.c_date_null;

-- query 85
SELECT count(1), count(t1.k1), count(t1.c_date_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_date_null = t2.c_date_null;

-- ========== c_datetime (non-null) ==========

-- query 86
SELECT count(1), count(t1.k1), count(t1.c_datetime)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_datetime = t2.c_datetime;

-- query 87
SELECT count(1), count(t1.k1), count(t1.c_datetime)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_datetime = t2.c_datetime;

-- query 88
SELECT count(1), count(t1.k1), count(t1.c_datetime)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_datetime_null ==========

-- query 89
SELECT count(1), count(t1.k1), count(t1.c_datetime_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_datetime_null = t2.c_datetime_null;

-- query 90
SELECT count(1), count(t1.k1), count(t1.c_datetime_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_datetime_null = t2.c_datetime_null;

-- query 91
SELECT count(1), count(t1.k1), count(t1.c_datetime_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_datetime_null = t2.c_datetime_null;

-- ========== c_char (non-null) ==========

-- query 92
SELECT count(1), count(t1.k1), count(t1.c_char)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_char = t2.c_char;

-- query 93
SELECT count(1), count(t1.k1), count(t1.c_char)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_char = t2.c_char;

-- query 94
SELECT count(1), count(t1.k1), count(t1.c_char)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_char_null ==========

-- query 95
SELECT count(1), count(t1.k1), count(t1.c_char_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_char_null = t2.c_char_null;

-- query 96
SELECT count(1), count(t1.k1), count(t1.c_char_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_char_null = t2.c_char_null;

-- query 97
SELECT count(1), count(t1.k1), count(t1.c_char_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_char_null = t2.c_char_null;

-- ========== c_varchar (non-null) ==========

-- query 98
SELECT count(1), count(t1.k1), count(t1.c_varchar)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_varchar = t2.c_varchar;

-- query 99
SELECT count(1), count(t1.k1), count(t1.c_varchar)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_varchar = t2.c_varchar;

-- query 100
SELECT count(1), count(t1.k1), count(t1.c_varchar)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_float = t2.c_float;

-- ========== c_varchar_null ==========

-- query 101
SELECT count(1), count(t1.k1), count(t1.c_varchar_null)
FROM ${case_db}.t1 t1 JOIN ${case_db}.t1 t2 ON t1.c_varchar_null = t2.c_varchar_null;

-- query 102
SELECT count(1), count(t1.k1), count(t1.c_varchar_null)
FROM ${case_db}.t1 t1 FULL JOIN ${case_db}.t1 t2 ON t1.c_varchar_null = t2.c_varchar_null;

-- query 103
SELECT count(1), count(t1.k1), count(t1.c_varchar_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN ${case_db}.t1 t2 ON t1.c_varchar_null = t2.c_varchar_null;
