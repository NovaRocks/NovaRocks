-- Migrated from dev/test/sql/test_agg_function/R/test_always_null_statistic_funcs
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_always_null_statistic_funcs FORCE;
CREATE DATABASE sql_tests_test_always_null_statistic_funcs;
USE sql_tests_test_always_null_statistic_funcs;

-- name: test_always_null_statistic_funcs
-- query 2
-- @skip_result_check=true
USE sql_tests_test_always_null_statistic_funcs;
CREATE TABLE t1 (
    idx BIGINT,
    k BIGINT NULL,
    val1 BIGINT NULL,
    val2 BIGINT NULL
) PRIMARY KEY(idx)
DISTRIBUTED BY HASH (idx) BUCKETS 32
PROPERTIES("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE sql_tests_test_always_null_statistic_funcs;
CREATE TABLE t1_nonnull (
    idx BIGINT,
    k BIGINT NOT NULL,
    val1 BIGINT NOT NULL,
    val2 BIGINT NOT NULL
) PRIMARY KEY(idx)
DISTRIBUTED BY HASH (idx) BUCKETS 32
PROPERTIES("replication_num" = "1");

-- query 4
-- @skip_result_check=true
USE sql_tests_test_always_null_statistic_funcs;
INSERT INTO t1 (val2, val1, k, idx) values
    (10, 1,1,2),
    (20, 2,2,4),
    (30, 3,3,6),
    (NULL, 4,4,8),
    (50, NULL,5,10),
    (NULL, NULL,6,12),
    (70, NULL,7,14),
    (80, 8,8,16),
    (90, 9,9,18),
    (100, 10,10,20);

-- query 5
-- @skip_result_check=true
USE sql_tests_test_always_null_statistic_funcs;
INSERT INTO t1_nonnull (val2, val1, k, idx) values
    (10, 1,1,2),
    (20, 2,2,4),
    (30, 3,3,6),
    (40, 4,4,8),
    (50, 5,5,10),
    (60, 6,6,12),
    (70, 7,7,14),
    (80, 8,8,16),
    (90, 9,9,18),
    (100, 10,10,20);

-- query 6
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(var_samp(c1), 3) from w1 where c1 > 10;

-- query 7
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(var_samp(c1), 3) from w1;

-- query 8
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(var_samp(c1), 3) from w1;

-- query 9
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(var_samp(c1), 3) from w1;

-- query 10
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(var_samp(c1), 3) from w1;

-- query 11
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(var_samp(c1), 3) from w1;

-- query 12
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(VAR_SAMP(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS var_samp_val1
FROM t1
order by k;

-- query 13
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(VAR_SAMP(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS var_samp_val1
FROM t1_nonnull
order by k;

-- query 14
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1 where k > 100;

-- query 15
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1_nonnull where k > 100;

-- query 16
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1 where k = 2;

-- query 17
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1_nonnull where k = 2;

-- query 18
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 19
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1 where k in (5, 6);

-- query 20
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1_nonnull;

-- query 21
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1;

-- query 22
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(variance_samp(c1), 3) from w1 where c1 > 10;

-- query 23
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(variance_samp(c1), 3) from w1;

-- query 24
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(variance_samp(c1), 3) from w1;

-- query 25
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(variance_samp(c1), 3) from w1;

-- query 26
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(variance_samp(c1), 3) from w1;

-- query 27
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(variance_samp(c1), 3) from w1;

-- query 28
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(variance_samp(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS variance_samp_val1
FROM t1
order by k;

-- query 29
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(variance_samp(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS variance_samp_val1
FROM t1_nonnull
order by k;

-- query 30
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1 where k > 100;

-- query 31
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1_nonnull where k > 100;

-- query 32
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1 where k = 2;

-- query 33
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1_nonnull where k = 2;

-- query 34
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 35
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1 where k in (5, 6);

-- query 36
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1_nonnull;

-- query 37
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1;

-- query 38
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(stddev_samp(c1), 3) from w1 where c1 > 10;

-- query 39
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 40
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 41
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 42
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 43
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 44
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(stddev_samp(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS stddev_samp_val1
FROM t1
order by k;

-- query 45
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(stddev_samp(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS stddev_samp_val1
FROM t1_nonnull
order by k;

-- query 46
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1 where k > 100;

-- query 47
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1_nonnull where k > 100;

-- query 48
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1 where k = 2;

-- query 49
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1_nonnull where k = 2;

-- query 50
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 51
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1 where k in (5, 6);

-- query 52
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1_nonnull;

-- query 53
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1;

-- query 54
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1 where c1 > 10;

-- query 55
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 56
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, null), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 57
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 58
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (3, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 59
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (null, null), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 60
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 61
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (null, null), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 62
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (4, null), (3, 30), (null, 50) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 63
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    val2,
    round(covar_samp(val1, val2) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS covar_samp_val1
FROM t1
order by k;

-- query 64
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    val2,
    round(covar_samp(val1, val2) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS covar_samp_val1
FROM t1_nonnull
order by k;

-- query 65
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1 where k > 100;

-- query 66
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1_nonnull where k > 100;

-- query 67
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1 where k = 2;

-- query 68
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1_nonnull where k = 2;

-- query 69
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1 where k in (2, 5, 6);

-- query 70
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1 where k in (5, 6);

-- query 71
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1_nonnull;

-- query 72
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1;

-- query 73
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(corr(c1, c2), 3) from w1 where c1 > 10;

-- query 74
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10) )t)
select round(corr(c1, c2), 3) from w1;

-- query 75
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, null), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 76
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 77
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (3, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 78
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (null, null), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 79
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(corr(c1, c2), 3) from w1;

-- query 80
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (null, null), (3, 30) )t)
select round(corr(c1, c2), 3) from w1;

-- query 81
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (4, null), (3, 30), (null, 50) )t)
select round(corr(c1, c2), 3) from w1;

-- query 82
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    val2,
    round(corr(val1, val2) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS corr_val1
FROM t1
order by k;

-- query 83
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    val2,
    round(corr(val1, val2) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS corr_val1
FROM t1_nonnull
order by k;

-- query 84
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1 where k > 100;

-- query 85
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1_nonnull where k > 100;

-- query 86
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1 where k = 2;

-- query 87
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1_nonnull where k = 2;

-- query 88
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1 where k in (2, 5, 6);

-- query 89
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1 where k in (5, 6);

-- query 90
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1_nonnull;

-- query 91
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1;

-- query 92
-- @skip_result_check=true
USE sql_tests_test_always_null_statistic_funcs;
set enable_spill=true;

-- query 93
-- @skip_result_check=true
USE sql_tests_test_always_null_statistic_funcs;
set spill_mode="force";

-- query 94
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(var_samp(c1), 3) from w1 where c1 > 10;

-- query 95
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(var_samp(c1), 3) from w1;

-- query 96
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(var_samp(c1), 3) from w1;

-- query 97
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(var_samp(c1), 3) from w1;

-- query 98
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(var_samp(c1), 3) from w1;

-- query 99
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(var_samp(c1), 3) from w1;

-- query 100
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(VAR_SAMP(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS var_samp_val1
FROM t1
order by k;

-- query 101
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(VAR_SAMP(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS var_samp_val1
FROM t1_nonnull
order by k;

-- query 102
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1 where k > 100;

-- query 103
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1_nonnull where k > 100;

-- query 104
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1 where k = 2;

-- query 105
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1_nonnull where k = 2;

-- query 106
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 107
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1 where k in (5, 6);

-- query 108
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1_nonnull;

-- query 109
USE sql_tests_test_always_null_statistic_funcs;
select round(var_samp(val1), 3) from t1;

-- query 110
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(variance_samp(c1), 3) from w1 where c1 > 10;

-- query 111
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(variance_samp(c1), 3) from w1;

-- query 112
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(variance_samp(c1), 3) from w1;

-- query 113
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(variance_samp(c1), 3) from w1;

-- query 114
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(variance_samp(c1), 3) from w1;

-- query 115
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(variance_samp(c1), 3) from w1;

-- query 116
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(variance_samp(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS variance_samp_val1
FROM t1
order by k;

-- query 117
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(variance_samp(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS variance_samp_val1
FROM t1_nonnull
order by k;

-- query 118
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1 where k > 100;

-- query 119
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1_nonnull where k > 100;

-- query 120
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1 where k = 2;

-- query 121
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1_nonnull where k = 2;

-- query 122
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 123
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1 where k in (5, 6);

-- query 124
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1_nonnull;

-- query 125
USE sql_tests_test_always_null_statistic_funcs;
select round(variance_samp(val1), 3) from t1;

-- query 126
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(stddev_samp(c1), 3) from w1 where c1 > 10;

-- query 127
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 128
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 129
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 130
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 131
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 132
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(stddev_samp(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS stddev_samp_val1
FROM t1
order by k;

-- query 133
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    round(stddev_samp(val1) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS stddev_samp_val1
FROM t1_nonnull
order by k;

-- query 134
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1 where k > 100;

-- query 135
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1_nonnull where k > 100;

-- query 136
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1 where k = 2;

-- query 137
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1_nonnull where k = 2;

-- query 138
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 139
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1 where k in (5, 6);

-- query 140
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1_nonnull;

-- query 141
USE sql_tests_test_always_null_statistic_funcs;
select round(stddev_samp(val1), 3) from t1;

-- query 142
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1 where c1 > 10;

-- query 143
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 144
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, null), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 145
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 146
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (3, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 147
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (null, null), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 148
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 149
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (null, null), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 150
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (4, null), (3, 30), (null, 50) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 151
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    val2,
    round(covar_samp(val1, val2) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS covar_samp_val1
FROM t1
order by k;

-- query 152
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    val2,
    round(covar_samp(val1, val2) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS covar_samp_val1
FROM t1_nonnull
order by k;

-- query 153
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1 where k > 100;

-- query 154
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1_nonnull where k > 100;

-- query 155
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1 where k = 2;

-- query 156
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1_nonnull where k = 2;

-- query 157
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1 where k in (2, 5, 6);

-- query 158
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1 where k in (5, 6);

-- query 159
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1_nonnull;

-- query 160
USE sql_tests_test_always_null_statistic_funcs;
select round(covar_samp(val1, val2), 3) from t1;

-- query 161
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(corr(c1, c2), 3) from w1 where c1 > 10;

-- query 162
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10) )t)
select round(corr(c1, c2), 3) from w1;

-- query 163
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, null), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 164
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 165
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (3, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 166
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (null, null), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 167
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(corr(c1, c2), 3) from w1;

-- query 168
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (null, null), (3, 30) )t)
select round(corr(c1, c2), 3) from w1;

-- query 169
USE sql_tests_test_always_null_statistic_funcs;
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (4, null), (3, 30), (null, 50) )t)
select round(corr(c1, c2), 3) from w1;

-- query 170
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    val2,
    round(corr(val1, val2) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS corr_val1
FROM t1
order by k;

-- query 171
USE sql_tests_test_always_null_statistic_funcs;
SELECT
    k,
    val1,
    val2,
    round(corr(val1, val2) over (
        ORDER BY k ASC
        ROWS BETWEEN 2 PRECEDING and CURRENT ROW
    ), 3) AS corr_val1
FROM t1_nonnull
order by k;

-- query 172
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1 where k > 100;

-- query 173
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1_nonnull where k > 100;

-- query 174
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1 where k = 2;

-- query 175
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1_nonnull where k = 2;

-- query 176
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1 where k in (2, 5, 6);

-- query 177
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1 where k in (5, 6);

-- query 178
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1_nonnull;

-- query 179
USE sql_tests_test_always_null_statistic_funcs;
select round(corr(val1, val2), 3) from t1;
