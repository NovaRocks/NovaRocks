-- Migrated from dev/test/sql/test_agg_function/R/test_always_null_statistic_funcs
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_always_null_statistic_funcs
-- query 2
-- @skip_result_check=true
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(var_samp(c1), 3) from w1 where c1 > 10;

-- query 7
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(var_samp(c1), 3) from w1;

-- query 8
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(var_samp(c1), 3) from w1;

-- query 9
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(var_samp(c1), 3) from w1;

-- query 10
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(var_samp(c1), 3) from w1;

-- query 11
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(var_samp(c1), 3) from w1;

-- query 12
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(var_samp(val1), 3) from t1 where k > 100;

-- query 15
USE ${case_db};
select round(var_samp(val1), 3) from t1_nonnull where k > 100;

-- query 16
USE ${case_db};
select round(var_samp(val1), 3) from t1 where k = 2;

-- query 17
USE ${case_db};
select round(var_samp(val1), 3) from t1_nonnull where k = 2;

-- query 18
USE ${case_db};
select round(var_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 19
USE ${case_db};
select round(var_samp(val1), 3) from t1 where k in (5, 6);

-- query 20
USE ${case_db};
select round(var_samp(val1), 3) from t1_nonnull;

-- query 21
USE ${case_db};
select round(var_samp(val1), 3) from t1;

-- query 22
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(variance_samp(c1), 3) from w1 where c1 > 10;

-- query 23
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(variance_samp(c1), 3) from w1;

-- query 24
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(variance_samp(c1), 3) from w1;

-- query 25
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(variance_samp(c1), 3) from w1;

-- query 26
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(variance_samp(c1), 3) from w1;

-- query 27
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(variance_samp(c1), 3) from w1;

-- query 28
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(variance_samp(val1), 3) from t1 where k > 100;

-- query 31
USE ${case_db};
select round(variance_samp(val1), 3) from t1_nonnull where k > 100;

-- query 32
USE ${case_db};
select round(variance_samp(val1), 3) from t1 where k = 2;

-- query 33
USE ${case_db};
select round(variance_samp(val1), 3) from t1_nonnull where k = 2;

-- query 34
USE ${case_db};
select round(variance_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 35
USE ${case_db};
select round(variance_samp(val1), 3) from t1 where k in (5, 6);

-- query 36
USE ${case_db};
select round(variance_samp(val1), 3) from t1_nonnull;

-- query 37
USE ${case_db};
select round(variance_samp(val1), 3) from t1;

-- query 38
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(stddev_samp(c1), 3) from w1 where c1 > 10;

-- query 39
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 40
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 41
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 42
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 43
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 44
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(stddev_samp(val1), 3) from t1 where k > 100;

-- query 47
USE ${case_db};
select round(stddev_samp(val1), 3) from t1_nonnull where k > 100;

-- query 48
USE ${case_db};
select round(stddev_samp(val1), 3) from t1 where k = 2;

-- query 49
USE ${case_db};
select round(stddev_samp(val1), 3) from t1_nonnull where k = 2;

-- query 50
USE ${case_db};
select round(stddev_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 51
USE ${case_db};
select round(stddev_samp(val1), 3) from t1 where k in (5, 6);

-- query 52
USE ${case_db};
select round(stddev_samp(val1), 3) from t1_nonnull;

-- query 53
USE ${case_db};
select round(stddev_samp(val1), 3) from t1;

-- query 54
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1 where c1 > 10;

-- query 55
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 56
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, null), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 57
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 58
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (3, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 59
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (null, null), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 60
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 61
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (null, null), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 62
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (4, null), (3, 30), (null, 50) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 63
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1 where k > 100;

-- query 66
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1_nonnull where k > 100;

-- query 67
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1 where k = 2;

-- query 68
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1_nonnull where k = 2;

-- query 69
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1 where k in (2, 5, 6);

-- query 70
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1 where k in (5, 6);

-- query 71
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1_nonnull;

-- query 72
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1;

-- query 73
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(corr(c1, c2), 3) from w1 where c1 > 10;

-- query 74
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10) )t)
select round(corr(c1, c2), 3) from w1;

-- query 75
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, null), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 76
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 77
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (3, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 78
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (null, null), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 79
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(corr(c1, c2), 3) from w1;

-- query 80
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (null, null), (3, 30) )t)
select round(corr(c1, c2), 3) from w1;

-- query 81
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (4, null), (3, 30), (null, 50) )t)
select round(corr(c1, c2), 3) from w1;

-- query 82
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(corr(val1, val2), 3) from t1 where k > 100;

-- query 85
USE ${case_db};
select round(corr(val1, val2), 3) from t1_nonnull where k > 100;

-- query 86
USE ${case_db};
select round(corr(val1, val2), 3) from t1 where k = 2;

-- query 87
USE ${case_db};
select round(corr(val1, val2), 3) from t1_nonnull where k = 2;

-- query 88
USE ${case_db};
select round(corr(val1, val2), 3) from t1 where k in (2, 5, 6);

-- query 89
USE ${case_db};
select round(corr(val1, val2), 3) from t1 where k in (5, 6);

-- query 90
USE ${case_db};
select round(corr(val1, val2), 3) from t1_nonnull;

-- query 91
USE ${case_db};
select round(corr(val1, val2), 3) from t1;

-- query 92
-- @skip_result_check=true
USE ${case_db};
set enable_spill=true;

-- query 93
-- @skip_result_check=true
USE ${case_db};
set spill_mode="force";

-- query 94
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(var_samp(c1), 3) from w1 where c1 > 10;

-- query 95
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(var_samp(c1), 3) from w1;

-- query 96
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(var_samp(c1), 3) from w1;

-- query 97
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(var_samp(c1), 3) from w1;

-- query 98
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(var_samp(c1), 3) from w1;

-- query 99
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(var_samp(c1), 3) from w1;

-- query 100
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(var_samp(val1), 3) from t1 where k > 100;

-- query 103
USE ${case_db};
select round(var_samp(val1), 3) from t1_nonnull where k > 100;

-- query 104
USE ${case_db};
select round(var_samp(val1), 3) from t1 where k = 2;

-- query 105
USE ${case_db};
select round(var_samp(val1), 3) from t1_nonnull where k = 2;

-- query 106
USE ${case_db};
select round(var_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 107
USE ${case_db};
select round(var_samp(val1), 3) from t1 where k in (5, 6);

-- query 108
USE ${case_db};
select round(var_samp(val1), 3) from t1_nonnull;

-- query 109
USE ${case_db};
select round(var_samp(val1), 3) from t1;

-- query 110
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(variance_samp(c1), 3) from w1 where c1 > 10;

-- query 111
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(variance_samp(c1), 3) from w1;

-- query 112
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(variance_samp(c1), 3) from w1;

-- query 113
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(variance_samp(c1), 3) from w1;

-- query 114
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(variance_samp(c1), 3) from w1;

-- query 115
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(variance_samp(c1), 3) from w1;

-- query 116
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(variance_samp(val1), 3) from t1 where k > 100;

-- query 119
USE ${case_db};
select round(variance_samp(val1), 3) from t1_nonnull where k > 100;

-- query 120
USE ${case_db};
select round(variance_samp(val1), 3) from t1 where k = 2;

-- query 121
USE ${case_db};
select round(variance_samp(val1), 3) from t1_nonnull where k = 2;

-- query 122
USE ${case_db};
select round(variance_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 123
USE ${case_db};
select round(variance_samp(val1), 3) from t1 where k in (5, 6);

-- query 124
USE ${case_db};
select round(variance_samp(val1), 3) from t1_nonnull;

-- query 125
USE ${case_db};
select round(variance_samp(val1), 3) from t1;

-- query 126
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(stddev_samp(c1), 3) from w1 where c1 > 10;

-- query 127
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 128
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (null), (null) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 129
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (null), (null) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 130
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (3) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 131
USE ${case_db};
with w1 as (select column_0 as c1 from ( values (1), (2), (null), (3) )t)
select round(stddev_samp(c1), 3) from w1;

-- query 132
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(stddev_samp(val1), 3) from t1 where k > 100;

-- query 135
USE ${case_db};
select round(stddev_samp(val1), 3) from t1_nonnull where k > 100;

-- query 136
USE ${case_db};
select round(stddev_samp(val1), 3) from t1 where k = 2;

-- query 137
USE ${case_db};
select round(stddev_samp(val1), 3) from t1_nonnull where k = 2;

-- query 138
USE ${case_db};
select round(stddev_samp(val1), 3) from t1 where k in (2, 5, 6);

-- query 139
USE ${case_db};
select round(stddev_samp(val1), 3) from t1 where k in (5, 6);

-- query 140
USE ${case_db};
select round(stddev_samp(val1), 3) from t1_nonnull;

-- query 141
USE ${case_db};
select round(stddev_samp(val1), 3) from t1;

-- query 142
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1 where c1 > 10;

-- query 143
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 144
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, null), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 145
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 146
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (3, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 147
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (null, null), (null, null) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 148
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 149
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (null, null), (3, 30) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 150
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (4, null), (3, 30), (null, 50) )t)
select round(covar_samp(c1, c2), 3) from w1;

-- query 151
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1 where k > 100;

-- query 154
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1_nonnull where k > 100;

-- query 155
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1 where k = 2;

-- query 156
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1_nonnull where k = 2;

-- query 157
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1 where k in (2, 5, 6);

-- query 158
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1 where k in (5, 6);

-- query 159
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1_nonnull;

-- query 160
USE ${case_db};
select round(covar_samp(val1, val2), 3) from t1;

-- query 161
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(corr(c1, c2), 3) from w1 where c1 > 10;

-- query 162
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10) )t)
select round(corr(c1, c2), 3) from w1;

-- query 163
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, null), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 164
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 165
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (null, 20), (3, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 166
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (null, null), (null, null) )t)
select round(corr(c1, c2), 3) from w1;

-- query 167
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (3, 30) )t)
select round(corr(c1, c2), 3) from w1;

-- query 168
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (null, null), (3, 30) )t)
select round(corr(c1, c2), 3) from w1;

-- query 169
USE ${case_db};
with w1 as (select column_0 as c1, column_1 as c2 from ( values (1, 10), (2, 20), (4, null), (3, 30), (null, 50) )t)
select round(corr(c1, c2), 3) from w1;

-- query 170
USE ${case_db};
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
USE ${case_db};
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
USE ${case_db};
select round(corr(val1, val2), 3) from t1 where k > 100;

-- query 173
USE ${case_db};
select round(corr(val1, val2), 3) from t1_nonnull where k > 100;

-- query 174
USE ${case_db};
select round(corr(val1, val2), 3) from t1 where k = 2;

-- query 175
USE ${case_db};
select round(corr(val1, val2), 3) from t1_nonnull where k = 2;

-- query 176
USE ${case_db};
select round(corr(val1, val2), 3) from t1 where k in (2, 5, 6);

-- query 177
USE ${case_db};
select round(corr(val1, val2), 3) from t1 where k in (5, 6);

-- query 178
USE ${case_db};
select round(corr(val1, val2), 3) from t1_nonnull;

-- query 179
USE ${case_db};
select round(corr(val1, val2), 3) from t1;
