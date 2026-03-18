-- @tags=join,range_direct_mapping,broadcast
-- Test Objective:
-- Validate hash join correctness with broadcast join hint and the
-- enable_hash_join_range_direct_mapping_opt optimization (both enabled and
-- disabled). Tests cover inner join, left join, left semi join, and left anti
-- join with nullable and non-nullable int/bigint columns on a large dataset
-- (1.28M rows). Also tests CTE-based union-all doubling to exercise join on
-- non-trivial input shapes. Filters (modulo-based) are applied to verify
-- correct row counts after join with the optimization active.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.__row_util_base;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.__row_util;

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t1;

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.__row_util_base (
  k1 bigint NULL
);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT generate_series FROM TABLE(generate_series(0, 10000 - 1));

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
INSERT INTO ${case_db}.__row_util_base SELECT * FROM ${case_db}.__row_util_base;

-- query 12
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util_base SELECT * FROM ${case_db}.__row_util_base;

-- query 13
-- @skip_result_check=true
CREATE TABLE ${case_db}.__row_util (
  idx bigint NULL
);

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.__row_util SELECT row_number() over() as idx FROM ${case_db}.__row_util_base;

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 bigint NULL,
    c_int int,
    c_int_null int NULL,
    c_bigint bigint,
    c_bigint_null bigint NULL,
    c_largeint bigint,
    c_largeint_null bigint NULL,
    c_double double,
    c_double_null double NULL,
    c_string STRING,
    c_string_null STRING NULL
);

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.t1
SELECT
    idx,
    idx,
    if (idx % 13 = 0, idx, null),
    idx,
    if (idx % 14 = 0, idx, null),
    idx,
    if (idx % 15 = 0, idx, null),
    idx,
    if (idx % 16 = 0, idx, null),
    concat('str-', idx),
    if (idx % 17 = 0, concat('str-', idx), null)
FROM ${case_db}.__row_util;

-- ============================================================
-- Basic broadcast join (optimization enabled by default)
-- ============================================================

-- query 17
-- inner join on c_int (non-null)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int = t2.c_int;

-- query 18
-- left join on c_bigint (non-null)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint;

-- query 19
-- inner join on c_int_null (nullable)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null;

-- query 20
-- left join on c_bigint_null (nullable)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null;

-- ============================================================
-- CTE union-all doubling + broadcast join
-- ============================================================

-- query 21
-- CTE inner join on c_int
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int;

-- query 22
-- CTE inner join on c_int (duplicate to match original)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int;

-- query 23
-- CTE inner join on c_bigint
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint;

-- query 24
-- CTE inner join on c_bigint (duplicate to match original)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint;

-- ============================================================
-- Broadcast join with modulo filter (% 10 != 0)
-- ============================================================

-- query 25
-- inner join on c_int with modulo filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int = t2.c_int where t2.c_int % 10 != 0;

-- query 26
-- left join on c_bigint with modulo filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint where t2.c_int % 10 != 0;

-- query 27
-- inner join on c_int_null with modulo filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null where t2.c_int % 10 != 0;

-- query 28
-- left join on c_bigint_null with modulo filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null where t2.c_int % 10 != 0;

-- query 29
-- CTE inner join on c_int with modulo filter
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int where t2.c_int % 10 != 0;

-- query 30
-- CTE inner join on c_int with modulo filter (duplicate)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int where t2.c_int % 10 != 0;

-- query 31
-- CTE inner join on c_bigint with modulo filter
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint where t2.c_int % 10 != 0;

-- query 32
-- CTE inner join on c_bigint with modulo filter (duplicate)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint where t2.c_int % 10 != 0;

-- ============================================================
-- Broadcast join with modulo filter (% 10 < 5)
-- ============================================================

-- query 33
-- inner join on c_int with modulo < 5 filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int = t2.c_int where t2.c_int % 10 < 5;

-- query 34
-- left join on c_bigint with modulo < 5 filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint where t2.c_int % 10 < 5;

-- query 35
-- inner join on c_int_null with modulo < 5 filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null where t2.c_int % 10 < 5;

-- query 36
-- left join on c_bigint_null with modulo < 5 filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null where t2.c_int % 10 < 5;

-- query 37
-- CTE inner join on c_int with modulo < 5 filter
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int where t2.c_int % 10 < 5;

-- query 38
-- CTE inner join on c_int with modulo < 5 filter (duplicate)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int where t2.c_int % 10 < 5;

-- query 39
-- CTE inner join on c_bigint with modulo < 5 filter
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint where t2.c_int % 10 < 5;

-- query 40
-- CTE inner join on c_bigint with modulo < 5 filter (duplicate)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM w1 t1 JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint where t2.c_int % 10 < 5;

-- ============================================================
-- LEFT SEMI JOIN with broadcast hint
-- ============================================================

-- query 41
-- left semi join on c_int
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT SEMI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int = t2.c_int;

-- query 42
-- left semi join on c_bigint
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT SEMI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint;

-- query 43
-- left semi join on c_int_null
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT SEMI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null;

-- query 44
-- left semi join on c_bigint_null
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT SEMI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null;

-- query 45
-- CTE left semi join on c_int
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM w1 t1 LEFT SEMI JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int;

-- query 46
-- CTE left semi join on c_int (duplicate)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM w1 t1 LEFT SEMI JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int;

-- query 47
-- CTE left semi join on c_bigint
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM w1 t1 LEFT SEMI JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint;

-- query 48
-- CTE left semi join on c_bigint (duplicate)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM w1 t1 LEFT SEMI JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint;

-- ============================================================
-- LEFT ANTI JOIN with broadcast hint
-- ============================================================

-- query 49
-- left anti join on c_int
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int = t2.c_int;

-- query 50
-- left anti join on c_bigint
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint;

-- query 51
-- left anti join on c_int_null
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null;

-- query 52
-- left anti join on c_bigint_null
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null;

-- query 53
-- CTE left anti join on c_int
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM w1 t1 LEFT ANTI JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int;

-- query 54
-- CTE left anti join on c_int (duplicate)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM w1 t1 LEFT ANTI JOIN [broadcast] w1 t2 on t1.c_int = t2.c_int;

-- query 55
-- CTE left anti join on c_bigint
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM w1 t1 LEFT ANTI JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint;

-- query 56
-- CTE left anti join on c_bigint (duplicate)
with w1 as (
    select * from ${case_db}.t1 union all select * from ${case_db}.t1
)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM w1 t1 LEFT ANTI JOIN [broadcast] w1 t2 on t1.c_bigint = t2.c_bigint;

-- ============================================================
-- With optimization disabled: enable_hash_join_range_direct_mapping_opt = false
-- ============================================================

-- query 57
-- @skip_result_check=true
set enable_hash_join_range_direct_mapping_opt = false;

-- query 58
-- opt disabled: inner join on c_int with modulo < 5 filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int = t2.c_int where t2.c_int % 10 < 5;

-- query 59
-- opt disabled: left join on c_bigint with modulo < 5 filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint where t2.c_int % 10 < 5;

-- query 60
-- opt disabled: inner join on c_int_null with modulo < 5 filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null where t2.c_int % 10 < 5;

-- query 61
-- opt disabled: left join on c_bigint_null with modulo < 5 filter
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null where t2.c_int % 10 < 5;

-- query 62
-- opt disabled: inner join on c_int with modulo < 5 filter (duplicate)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int = t2.c_int where t2.c_int % 10 < 5;

-- query 63
-- opt disabled: left join on c_bigint with modulo < 5 filter (duplicate)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint where t2.c_int % 10 < 5;

-- query 64
-- opt disabled: inner join on c_int_null with modulo < 5 filter (duplicate)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null where t2.c_int % 10 < 5;

-- query 65
-- opt disabled: left join on c_bigint_null with modulo < 5 filter (duplicate)
SELECT count(t1.c_int), count(t1.c_bigint), count(t2.c_int), count(t2.c_bigint)
FROM ${case_db}.t1 LEFT JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null where t2.c_int % 10 < 5;

-- query 66
-- opt disabled: left anti join on c_int
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int = t2.c_int;

-- query 67
-- opt disabled: left anti join on c_bigint
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint;

-- query 68
-- opt disabled: left anti join on c_int_null
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null;

-- query 69
-- opt disabled: left anti join on c_bigint_null
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null;

-- query 70
-- opt disabled: left anti join on c_int (duplicate)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int = t2.c_int;

-- query 71
-- opt disabled: left anti join on c_bigint (duplicate)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint;

-- query 72
-- opt disabled: left anti join on c_int_null (duplicate)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null;

-- query 73
-- opt disabled: left anti join on c_bigint_null (duplicate)
SELECT count(t1.c_int), count(t1.c_bigint)
FROM ${case_db}.t1 LEFT ANTI JOIN [broadcast] ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null;
