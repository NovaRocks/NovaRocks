-- Test Objective:
-- 1. Validate runtime bitset filter for supported column types: BOOLEAN, TINYINT,
--    SMALLINT, INT, BIGINT, DECIMAL64, DATE, and low-cardinality VARCHAR.
-- 2. Verify bitset filter triggers when value interval is small (join with t2)
--    and does NOT trigger when interval is large (join with t3).
-- 3. Cover null-safe equality (<=>), cross-exchange (distinct before join),
--    shuffle join (should not use bitset), and disabled bitset filter via SET_VAR.
-- 4. Verify multiple simultaneous filters (BLOOM + BITSET + EMPTY).
-- Note: @sequential in dev/test means no parallel interference; sql-tests runs
-- all steps in a single case sequentially so this is naturally preserved.

-- Prepare helper utility tables

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.row_util_base (
  k1 BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 32
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.row_util_base SELECT generate_series FROM TABLE(generate_series(0, 10000 - 1));
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base; -- 20000
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base; -- 40000
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base; -- 80000
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base; -- 160000
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base; -- 320000
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base; -- 640000
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base; -- 1280000

CREATE TABLE ${case_db}.row_util (
  idx BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(idx)
DISTRIBUTED BY HASH(idx) BUCKETS 32
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.row_util SELECT row_number() over() AS idx FROM ${case_db}.row_util_base;

-- Main test tables
CREATE TABLE ${case_db}.t1 (
  k1 BIGINT NULL,
  c_bool_1_null BOOLEAN NULL,
  c_bool_2_notnull BOOLEAN NOT NULL,
  c_tinyint_1_null TINYINT NULL,
  c_tinyint_2_notnull TINYINT NOT NULL,
  c_smallint_1_null SMALLINT NULL,
  c_smallint_2_notnull SMALLINT NOT NULL,
  c_int_1_null INT NULL,
  c_int_2_notnull INT NOT NULL,
  c_bigint_1_null BIGINT NULL,
  c_bigint_2_notnull BIGINT NOT NULL,
  c_date_1_null DATE NULL,
  c_date_2_notnull DATE NULL,
  c_decimal64_1_null DECIMAL(18) NULL,
  c_decimal64_2_notnull DECIMAL(18) NOT NULL,
  c_str_1_null STRING NULL,
  c_str_2_notnull STRING NOT NULL,
  c_str_3_low_null STRING NULL,
  c_str_4_low_notnull STRING NOT NULL,
  c_datetime_1_seq DATETIME NULL,
  c_datetime_2_seq DATETIME NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 32
PROPERTIES ("replication_num" = "1");

-- t2: small value interval, triggers bitset filter
CREATE TABLE ${case_db}.t2 (
  k1 BIGINT NULL,
  c_bool_1_null BOOLEAN NULL,
  c_bool_2_notnull BOOLEAN NOT NULL,
  c_tinyint_1_null TINYINT NULL,
  c_tinyint_2_notnull TINYINT NOT NULL,
  c_smallint_1_null SMALLINT NULL,
  c_smallint_2_notnull SMALLINT NOT NULL,
  c_int_1_null INT NULL,
  c_int_2_notnull INT NOT NULL,
  c_bigint_1_null BIGINT NULL,
  c_bigint_2_notnull BIGINT NOT NULL,
  c_date_1_null DATE NULL,
  c_date_2_notnull DATE NULL,
  c_decimal64_1_null DECIMAL(18) NULL,
  c_decimal64_2_notnull DECIMAL(18) NOT NULL,
  c_str_1_null STRING NULL,
  c_str_2_notnull STRING NOT NULL,
  c_str_3_low_null STRING NULL,
  c_str_4_low_notnull STRING NOT NULL,
  c_datetime_1_seq DATETIME NULL,
  c_datetime_2_seq DATETIME NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 32
PROPERTIES ("replication_num" = "1");

-- t3: large value interval, should NOT trigger bitset filter
CREATE TABLE ${case_db}.t3 (
  k1 BIGINT NULL,
  c_bool_1_null BOOLEAN NULL,
  c_bool_2_notnull BOOLEAN NOT NULL,
  c_tinyint_1_null TINYINT NULL,
  c_tinyint_2_notnull TINYINT NOT NULL,
  c_smallint_1_null SMALLINT NULL,
  c_smallint_2_notnull SMALLINT NOT NULL,
  c_int_1_null INT NULL,
  c_int_2_notnull INT NOT NULL,
  c_bigint_1_null BIGINT NULL,
  c_bigint_2_notnull BIGINT NOT NULL,
  c_date_1_null DATE NULL,
  c_date_2_notnull DATE NULL,
  c_decimal64_1_null DECIMAL(18) NULL,
  c_decimal64_2_notnull DECIMAL(18) NOT NULL,
  c_str_1_null STRING NULL,
  c_str_2_notnull STRING NOT NULL,
  c_str_3_low_null STRING NULL,
  c_str_4_low_notnull STRING NOT NULL,
  c_datetime_1_seq DATETIME NULL,
  c_datetime_2_seq DATETIME NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 32
PROPERTIES ("replication_num" = "1");

-- Insert t2: small value interval (idx % N for bounded values)
INSERT INTO ${case_db}.t2
SELECT
    idx,
    idx % 2 = 0, idx % 2 = 0,
    idx % 128, idx % 128,
    idx % 32768, idx % 32768,
    idx % 2147483648, idx % 2147483648,
    idx, idx,
    cast(date_add('2023-01-01', interval idx day) as date),
    cast(date_add('2023-01-01', interval idx day) as date),
    idx, idx,
    concat('str-abc-', idx), concat('str-abc-', idx),
    concat('str-abc-', idx % 256), concat('str-abc-', idx % 256),
    cast(date_add('2023-01-01', interval idx second) as datetime),
    cast(date_add('2023-01-01', interval idx second) as datetime)
FROM ${case_db}.row_util
ORDER BY idx LIMIT 100000;

-- Insert t2 rows with some NULLs
INSERT INTO ${case_db}.t2
SELECT
    idx,
    NULL, idx % 2 = 0,
    NULL, idx % 128,
    NULL, idx % 32768,
    NULL, idx % 2147483648,
    NULL, idx,
    NULL, cast(date_add('2023-01-01', interval idx day) as date),
    NULL, idx,
    NULL, concat('str-abc-', idx),
    NULL, concat('str-abc-', idx % 256),
    NULL, cast(date_add('2023-01-01', interval idx second) as datetime)
FROM ${case_db}.row_util
ORDER BY idx LIMIT 100000, 10000;

-- Insert t3: large value interval (idx * 37 for spread values)
INSERT INTO ${case_db}.t3
SELECT
    idx,
    idx % 2 = 0, idx % 2 = 0,
    idx % 128, idx % 128,
    idx % 32768, idx % 32768,
    idx * 37 % 2147483648, idx * 37 % 2147483648,
    idx * 37, idx * 37,
    cast(date_add('2023-01-01', interval idx * 37 day) as date),
    cast(date_add('2023-01-01', interval idx * 37 day) as date),
    idx * 37, idx * 37,
    concat('str-abc-', idx), concat('str-abc-', idx),
    concat('str-abc-', idx % 256), concat('str-abc-', idx % 256),
    cast(date_add('2023-01-01', interval idx second) as datetime),
    cast(date_add('2023-01-01', interval idx second) as datetime)
FROM ${case_db}.row_util
ORDER BY idx LIMIT 100000;

-- Insert t3 rows with some NULLs
INSERT INTO ${case_db}.t3
SELECT
    idx,
    NULL, idx % 2 = 0,
    NULL, idx % 128,
    NULL, idx % 32768,
    NULL, idx * 37 % 2147483648,
    NULL, idx * 37,
    NULL, cast(date_add('2023-01-01', interval idx * 37 day) as date),
    NULL, idx * 37,
    NULL, concat('str-abc-', idx),
    NULL, concat('str-abc-', idx % 256),
    NULL, cast(date_add('2023-01-01', interval idx second) as datetime)
FROM ${case_db}.row_util
ORDER BY idx LIMIT 100000, 10000;

-- Insert t1: all rows from row_util (1.28M rows non-null)
INSERT INTO ${case_db}.t1
SELECT
    idx,
    idx % 2 = 0, idx % 2 = 0,
    idx % 128, idx % 128,
    idx % 32768, idx % 32768,
    idx % 2147483648, idx % 2147483648,
    idx, idx,
    cast(date_add('2023-01-01', interval idx day) as date),
    cast(date_add('2023-01-01', interval idx day) as date),
    idx, idx,
    concat('str-abc-', idx), concat('str-abc-', idx),
    concat('str-abc-', idx % 256), concat('str-abc-', idx % 256),
    cast(date_add('2023-01-01', interval idx second) as datetime),
    cast(date_add('2023-01-01', interval idx second) as datetime)
FROM ${case_db}.row_util;

-- Insert t1 rows with some NULLs
INSERT INTO ${case_db}.t1
SELECT
    idx,
    NULL, idx % 2 = 0,
    NULL, idx % 128,
    NULL, idx % 32768,
    NULL, idx % 2147483648,
    NULL, idx,
    NULL, cast(date_add('2023-01-01', interval idx day) as date),
    NULL, idx,
    NULL, concat('str-abc-', idx),
    NULL, concat('str-abc-', idx % 256),
    NULL, cast(date_add('2023-01-01', interval idx second) as datetime)
FROM ${case_db}.row_util
ORDER BY idx LIMIT 100000, 10000;

-- ===========================================================================
-- Section 1: type coverage - broadcast join with small build side (limit 10)
-- ===========================================================================

-- query 2
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_bool_1_null);
-- query 3
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_bool_2_notnull);
-- query 4
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_tinyint_1_null);
-- query 5
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_tinyint_2_notnull);
-- query 6
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_smallint_1_null);
-- query 7
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_smallint_2_notnull);
-- query 8
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_int_1_null);
-- query 9
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_int_2_notnull);
-- query 10
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_bigint_1_null);
-- query 11
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_bigint_2_notnull);
-- query 12
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_date_1_null);
-- query 13
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_date_2_notnull);
-- query 14
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_decimal64_1_null);
-- query 15
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_decimal64_2_notnull);
-- query 16
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_str_1_null);
-- query 17
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_str_2_notnull);
-- query 18
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_str_3_low_null);
-- query 19
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_str_4_low_notnull);
-- query 20
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_datetime_1_seq);
-- query 21
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_datetime_2_seq);

-- ===========================================================================
-- Section 2: null-safe equality (<=>)
-- ===========================================================================
-- query 22
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 on t1.c_bool_1_null <=> w1.c_bool_1_null;
-- query 23
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 on t1.c_bool_2_notnull <=> w1.c_bool_2_notnull;
-- query 24
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 on t1.c_tinyint_1_null <=> w1.c_tinyint_1_null;
-- query 25
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 on t1.c_tinyint_2_notnull <=> w1.c_tinyint_2_notnull;
-- query 26
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_smallint_1_null <=> t2.c_smallint_1_null;
-- query 27
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_smallint_2_notnull <=> t2.c_smallint_2_notnull;
-- query 28
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_int_1_null <=> t2.c_int_1_null;
-- query 29
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_int_2_notnull <=> t2.c_int_2_notnull;
-- query 30
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_bigint_1_null <=> t2.c_bigint_1_null;
-- query 31
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_bigint_2_notnull <=> t2.c_bigint_2_notnull;
-- query 32
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_date_1_null <=> t2.c_date_1_null;
-- query 33
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_date_2_notnull <=> t2.c_date_2_notnull;
-- query 34
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_decimal64_1_null <=> t2.c_decimal64_1_null;
-- query 35
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_decimal64_2_notnull <=> t2.c_decimal64_2_notnull;
-- query 36
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_str_1_null <=> t2.c_str_1_null;
-- query 37
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_str_2_notnull <=> t2.c_str_2_notnull;
-- query 38
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 on t1.c_str_3_low_null <=> w1.c_str_3_low_null;
-- query 39
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 on t1.c_str_4_low_notnull <=> w1.c_str_4_low_notnull;
-- query 40
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_datetime_1_seq <=> t2.c_datetime_1_seq;
-- query 41
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 on t1.c_datetime_2_seq <=> t2.c_datetime_2_seq;

-- ===========================================================================
-- Section 3: left scan and broadcast join cross exchange (distinct before join)
-- ===========================================================================
-- query 42
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from (select distinct c_bool_1_null from ${case_db}.t1)t join [broadcast] w1 using(c_bool_1_null);
-- query 43
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from (select distinct c_bool_2_notnull from ${case_db}.t1)t join [broadcast] w1 using(c_bool_2_notnull);
-- query 44
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from (select distinct c_tinyint_1_null from ${case_db}.t1)t join [broadcast] w1 using(c_tinyint_1_null);
-- query 45
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from (select distinct c_tinyint_2_notnull from ${case_db}.t1)t join [broadcast] w1 using(c_tinyint_2_notnull);
-- query 46
select count(1) from (select distinct c_smallint_1_null from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_smallint_1_null);
-- query 47
select count(1) from (select distinct c_smallint_2_notnull from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_smallint_2_notnull);
-- query 48
select count(1) from (select distinct c_int_1_null from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_int_1_null);
-- query 49
select count(1) from (select distinct c_int_2_notnull from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_int_2_notnull);
-- query 50
select count(1) from (select distinct c_bigint_1_null from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_bigint_1_null);
-- query 51
select count(1) from (select distinct c_bigint_2_notnull from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_bigint_2_notnull);
-- query 52
select count(1) from (select distinct c_date_1_null from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_date_1_null);
-- query 53
select count(1) from (select distinct c_date_2_notnull from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_date_2_notnull);
-- query 54
select count(1) from (select distinct c_decimal64_1_null from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_decimal64_1_null);
-- query 55
select count(1) from (select distinct c_decimal64_2_notnull from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_decimal64_2_notnull);
-- query 56
select count(1) from (select distinct c_str_1_null from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_str_1_null);
-- query 57
select count(1) from (select distinct c_str_2_notnull from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_str_2_notnull);
-- query 58
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from (select distinct c_str_3_low_null from ${case_db}.t1)t join [broadcast] w1 using(c_str_3_low_null);
-- query 59
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from (select distinct c_str_4_low_notnull from ${case_db}.t1)t join [broadcast] w1 using(c_str_4_low_notnull);
-- query 60
select count(1) from (select distinct c_datetime_1_seq from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_datetime_1_seq);
-- query 61
select count(1) from (select distinct c_datetime_2_seq from ${case_db}.t1)t join [broadcast] ${case_db}.t2 using(c_datetime_2_seq);

-- ===========================================================================
-- Section 4: shuffle join should NOT use bitset filter
-- ===========================================================================
-- query 62
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [shuffle] w1 using(c_bool_1_null);
-- query 63
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [shuffle] w1 using(c_bool_2_notnull);
-- query 64
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [shuffle] w1 using(c_tinyint_1_null);
-- query 65
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [shuffle] w1 using(c_tinyint_2_notnull);
-- query 66
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_smallint_1_null);
-- query 67
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_smallint_2_notnull);
-- query 68
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_int_1_null);
-- query 69
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_int_2_notnull);
-- query 70
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_bigint_1_null);
-- query 71
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_bigint_2_notnull);
-- query 72
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_date_1_null);
-- query 73
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_date_2_notnull);
-- query 74
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_decimal64_1_null);
-- query 75
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_decimal64_2_notnull);
-- query 76
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_str_1_null);
-- query 77
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_str_2_notnull);
-- query 78
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [shuffle] w1 using(c_str_3_low_null);
-- query 79
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [shuffle] w1 using(c_str_4_low_notnull);
-- query 80
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_datetime_1_seq);
-- query 81
select count(1) from ${case_db}.t1 join [shuffle] ${case_db}.t2 using(c_datetime_2_seq);

-- ===========================================================================
-- Section 5: disable enable_join_runtime_bitset_filter via SET_VAR
-- ===========================================================================
-- query 82
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] w1 using(c_bool_1_null);
-- query 83
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] w1 using(c_bool_2_notnull);
-- query 84
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] w1 using(c_tinyint_1_null);
-- query 85
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] w1 using(c_tinyint_2_notnull);
-- query 86
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_smallint_1_null);
-- query 87
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_smallint_2_notnull);
-- query 88
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_int_1_null);
-- query 89
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_int_2_notnull);
-- query 90
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_bigint_1_null);
-- query 91
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_bigint_2_notnull);
-- query 92
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_date_1_null);
-- query 93
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_date_2_notnull);
-- query 94
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_decimal64_1_null);
-- query 95
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_decimal64_2_notnull);
-- query 96
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_str_1_null);
-- query 97
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_str_2_notnull);
-- query 98
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] w1 using(c_str_3_low_null);
-- query 99
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] w1 using(c_str_4_low_notnull);
-- query 100
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_datetime_1_seq);
-- query 101
select /*+SET_VAR(enable_join_runtime_bitset_filter=false)*/ count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t2 using(c_datetime_2_seq);

-- ===========================================================================
-- Section 6: join with t3 (large value interval, bitset filter should NOT trigger)
-- ===========================================================================
-- query 102
with w1 as (select * from ${case_db}.t3 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_bool_1_null);
-- query 103
with w1 as (select * from ${case_db}.t3 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_bool_2_notnull);
-- query 104 (t2 still used for tinyint which has same small cardinality)
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_tinyint_1_null);
-- query 105
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_tinyint_2_notnull);
-- query 106
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_smallint_1_null);
-- query 107
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_smallint_2_notnull);
-- query 108
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_int_1_null);
-- query 109
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_int_2_notnull);
-- query 110
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_bigint_1_null);
-- query 111
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_bigint_2_notnull);
-- query 112
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_date_1_null);
-- query 113
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_date_2_notnull);
-- query 114
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_decimal64_1_null);
-- query 115
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_decimal64_2_notnull);
-- query 116
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_str_1_null);
-- query 117
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_str_2_notnull);
-- query 118 (t2 still used for low-cardinality str)
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_str_3_low_null);
-- query 119
with w1 as (select * from ${case_db}.t2 order by k1 limit 10)
select count(1) from ${case_db}.t1 join [broadcast] w1 using(c_str_4_low_notnull);
-- query 120
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_datetime_1_seq);
-- query 121
select count(1) from ${case_db}.t1 join [broadcast] ${case_db}.t3 using(c_datetime_2_seq);

-- ===========================================================================
-- Section 7: multiple simultaneous filters (BLOOM + BITSET + EMPTY)
-- ===========================================================================
-- query 122
select count(1) from
    ${case_db}.t1
    join [broadcast] ${case_db}.t2 on t1.c_int_1_null = t2.c_int_1_null
    join [shuffle] ${case_db}.t3 on t1.c_int_1_null = t3.c_int_1_null;
-- query 123
select count(1) from
    ${case_db}.t1
    join [broadcast] ${case_db}.t2 on t1.c_int_1_null = t2.c_int_1_null
    join [shuffle] ${case_db}.t3 on t1.c_date_1_null = t3.c_date_1_null;
-- query 124
select count(1) from
    (select distinct c_int_1_null from ${case_db}.t1) t1
    join [broadcast] ${case_db}.t2 on t1.c_int_1_null = t2.c_int_1_null
    join [shuffle] ${case_db}.t3 on t1.c_int_1_null = t3.c_int_1_null;
-- query 125
select count(1) from
    (select distinct c_int_1_null, c_date_1_null from ${case_db}.t1) t1
    join [broadcast] ${case_db}.t2 on t1.c_int_1_null = t2.c_int_1_null
    join [shuffle] ${case_db}.t3 on t1.c_date_1_null = t3.c_date_1_null;
