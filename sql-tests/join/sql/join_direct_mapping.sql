-- @tags=join,direct_mapping
-- Test Objective:
-- Validate direct column mapping correctness across all join types (INNER, LEFT/RIGHT OUTER,
-- LEFT/RIGHT SEMI/ANTI, FULL OUTER) with boolean, tinyint, and smallint join keys.
-- Each key type is tested under three build-side selectivity scenarios:
--   (a) moderate selectivity,  (b) single-value filter,  (c) empty build side.
-- Also tests join + post-filter predicate combinations.

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
    c_float float,
    c_float_null float NULL,
    c_double double,
    c_double_null double NULL,
    c_date date,
    c_date_null date NULL,
    c_datetime datetime,
    c_datetime_null datetime NULL
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
    idx % 2147483648,
    if (idx % 14 = 0, idx % 2147483648, null),
    idx,
    if (idx % 15 = 0, idx, null),
    idx,
    if (idx % 16 = 0, idx, null),
    idx,
    if (idx % 16 = 0, idx, null),
    date_add('2023-01-01', idx % 365),
    if (idx % 17 = 0, date_add('2023-01-01', idx % 365), null),
    date_add('2023-01-01 00:00:00', idx % 365 * 24 * 3600 + idx % 86400),
    if (idx % 18 = 0, date_add('2023-01-01 00:00:00', idx % 365 * 24 * 3600 + idx % 86400), null)
FROM ${case_db}.__row_util;

-- ========== c_bool_null: moderate build selectivity (k1 < 100) ==========

-- query 17
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 18
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 19
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 20
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 21
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 22
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 23
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 24
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- ========== c_bool_null: single-value filter (c_bool_null = true) ==========

-- query 25
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 26
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 27
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 28
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 29
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 30
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null = true)
SELECT count(1), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 31
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null = true)
SELECT count(1), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 32
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- ========== c_bool_null: empty build side (is null AND = true contradicts) ==========

-- query 33
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null IS NULL AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 34
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null IS NULL AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 35
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null IS NULL AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 36
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null IS NULL AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 37
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null IS NULL AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 38
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null IS NULL AND c_bool_null = true)
SELECT count(1), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 39
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null IS NULL AND c_bool_null = true)
SELECT count(1), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- query 40
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100 AND c_bool_null IS NULL AND c_bool_null = true)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null;

-- ========== c_bool_null: join + post-filter predicate (k1 < 10 build, mod 7 filter) ==========

-- query 41
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null WHERE (t1.k1 + t2.k1) % 7 = 0;

-- query 42
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null WHERE (t1.k1 + t2.k1) % 7 = 0;

-- query 43
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 44
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_bool_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 45
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 46
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 47
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 48
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_bool_null), count(t2.c_bool_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_bool_null = t2.c_bool_null WHERE (t1.k1 + t2.k1) % 7 = 0;

-- ========== c_tinyint_null: moderate build selectivity (k1 < 300, even) ==========

-- query 49
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 50
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 51
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0)
SELECT count(1), count(t1.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 52
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0)
SELECT count(1), count(t1.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 53
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 54
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0)
SELECT count(1), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 55
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0)
SELECT count(1), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 56
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- ========== c_tinyint_null: empty build side ==========

-- query 57
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0 AND c_tinyint_null IS NULL)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 58
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0 AND c_tinyint_null IS NULL)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 59
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0 AND c_tinyint_null IS NULL)
SELECT count(1), count(t1.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 60
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0 AND c_tinyint_null IS NULL)
SELECT count(1), count(t1.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 61
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0 AND c_tinyint_null IS NULL)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 62
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0 AND c_tinyint_null IS NULL)
SELECT count(1), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 63
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0 AND c_tinyint_null IS NULL)
SELECT count(1), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- query 64
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_tinyint_null % 2 = 0 AND c_tinyint_null IS NULL)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;

-- ========== c_tinyint_null: join + post-filter predicate ==========

-- query 65
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null WHERE (t1.k1 + t2.k1) % 7 = 0;

-- query 66
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null WHERE (t1.k1 + t2.k1) % 7 = 0;

-- query 67
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 68
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_tinyint_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 69
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 70
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 71
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 72
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_tinyint_null), count(t2.c_tinyint_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null WHERE (t1.k1 + t2.k1) % 7 = 0;

-- ========== c_smallint_null: moderate build selectivity (k1 < 300, even) ==========

-- query 73
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 74
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 75
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0)
SELECT count(1), count(t1.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 76
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0)
SELECT count(1), count(t1.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 77
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 78
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0)
SELECT count(1), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 79
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0)
SELECT count(1), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 80
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- ========== c_smallint_null: empty build side ==========

-- query 81
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0 AND c_smallint_null IS NULL)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 82
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0 AND c_smallint_null IS NULL)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 83
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0 AND c_smallint_null IS NULL)
SELECT count(1), count(t1.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 84
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0 AND c_smallint_null IS NULL)
SELECT count(1), count(t1.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 85
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0 AND c_smallint_null IS NULL)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 86
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0 AND c_smallint_null IS NULL)
SELECT count(1), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 87
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0 AND c_smallint_null IS NULL)
SELECT count(1), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- query 88
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 300 AND c_smallint_null % 2 = 0 AND c_smallint_null IS NULL)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null;

-- ========== c_smallint_null: join + post-filter predicate ==========

-- query 89
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null WHERE (t1.k1 + t2.k1) % 7 = 0;

-- query 90
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT OUTER JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null WHERE (t1.k1 + t2.k1) % 7 = 0;

-- query 91
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 92
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_smallint_null)
FROM ${case_db}.t1 t1 LEFT ANTI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 93
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 RIGHT OUTER JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 94
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 RIGHT SEMI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 95
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 RIGHT ANTI JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null AND (t1.k1 + t2.k1) % 7 = 0;

-- query 96
WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 10)
SELECT count(1), count(t1.c_smallint_null), count(t2.c_smallint_null)
FROM ${case_db}.t1 t1 FULL JOIN w1 t2 ON t1.c_smallint_null = t2.c_smallint_null WHERE (t1.k1 + t2.k1) % 7 = 0;
