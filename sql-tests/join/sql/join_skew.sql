-- @tags=join,skew
-- Test Objective:
-- 1. Validate skew join hint (v1) correctness across all column types.
-- 2. Verify join and left join with [skew|...] hint produce correct results
--    when skew values are specified for tinyint, smallint, int, bigint,
--    largeint, float, double, decimal, date, datetime, string, and cross-type joins.
-- Test Flow:
-- 1. Create two tables with multiple typed columns.
-- 2. For each type, truncate, insert skewed data, then run join/left join
--    with skew hint covering representative types and join directions.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t1;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t2;

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    c_key      INT NOT NULL,
    c_tinyint  TINYINT,
    c_smallint SMALLINT,
    c_int      INT,
    c_bigint   BIGINT,
    c_largeint LARGEINT,
    c_float    FLOAT,
    c_double   DOUBLE,
    c_decimal  DECIMAL(26,2),
    c_date     DATE,
    c_datetime DATETIME,
    c_string   STRING
)
DUPLICATE KEY(c_key)
DISTRIBUTED BY HASH(c_key) BUCKETS 1
PROPERTIES (
    "replication_num"="1"
);

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
    c_key      INT NOT NULL,
    c_tinyint  TINYINT,
    c_smallint SMALLINT,
    c_int      INT,
    c_bigint   BIGINT,
    c_largeint LARGEINT,
    c_float    FLOAT,
    c_double   DOUBLE,
    c_decimal  DECIMAL(26,2),
    c_date     DATE,
    c_datetime DATETIME,
    c_string   STRING
)
DUPLICATE KEY(c_key)
DISTRIBUTED BY HASH(c_key) BUCKETS 1
PROPERTIES (
    "replication_num"="1"
);

-- query 5
-- @skip_result_check=true
SET enable_optimize_skew_join_v1=false;

-- query 6
-- @skip_result_check=true
SET enable_optimize_skew_join_v2=false;

-- ============================================================
-- TINYINT skew join
-- ============================================================
-- query 7
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 8
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 9
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_tinyint) VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 2), (6, 3);

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_tinyint) VALUES (1, 1), (2, 2), (3, 3);

-- query 11
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_tinyint(1,2,8)] ${case_db}.t2 t2 ON t1.c_tinyint=t2.c_tinyint ORDER BY t1.c_key, t2.c_key;

-- query 12
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t2 t2 JOIN [skew|t1.c_tinyint(1,2,8)] ${case_db}.t1 t1 ON t1.c_tinyint=t2.c_tinyint ORDER BY t1.c_key, t2.c_key;

-- query 13
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_tinyint(1,2,8)] ${case_db}.t2 t2 ON t1.c_tinyint=t2.c_tinyint ORDER BY t1.c_key, t2.c_key;

-- query 14
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t2 t2 LEFT JOIN [skew|t1.c_tinyint(1,2,8)] ${case_db}.t1 t1 ON t1.c_tinyint=t2.c_tinyint ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- INT skew join
-- ============================================================
-- query 15
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 16
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 17
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_int) VALUES (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);

-- query 18
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_int) VALUES (1, 1), (2, 1234567), (3, 3);

-- query 19
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_int(1,1234567,4444)] ${case_db}.t2 t2 ON t1.c_int=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- query 20
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_int(1,1234567,4444)] ${case_db}.t2 t2 ON t1.c_int=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- BIGINT skew join
-- ============================================================
-- query 21
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 22
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 23
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_bigint) VALUES (1, 1), (2, 1), (3, 1), (4, 12345678912), (5, 12345678912), (6, 3);

-- query 24
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_bigint) VALUES (1, 1), (2, 12345678912), (3, 3);

-- query 25
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(1,12345678912,4444)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_bigint ORDER BY t1.c_key, t2.c_key;

-- query 26
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,12345678912,4444)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_bigint ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- LARGEINT skew join
-- ============================================================
-- query 27
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 28
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 29
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_largeint) VALUES (1, 1), (2, 1), (3, 1), (4, 18446744073709551620), (5, 18446744073709551620), (6, 3);

-- query 30
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_largeint) VALUES (1, 1), (2, 18446744073709551620), (3, 3);

-- query 31
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_largeint(1,18446744073709551620,4444)] ${case_db}.t2 t2 ON t1.c_largeint=t2.c_largeint ORDER BY t1.c_key, t2.c_key;

-- query 32
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_largeint(1,18446744073709551620,4444)] ${case_db}.t2 t2 ON t1.c_largeint=t2.c_largeint ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- DOUBLE skew join
-- ============================================================
-- query 33
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 34
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 35
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_double) VALUES (1, 1.1), (2, 1.1), (3, 1.1), (4, 2.1), (5, 2.1), (6, 3);

-- query 36
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_double) VALUES (1, 1.1), (2, 2.1), (3, 3);

-- query 37
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_double(1.1,2.1,4444)] ${case_db}.t2 t2 ON t1.c_double=t2.c_double ORDER BY t1.c_key, t2.c_key;

-- query 38
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_double(1.1,2.1,4444)] ${case_db}.t2 t2 ON t1.c_double=t2.c_double ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- DECIMAL skew join
-- ============================================================
-- query 39
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 40
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 41
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_decimal) VALUES (1, 1.1), (2, 1.1), (3, 1.1), (4, 2.1), (5, 2.1), (6, 3);

-- query 42
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_decimal) VALUES (1, 1.1), (2, 2.1), (3, 3);

-- query 43
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_decimal(1.1,2.1,4444)] ${case_db}.t2 t2 ON t1.c_decimal=t2.c_decimal ORDER BY t1.c_key, t2.c_key;

-- query 44
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_decimal(1.1,2.1,4444)] ${case_db}.t2 t2 ON t1.c_decimal=t2.c_decimal ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- DATE skew join
-- ============================================================
-- query 45
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 46
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 47
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_date) VALUES
    (1, "2024-01-01"), (2, "2024-01-01"), (3, "2024-01-01"),
    (4, "2024-01-02"), (5, "2024-01-02"), (6, "2024-01-03");

-- query 48
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_date) VALUES (1, "2024-01-01"), (2, "2024-01-02"), (3, "2024-01-03");

-- query 49
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_date("2024-01-01","2024-01-02","2024-01-04")] ${case_db}.t2 t2 ON t1.c_date=t2.c_date ORDER BY t1.c_key, t2.c_key;

-- query 50
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_date("2024-01-01","2024-01-02","2024-01-04")] ${case_db}.t2 t2 ON t1.c_date=t2.c_date ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- STRING skew join
-- ============================================================
-- query 51
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 52
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 53
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_string) VALUES ("1", "1"), ("2", "1"), ("3", "1"), ("4", "1234567"), ("5", "1234567"), ("6", "3");

-- query 54
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_string) VALUES ("1", "1"), ("2", "1234567"), ("3", "3");

-- query 55
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_string("1","1234567","4444")] ${case_db}.t2 t2 ON t1.c_string=t2.c_string ORDER BY t1.c_key, t2.c_key;

-- query 56
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_string("1","1234567","4444")] ${case_db}.t2 t2 ON t1.c_string=t2.c_string ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- Cross-type skew join: t1.c_int vs t2.c_bigint
-- ============================================================
-- query 57
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 58
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 59
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_int) VALUES (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);

-- query 60
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_bigint) VALUES (1, 1), (2, 1234567), (3, 3);

-- query 61
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_int(1,2,99999)] ${case_db}.t2 t2 ON t1.c_int=t2.c_bigint ORDER BY t1.c_key, t2.c_key;

-- query 62
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_int(1,2,99999)] ${case_db}.t2 t2 ON t1.c_int=t2.c_bigint ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- Cross-type skew join: t1.c_bigint vs t2.c_int
-- ============================================================
-- query 63
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 64
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 65
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_bigint) VALUES (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);

-- query 66
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_int) VALUES (1, 1), (2, 1234567), (3, 3);

-- query 67
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- query 68
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- Cross-type skew join: t1.c_string vs t2.c_int
-- ============================================================
-- query 69
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 70
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 71
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_string) VALUES (1, "1"), (2, "1"), (3, "1"), (4, "1234567"), (5, "1234567"), (6, "3");

-- query 72
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_int) VALUES (1, 1), (2, 1234567), (3, 3);

-- query 73
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_string("1","2","99999")] ${case_db}.t2 t2 ON t1.c_string=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- query 74
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_string("1","2","99999")] ${case_db}.t2 t2 ON t1.c_string=t2.c_int ORDER BY t1.c_key, t2.c_key;
