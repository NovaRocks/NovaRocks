-- @tags=join,skew_v2
-- Test Objective:
-- 1. Validate skew join v2 optimization correctness across all column types.
-- 2. Verify advanced skew join v2 scenarios: abs() expressions, multi-key conditions,
--    NULL handling, LIMIT, aggregation, non-matching skew values, and MCV-based
--    automatic skew detection via stats/histogram.
-- Test Flow:
-- 1. Create tables, enable skew v2 optimization.
-- 2. Test each data type with join/left join + skew hint.
-- 3. Test expression-based join keys (abs()), compound predicates, NULL skew values.
-- 4. Test aggregate queries over skew joins, LIMIT, non-matching skew values.
-- 5. Test large-scale skew data with skew_t1 table.
-- 6. Test MCV-based automatic skew detection (t_mcv_l/r, t_mcv_l2/r2, t_null_l/r).

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
SET enable_optimize_skew_join_v2=true;

-- ============================================================
-- TINYINT skew join v2
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
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_tinyint(1,2,8)] ${case_db}.t2 t2 ON t1.c_tinyint=t2.c_tinyint ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- INT skew join v2
-- ============================================================
-- query 13
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 14
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 15
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_int) VALUES (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_int) VALUES (1, 1), (2, 1234567), (3, 3);

-- query 17
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_int(1,1234567,4444)] ${case_db}.t2 t2 ON t1.c_int=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- query 18
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_int(1,1234567,4444)] ${case_db}.t2 t2 ON t1.c_int=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- BIGINT skew join v2
-- ============================================================
-- query 19
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 20
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 21
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_bigint) VALUES (1, 1), (2, 1), (3, 1), (4, 12345678912), (5, 12345678912), (6, 3);

-- query 22
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_bigint) VALUES (1, 1), (2, 12345678912), (3, 3);

-- query 23
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(1,12345678912,4444)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_bigint ORDER BY t1.c_key, t2.c_key;

-- query 24
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,12345678912,4444)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_bigint ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- DATE skew join v2
-- ============================================================
-- query 25
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 26
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 27
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_date) VALUES
    (1, "2024-01-01"), (2, "2024-01-01"), (3, "2024-01-01"),
    (4, "2024-01-02"), (5, "2024-01-02"), (6, "2024-01-03");

-- query 28
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_date) VALUES (1, "2024-01-01"), (2, "2024-01-02"), (3, "2024-01-03");

-- query 29
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_date("2024-01-01","2024-01-02","2024-01-04")] ${case_db}.t2 t2 ON t1.c_date=t2.c_date ORDER BY t1.c_key, t2.c_key;

-- query 30
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_date("2024-01-01","2024-01-02","2024-01-04")] ${case_db}.t2 t2 ON t1.c_date=t2.c_date ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- STRING skew join v2
-- ============================================================
-- query 31
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 32
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 33
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_string) VALUES ("1", "1"), ("2", "1"), ("3", "1"), ("4", "1234567"), ("5", "1234567"), ("6", "3");

-- query 34
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_string) VALUES ("1", "1"), ("2", "1234567"), ("3", "3");

-- query 35
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_string("1","1234567","4444")] ${case_db}.t2 t2 ON t1.c_string=t2.c_string ORDER BY t1.c_key, t2.c_key;

-- query 36
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_string("1","1234567","4444")] ${case_db}.t2 t2 ON t1.c_string=t2.c_string ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- Cross-type: t1.c_int vs t2.c_bigint
-- ============================================================
-- query 37
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 38
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 39
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_int) VALUES (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);

-- query 40
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_bigint) VALUES (1, 1), (2, 1234567), (3, 3);

-- query 41
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_int(1,2,99999)] ${case_db}.t2 t2 ON t1.c_int=t2.c_bigint ORDER BY t1.c_key, t2.c_key;

-- query 42
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_int(1,2,99999)] ${case_db}.t2 t2 ON t1.c_int=t2.c_bigint ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- Expression-based join key: abs() with skew on build side
-- ============================================================
-- query 43
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 44
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 45
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_bigint) VALUES (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);

-- query 46
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_int) VALUES (1, 1), (2, 1234567), (3, 3);

-- query 47
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t2 t2 JOIN [skew|t2.c_int(1,2,99999)] ${case_db}.t1 t1 ON abs(t1.c_bigint)=abs(t2.c_int) ORDER BY t1.c_key, t2.c_key;

-- query 48
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t2 t2 JOIN [skew|t2.c_int(1,2,99999)] ${case_db}.t1 t1 ON abs(t2.c_int) = abs(t1.c_bigint) ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- Compound predicate: skew join with additional equality condition
-- ============================================================
-- query 49
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t2 t2 JOIN [skew|t2.c_int(1,2,99999)] ${case_db}.t1 t1 ON t1.c_key = t2.c_key AND abs(t2.c_int) = abs(t1.c_bigint) ORDER BY t1.c_key, t2.c_key;

-- query 50
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t2.c_int(1,2,99999)] ${case_db}.t2 t2 ON t1.c_key = t2.c_key AND abs(t1.c_bigint) = abs(t2.c_int) ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- NULL skew values
-- ============================================================
-- query 51
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 52
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 53
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_bigint) VALUES (1, null), (2, 1), (3, 2), (4, -1);

-- query 54
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_int) VALUES (1, 1), (2, 2), (3, 3);

-- query 55
SELECT count(*) FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,null)] ${case_db}.t2 t2 ON abs(t1.c_bigint)=abs(t2.c_int);

-- query 56
SELECT count(*) FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(null)] ${case_db}.t2 t2 ON abs(t1.c_bigint)=abs(t2.c_int);

-- ============================================================
-- Skew join with multi-key and additional predicate + NULL results
-- ============================================================
-- query 57
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t1;

-- query 58
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 59
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_bigint) VALUES (1, 1), (2, 1), (3, 1), (4, 1234567), (5, 1234567), (6, 3);

-- query 60
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_int) VALUES (1, 1), (2, 1234567), (3, 3);

-- query 61
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int AND t1.c_key = t2.c_key ORDER BY t1.c_key, t2.c_key;

-- query 62
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int AND t1.c_key = t2.c_key ORDER BY t1.c_key, t2.c_key;

-- query 63
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t2 t2 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t1 t1 ON t1.c_bigint=t2.c_int AND t1.c_key = t2.c_key ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- Skew join with LIMIT
-- ============================================================
-- query 64
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key LIMIT 2;

-- query 65
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key LIMIT 2;

-- ============================================================
-- Non-matching skew values (no actual skew)
-- ============================================================
-- query 66
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(555,666,777)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- query 67
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(555,666,777)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- All-matching skew values
-- ============================================================
-- query 68
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(1,1234567,3)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- query 69
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,1234567,3)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- Additional rows and 4 skew values
-- ============================================================
-- query 70
-- @skip_result_check=true
INSERT INTO ${case_db}.t1(c_key, c_bigint) VALUES (7,4);

-- query 71
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_int) VALUES (4,9);

-- query 72
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(1,1234567,3,4)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- query 73
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,1234567,3,4)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- query 74
-- @order_sensitive=true
SELECT t1.c_key, t2.c_key FROM ${case_db}.t2 t2 LEFT JOIN [skew|t1.c_bigint(1,1234567,3,4)] ${case_db}.t1 t1 ON t1.c_bigint=t2.c_int ORDER BY t1.c_key, t2.c_key;

-- ============================================================
-- Aggregate queries with skew join
-- ============================================================
-- query 75
-- @order_sensitive=true
SELECT sum(t1.c_key), t1.c_bigint FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int GROUP BY t1.c_bigint ORDER BY c_bigint LIMIT 1;

-- query 76
SELECT sum(t2.c_key), t2.c_bigint FROM ${case_db}.t2 t2 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t1 t1 ON t1.c_bigint=t2.c_int GROUP BY t2.c_bigint;

-- query 77
-- @order_sensitive=true
SELECT sum(t1.c_key), t1.c_bigint FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int GROUP BY t1.c_bigint ORDER BY c_bigint LIMIT 1;

-- query 78
SELECT sum(t1.c_key), t2.c_bigint FROM ${case_db}.t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int GROUP BY t2.c_bigint;

-- query 79
SELECT sum(t1.c_key), t2.c_bigint FROM ${case_db}.t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int GROUP BY t2.c_bigint;

-- ============================================================
-- Large-scale skew data test
-- ============================================================
-- query 80
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.skew_t1;

-- query 81
-- @skip_result_check=true
CREATE TABLE ${case_db}.skew_t1 (
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

-- query 82
-- @skip_result_check=true
INSERT INTO ${case_db}.skew_t1(c_key, c_tinyint, c_smallint, c_int, c_bigint, c_largeint, c_float, c_double, c_decimal, c_date, c_datetime, c_string)
SELECT t.c_key, t.c_key % 100 as c_tinyint, t.c_key % 1000 as c_smallint, t.c_key % 10000 as c_int, t.c_key % 100000 as c_bigint, t.c_key % 1000000 as c_largeint, t.c_key % 10000000 as c_float, t.c_key % 100000000 as c_double, t.c_key % 1000000000 as c_decimal, t.c_key % 10000000000 as c_date, t.c_key % 100000000000 as c_datetime, t.c_key % 1000000000000 as c_string
FROM TABLE(generate_series(1, 4095*2)) t(c_key);

-- query 83
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t2;

-- query 84
-- @skip_result_check=true
INSERT INTO ${case_db}.t2(c_key, c_tinyint, c_smallint, c_int, c_bigint, c_largeint, c_float, c_double, c_decimal, c_date, c_datetime, c_string)
SELECT t.c_key, t.c_key % 100 as c_tinyint, t.c_key % 1000 as c_smallint, t.c_key % 10000 as c_int, t.c_key % 100000 as c_bigint, t.c_key % 1000000 as c_largeint, t.c_key % 10000000 as c_float, t.c_key % 100000000 as c_double, t.c_key % 1000000000 as c_decimal, t.c_key % 10000000000 as c_date, t.c_key % 100000000000 as c_datetime, t.c_key % 1000000000000 as c_string
FROM TABLE(generate_series(1, 4095*2)) t(c_key);

-- query 85
-- @skip_result_check=true
INSERT INTO ${case_db}.skew_t1(c_key, c_tinyint, c_smallint, c_int, c_bigint, c_largeint, c_float, c_double, c_decimal, c_date, c_datetime, c_string)
SELECT t.c_key % 1, t.c_key % 1 as c_tinyint, t.c_key % 1 as c_smallint, t.c_key % 1 as c_int, t.c_key % 1 as c_bigint, t.c_key % 1 as c_largeint, t.c_key % 1 as c_float, t.c_key % 1 as c_double, t.c_key % 1 as c_decimal, t.c_key % 1 as c_date, t.c_key % 1 as c_datetime, t.c_key % 1 as c_string
FROM TABLE(generate_series(1, 4095*2)) t(c_key);

-- query 86
SELECT count(1) FROM ${case_db}.skew_t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int;

-- query 87
SELECT count(1) FROM ${case_db}.t2 t2 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.skew_t1 t1 ON t1.c_bigint=t2.c_int;

-- query 88
SELECT count(1) FROM ${case_db}.skew_t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_bigint;

-- query 89
SELECT count(1) FROM ${case_db}.skew_t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_smallint=t2.c_smallint;

-- query 90
SELECT count(1) FROM ${case_db}.skew_t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_int=t2.c_int;

-- query 91
SELECT count(1) FROM ${case_db}.skew_t1 t1 JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_string=t2.c_string;

-- query 92
SELECT count(1) FROM ${case_db}.skew_t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_int;

-- query 93
SELECT count(1) FROM ${case_db}.t2 t2 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.skew_t1 t1 ON t1.c_bigint=t2.c_int;

-- query 94
SELECT count(1) FROM ${case_db}.skew_t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_bigint=t2.c_bigint;

-- query 95
SELECT count(1) FROM ${case_db}.skew_t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_int=t2.c_int;

-- query 96
SELECT count(1) FROM ${case_db}.skew_t1 t1 LEFT JOIN [skew|t1.c_bigint(1,2,99999)] ${case_db}.t2 t2 ON t1.c_string=t2.c_string;

-- ============================================================
-- MCV-based skew join: t_mcv_l / t_mcv_r
-- ============================================================
-- query 97
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_mcv_l;

-- query 98
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_mcv_r;

-- query 99
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_mcv_l (
    k bigint null,
    v int null
)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES("replication_num"="1");

-- query 100
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_mcv_r (
    k bigint null,
    v int null
)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES("replication_num"="1");

-- query 101
-- @skip_result_check=true
SET enable_stats_to_optimize_skew_join = true;

-- query 102
-- @skip_result_check=true
SET skew_join_use_mcv_count = 5;

-- query 103
-- @skip_result_check=true
SET skew_join_data_skew_threshold = 0.1;

-- query 104
-- @skip_result_check=true
SET skew_join_mcv_single_threshold = 0.05;

-- query 105
-- @skip_result_check=true
SET skew_join_mcv_min_input_rows = 1000;

-- query 106
-- @skip_result_check=true
SET broadcast_row_limit = 0;

-- query 107
-- @skip_result_check=true
INSERT INTO ${case_db}.t_mcv_l SELECT 1, 1 FROM TABLE(generate_series(1, 5000));

-- query 108
-- @skip_result_check=true
INSERT INTO ${case_db}.t_mcv_l SELECT 2, 1 FROM TABLE(generate_series(1, 2500));

-- query 109
-- @skip_result_check=true
INSERT INTO ${case_db}.t_mcv_l SELECT generate_series + 2, 1 FROM TABLE(generate_series(1, 2500));

-- query 110
-- @skip_result_check=true
INSERT INTO ${case_db}.t_mcv_r SELECT (generate_series % 2502) + 1, 1 FROM TABLE(generate_series(1, 20000));

-- query 111
-- @skip_result_check=true
ANALYZE FULL TABLE ${case_db}.t_mcv_l;

-- query 112
-- @skip_result_check=true
ANALYZE FULL TABLE ${case_db}.t_mcv_r;

-- query 113
-- @skip_result_check=true
ANALYZE TABLE ${case_db}.t_mcv_l UPDATE HISTOGRAM ON k PROPERTIES('histogram_sample_ratio' = '1.0', 'histogram_mcv_size' = '20');

-- query 114
-- @skip_result_check=true
ANALYZE TABLE ${case_db}.t_mcv_r UPDATE HISTOGRAM ON k PROPERTIES('histogram_sample_ratio' = '1.0', 'histogram_mcv_size' = '20');

-- query 115
SELECT count(*) FROM ${case_db}.t_mcv_l JOIN[shuffle] ${case_db}.t_mcv_r ON ${case_db}.t_mcv_l.k = ${case_db}.t_mcv_r.k;

-- query 116
WITH l AS (SELECT (cast(k AS bigint) + 10) AS k2, v FROM ${case_db}.t_mcv_l), r AS (SELECT (cast(k AS bigint) + 10) AS k2, v FROM ${case_db}.t_mcv_r) SELECT count(*) FROM l JOIN[shuffle] r ON l.k2 = r.k2;

-- ============================================================
-- MCV thresholds: high skew threshold
-- ============================================================
-- query 117
-- @skip_result_check=true
SET skew_join_data_skew_threshold = 0.9;

-- query 118
SELECT count(*) FROM ${case_db}.t_mcv_l JOIN[shuffle] ${case_db}.t_mcv_r ON ${case_db}.t_mcv_l.k = ${case_db}.t_mcv_r.k;

-- query 119
-- @skip_result_check=true
SET skew_join_data_skew_threshold = 0.1;

-- ============================================================
-- MCV thresholds: high single threshold
-- ============================================================
-- query 120
-- @skip_result_check=true
SET skew_join_mcv_single_threshold = 0.3;

-- query 121
SELECT count(*) FROM ${case_db}.t_mcv_l JOIN[shuffle] ${case_db}.t_mcv_r ON ${case_db}.t_mcv_l.k = ${case_db}.t_mcv_r.k;

-- query 122
-- @skip_result_check=true
SET skew_join_mcv_single_threshold = 0.05;

-- ============================================================
-- MCV thresholds: high min input rows
-- ============================================================
-- query 123
-- @skip_result_check=true
SET skew_join_mcv_min_input_rows = 20000;

-- query 124
SELECT count(*) FROM ${case_db}.t_mcv_l JOIN[shuffle] ${case_db}.t_mcv_r ON ${case_db}.t_mcv_l.k = ${case_db}.t_mcv_r.k;

-- query 125
-- @skip_result_check=true
SET skew_join_mcv_min_input_rows = 1000;

-- ============================================================
-- MCV right-side skew: t_mcv_l2 / t_mcv_r2
-- ============================================================
-- query 126
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_mcv_l2;

-- query 127
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_mcv_r2;

-- query 128
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_mcv_l2 (
    k bigint null,
    v int null
)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES("replication_num"="1");

-- query 129
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_mcv_r2 (
    k bigint null,
    v int null
)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES("replication_num"="1");

-- query 130
-- @skip_result_check=true
INSERT INTO ${case_db}.t_mcv_l2 SELECT generate_series, 1 FROM TABLE(generate_series(1, 10000));

-- query 131
-- @skip_result_check=true
INSERT INTO ${case_db}.t_mcv_r2 SELECT 1, 1 FROM TABLE(generate_series(1, 8000));

-- query 132
-- @skip_result_check=true
INSERT INTO ${case_db}.t_mcv_r2 SELECT 2, 1 FROM TABLE(generate_series(1, 4000));

-- query 133
-- @skip_result_check=true
INSERT INTO ${case_db}.t_mcv_r2 SELECT generate_series + 2, 1 FROM TABLE(generate_series(1, 8000));

-- query 134
-- @skip_result_check=true
ANALYZE FULL TABLE ${case_db}.t_mcv_l2;

-- query 135
-- @skip_result_check=true
ANALYZE FULL TABLE ${case_db}.t_mcv_r2;

-- query 136
-- @skip_result_check=true
ANALYZE TABLE ${case_db}.t_mcv_r2 UPDATE HISTOGRAM ON k PROPERTIES('histogram_sample_ratio' = '1.0', 'histogram_mcv_size' = '20');

-- query 137
SELECT count(*) FROM ${case_db}.t_mcv_l2 JOIN[shuffle] ${case_db}.t_mcv_r2 ON ${case_db}.t_mcv_l2.k = ${case_db}.t_mcv_r2.k;

-- query 138
SELECT count(*) FROM ${case_db}.t_mcv_l2 LEFT JOIN[shuffle] ${case_db}.t_mcv_r2 ON ${case_db}.t_mcv_l2.k = ${case_db}.t_mcv_r2.k;

-- query 139
-- @skip_result_check=true
DROP TABLE ${case_db}.t_mcv_l2;

-- query 140
-- @skip_result_check=true
DROP TABLE ${case_db}.t_mcv_r2;

-- ============================================================
-- NULL-heavy left side with MCV: t_null_l / t_null_r
-- ============================================================
-- query 141
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_null_l;

-- query 142
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_null_r;

-- query 143
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_null_l (
    k bigint null,
    v int null
)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES("replication_num"="1");

-- query 144
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_null_r (
    k bigint null,
    v int null
)
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1
PROPERTIES("replication_num"="1");

-- query 145
-- @skip_result_check=true
INSERT INTO ${case_db}.t_null_l SELECT null, 1 FROM TABLE(generate_series(1, 7000));

-- query 146
-- @skip_result_check=true
INSERT INTO ${case_db}.t_null_l SELECT 1, 1 FROM TABLE(generate_series(1, 2000));

-- query 147
-- @skip_result_check=true
INSERT INTO ${case_db}.t_null_l SELECT 2, 1 FROM TABLE(generate_series(1, 1000));

-- query 148
-- @skip_result_check=true
INSERT INTO ${case_db}.t_null_r SELECT (generate_series % 2000) + 1, 1 FROM TABLE(generate_series(1, 10000));

-- query 149
-- @skip_result_check=true
ANALYZE FULL TABLE ${case_db}.t_null_l;

-- query 150
-- @skip_result_check=true
ANALYZE FULL TABLE ${case_db}.t_null_r;

-- query 151
-- @skip_result_check=true
ANALYZE TABLE ${case_db}.t_null_l UPDATE HISTOGRAM ON k PROPERTIES('histogram_sample_ratio' = '1.0', 'histogram_mcv_size' = '20');

-- query 152
-- @skip_result_check=true
ANALYZE TABLE ${case_db}.t_null_r UPDATE HISTOGRAM ON k PROPERTIES('histogram_sample_ratio' = '1.0', 'histogram_mcv_size' = '20');

-- query 153
SELECT count(*) FROM ${case_db}.t_null_l LEFT JOIN[shuffle] ${case_db}.t_null_r ON ${case_db}.t_null_l.k = ${case_db}.t_null_r.k;

-- ============================================================
-- Cleanup
-- ============================================================
-- query 154
-- @skip_result_check=true
DROP TABLE ${case_db}.t_null_l;

-- query 155
-- @skip_result_check=true
DROP TABLE ${case_db}.t_null_r;

-- query 156
-- @skip_result_check=true
DROP TABLE ${case_db}.t_mcv_l;

-- query 157
-- @skip_result_check=true
DROP TABLE ${case_db}.t_mcv_r;

-- query 158
-- @skip_result_check=true
DROP TABLE ${case_db}.skew_t1;

-- query 159
-- @skip_result_check=true
DROP TABLE ${case_db}.t1;

-- query 160
-- @skip_result_check=true
DROP TABLE ${case_db}.t2;
