-- Migrated from: dev/test/sql/test_window_function/T/test_window_functions_with_complex_types
-- Test Objective:
-- 1. Validate FIRST_VALUE, LAST_VALUE, LEAD, LAG window functions on complex type columns
--    (array, map, struct, json, varbinary, char, varchar, decimal, etc.).
-- 2. Validate IGNORE NULLS variants for LEAD/LAG on complex columns.
-- 3. Confirm that map, struct, varbinary, and json-with-wrong-default raise expected FE errors.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
k1  date,
c0 boolean,
c1 tinyint(4),
c2 smallint(6),
c3 int(11),
c4 bigint(20),
c5 largeint(40),
c6 double,
c7 float,
c8 decimal(10, 2),
c9 char(100),
c10 date,
c11 datetime,
c12 array<boolean>,
c13 array<tinyint(4)>,
c14 array<smallint(6)>,
c15 array<int(11)>,
c16 array<bigint(20)>,
c17 array<largeint(40)>,
c18 array<double>,
c19 array<float>,
c20 array<DECIMAL64(10,2)>,
c21 array<char(100)>,
c22 array<date>,
c23 array<datetime>,
c24 varchar(100),
c25 json,
c26 varbinary,
c27 map<varchar(1048576),varchar(1048576)>,
c28 struct<col1 array<varchar(1048576)>>,
c29 array<varchar(100)>) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1)
PROPERTIES (  "replication_num" = "1");
INSERT INTO ${case_db}.t1 VALUES
(
  '2024-01-01',                    -- k1
  TRUE,                            -- c0
  127,                             -- c1
  32000,                           -- c2
  2147483647,                      -- c3
  9223372036854775807,             -- c4
  170141183460469231731687303715884105727,  -- c5 (example large integer)
  12345.6789,                      -- c6
  12345.67,                        -- c7
  123.45678901,                    -- c8
  'ExampleCharData',               -- c9
  '2024-01-02',                    -- c10
  '2024-01-01 12:34:56',           -- c11
  [TRUE, FALSE, TRUE],        -- c15 (<boolean>)
  [1, -1, 127],               -- c16 (<tinyint>)
  [100, -100, 32767],         -- c17 (<smallint>)
  [100000, -100000, 2147483647], -- c18 (<int>)
  [10000000000, -10000000000, 9223372036854775807], -- c19 (<bigint>)
  [12345678901234567890, -12345678901234567890, 170141183460469231731687303715884105727], -- c20 (<largeint>)
  [1.23, 4.56, 7.89],         -- c21 (<double>)
  [1.23, 4.56, 7.89],         -- c22 (<float>)
  [1.23456789, 2.34567890, 3.45678901], -- c23 (<decimal(10,8)>)
  ['abc', 'def', 'ghi'],      -- c24 (<char(100)>)
  ['2024-01-01', '2024-01-02'], -- c25 (<date>)
  ['2024-01-01 12:34:56', '2024-01-02 12:34:56'], -- c26 (<datetime>)
  'ExampleVarcharData',            -- c12
  '{"key": "value"}',              -- c13 (JSON data)
  x'4D7956617262696E61727944617461', -- c14 (VARBINARY data in hexadecimal format)
  MAP{'key1':'value1', 'key2':'value2'}, -- c27 (map<varchar, varchar>)
  row('val1'),    -- c28 (struct<col1 <varchar(1048576)>>)
  ['str1', 'str2', 'str3']    -- c29 (<varchar(100)>)
),
(
  '2024-01-02',                    -- k1
  FALSE,                            -- c0
  128,                             -- c1
  32100,                           -- c2
  2047483647,                      -- c3
  9023372036854775807,             -- c4
  100141183460469231731687303715884105727,  -- c5 (example large integer)
  123450.6789,                      -- c6
  123450.67,                        -- c7
  1230.45678901,                    -- c8
  'a',               -- c9
  '2024-01-01',                    -- c10
  '2024-01-01 13:34:56',           -- c11
  [FALSE, FALSE, FALSE],        -- c15 (<boolean>)
  [1, 1, 127],               -- c16 (<tinyint>)
  [100, 100, 32767],         -- c17 (<smallint>)
  [100000, 100000, 2147483647], -- c18 (<int>)
  [10000000000, 10000000000, 9223372036854775807], -- c19 (<bigint>)
  [12345678901234567890, 12345678901234567890, 170141183460469231731687303715884105727], -- c20 (<largeint>)
  [1.23, 4.56, 7.89],         -- c21 (<double>)
  [1.23, 4.56, 7.89],         -- c22 (<float>)
  [1.23456789, 2.34567890, 3.45678901], -- c23 (<decimal(10,8)>)
  ['abc', 'def', 'ghi'],      -- c24 (<char(100)>)
  ['2024-01-01', '2024-01-02'], -- c25 (<date>)
  ['2024-01-01 12:34:56', '2024-01-02 12:34:56'], -- c26 (<datetime>)
  'ExampleVarcharData',            -- c12
  '{"key": "value"}',              -- c13 (JSON data)
  x'4D7956617262696E61727944617461', -- c14 (VARBINARY data in hexadecimal format)
  MAP{'key1':'value1', 'key2':'value2'}, -- c27 (map<varchar, varchar>)
  row('val1'),    -- c28 (struct<col1 <varchar(1048576)>>)
  ['str1', 'str2', 'str3']    -- c29 (<varchar(100)>)
),
(
  '2024-01-03',                    -- k1
  FALSE,                            -- c0
  128,                             -- c1
  32100,                           -- c2
  2047483647,                      -- c3
  9023372036854775807,             -- c4
  100141183460469231731687303715884105727,  -- c5 (example large integer)
  123450.6789,                      -- c6
  123450.67,                        -- c7
  1230.45678901,                    -- c8
  'a',               -- c9
  '2024-01-01',                    -- c10
  '2024-01-01 13:34:56',           -- c11
  [FALSE, FALSE, FALSE],        -- c15 (<boolean>)
  [1, 1, 127],               -- c16 (<tinyint>)
  [100, 100, 32767],         -- c17 (<smallint>)
  [100000, 100000, 2147483647], -- c18 (<int>)
  [10000000000, 10000000000, 9223372036854775807], -- c19 (<bigint>)
  [12345678901234567890, 12345678901234567890, 170141183460469231731687303715884105727], -- c20 (<largeint>)
  [1.23, 4.56, 7.89],         -- c21 (<double>)
  [1.23, 4.56, 7.89],         -- c22 (<float>)
  [1.23456789, 2.34567890, 3.45678901], -- c23 (<decimal(10,8)>)
  ['abc', 'def', 'ghi'],      -- c24 (<char(100)>)
  ['2024-01-01', '2024-01-02'], -- c25 (<date>)
  ['2024-01-01 12:34:56', '2024-01-02 12:34:56'], -- c26 (<datetime>)
  'ExampleVarcharData',            -- c12
  '{"key": "value"}',              -- c13 (JSON data)
  x'4D7956617262696E61727944617461', -- c14 (VARBINARY data in hexadecimal format)
  MAP{'key1':'value1', 'key2':'value2'}, -- c27 (map<varchar, varchar>)
  row('val1'),    -- c28 (struct<col1 <varchar(1048576)>>)
  ['str1', 'str2', 'str3']    -- c29 (<varchar(100)>)
),
(
  '2024-01-04',                    -- k1
  FALSE,                            -- c0
  128,                             -- c1
  32100,                           -- c2
  2047483647,                      -- c3
  9023372036854775807,             -- c4
  100141183460469231731687303715884105727,  -- c5 (example large integer)
  123450.6789,                      -- c6
  123450.67,                        -- c7
  1230.45678901,                    -- c8
  'a',               -- c9
  '2024-01-01',                    -- c10
  '2024-01-01 13:34:56',           -- c11
  [FALSE, FALSE, FALSE],        -- c15 (<boolean>)
  [1, 1, 127],               -- c16 (<tinyint>)
  [100, 100, 32767],         -- c17 (<smallint>)
  [100000, 100000, 2147483647], -- c18 (<int>)
  [10000000000, 10000000000, 9223372036854775807], -- c19 (<bigint>)
  [12345678901234567890, 12345678901234567890, 170141183460469231731687303715884105727], -- c20 (<largeint>)
  [1.23, 4.56, 7.89],         -- c21 (<double>)
  [1.23, 4.56, 7.89],         -- c22 (<float>)
  [1.23456789, 2.34567890, 3.45678901], -- c23 (<decimal(10,8)>)
  ['abc', 'def', 'ghi'],      -- c24 (<char(100)>)
  ['2024-01-01', '2024-01-02'], -- c25 (<date>)
  ['2024-01-01 12:34:56', '2024-01-02 12:34:56'], -- c26 (<datetime>)
  'ExampleVarcharData',            -- c12
  '{"key": "value"}',              -- c13 (JSON data)
  x'4D7956617262696E61727944617461', -- c14 (VARBINARY data in hexadecimal format)
  MAP{'key1':'value1', 'key2':'value2'}, -- c27 (map<varchar, varchar>)
  row('val1'),    -- c28 (struct<col1 <varchar(1048576)>>)
  ['str1', 'str2', 'str3']    -- c29 (<varchar(100)>)
),
(
  '2024-01-05',                    -- k1
  NULL,                            -- c0
  NULL,                             -- c1
  NULL,                           -- c2
  NULL,                      -- c3
  NULL,             -- c4
  NULL,  -- c5 (example large integer)
  NULL,                      -- c6
  NULL,                        -- c7
  NULL,                    -- c8
  NULL,               -- c9
  NULL,                    -- c10
  NULL,           -- c11
  NULL,        -- c15 (<boolean>)
  NULL,               -- c16 (<tinyint>)
  NULL,         -- c17 (<smallint>)
  NULL, -- c18 (<int>)
  NULL, -- c19 (<bigint>)
  NULL, -- c20 (<largeint>)
  NULL,         -- c21 (<double>)
  NULL,         -- c22 (<float>)
  NULL, -- c23 (<decimal(10,8)>)
  NULL,      -- c24 (<char(100)>)
  NULL, -- c25 (<date>)
  NULL, -- c26 (<datetime>)
  NULL,            -- c12
  NULL,              -- c13 (JSON data)
  NULL, -- c14 (VARBINARY data in hexadecimal format)
  NULL, -- c27 (map<varchar, varchar>)
  NULL,    -- c28 (struct<col1 <varchar(1048576)>>)
  NULL    -- c29 (<varchar(100)>)
);
-- ========================================================= FIRST_VALUE =========================================================

-- query 2
SELECT FIRST_VALUE(c0) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 3
SELECT FIRST_VALUE(c1) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 4
SELECT FIRST_VALUE(c2) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 5
SELECT FIRST_VALUE(c3) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 6
SELECT FIRST_VALUE(c4) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 7
SELECT FIRST_VALUE(c5) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 8
SELECT FIRST_VALUE(c6) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 9
SELECT FIRST_VALUE(c7) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 10
SELECT FIRST_VALUE(c8) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 11
SELECT FIRST_VALUE(c9) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 12
SELECT FIRST_VALUE(c10) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 13
SELECT FIRST_VALUE(c11) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 14
SELECT FIRST_VALUE(c12) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 15
SELECT FIRST_VALUE(c13) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 16
SELECT FIRST_VALUE(c14) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 17
SELECT FIRST_VALUE(c15) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 18
SELECT FIRST_VALUE(c16) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 19
SELECT FIRST_VALUE(c17) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 20
SELECT FIRST_VALUE(c18) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 21
SELECT FIRST_VALUE(c19) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 22
SELECT FIRST_VALUE(c20) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 23
with cte as( SELECT FIRST_VALUE(c21) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 24
with cte as( SELECT FIRST_VALUE(c22) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 25
with cte as( SELECT FIRST_VALUE(c23) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 26
SELECT FIRST_VALUE(c24) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 27
SELECT FIRST_VALUE(c25) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 28
SELECT FIRST_VALUE(c26) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 29
-- @expect_error=No matching function
SELECT FIRST_VALUE(c27) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 30
-- @expect_error=No matching function
SELECT FIRST_VALUE(c28) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 31
-- FIRST_VALUE(HAS_NULL) without partition by
SELECT FIRST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wv FROM ${case_db}.t1;

-- query 32
SELECT FIRST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 33
SELECT FIRST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) wv FROM ${case_db}.t1;

-- query 34
SELECT FIRST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 35
SELECT FIRST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) wv FROM ${case_db}.t1;

-- query 36
SELECT FIRST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 37
SELECT FIRST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN 5 FOLLOWING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 38
SELECT FIRST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 39
SELECT FIRST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 40
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wv FROM ${case_db}.t1;

-- query 41
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 42
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) wv FROM ${case_db}.t1;

-- query 43
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 44
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) wv FROM ${case_db}.t1;

-- query 45
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 46
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN 5 FOLLOWING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 47
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 48
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 49
-- FIRST_VALUE(HAS_NULL) with partition by
SELECT FIRST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wv FROM ${case_db}.t1;

-- query 50
SELECT FIRST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 51
SELECT FIRST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) wv FROM ${case_db}.t1;

-- query 52
SELECT FIRST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 53
SELECT FIRST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) wv FROM ${case_db}.t1;

-- query 54
SELECT FIRST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 55
SELECT FIRST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 FOLLOWING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 56
SELECT FIRST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 57
SELECT FIRST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 58
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wv FROM ${case_db}.t1;

-- query 59
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 60
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) wv FROM ${case_db}.t1;

-- query 61
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 62
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) wv FROM ${case_db}.t1;

-- query 63
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 64
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 FOLLOWING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 65
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 66
SELECT FIRST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 67
-- ========================================================= LAST_VALUE =========================================================
SELECT LAST_VALUE(c0) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 68
SELECT LAST_VALUE(c1) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 69
SELECT LAST_VALUE(c2) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 70
SELECT LAST_VALUE(c3) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 71
SELECT LAST_VALUE(c4) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 72
SELECT LAST_VALUE(c5) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 73
SELECT LAST_VALUE(c6) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 74
SELECT LAST_VALUE(c7) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 75
SELECT LAST_VALUE(c8) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 76
SELECT LAST_VALUE(c9) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 77
SELECT LAST_VALUE(c10) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 78
SELECT LAST_VALUE(c11) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 79
SELECT LAST_VALUE(c12) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 80
SELECT LAST_VALUE(c13) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 81
SELECT LAST_VALUE(c14) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 82
SELECT LAST_VALUE(c15) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 83
SELECT LAST_VALUE(c16) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 84
SELECT LAST_VALUE(c17) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 85
SELECT LAST_VALUE(c18) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 86
SELECT LAST_VALUE(c19) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 87
SELECT LAST_VALUE(c20) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 88
with cte as( SELECT LAST_VALUE(c21) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 89
with cte as( SELECT LAST_VALUE(c22) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 90
with cte as( SELECT LAST_VALUE(c23) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 91
SELECT LAST_VALUE(c24) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 92
SELECT LAST_VALUE(c25) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 93
SELECT LAST_VALUE(c26) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 94
-- @expect_error=No matching function
SELECT LAST_VALUE(c27) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 95
-- @expect_error=No matching function
SELECT LAST_VALUE(c28) OVER(ORDER BY k1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 96
-- LAST_VALUE(HAS_NULL) without partition by
SELECT LAST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wv FROM ${case_db}.t1;

-- query 97
SELECT LAST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 98
SELECT LAST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) wv FROM ${case_db}.t1;

-- query 99
SELECT LAST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 100
SELECT LAST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) wv FROM ${case_db}.t1;

-- query 101
SELECT LAST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 102
SELECT LAST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN 5 FOLLOWING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 103
SELECT LAST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 104
SELECT LAST_VALUE(c25) OVER(ORDER BY c3 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 105
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wv FROM ${case_db}.t1;

-- query 106
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 107
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) wv FROM ${case_db}.t1;

-- query 108
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 109
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) wv FROM ${case_db}.t1;

-- query 110
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 111
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN 5 FOLLOWING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 112
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 113
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(ORDER BY c3 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 114
-- LAST_VALUE(HAS_NULL) with partition by
SELECT LAST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wv FROM ${case_db}.t1;

-- query 115
SELECT LAST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 116
SELECT LAST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) wv FROM ${case_db}.t1;

-- query 117
SELECT LAST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 118
SELECT LAST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) wv FROM ${case_db}.t1;

-- query 119
SELECT LAST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 120
SELECT LAST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 FOLLOWING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 121
SELECT LAST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 122
SELECT LAST_VALUE(c25) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 123
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) wv FROM ${case_db}.t1;

-- query 124
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 125
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) wv FROM ${case_db}.t1;

-- query 126
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 127
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING) wv FROM ${case_db}.t1;

-- query 128
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 129
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 FOLLOWING AND 10 FOLLOWING) wv FROM ${case_db}.t1;

-- query 130
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) wv FROM ${case_db}.t1;

-- query 131
SELECT LAST_VALUE(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY c3 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) wv FROM ${case_db}.t1;

-- query 132
-- ========================================================= LAST_VALUE =========================================================
SELECT LEAD(c0) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 133
SELECT LEAD(c1) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 134
SELECT LEAD(c2) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 135
SELECT LEAD(c3) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 136
SELECT LEAD(c4) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 137
SELECT LEAD(c5) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 138
SELECT LEAD(c6) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 139
SELECT LEAD(c7) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 140
SELECT LEAD(c8) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 141
SELECT LEAD(c9) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 142
SELECT LEAD(c10) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 143
SELECT LEAD(c11) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 144
SELECT LEAD(c12) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 145
SELECT LEAD(c13) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 146
SELECT LEAD(c14) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 147
SELECT LEAD(c15) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 148
SELECT LEAD(c16) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 149
SELECT LEAD(c17) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 150
SELECT LEAD(c18) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 151
SELECT LEAD(c19) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 152
SELECT LEAD(c20) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 153
with cte as( SELECT LEAD(c21) OVER(ORDER BY k1) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 154
with cte as( SELECT LEAD(c22) OVER(ORDER BY k1) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 155
with cte as( SELECT LEAD(c23) OVER(ORDER BY k1) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 156
SELECT LEAD(c24) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 157
SELECT LEAD(c25) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 158
-- @expect_error=No assignment from VARBINARY
SELECT LEAD(c26) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 159
-- @expect_error=No matching function
SELECT LEAD(c27) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 160
-- @expect_error=No matching function
SELECT LEAD(c28) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 161
-- LEAD(HAS_NULL) without partition by
SELECT LEAD(c25) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 162
SELECT LEAD(c25, 3) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 163
-- @expect_error=type of the third parameter
SELECT LEAD(c25, 5, -1) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 164
SELECT LEAD(c25 IGNORE NULLS) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 165
SELECT LEAD(c25 IGNORE NULLS, 3) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 166
-- @expect_error=type of the third parameter
SELECT LEAD(c25 IGNORE NULLS, 5, -1) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 167
-- LEAD(HAS_NULL) with partition by
SELECT LEAD(c25) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 168
SELECT LEAD(c25, 3) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 169
-- @expect_error=type of the third parameter
SELECT LEAD(c25, 5, -1) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 170
SELECT LEAD(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 171
SELECT LEAD(c25 IGNORE NULLS, 3) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 172
-- @expect_error=type of the third parameter
SELECT LEAD(c25 IGNORE NULLS, 5, -1) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 173
-- ========================================================= LAG =========================================================
SELECT LAG(c0) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 174
SELECT LAG(c1) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 175
SELECT LAG(c2) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 176
SELECT LAG(c3) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 177
SELECT LAG(c4) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 178
SELECT LAG(c5) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 179
SELECT LAG(c6) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 180
SELECT LAG(c7) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 181
SELECT LAG(c8) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 182
SELECT LAG(c9) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 183
SELECT LAG(c10) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 184
SELECT LAG(c11) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 185
SELECT LAG(c12) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 186
SELECT LAG(c13) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 187
SELECT LAG(c14) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 188
SELECT LAG(c15) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 189
SELECT LAG(c16) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 190
SELECT LAG(c17) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 191
SELECT LAG(c18) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 192
SELECT LAG(c19) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 193
SELECT LAG(c20) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 194
with cte as( SELECT LAG(c21) OVER(ORDER BY k1) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 195
with cte as( SELECT LAG(c22) OVER(ORDER BY k1) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 196
with cte as( SELECT LAG(c23) OVER(ORDER BY k1) wv FROM ${case_db}.t1) select sum(murmur_hash3_32(array_join(array_map(x->murmur_hash3_32(x),wv),","))) from cte;

-- query 197
SELECT LAG(c24) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 198
SELECT LAG(c25) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 199
-- @expect_error=No assignment from VARBINARY
SELECT LAG(c26) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 200
-- @expect_error=No matching function
SELECT LAG(c27) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 201
-- @expect_error=No matching function
SELECT LAG(c28) OVER(ORDER BY k1) wv FROM ${case_db}.t1;

-- query 202
-- LAG(HAS_NULL) without partition by
SELECT LAG(c25) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 203
SELECT LAG(c25, 3) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 204
-- @expect_error=type of the third parameter
SELECT LAG(c25, 5, -1) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 205
SELECT LAG(c25 IGNORE NULLS) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 206
SELECT LAG(c25 IGNORE NULLS, 3) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 207
-- @expect_error=type of the third parameter
SELECT LAG(c25 IGNORE NULLS, 5, -1) OVER(ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 208
-- LAG(HAS_NULL) with partition by
SELECT LAG(c25) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 209
SELECT LAG(c25, 3) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 210
-- @expect_error=type of the third parameter
SELECT LAG(c25, 5, -1) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 211
SELECT LAG(c25 IGNORE NULLS) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 212
SELECT LAG(c25 IGNORE NULLS, 3) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

-- query 213
-- @expect_error=type of the third parameter
SELECT LAG(c25 IGNORE NULLS, 5, -1) OVER(PARTITION BY k1 ORDER BY k1) AS wv FROM ${case_db}.t1;

