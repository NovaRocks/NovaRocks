-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- Migrated from dev/test/sql/test_agg/R/test_agg_compressed_key
-- Test Objective:
-- Exercise DISTINCT and ordered grouped aggregates over scalar and composite keys.
-- Use explicit conversions for legacy fixture inputs and use DECIMAL(38, 0)
-- for wide keys, including both extremes of that decimal domain.
-- Spell out NULL placement so both engines select the same ordered groups.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_compressed_key
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table all_t0 (
    c1 tinyint,
    c2 smallint,
    c3 int,
    c4 bigint,
    c5 DECIMAL(38, 0),
    c6 date,
    c7 TIMESTAMP(6),
    c8 VARCHAR,
    c9 VARCHAR,
    c10 VARCHAR,
    c11 REAL,
    c12 double,
    c13 tinyint NOT NULL,
    c14 smallint NOT NULL,
    c15 int NOT NULL,
    c16 bigint NOT NULL,
    c17 DECIMAL(38, 0) NOT NULL,
    c18 date NOT NULL,
    c19 TIMESTAMP(6) NOT NULL,
    c20 VARCHAR NOT NULL,
    c21 VARCHAR NOT NULL,
    c22 VARCHAR NOT NULL,
    c23 REAL NOT NULL,
    c24 double NOT NULL
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
-- Map the compact YYMMDD inputs to ISO dates and retain NULL for invalid dates.
INSERT INTO all_t0
SELECT TRY_CAST(x % 200 AS TINYINT), CAST(x % 200 AS SMALLINT),
       CAST(x % 200 AS INTEGER), x % 200, CAST(x % 200 AS DECIMAL(38, 0)),
       day_key, CAST(day_key AS TIMESTAMP(6)),
       CAST(x % 200 AS VARCHAR), CAST(x AS VARCHAR), CAST(x AS VARCHAR),
       CAST(x AS REAL), CAST(x AS DOUBLE),
       CAST(x % 8 AS TINYINT), CAST(x % 8 AS SMALLINT), CAST(x % 16 AS INTEGER),
       x % 200, CAST(x % 200 AS DECIMAL(38, 0)),
       DATE '2020-02-02', TIMESTAMP '2020-02-02 00:00:00.000000',
       CAST(x % 200 AS VARCHAR), CAST(x AS VARCHAR), CAST(x AS VARCHAR),
       CAST(x AS REAL), CAST(x AS DOUBLE)
FROM (
    SELECT x, TRY_CAST(CONCAT(
        CAST(2000 + x / 10000 AS VARCHAR), '-',
        LPAD(CAST(x / 100 % 100 AS VARCHAR), 2, '0'), '-',
        LPAD(CAST(x % 100 AS VARCHAR), 2, '0')
    ) AS DATE) AS day_key
    FROM TABLE(sequence(1, 30000)) AS g(x)
) AS source;

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 values ROW(null, null, null, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, DATE '2000-01-28', TIMESTAMP '2000-01-28 00:00:00.000000', 'literal', 'literal', 'literal', -1, -1);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 values ROW(-1, -2, -3, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, DATE '2000-01-28', TIMESTAMP '2000-01-28 00:00:00.000000', 'literal', 'literal', 'literal', -1, -1);

-- query 6
-- @skip_result_check=true
USE ${case_db};
SET SESSION pipeline_dop = 2;

-- query 7
USE ${case_db};
SELECT
  trc2_output.c1,
  trc2_output.c2,
  trc2_output.c3,
  trc2_output.c4,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c6,
  trc2_output.c7,
  trc2_output.c8
FROM (
  select distinct c1,c2,c3,c4,c5,c6,c7,c8 from all_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST OFFSET 100
LIMIT 3
) AS trc2_output
ORDER BY trc2_output.c1 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c6 NULLS FIRST, trc2_output.c7 NULLS FIRST, trc2_output.c8 NULLS FIRST;

-- query 8
USE ${case_db};
select distinct c9,c10,c11,c12,c13,c14,c15,c16 from all_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST OFFSET 100
LIMIT 3;

-- query 9
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output.c18,
  trc2_output.c19,
  trc2_output.c20,
  trc2_output.c21,
  trc2_output.c22,
  trc2_output.c23,
  trc2_output.c24
FROM (
  select distinct c17,c18,c19,c20,c21,c22,c23,c24 from all_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST OFFSET 100
LIMIT 3
) AS trc2_output
ORDER BY trc2_output.c17 NULLS FIRST, trc2_output.c18 NULLS FIRST, trc2_output.c19 NULLS FIRST, trc2_output.c20 NULLS FIRST, trc2_output.c21 NULLS FIRST, trc2_output.c22 NULLS FIRST, trc2_output.c23 NULLS FIRST, trc2_output.c24 NULLS FIRST;

-- query 10
USE ${case_db};
select c1, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST limit 3;

-- query 11
USE ${case_db};
select c1, c2, sum(c1) from all_t0 group by 1,2 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST limit 3;

-- query 12
USE ${case_db};
select c2, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 13
USE ${case_db};
select c3, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 14
USE ${case_db};
select c4, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 15
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output."sum(c1)"
FROM (
  select c5, sum(c1) AS "sum(c1)" from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1
) AS trc2_output
ORDER BY trc2_output.c5 NULLS FIRST, trc2_output."sum(c1)" NULLS FIRST;

-- query 16
USE ${case_db};
select c6, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 17
USE ${case_db};
select c7, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 18
USE ${case_db};
select c8, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 19
USE ${case_db};
select c9, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 20
USE ${case_db};
select c13, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 21
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 22
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 23
USE ${case_db};
select c16, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 24
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output."sum(c1)"
FROM (
  select c17, sum(c1) AS "sum(c1)" from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1
) AS trc2_output
ORDER BY trc2_output.c17 NULLS FIRST, trc2_output."sum(c1)" NULLS FIRST;

-- query 25
USE ${case_db};
select c18, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 26
USE ${case_db};
select c19, sum(c1) from all_t0 group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST OFFSET 3
LIMIT  1;

-- query 27
USE ${case_db};
select c2, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 28
USE ${case_db};
select c3, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 29
USE ${case_db};
select c4, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 30
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output."sum(c1)"
FROM (
  select c5, sum(c1) AS "sum(c1)" from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1
) AS trc2_output
ORDER BY trc2_output.c5 desc NULLS LAST, trc2_output."sum(c1)" desc NULLS LAST;

-- query 31
USE ${case_db};
select c6, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 32
USE ${case_db};
select c7, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 33
USE ${case_db};
select c8, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 34
USE ${case_db};
select c9, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 35
USE ${case_db};
select c13, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 36
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 37
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 38
USE ${case_db};
select c16, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 39
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output."sum(c1)"
FROM (
  select c17, sum(c1) AS "sum(c1)" from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1
) AS trc2_output
ORDER BY trc2_output.c17 desc NULLS LAST, trc2_output."sum(c1)" desc NULLS LAST;

-- query 40
USE ${case_db};
select c18, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 41
USE ${case_db};
select c19, sum(c1) from all_t0 group by 1 order by 1 desc NULLS LAST, 2 desc NULLS LAST limit 1;

-- query 42
USE ${case_db};
select c3, c4, sum(c1) from all_t0 group by 1,2 order by 1 NULLS FIRST, 2 NULLS FIRST, 3 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 43
USE ${case_db};
SELECT
  trc2_output.c3,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output."sum(c1)"
FROM (
  select c3, c5, sum(c1) AS "sum(c1)" from all_t0 group by 1,2 order by 1 NULLS FIRST, 2 NULLS FIRST, 3 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c3 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output."sum(c1)" NULLS FIRST;

-- query 44
USE ${case_db};
select c3, c7, sum(c1) from all_t0 group by 1,2 order by 1 NULLS FIRST, 2 NULLS FIRST, 3 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 45
USE ${case_db};
SELECT
  trc2_output.c1,
  trc2_output.c2,
  trc2_output.c3,
  trc2_output.c4,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c6,
  trc2_output.c8,
  trc2_output."sum(c1)"
FROM (
  select c1,c2,c3,c4,c5,c6,c8,sum(c1) AS "sum(c1)" from all_t0 group by 1,2,3,4,5,6,7 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST OFFSET 30
LIMIT  1
) AS trc2_output
ORDER BY trc2_output.c1 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c6 NULLS FIRST, trc2_output.c8 NULLS FIRST, trc2_output."sum(c1)" NULLS FIRST;

-- query 46
USE ${case_db};
SELECT
  trc2_output.c1,
  trc2_output.c2,
  trc2_output.c3,
  trc2_output.c4,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c6,
  trc2_output.c8,
  trc2_output.c13,
  trc2_output.c14,
  trc2_output.c15,
  trc2_output.c16,
  trc2_output."sum(c1)"
FROM (
  select c1,c2,c3,c4,c5,c6,c8,c13,c14,c15,c16, sum(c1) AS "sum(c1)" from all_t0 group by 1,2,3,4,5,6,7,8,9,10,11 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST,9 NULLS FIRST,10 NULLS FIRST,11 NULLS FIRST OFFSET 30
LIMIT  1
) AS trc2_output
ORDER BY trc2_output.c1 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c6 NULLS FIRST, trc2_output.c8 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c15 NULLS FIRST, trc2_output.c16 NULLS FIRST;

-- query 47
USE ${case_db};
SELECT
  trc2_output.c1,
  trc2_output.c2,
  trc2_output.c3,
  trc2_output.c4,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c6,
  trc2_output.c8,
  trc2_output.c11,
  trc2_output.c12,
  trc2_output.c13,
  trc2_output.c14,
  trc2_output.c15,
  trc2_output.c16,
  trc2_output."sum(c1)"
FROM (
  select c1,c2,c3,c4,c5,c6,c8,c11,c12,c13,c14,c15,c16, sum(c1) AS "sum(c1)" from all_t0 group by 1,2,3,4,5,6,7,8,9,10,11,12,13 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST,9 NULLS FIRST,10 NULLS FIRST,11 NULLS FIRST,12 NULLS FIRST,13 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c1 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c6 NULLS FIRST, trc2_output.c8 NULLS FIRST, trc2_output.c11 NULLS FIRST, trc2_output.c12 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c15 NULLS FIRST, trc2_output.c16 NULLS FIRST;

-- query 48
USE ${case_db};
SELECT
  trc2_output.c1,
  trc2_output.c2,
  trc2_output.c3,
  trc2_output.c4,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c6,
  trc2_output.c8,
  trc2_output."sum(c1)"
FROM (
  select c1,c2,c3,c4,c5,c6,c8, sum(c1) AS "sum(c1)" from all_t0 where CAST(c10 AS BIGINT) > 0 group by 1,2,3,4,5,6,7 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST limit 1
) AS trc2_output
ORDER BY trc2_output.c1 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c6 NULLS FIRST, trc2_output.c8 NULLS FIRST, trc2_output."sum(c1)" NULLS FIRST;

-- query 49
-- @skip_result_check=true
USE ${case_db};
create table all_decimal (
    c1 decimal(4,2),
    c2 decimal(10,2),
    c3 decimal(27,9),
    c4 decimal(38,5)
) WITH (format_version = 3);

-- query 50
-- @skip_result_check=true
USE ${case_db};
insert into all_decimal SELECT CAST(x % 100 AS DECIMAL(4, 2)), x%200, x%200, x%200 FROM TABLE(sequence(1,  30000)) as g(x);

-- query 51
USE ${case_db};
select distinct c1,c2,c3,c4 from all_decimal order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST OFFSET 100
LIMIT 3;

-- query 52
USE ${case_db};
select c1, sum(c1) from all_decimal group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST limit 1;

-- query 53
USE ${case_db};
select c2, sum(c1) from all_decimal group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST limit 1;

-- query 54
USE ${case_db};
select c3, sum(c1) from all_decimal group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST limit 1;

-- query 55
USE ${case_db};
select c4, sum(c1) from all_decimal group by 1 order by 1 NULLS FIRST, 2 NULLS FIRST limit 1;

-- query 56
USE ${case_db};
select c1, c2, sum(c1) from all_decimal group by 1,2 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST limit 1;

-- query 57
USE ${case_db};
select c1, c3, sum(c1) from all_decimal group by 1,2 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST limit 1;

-- query 58
USE ${case_db};
select c1, c4, sum(c1) from all_decimal group by 1,2 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST limit 1;

-- query 59
USE ${case_db};
select c2, c3, sum(c1) from all_decimal group by 1,2 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST limit 1;

-- query 60
USE ${case_db};
select c2, c4, sum(c1) from all_decimal group by 1,2 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST limit 1;

-- query 61
USE ${case_db};
select c3, c4, sum(c1) from all_decimal group by 1,2 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST limit 1;

-- query 62
USE ${case_db};
select c1, c2, c3, sum(c1) from all_decimal group by 1,2,3 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST limit 1;

-- query 63
USE ${case_db};
select c1, c2, c4, sum(c1) from all_decimal group by 1,2,3 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST limit 1;

-- query 64
USE ${case_db};
select c2, c3, c4, sum(c1) from all_decimal group by 1,2,3 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST limit 1;

-- query 65
USE ${case_db};
select c1, c2, c3, c4, sum(c1) from all_decimal group by 1,2,3,4 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST limit 1;

-- query 66
-- @skip_result_check=true
USE ${case_db};
create table all_numbers_t0 (
    c1 tinyint,
    c2 smallint,
    c3 int,
    c4 bigint,
    c5 DECIMAL(38, 0),
    c13 tinyint NOT NULL,
    c14 smallint NOT NULL,
    c15 int NOT NULL,
    c16 bigint NOT NULL,
    c17 DECIMAL(38, 0) NOT NULL
) WITH (format_version = 3);

-- query 67
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values ROW(-128, -32768, -2147483648, -9223372036854775808, DECIMAL '-99999999999999999999999999999999999999', -128, -32768, -2147483648, -9223372036854775808, DECIMAL '-99999999999999999999999999999999999999');

-- query 68
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values ROW(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

-- query 69
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values ROW(null, null, null, null, null, 0, 0, 0, 0, 0);

-- query 70
-- @skip_result_check=true
USE ${case_db};
INSERT INTO all_numbers_t0
SELECT CAST(x % 128 AS TINYINT), CAST(x % 200 AS SMALLINT),
       CAST(x % 200 AS INTEGER), x % 200, CAST(x % 200 AS DECIMAL(38, 0)),
       CAST(x % 128 AS TINYINT), CAST(x % 200 AS SMALLINT),
       CAST(x % 200 AS INTEGER), x % 200, CAST(x % 200 AS DECIMAL(38, 0))
FROM TABLE(sequence(1, 30000)) AS g(x);

-- query 71
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output.c16,
  trc2_output.c15,
  trc2_output.c14,
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c17,c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST,9 NULLS FIRST,10 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c17 NULLS FIRST, trc2_output.c16 NULLS FIRST, trc2_output.c15 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 72
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 73
USE ${case_db};
select distinct c2 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 74
USE ${case_db};
select distinct c3 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 75
USE ${case_db};
select distinct c4 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 76
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5
FROM (
  select distinct c5 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c5 NULLS FIRST;

-- query 77
USE ${case_db};
select distinct c13 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 78
USE ${case_db};
select distinct c14 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 79
USE ${case_db};
select distinct c15 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 80
USE ${case_db};
select distinct c16 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 81
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17
FROM (
  select distinct c17 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c17 NULLS FIRST;

-- query 82
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 83
USE ${case_db};
select distinct c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 84
USE ${case_db};
select distinct c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 85
USE ${case_db};
select distinct c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 86
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 87
USE ${case_db};
SELECT
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 88
USE ${case_db};
SELECT
  trc2_output.c14,
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 89
USE ${case_db};
SELECT
  trc2_output.c15,
  trc2_output.c14,
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c15 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 90
USE ${case_db};
SELECT
  trc2_output.c16,
  trc2_output.c15,
  trc2_output.c14,
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST,9 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c16 NULLS FIRST, trc2_output.c15 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 91
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output.c16,
  trc2_output.c15,
  trc2_output.c14,
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c17,c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST,9 NULLS FIRST,10 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c17 NULLS FIRST, trc2_output.c16 NULLS FIRST, trc2_output.c15 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 92
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values ROW(127, 32767, 2147483647, 9223372036854775807, DECIMAL '99999999999999999999999999999999999999', 127, 32767, 2147483647, 9223372036854775807, DECIMAL '99999999999999999999999999999999999999');

-- query 93
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 94
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output.c16,
  trc2_output.c15,
  trc2_output.c14,
  trc2_output.c13
FROM (
  select distinct c17,c16,c15,c14,c13 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c17 NULLS FIRST, trc2_output.c16 NULLS FIRST, trc2_output.c15 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST;

-- query 95
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 96
USE ${case_db};
select distinct c2 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 97
USE ${case_db};
select distinct c3 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 98
USE ${case_db};
select distinct c4 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 99
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5
FROM (
  select distinct c5 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c5 NULLS FIRST;

-- query 100
USE ${case_db};
select distinct c13 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 101
USE ${case_db};
select distinct c14 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 102
USE ${case_db};
select distinct c15 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 103
USE ${case_db};
select distinct c16 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 104
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17
FROM (
  select distinct c17 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c17 NULLS FIRST;

-- query 105
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 106
USE ${case_db};
select distinct c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 107
USE ${case_db};
select distinct c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 108
USE ${case_db};
select distinct c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST OFFSET 30
LIMIT 1;

-- query 109
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 110
USE ${case_db};
SELECT
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 111
USE ${case_db};
SELECT
  trc2_output.c14,
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 112
USE ${case_db};
SELECT
  trc2_output.c15,
  trc2_output.c14,
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c15 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 113
USE ${case_db};
SELECT
  trc2_output.c16,
  trc2_output.c15,
  trc2_output.c14,
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST,9 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c16 NULLS FIRST, trc2_output.c15 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 114
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output.c16,
  trc2_output.c15,
  trc2_output.c14,
  trc2_output.c13,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c17,c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 NULLS FIRST,9 NULLS FIRST,10 NULLS FIRST OFFSET 30
LIMIT 1
) AS trc2_output
ORDER BY trc2_output.c17 NULLS FIRST, trc2_output.c16 NULLS FIRST, trc2_output.c15 NULLS FIRST, trc2_output.c14 NULLS FIRST, trc2_output.c13 NULLS FIRST, trc2_output.c5 NULLS FIRST, trc2_output.c4 NULLS FIRST, trc2_output.c3 NULLS FIRST, trc2_output.c2 NULLS FIRST, trc2_output.c1 NULLS FIRST;

-- query 115
USE ${case_db};
select distinct c2,c1 from all_numbers_t0 where c2 = 7 order by 1 NULLS FIRST,2 NULLS FIRST limit 1;

-- query 116
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE agged_table (
    k1 int,
    k2 int
) WITH (format_version = 3);

-- query 117
-- @skip_result_check=true
USE ${case_db};
insert into agged_table values ROW(1,10);

-- query 118
-- @skip_result_check=true
USE ${case_db};
-- Append-table setup already inserted the single expected value.

-- query 119
-- @skip_result_check=true
USE ${case_db};
-- Append-table setup already inserted the single expected value.

-- query 120
-- @skip_result_check=true
USE ${case_db};
-- Append-table setup already inserted the single expected value.

-- query 121
USE ${case_db};
select distinct k2 from agged_table;

-- query 122
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE trand (
    k1 int,
    k2 int
) WITH (format_version = 3);

-- query 123
-- @skip_result_check=true
USE ${case_db};
insert into trand values ROW(1,1);

-- query 124
USE ${case_db};
select k1 from trand group by k1 order by k1 NULLS FIRST;

-- query 125
-- @skip_result_check=true
USE ${case_db};
insert into trand values ROW(2,2);

-- query 126
USE ${case_db};
select k1 from trand group by k1 order by k1 NULLS FIRST;

-- query 127
-- @skip_result_check=true
USE ${case_db};
create table all_t1 (
    c1 tinyint,
    c2 tinyint,
    c3 tinyint,
    c4 tinyint,
    c5 smallint,
    c6 smallint,
    c7 smallint,
    c8 smallint,
    c9 int,
    c10 int,
    c11 int,
    c12 int,
    c13 bigint,
    c14 bigint,
    c15 bigint,
    c16 bigint,
    c17 DECIMAL(38, 0),
    c18 DECIMAL(38, 0),
    c19 DECIMAL(38, 0),
    c20 DECIMAL(38, 0),
    c21 date,
    c22 date,
    c23 date,
    c24 date
) WITH (format_version = 3);

-- query 128
-- @skip_result_check=true
USE ${case_db};
INSERT INTO all_t1
SELECT ti, ti, ti, ti, si, si, si, si,
       i, i, i, i, x, x, x, x, magnitude, magnitude, magnitude, magnitude,
       day_key, day_key, day_key, day_key
FROM (
    SELECT x, TRY_CAST(x AS TINYINT) AS ti, TRY_CAST(x AS SMALLINT) AS si,
           CAST(x AS INTEGER) AS i, CAST(x AS DECIMAL(38, 0)) AS magnitude,
           TRY_CAST(CONCAT(
        CAST(2000 + x / 10000 AS VARCHAR), '-',
        LPAD(CAST(x / 100 % 100 AS VARCHAR), 2, '0'), '-',
        LPAD(CAST(x % 100 AS VARCHAR), 2, '0')
    ) AS DATE) AS day_key
    FROM TABLE(sequence(1, 300000)) AS g(x)
) AS source;

-- query 129
USE ${case_db};
select distinct c1, c2, c3, c4, c5, c6, c7, c8 from all_t1 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 desc NULLS LAST limit 1;

-- query 130
USE ${case_db};
select distinct c9, c10, c11, c12, c13, c14, c15, c16 from all_t1 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 desc NULLS LAST limit 1;

-- query 131
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  CAST(trc2_output.c18 AS VARCHAR) AS c18,
  CAST(trc2_output.c19 AS VARCHAR) AS c19,
  CAST(trc2_output.c20 AS VARCHAR) AS c20,
  trc2_output.c21,
  trc2_output.c22,
  trc2_output.c23,
  trc2_output.c24
FROM (
  select distinct c17, c18, c19, c20, c21, c22, c23, c24 from all_t1 order by 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST,4 NULLS FIRST,5 NULLS FIRST,6 NULLS FIRST,7 NULLS FIRST,8 desc NULLS LAST limit 1
) AS trc2_output
ORDER BY trc2_output.c17 NULLS FIRST, trc2_output.c18 NULLS FIRST, trc2_output.c19 NULLS FIRST, trc2_output.c20 NULLS FIRST, trc2_output.c21 NULLS FIRST, trc2_output.c22 NULLS FIRST, trc2_output.c23 NULLS FIRST, trc2_output.c24 desc NULLS LAST;

-- query 132
-- @skip_result_check=true
USE ${case_db};

-- query 133
USE ${case_db};
SELECT c1, COUNT(*) AS cnt FROM all_t0 GROUP BY c1 ORDER BY c1 NULLS FIRST LIMIT 100;

-- query 134
USE ${case_db};
SELECT c2, COUNT(*) AS cnt FROM all_t0 GROUP BY c2 ORDER BY c2 NULLS FIRST LIMIT 100;

-- query 135
USE ${case_db};
SELECT c1, COUNT(*) AS cnt FROM all_decimal GROUP BY c1 ORDER BY c1 NULLS FIRST LIMIT 100;

-- query 136
USE ${case_db};
SELECT c1, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c1 ORDER BY c1 NULLS FIRST LIMIT 100;

-- query 137
USE ${case_db};
SELECT c1, c2, c3, COUNT(*) AS cnt FROM all_t0 GROUP BY c1, c2, c3 ORDER BY c1 NULLS FIRST, c2 NULLS FIRST, c3 NULLS FIRST LIMIT 100;

-- query 138
USE ${case_db};
SELECT c6, c7, c8, COUNT(*) AS cnt FROM all_t0 GROUP BY c6, c7, c8 ORDER BY c6 NULLS FIRST, c7 NULLS FIRST, c8 NULLS FIRST LIMIT 100;

-- query 139
USE ${case_db};
SELECT c13, c14, c15, COUNT(*) AS cnt FROM all_t0 GROUP BY c13, c14, c15 ORDER BY c13 NULLS FIRST, c14 NULLS FIRST, c15 NULLS FIRST LIMIT 100;

-- query 140
USE ${case_db};
SELECT c9, COUNT(*) AS cnt FROM all_t0 GROUP BY c9 ORDER BY c9 NULLS FIRST LIMIT 100;

-- query 141
USE ${case_db};
SELECT c9, COUNT(*) AS cnt FROM all_t1 GROUP BY c9 ORDER BY c9 NULLS FIRST LIMIT 1000;

-- query 142
USE ${case_db};
SELECT c18, COUNT(*) AS cnt FROM all_t0 GROUP BY c18 ORDER BY c18 NULLS FIRST LIMIT 100;

-- query 143
USE ${case_db};
SELECT c13, COUNT(*) AS cnt FROM all_t0 GROUP BY c13 ORDER BY c13 NULLS FIRST LIMIT 100;

-- query 144
USE ${case_db};
SELECT c1, COUNT(*) AS cnt FROM all_t0 GROUP BY c1 ORDER BY c1 NULLS FIRST LIMIT 100;

-- query 145
USE ${case_db};
SELECT c5, COUNT(*) AS cnt FROM all_t0 GROUP BY c5 ORDER BY c5 NULLS FIRST LIMIT 100;

-- query 146
USE ${case_db};
SELECT c1, c5, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c1, c5 ORDER BY c1 NULLS FIRST, c5 NULLS FIRST LIMIT 100;

-- query 147
USE ${case_db};
SELECT c1, c13, COUNT(*) AS cnt FROM all_t0 GROUP BY c1, c13 ORDER BY c1 NULLS FIRST, c13 NULLS FIRST LIMIT 100;

-- query 148
USE ${case_db};
SELECT c1, c2, COUNT(*) AS cnt FROM all_t0 GROUP BY ROLLUP (c1, c2) ORDER BY 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST LIMIT 100;

-- query 149
USE ${case_db};
SELECT c3, c6, COUNT(*) AS cnt FROM all_t0 GROUP BY CUBE(c3, c6) ORDER BY 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST LIMIT 100;

-- query 150
USE ${case_db};
SELECT c13, c14, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY ROLLUP (c13, c14) ORDER BY 1 NULLS FIRST,2 NULLS FIRST,3 NULLS FIRST LIMIT 100;

-- query 151
USE ${case_db};
SELECT c1, COUNT(*) AS cnt FROM all_t0 GROUP BY c1 ORDER BY c1 NULLS FIRST LIMIT 100;

-- query 152
USE ${case_db};
SELECT c1, COUNT(*) AS cnt FROM all_t0 WHERE c1 > 200 GROUP BY c1 ORDER BY c1 NULLS FIRST LIMIT 100;

-- query 153
USE ${case_db};
SELECT k1, COUNT(*) AS cnt FROM trand GROUP BY k1 ORDER BY k1 NULLS FIRST;

-- query 154
USE ${case_db};
SELECT c8, COUNT(*) AS cnt FROM all_t0 GROUP BY c8 ORDER BY c8 NULLS FIRST LIMIT 100;

-- query 155
USE ${case_db};
SELECT c20, COUNT(*) AS cnt FROM all_t0 GROUP BY c20 ORDER BY c20 NULLS FIRST LIMIT 100;

-- query 156
USE ${case_db};
SELECT c1, c5, c9, c13, c17, COUNT(*) AS cnt FROM all_t1 GROUP BY c1, c5, c9, c13, c17 ORDER BY c1 NULLS FIRST, c5 NULLS FIRST, c9 NULLS FIRST, c13 NULLS FIRST, c17 NULLS FIRST LIMIT 1000;

-- query 157
USE ${case_db};
SELECT c1, c2, c3, c4, c5, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c1, c2, c3, c4, c5 ORDER BY c1 NULLS FIRST, c2 NULLS FIRST, c3 NULLS FIRST, c4 NULLS FIRST, c5 NULLS FIRST LIMIT 100;

-- query 158
USE ${case_db};
SELECT c13, c14, c15, c16, c17, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c13, c14, c15, c16, c17 ORDER BY c13 NULLS FIRST, c14 NULLS FIRST, c15 NULLS FIRST, c16 NULLS FIRST, c17 NULLS FIRST LIMIT 100;
