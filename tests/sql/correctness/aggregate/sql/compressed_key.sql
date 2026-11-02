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

-- Migrated from dev/test/sql/test_agg/R/test_agg_compressed_key
-- Test Objective:
-- Preserve aggregate and 128-bit LARGEINT correctness coverage without
-- coupling it to low-cardinality dictionary or optimizer-statistics behavior.
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
    c5 largeint,
    c6 date,
    c7 datetime,
    c8 string,
    c9 string,
    c10 char(100),
    c11 float,
    c12 double,
    c13 tinyint NOT NULL,
    c14 smallint NOT NULL,
    c15 int NOT NULL,
    c16 bigint NOT NULL,
    c17 largeint NOT NULL,
    c18 date NOT NULL,
    c19 datetime NOT NULL,
    c20 string NOT NULL,
    c21 string NOT NULL,
    c22 char(100) NOT NULL,
    c23 float NOT NULL,
    c24 double NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 SELECT x%200, x%200, x%200, x%200, x%200, x, x, x%200, x, x, x, x, x % 8, x % 8, x % 16, x %200, x%200, '2020-02-02', '2020-02-02', x%200, x, x, x, x FROM TABLE(generate_series(1,  30000)) as g(x);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 values (null, null, null, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, '2000-01-28', '2000-01-28', 'literal', 'literal', 'literal', -1, -1);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 values (-1, -2, -3, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, '2000-01-28', '2000-01-28', 'literal', 'literal', 'literal', -1, -1);

-- query 6
-- @skip_result_check=true
USE ${case_db};
set pipeline_dop=2;

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
  select distinct c1,c2,c3,c4,c5,c6,c7,c8 from all_t0 order by 1,2,3,4,5,6,7,8 limit 100,3
) AS trc2_output
ORDER BY trc2_output.c1, trc2_output.c2, trc2_output.c3, trc2_output.c4, trc2_output.c5, trc2_output.c6, trc2_output.c7, trc2_output.c8;

-- query 8
USE ${case_db};
select distinct c9,c10,c11,c12,c13,c14,c15,c16 from all_t0 order by 1,2,3,4,5,6,7,8 limit 100,3;

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
  select distinct c17,c18,c19,c20,c21,c22,c23,c24 from all_t0 order by 1,2,3,4,5,6,7,8 limit 100,3
) AS trc2_output
ORDER BY trc2_output.c17, trc2_output.c18, trc2_output.c19, trc2_output.c20, trc2_output.c21, trc2_output.c22, trc2_output.c23, trc2_output.c24;

-- query 10
USE ${case_db};
select c1, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3;

-- query 11
USE ${case_db};
select c1, c2, sum(c1) from all_t0 group by 1,2 order by 1,2,3 limit 3;

-- query 12
USE ${case_db};
select c2, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 13
USE ${case_db};
select c3, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 14
USE ${case_db};
select c4, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 15
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.`sum(c1)`
FROM (
  select c5, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1
) AS trc2_output
ORDER BY trc2_output.c5, trc2_output.`sum(c1)`;

-- query 16
USE ${case_db};
select c6, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 17
USE ${case_db};
select c7, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 18
USE ${case_db};
select c8, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 19
USE ${case_db};
select c9, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 20
USE ${case_db};
select c13, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 21
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 22
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 23
USE ${case_db};
select c16, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 24
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output.`sum(c1)`
FROM (
  select c17, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1
) AS trc2_output
ORDER BY trc2_output.c17, trc2_output.`sum(c1)`;

-- query 25
USE ${case_db};
select c18, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 26
USE ${case_db};
select c19, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 27
USE ${case_db};
select c2, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 28
USE ${case_db};
select c3, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 29
USE ${case_db};
select c4, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 30
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.`sum(c1)`
FROM (
  select c5, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1
) AS trc2_output
ORDER BY trc2_output.c5 desc, trc2_output.`sum(c1)` desc;

-- query 31
USE ${case_db};
select c6, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 32
USE ${case_db};
select c7, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 33
USE ${case_db};
select c8, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 34
USE ${case_db};
select c9, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 35
USE ${case_db};
select c13, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 36
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 37
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 38
USE ${case_db};
select c16, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 39
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output.`sum(c1)`
FROM (
  select c17, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1
) AS trc2_output
ORDER BY trc2_output.c17 desc, trc2_output.`sum(c1)` desc;

-- query 40
USE ${case_db};
select c18, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 41
USE ${case_db};
select c19, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 42
USE ${case_db};
select c3, c4, sum(c1) from all_t0 group by 1,2 order by 1, 2, 3 limit 30,1;

-- query 43
USE ${case_db};
SELECT
  trc2_output.c3,
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.`sum(c1)`
FROM (
  select c3, c5, sum(c1) from all_t0 group by 1,2 order by 1, 2, 3 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c3, trc2_output.c5, trc2_output.`sum(c1)`;

-- query 44
USE ${case_db};
select c3, c7, sum(c1) from all_t0 group by 1,2 order by 1, 2, 3 limit 30,1;

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
  trc2_output.`sum(c1)`
FROM (
  select c1,c2,c3,c4,c5,c6,c8,sum(c1) from all_t0 group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6,7,8 limit 30, 1
) AS trc2_output
ORDER BY trc2_output.c1, trc2_output.c2, trc2_output.c3, trc2_output.c4, trc2_output.c5, trc2_output.c6, trc2_output.c8, trc2_output.`sum(c1)`;

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
  trc2_output.`sum(c1)`
FROM (
  select c1,c2,c3,c4,c5,c6,c8,c13,c14,c15,c16, sum(c1) from all_t0 group by 1,2,3,4,5,6,7,8,9,10,11 order by 1,2,3,4,5,6,7,8,9,10,11 limit 30, 1
) AS trc2_output
ORDER BY trc2_output.c1, trc2_output.c2, trc2_output.c3, trc2_output.c4, trc2_output.c5, trc2_output.c6, trc2_output.c8, trc2_output.c13, trc2_output.c14, trc2_output.c15, trc2_output.c16;

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
  trc2_output.`sum(c1)`
FROM (
  select c1,c2,c3,c4,c5,c6,c8,c11,c12,c13,c14,c15,c16, sum(c1) from all_t0 group by 1,2,3,4,5,6,7,8,9,10,11,12,13 order by 1,2,3,4,5,6,7,8,9,10,11,12,13 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c1, trc2_output.c2, trc2_output.c3, trc2_output.c4, trc2_output.c5, trc2_output.c6, trc2_output.c8, trc2_output.c11, trc2_output.c12, trc2_output.c13, trc2_output.c14, trc2_output.c15, trc2_output.c16;

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
  trc2_output.`sum(c1)`
FROM (
  select c1,c2,c3,c4,c5,c6,c8, sum(c1) from all_t0 where c10 > 0 group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6,7,8 limit 1
) AS trc2_output
ORDER BY trc2_output.c1, trc2_output.c2, trc2_output.c3, trc2_output.c4, trc2_output.c5, trc2_output.c6, trc2_output.c8, trc2_output.`sum(c1)`;

-- query 49
-- @skip_result_check=true
USE ${case_db};
create table all_decimal (
    c1 decimal(4,2),
    c2 decimal(10,2),
    c3 decimal(27,9),
    c4 decimal(38,5)
)
TBLPROPERTIES ("format-version" = "3");

-- query 50
-- @skip_result_check=true
USE ${case_db};
insert into all_decimal SELECT x%100, x%200, x%200, x%200 FROM TABLE(generate_series(1,  30000)) as g(x);

-- query 51
USE ${case_db};
select distinct c1,c2,c3,c4 from all_decimal order by 1,2,3,4 limit 100,3;

-- query 52
USE ${case_db};
select c1, sum(c1) from all_decimal group by 1 order by 1, 2 limit 1;

-- query 53
USE ${case_db};
select c2, sum(c1) from all_decimal group by 1 order by 1, 2 limit 1;

-- query 54
USE ${case_db};
select c3, sum(c1) from all_decimal group by 1 order by 1, 2 limit 1;

-- query 55
USE ${case_db};
select c4, sum(c1) from all_decimal group by 1 order by 1, 2 limit 1;

-- query 56
USE ${case_db};
select c1, c2, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 57
USE ${case_db};
select c1, c3, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 58
USE ${case_db};
select c1, c4, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 59
USE ${case_db};
select c2, c3, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 60
USE ${case_db};
select c2, c4, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 61
USE ${case_db};
select c3, c4, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 62
USE ${case_db};
select c1, c2, c3, sum(c1) from all_decimal group by 1,2,3 order by 1,2,3,4 limit 1;

-- query 63
USE ${case_db};
select c1, c2, c4, sum(c1) from all_decimal group by 1,2,3 order by 1,2,3,4 limit 1;

-- query 64
USE ${case_db};
select c2, c3, c4, sum(c1) from all_decimal group by 1,2,3 order by 1,2,3,4 limit 1;

-- query 65
USE ${case_db};
select c1, c2, c3, c4, sum(c1) from all_decimal group by 1,2,3,4 order by 1,2,3,4,5 limit 1;

-- query 66
-- @skip_result_check=true
USE ${case_db};
create table all_numbers_t0 (
    c1 tinyint,
    c2 smallint,
    c3 int,
    c4 bigint,
    c5 largeint,
    c13 tinyint NOT NULL,
    c14 smallint NOT NULL,
    c15 int NOT NULL,
    c16 bigint NOT NULL,
    c17 largeint NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 67
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (-128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728);

-- query 68
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

-- query 69
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (null, null, null, null, null, 0, 0, 0, 0, 0);

-- query 70
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 SELECT x%128, x%200, x%200, x%200, x%200, x%128, x%200, x%200, x%200, x%200 FROM TABLE(generate_series(1,  30000)) as g(x);

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
  select distinct c17,c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9,10 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c17, trc2_output.c16, trc2_output.c15, trc2_output.c14, trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

-- query 72
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 limit 30,1;

-- query 73
USE ${case_db};
select distinct c2 from all_numbers_t0 order by 1 limit 30,1;

-- query 74
USE ${case_db};
select distinct c3 from all_numbers_t0 order by 1 limit 30,1;

-- query 75
USE ${case_db};
select distinct c4 from all_numbers_t0 order by 1 limit 30,1;

-- query 76
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5
FROM (
  select distinct c5 from all_numbers_t0 order by 1 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c5;

-- query 77
USE ${case_db};
select distinct c13 from all_numbers_t0 order by 1 limit 30,1;

-- query 78
USE ${case_db};
select distinct c14 from all_numbers_t0 order by 1 limit 30,1;

-- query 79
USE ${case_db};
select distinct c15 from all_numbers_t0 order by 1 limit 30,1;

-- query 80
USE ${case_db};
select distinct c16 from all_numbers_t0 order by 1 limit 30,1;

-- query 81
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17
FROM (
  select distinct c17 from all_numbers_t0 order by 1 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c17;

-- query 82
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 limit 30,1;

-- query 83
USE ${case_db};
select distinct c2,c1 from all_numbers_t0 order by 1,2 limit 30,1;

-- query 84
USE ${case_db};
select distinct c3,c2,c1 from all_numbers_t0 order by 1,2,3 limit 30,1;

-- query 85
USE ${case_db};
select distinct c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4 limit 30,1;

-- query 86
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c14, trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c15, trc2_output.c14, trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c16, trc2_output.c15, trc2_output.c14, trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c17,c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9,10 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c17, trc2_output.c16, trc2_output.c15, trc2_output.c14, trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

-- query 92
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727);

-- query 93
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

-- query 94
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17,
  trc2_output.c16,
  trc2_output.c15,
  trc2_output.c14,
  trc2_output.c13
FROM (
  select distinct c17,c16,c15,c14,c13 from all_numbers_t0 order by 1,2,3,4,5 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c17, trc2_output.c16, trc2_output.c15, trc2_output.c14, trc2_output.c13;

-- query 95
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 limit 30,1;

-- query 96
USE ${case_db};
select distinct c2 from all_numbers_t0 order by 1 limit 30,1;

-- query 97
USE ${case_db};
select distinct c3 from all_numbers_t0 order by 1 limit 30,1;

-- query 98
USE ${case_db};
select distinct c4 from all_numbers_t0 order by 1 limit 30,1;

-- query 99
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5
FROM (
  select distinct c5 from all_numbers_t0 order by 1 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c5;

-- query 100
USE ${case_db};
select distinct c13 from all_numbers_t0 order by 1 limit 30,1;

-- query 101
USE ${case_db};
select distinct c14 from all_numbers_t0 order by 1 limit 30,1;

-- query 102
USE ${case_db};
select distinct c15 from all_numbers_t0 order by 1 limit 30,1;

-- query 103
USE ${case_db};
select distinct c16 from all_numbers_t0 order by 1 limit 30,1;

-- query 104
USE ${case_db};
SELECT
  CAST(trc2_output.c17 AS VARCHAR) AS c17
FROM (
  select distinct c17 from all_numbers_t0 order by 1 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c17;

-- query 105
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 limit 30,1;

-- query 106
USE ${case_db};
select distinct c2,c1 from all_numbers_t0 order by 1,2 limit 30,1;

-- query 107
USE ${case_db};
select distinct c3,c2,c1 from all_numbers_t0 order by 1,2,3 limit 30,1;

-- query 108
USE ${case_db};
select distinct c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4 limit 30,1;

-- query 109
USE ${case_db};
SELECT
  CAST(trc2_output.c5 AS VARCHAR) AS c5,
  trc2_output.c4,
  trc2_output.c3,
  trc2_output.c2,
  trc2_output.c1
FROM (
  select distinct c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c14, trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c15, trc2_output.c14, trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c16, trc2_output.c15, trc2_output.c14, trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

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
  select distinct c17,c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9,10 limit 30,1
) AS trc2_output
ORDER BY trc2_output.c17, trc2_output.c16, trc2_output.c15, trc2_output.c14, trc2_output.c13, trc2_output.c5, trc2_output.c4, trc2_output.c3, trc2_output.c2, trc2_output.c1;

-- query 115
USE ${case_db};
select distinct c2,c1 from all_numbers_t0 where c2 = 7 order by 1,2 limit 1;

-- query 116
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE agged_table (
    k1 int,
    k2 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 117
-- @skip_result_check=true
USE ${case_db};
insert into agged_table values(1,10);

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
)
TBLPROPERTIES ("format-version" = "3");

-- query 123
-- @skip_result_check=true
USE ${case_db};
insert into trand values(1,1);

-- query 124
USE ${case_db};
select k1 from trand group by k1;

-- query 125
-- @skip_result_check=true
USE ${case_db};
insert into trand values(2,2);

-- query 126
USE ${case_db};
select k1 from trand group by k1;

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
    c17 largeint,
    c18 largeint,
    c19 largeint,
    c20 largeint,
    c21 date,
    c22 date,
    c23 date,
    c24 date
)
TBLPROPERTIES ("format-version" = "3");

-- query 128
-- @skip_result_check=true
USE ${case_db};
insert into all_t1 SELECT x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x FROM TABLE(generate_series(1,  300000)) as g(x);

-- query 129
USE ${case_db};
select distinct c1, c2, c3, c4, c5, c6, c7, c8 from all_t1 order by 1,2,3,4,5,6,7,8 desc limit 1;

-- query 130
USE ${case_db};
select distinct c9, c10, c11, c12, c13, c14, c15, c16 from all_t1 order by 1,2,3,4,5,6,7,8 desc limit 1;

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
  select distinct c17, c18, c19, c20, c21, c22, c23, c24 from all_t1 order by 1,2,3,4,5,6,7,8 desc limit 1
) AS trc2_output
ORDER BY trc2_output.c17, trc2_output.c18, trc2_output.c19, trc2_output.c20, trc2_output.c21, trc2_output.c22, trc2_output.c23, trc2_output.c24 desc;

-- query 132
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len=65535;

-- query 133
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_t0 GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 1' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 134
USE ${case_db};
WITH result AS (
    SELECT c2, COUNT(*) AS cnt FROM all_t0 GROUP BY c2 ORDER BY c2 LIMIT 100
) SELECT 'Test Case 2' AS test_name, MD5(GROUP_CONCAT(CAST(c2 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 135
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_decimal GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 25' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 136
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 29' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 137
USE ${case_db};
WITH result AS (
    SELECT c1, c2, c3, COUNT(*) AS cnt FROM all_t0 GROUP BY c1, c2, c3 ORDER BY c1, c2, c3 LIMIT 100
) SELECT 'Test Case 39' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c2 AS STRING) || ':' || CAST(c3 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 138
USE ${case_db};
WITH result AS (
    SELECT c6, c7, c8, COUNT(*) AS cnt FROM all_t0 GROUP BY c6, c7, c8 ORDER BY c6, c7, c8 LIMIT 100
) SELECT 'Test Case 40' AS test_name, MD5(GROUP_CONCAT(CAST(c6 AS STRING) || ':' || CAST(c7 AS STRING) || ':' || c8 || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 139
USE ${case_db};
WITH result AS (
    SELECT c13, c14, c15, COUNT(*) AS cnt FROM all_t0 GROUP BY c13, c14, c15 ORDER BY c13, c14, c15 LIMIT 100
) SELECT 'Test Case 42' AS test_name, MD5(GROUP_CONCAT(CAST(c13 AS STRING) || ':' || CAST(c14 AS STRING) || ':' || CAST(c15 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 140
USE ${case_db};
WITH result AS (
    SELECT c9, COUNT(*) AS cnt FROM all_t0 GROUP BY c9 ORDER BY c9 LIMIT 100
) SELECT 'Test Case 51' AS test_name, MD5(GROUP_CONCAT(c9 || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 141
USE ${case_db};
WITH result AS (
    SELECT c9, COUNT(*) AS cnt FROM all_t1 GROUP BY c9 ORDER BY c9 LIMIT 1000
) SELECT 'Test Case 52' AS test_name, MD5(GROUP_CONCAT(CAST(c9 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 142
USE ${case_db};
WITH result AS (
    SELECT c18, COUNT(*) AS cnt FROM all_t0 GROUP BY c18 ORDER BY c18 LIMIT 100
) SELECT 'Test Case 53' AS test_name, MD5(GROUP_CONCAT(CAST(c18 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 143
USE ${case_db};
WITH result AS (
    SELECT c13, COUNT(*) AS cnt FROM all_t0 GROUP BY c13 ORDER BY c13 LIMIT 100
) SELECT 'Test Case 54' AS test_name, MD5(GROUP_CONCAT(CAST(c13 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 144
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_t0 GROUP BY c1 ORDER BY c1 NULLS FIRST LIMIT 100
) SELECT 'Test Case 55' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 145
USE ${case_db};
WITH result AS (
    SELECT c5, COUNT(*) AS cnt FROM all_t0 GROUP BY c5 ORDER BY c5 NULLS FIRST LIMIT 100
) SELECT 'Test Case 56' AS test_name, MD5(GROUP_CONCAT(CAST(c5 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 146
USE ${case_db};
WITH result AS (
    SELECT c1, c5, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c1, c5 ORDER BY c1, c5 NULLS FIRST LIMIT 100
) SELECT 'Test Case 57' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c5 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 147
USE ${case_db};
WITH result AS (
    SELECT c1, c13, COUNT(*) AS cnt FROM all_t0 GROUP BY c1, c13 ORDER BY c1, c13 NULLS FIRST LIMIT 100
) SELECT 'Test Case 58' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c13 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 148
USE ${case_db};
WITH result AS (
    SELECT c1, c2, COUNT(*) AS cnt FROM all_t0 GROUP BY ROLLUP (c1, c2) ORDER BY 1,2,3 LIMIT 100
) SELECT 'Test Case 59' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c2 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 149
USE ${case_db};
WITH result AS (
    SELECT c3, c6, COUNT(*) AS cnt FROM all_t0 GROUP BY CUBE(c3, c6) ORDER BY 1,2,3 LIMIT 100
) SELECT 'Test Case 60' AS test_name, MD5(GROUP_CONCAT(CAST(c3 AS STRING) || ':' || CAST(c6 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 150
USE ${case_db};
WITH result AS (
    SELECT c13, c14, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY ROLLUP (c13, c14) ORDER BY 1,2,3 LIMIT 100
) SELECT 'Test Case 61' AS test_name, MD5(GROUP_CONCAT(CAST(c13 AS STRING) || ':' || CAST(c14 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 151
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_t0 GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 62' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 152
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_t0 WHERE c1 > 200 GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 89' AS test_name, MD5('empty') AS result_hash FROM result LIMIT 1;

-- query 153
USE ${case_db};
WITH result AS (
    SELECT k1, COUNT(*) AS cnt FROM trand GROUP BY k1 ORDER BY k1
) SELECT 'Test Case 90' AS test_name, MD5(GROUP_CONCAT(CAST(k1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 154
USE ${case_db};
WITH result AS (
    SELECT c8, COUNT(*) AS cnt FROM all_t0 GROUP BY c8 ORDER BY c8 LIMIT 100
) SELECT 'Test Case 93' AS test_name, MD5(GROUP_CONCAT(c8 || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 155
USE ${case_db};
WITH result AS (
    SELECT c20, COUNT(*) AS cnt FROM all_t0 GROUP BY c20 ORDER BY c20 LIMIT 100
) SELECT 'Test Case 94' AS test_name, MD5(GROUP_CONCAT(c20 || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 156
USE ${case_db};
WITH result AS (
    SELECT c1, c5, c9, c13, c17, COUNT(*) AS cnt FROM all_t1 GROUP BY c1, c5, c9, c13, c17 ORDER BY c1, c5, c9, c13, c17 LIMIT 1000
) SELECT 'Test Case 95' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c5 AS STRING) || ':' || CAST(c9 AS STRING) || ':' || CAST(c13 AS STRING) || ':' || CAST(c17 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 157
USE ${case_db};
WITH result AS (
    SELECT c1, c2, c3, c4, c5, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c1, c2, c3, c4, c5 ORDER BY c1, c2, c3, c4, c5 LIMIT 100
) SELECT 'Test Case 96' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c2 AS STRING) || ':' || CAST(c3 AS STRING) || ':' || CAST(c4 AS STRING) || ':' || CAST(c5 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 158
USE ${case_db};
WITH result AS (
    SELECT c13, c14, c15, c16, c17, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c13, c14, c15, c16, c17 ORDER BY c13, c14, c15, c16, c17 LIMIT 100
) SELECT 'Test Case 97' AS test_name, MD5(GROUP_CONCAT(CAST(c13 AS STRING) || ':' || CAST(c14 AS STRING) || ':' || CAST(c15 AS STRING) || ':' || CAST(c16 AS STRING) || ':' || CAST(c17 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;
