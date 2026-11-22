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

-- @tags=join,predicate_pushdown
-- Test Objective:
-- Validate that predicates are correctly pushed down, pulled up, and applied
-- across various join types (inner, left, right, full) when using subqueries,
-- filtered CTEs, and multi-table join chains. Covers predicate move-around
-- optimizations including predicates with functions (abs), aggregations (max),
-- correlated subqueries, and BETWEEN/IN filters on complex type columns
-- (varchar, datetime, decimal).

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t0;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t1;

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t2;

-- query 4
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t3;

-- query 5
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.test_all_type;

-- query 6
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.test_all_type_not_null;

-- query 7
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0 (
  "v1" bigint ,
  "v2" bigint ,
  "v3" bigint
) WITH (format_version = 3);

-- query 8
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  "v4" bigint ,
  "v5" bigint ,
  "v6" bigint
) WITH (format_version = 3);

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
  "v7" bigint ,
  "v8" bigint ,
  "v9" bigint
) WITH (format_version = 3);

-- query 10
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (
  "v10" bigint ,
  "v11" bigint ,
  "v12" bigint
) WITH (format_version = 3);

-- query 11
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_all_type (
  "t1a" varchar(20) ,
  "t1b" SMALLINT ,
  "t1c" INTEGER ,
  "t1d" BIGINT ,
  "t1e" REAL ,
  "t1f" double ,
  "t1g" BIGINT ,
  "id_datetime" TIMESTAMP(6) ,
  "id_date" date ,
  "id_decimal" decimal(10,2)
) WITH (format_version = 3);

-- query 12
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_all_type_not_null (
  "t1a" varchar(20) NOT NULL,
  "t1b" SMALLINT NOT NULL,
  "t1c" INTEGER NOT NULL,
  "t1d" BIGINT NOT NULL,
  "t1e" REAL NOT NULL,
  "t1f" double NOT NULL,
  "t1g" BIGINT NOT NULL,
  "id_datetime" TIMESTAMP(6) NOT NULL,
  "id_date" date NOT NULL,
  "id_decimal" decimal(10,2) NOT NULL
) WITH (format_version = 3);

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES
  ROW(-10, -10, -10), ROW(0, 0, 0), ROW(1, 1, 1), ROW(2, 2, 2),
  ROW(10, 10, 10), ROW(20, 20, 20), ROW(75, 75, 75), ROW(511, 511, 511);

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t0;

-- query 15
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT v1 - 5, v2 + 5, v3 + 10 FROM ${case_db}.t0 ORDER BY v1 LIMIT 3;

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 SELECT * FROM ${case_db}.t0;

-- query 17
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT v1 + 5, v2 - 10, v3 + 5 FROM ${case_db}.t0 ORDER BY v1 LIMIT 3;

-- query 18
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 SELECT * FROM ${case_db}.t0;

-- query 19
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 SELECT v1 * 2, v2 + 5, v3 * 3 FROM ${case_db}.t0 ORDER BY v1 LIMIT 3;

-- query 20
-- @skip_result_check=true
INSERT INTO ${case_db}.test_all_type VALUES
  ROW('abc', 1, 1, 20, 1.1, 1.1, 1, '2021-01-01 00:00:00', '2021-01-01', 1.2),
  ROW('中文', 1, 1, 20, 1.1, 1.1, 1, '2021-02-01 00:00:00', '2021-01-01', 1.2),
  ROW('中文', 1, 1, 20, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
  ROW('中文', 1, 1, 1, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
  ROW('abcd', 2, 2, 2, 1.2, 1.2, 10, '2021-04-01 00:00:00', '2021-04-01', 1.25),
  ROW('abcd', 2, 2, 2, 1.2, 1.2, 10, '2021-04-02 00:00:00', '2021-04-02', 1.25),
  ROW('abcdefg', 20, 20, 20, 11.2, 11.2, 20, '2021-01-01 00:00:00', '2021-01-01', 2.25),
  ROW('中文', 100, 100, 100, 100.01, 100.02, 1, '2021-01-01 00:00:00', '2021-01-01', 100.25),
  ROW(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- query 21
-- @skip_result_check=true
INSERT INTO ${case_db}.test_all_type_not_null VALUES
  ROW('ab', 1, -1, 18, 1.1, 1.1, 1, '2022-01-01 00:00:00', '2022-01-01', 1.2),
  ROW('abc', 1, 1, 18, 1.1, 1.1, 1, '2021-01-01 00:00:00', '2021-01-01', 1.2),
  ROW('中文', 1, 1, 20, 1.1, 1.1, 1, '2021-02-01 00:00:00', '2021-01-01', 1.2),
  ROW('中文', 1, 1, 20, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
  ROW('中文', 1, 1, 21, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
  ROW('abc', 1, 1, 21, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
  ROW('中文', 21, 1, 21, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
  ROW('中文', 200, 1, 21, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
  ROW('中文', 1, 1, 1, 1.1, 1.1, 1, '2021-02-04 00:00:00', '2021-02-04', 1.2),
  ROW('abcd', 2, 2, 2, 1.2, 1.2, 10, '2021-04-01 00:00:00', '2021-04-01', 1.25),
  ROW('abcd', 2, 2, 1, 1.2, 1.2, 10, '2021-04-02 00:00:00', '2021-04-02', 1.25),
  ROW('abcdefg', 20, 20, 20, 11.2, 11.2, 20, '2021-01-01 00:00:00', '2021-01-01', 2.25),
  ROW('中文', 100, 100, 100, 100.01, 100.02, 1, '2021-01-01 00:00:00', '2021-01-01', 100.25);

-- ============================================================
-- Subquery inner join with multi-column predicate pushdown
-- ============================================================

-- query 22
-- inner join with subquery predicates on varchar/datetime/decimal columns
-- @order_sensitive=false
select * from
(select * from ${case_db}.test_all_type where t1a in ('abc', '中文') and t1b = 1 and t1c = 1
and t1d = 20 and t1f = 1.1 and id_datetime between '2021-01-01' and '2021-02-01' and id_decimal = 1.2) t1
 inner join ${case_db}.test_all_type_not_null t2
on t1.t1a > t2.t1a and t1.t1b = t2.t1b and t1.t1c > t2.t1c and t1.id_datetime < t2.id_datetime;

-- query 23
-- left join with subquery predicates on varchar/datetime/decimal columns
-- @order_sensitive=false
select * from
(select * from ${case_db}.test_all_type where t1a in ('abc', '中文') and t1b = 1 and t1c = 1
and t1d = 20 and t1f = 1.1 and id_datetime between '2021-01-01' and '2021-02-01' and id_decimal = 1.2) t1
 left join ${case_db}.test_all_type_not_null t2
on t1.t1a > t2.t1a and t1.t1b = t2.t1b and t1.t1c > t2.t1c and t1.id_datetime < t2.id_datetime;

-- query 24
-- inner join with OR predicate and date comparison
select * from
(select * from ${case_db}.test_all_type where (t1d in (1, 2, 3) and id_date = '2021-01-01') or (id_date > '2021-04-01')) t1
inner join ${case_db}.test_all_type_not_null t2
on t1.t1d > t2.t1d and t1.id_date = t2.id_date;

-- query 25
-- left join with OR predicate and date comparison
select * from
(select * from ${case_db}.test_all_type where (t1d in (1, 2, 3) and id_date = '2021-01-01') or (id_date > '2021-04-01')) t1
left join ${case_db}.test_all_type_not_null t2
on t1.t1d > t2.t1d and t1.id_date = t2.id_date;

-- query 26
-- inner join with abs() function in join key
-- @order_sensitive=false
select * from
(select * from ${case_db}.test_all_type where abs(t1b + t1d) > 20 and t1a in ('abc', '中文')) t1
inner join ${case_db}.test_all_type_not_null t2
on abs(t1.t1b + t1.t1d) = t2.t1b;

-- query 27
-- left join with abs() function in join key
-- @order_sensitive=false
select * from
(select * from ${case_db}.test_all_type where abs(t1b + t1d) > 20 and t1a in ('abc', '中文')) t1
left join ${case_db}.test_all_type_not_null t2
on abs(t1.t1b + t1.t1d) = t2.t1b;

-- ============================================================
-- Reversed table order (right table has subquery)
-- ============================================================

-- query 28
-- inner join with reversed table order
-- @order_sensitive=false
select * from ${case_db}.test_all_type_not_null t2
inner join (select * from ${case_db}.test_all_type where t1a in ('abc', '中文') and t1b = 1 and t1c = 1
and t1d = 20 and t1f = 1.1 and id_datetime between '2021-01-01' and '2021-02-01' and id_decimal = 1.2) t1
on t1.t1a > t2.t1a and t1.t1b = t2.t1b and t1.t1c > t2.t1c and t1.id_datetime < t2.id_datetime;

-- query 29
-- right join with reversed table order
-- @order_sensitive=false
select * from ${case_db}.test_all_type_not_null t2
right join (select * from ${case_db}.test_all_type where t1a in ('abc', '中文') and t1b = 1 and t1c = 1
and t1d = 20 and t1f = 1.1 and id_datetime between '2021-01-01' and '2021-02-01' and id_decimal = 1.2) t1
on t1.t1a > t2.t1a and t1.t1b = t2.t1b and t1.t1c > t2.t1c and t1.id_datetime < t2.id_datetime;

-- ============================================================
-- Aggregation subquery with HAVING predicate pushdown
-- ============================================================

-- query 30
-- inner join with aggregation subquery and HAVING predicate
-- @order_sensitive=false
select * from
(select max(t1d) t1d, t1a from ${case_db}.test_all_type group by t1a having max(t1d) > 10 and t1a in ('abc', '中文')) t1
inner join ${case_db}.test_all_type_not_null t2
on t1.t1d = t2.t1d and t1.t1a = t2.t1a;

-- query 31
-- left join with aggregation subquery and HAVING predicate
-- @order_sensitive=false
select * from
(select max(t1d) t1d, t1a from ${case_db}.test_all_type group by t1a having max(t1d) > 10 and t1a in ('abc', '中文')) t1
left join ${case_db}.test_all_type_not_null t2
on t1.t1d = t2.t1d and t1.t1a = t2.t1a;

-- ============================================================
-- Simple two-table subquery join predicate pushdown
-- ============================================================

-- query 32
-- inner join with equality predicate on filtered subqueries
select * from
(select * from ${case_db}.t0 where v1 = 1) t0
inner join
(select * from ${case_db}.t1 where v4 = 1) t1
on t0.v1 = t1.v4;

-- query 33
-- left join with equality predicate on filtered subqueries
select * from
(select * from ${case_db}.t0 where v1 = 1) t0
left join
(select * from ${case_db}.t1 where v4 = 1) t1
on t0.v1 = t1.v4;

-- query 34
-- inner join with inequality predicate on filtered subqueries (empty result)
select * from
(select * from ${case_db}.t0 where v1 = 1) t0
inner join
(select * from ${case_db}.t1 where v4 = 1) t1
on t0.v1 > t1.v4;

-- query 35
-- left join with inequality predicate, NULL fill
select * from
(select * from ${case_db}.t0 where v1 = 1) t0
left join
(select * from ${case_db}.t1 where v4 = 1) t1
on t0.v1 > t1.v4;

-- query 36
-- inner join with conflicting predicates (empty result)
select * from
(select * from ${case_db}.t0 where v1 > 4 and v2 = 3) t0
inner join
(select * from ${case_db}.t1 where v4 = 1) t1
on t0.v1 > t1.v4;

-- query 37
-- left join with conflicting predicates (empty result)
select * from
(select * from ${case_db}.t0 where v1 > 4 and v2 = 3) t0
left join
(select * from ${case_db}.t1 where v4 = 1) t1
on t0.v1 > t1.v4;

-- query 38
-- inner join with range predicate (empty result)
select * from
(select * from ${case_db}.t0 where v1 > 1 and v1 < 4) t0
inner join
(select * from ${case_db}.t1 where v4 = 2) t1
on t0.v1 > t1.v4;

-- query 39
-- right join with range predicate and NULL fill
select * from
(select * from ${case_db}.t0 where v1 > 1 and v1 < 4) t0
right join
(select * from ${case_db}.t1 where v4 = 2) t1
on t0.v1 > t1.v4;

-- ============================================================
-- Three-table and four-table join chains with predicate pushdown
-- ============================================================

-- query 40
-- three-table inner join chain
select * from (select * from ${case_db}.t0 where v1 < 10) t0
inner join ${case_db}.t1 on v1 = v4
inner join (select * from ${case_db}.t2 where v7 > 1) t2
on v4 = v7;

-- query 41
-- left + inner join chain
select * from (select * from ${case_db}.t0 where v1 < 10) t0
left join ${case_db}.t1 on v1 = v4
inner join (select * from ${case_db}.t2 where v7 > 1) t2
on v4 = v7;

-- query 42
-- inner + left join chain
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
inner join ${case_db}.t1 on v1 = v4
left join (select * from ${case_db}.t2 where v7 > 1) t2
on v4 = v7;

-- query 43
-- left + left join chain
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
left join ${case_db}.t1 on v1 = v4
left join (select * from ${case_db}.t2 where v7 > 1) t2
on v4 = v7;

-- query 44
-- four-table inner join chain with cross join
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
inner join ${case_db}.t1 on v1 = v4
CROSS join ${case_db}.t2
inner join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 = v10 and v10 = v4;

-- query 45
-- left + cross + left four-table join chain
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
left join ${case_db}.t1 on v1 = v4
CROSS join ${case_db}.t2
left join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 = v10 and v10 = v4;

-- query 46
-- right + cross + right four-table join chain
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
right join ${case_db}.t1 on v1 = v4
CROSS join ${case_db}.t2
right join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 = v10 and v10 = v4;

-- query 47
-- inner + cross + left four-table join chain
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
inner join ${case_db}.t1 on v1 = v4
CROSS join ${case_db}.t2
left join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 = v10 and v10 = v4;

-- query 48
-- left + cross + inner four-table join chain
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
left join ${case_db}.t1 on v1 = v4
CROSS join ${case_db}.t2
inner join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 = v10 and v10 = v4;

-- query 49
-- left + cross + right four-table join chain
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
left join ${case_db}.t1 on v1 = v4
CROSS join ${case_db}.t2
right join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 = v10 and v10 = v4;

-- query 50
-- right + cross + right four-table join chain (different data)
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
right join ${case_db}.t1 on v1 = v4
CROSS join ${case_db}.t2
right join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 = v10 and v10 = v4;

-- ============================================================
-- Four-table join chains with abs() expression in join key
-- ============================================================

-- query 51
-- inner + cross + inner with abs() expression join key
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
inner join ${case_db}.t1 on v1 = v4 + abs(1)
CROSS join ${case_db}.t2
inner join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 > v10 and v10 > v4;

-- query 52
-- left + cross + left with abs() expression join key
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
left join ${case_db}.t1 on v1 = v4 + abs(1)
CROSS join ${case_db}.t2
left join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 > v10 and v10 > v4;

-- query 53
-- right + cross + right with abs() expression join key
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
right join ${case_db}.t1 on v1 = v4 + abs(1)
CROSS join ${case_db}.t2
right join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 > v10 and v10 > v4;

-- query 54
-- inner + cross + left with abs() expression join key
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
inner join ${case_db}.t1 on v1 = v4 + abs(1)
CROSS join ${case_db}.t2
left join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 > v10 and v10 > v4;

-- query 55
-- left + cross + inner with abs() expression join key
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
left join ${case_db}.t1 on v1 = v4 + abs(1)
CROSS join ${case_db}.t2
inner join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 > v10 and v10 > v4;

-- query 56
-- left + cross + right with abs() expression join key
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
left join ${case_db}.t1 on v1 = v4 + abs(1)
CROSS join ${case_db}.t2
right join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 > v10 and v10 > v4;

-- query 57
-- right + cross + right with abs() expression join key (variant)
-- @order_sensitive=false
select * from (select * from ${case_db}.t0 where v1 < 10) t0
right join ${case_db}.t1 on v1 = v4 + abs(1)
CROSS join ${case_db}.t2
right join (select * from ${case_db}.t3 where v10 > 1) t3
on v7 > v10 and v10 > v4;

-- ============================================================
-- Correlated subqueries and complex join conditions
-- ============================================================

-- query 58
-- four-table inner join with BETWEEN and correlated IN subquery
SELECT *
FROM ${case_db}.t0
INNER JOIN ${case_db}.t1 ON t0.v1 = t1.v4 AND t0.v2 >= t1.v5
INNER JOIN ${case_db}.t2 ON t1.v5 = t2.v7 AND t2.v9 BETWEEN 0 AND 200
INNER JOIN ${case_db}.t3 ON t2.v8 = t3.v10 AND t3.v12 IN (SELECT MAX(v12) - 501 FROM ${case_db}.t3);

-- query 59
-- inner + left + right with correlated MIN subquery
-- @order_sensitive=false
SELECT *
FROM (select * from ${case_db}.t0 where v1 = 2) t0
INNER JOIN (select * from ${case_db}.t1 where v6 > 10 and v6 < 50) t1
  ON t0.v1 = t1.v4 AND t1.v6 = (SELECT MIN(v6) FROM ${case_db}.t1 WHERE v4 = t0.v1)
LEFT JOIN ${case_db}.t2 ON t1.v5 = t2.v7 AND t2.v9 < (SELECT AVG(v9) FROM ${case_db}.t2)
RIGHT JOIN ${case_db}.t3 ON t2.v8 = t3.v10 AND t2.v9 > t3.v12;

-- query 60
-- left + right + inner with IN subquery and NOT IN subquery
SELECT *
FROM ${case_db}.t0
LEFT JOIN ${case_db}.t1 ON t0.v1 = t1.v4 AND t1.v5 IN (SELECT v5 FROM ${case_db}.t1)
RIGHT JOIN ${case_db}.t2 ON t1.v5 = t2.v7 OR t2.v9 NOT IN (SELECT DISTINCT v9 FROM ${case_db}.t2)
INNER JOIN ${case_db}.t3 ON t2.v8 = t3.v10 AND t3.v12 >= (SELECT max(v12) FROM ${case_db}.t3);
