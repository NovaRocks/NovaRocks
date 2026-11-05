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

-- @order_sensitive=false
-- @tags=column-naming,self_contained
-- Test Objective: Preserve analyzed field names, anonymous output ordinals, relation aliases, and computed dereference names.

-- query 1
SELECT CAST(1 AS integer), CAST(2 AS integer) IS NULL, CAST('x' AS varchar);

-- query 2
SELECT x, x IS NULL, x AS y FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 3
SELECT t.x FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 4
SELECT X FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 5
SELECT x AS MixedAlias FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 6
SELECT (x) FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 7
SELECT CAST(x AS bigint) FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 8
SELECT count(*), sum(x) FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 9
SELECT row_number() OVER (ORDER BY x) FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 10
SELECT CASE WHEN x = 1 THEN x END FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 11
SELECT CAST(1 AS integer) AS _col1, CAST(2 AS integer);

-- query 12
SELECT CAST(1 AS integer) AS a, CAST(2 AS integer) AS a;

-- query 13
SELECT (SELECT CAST(1 AS integer));

-- query 14
SELECT * FROM (SELECT CAST(1 AS integer), CAST(2 AS integer) AS b) AS s;

-- query 15
SELECT s.* FROM (SELECT CAST(1 AS integer)) AS s;

-- query 16
SELECT a FROM (SELECT CAST(1 AS integer)) AS s (a);

-- query 17
SELECT * FROM (VALUES (CAST(1 AS integer))) AS s;

-- query 18
VALUES (CAST(1 AS integer));

-- query 19
SELECT CAST(1 AS integer) UNION ALL SELECT CAST(2 AS integer);

-- query 20
SELECT CAST(1 AS integer) AS a UNION ALL SELECT CAST(2 AS integer) AS b;

-- query 21
SELECT CAST(1 AS integer) UNION ALL SELECT CAST(2 AS integer) AS b;

-- query 22
WITH c AS (SELECT CAST(1 AS integer)) SELECT * FROM c;

-- query 23
WITH c (a) AS (SELECT CAST(1 AS integer)) SELECT * FROM c;

-- query 24
SELECT x, * FROM (SELECT CAST(1 AS integer) AS x, CAST(2 AS integer)) AS s;

-- query 25
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT _col0 FROM (SELECT CAST(1 AS integer)) AS s;

-- query 26
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT s._col0 FROM (SELECT CAST(1 AS integer)) AS s;

-- query 27
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
WITH c AS (SELECT CAST(1 AS integer)) SELECT _col0 FROM c;

-- query 28
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT column_0 FROM (VALUES (CAST(1 AS integer))) AS s;

-- query 29
-- @expect_trino_error=MISMATCHED_COLUMN_ALIASES,USER_ERROR
WITH c (a) AS (SELECT CAST(1 AS integer), CAST(2 AS integer)) SELECT * FROM c;

-- query 30
-- @expect_trino_error=MISMATCHED_COLUMN_ALIASES,USER_ERROR
SELECT * FROM (SELECT CAST(1 AS integer), CAST(2 AS integer)) AS s (a);

-- query 31
SELECT grouping(a) FROM (VALUES (CAST(1 AS integer))) AS t (a) GROUP BY ROLLUP (a);

-- query 32
SELECT t.X FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 33
SELECT k FROM (VALUES (CAST(1 AS integer))) AS a (k) FULL JOIN (VALUES (CAST(1 AS integer))) AS b (k) USING (k);

-- query 34
SELECT x AS y FROM (VALUES (CAST(1 AS integer))) AS t (x) ORDER BY y;

-- query 35
SELECT x + 1 FROM (VALUES (CAST(1 AS integer))) AS t (x) ORDER BY 1;

-- query 36
SELECT s.a FROM (SELECT CAST(ROW(1) AS STRUCT<a integer>) AS s) AS t;

-- query 37
SELECT a2[1].a FROM (VALUES ([CAST(ROW(1) AS STRUCT<a integer>)])) AS t (a2);

-- query 38
SELECT (s).a FROM (SELECT CAST(ROW(1) AS STRUCT<a integer>) AS s) AS t;

-- query 39
SELECT (CAST(s AS STRUCT<a bigint>)).a FROM (SELECT CAST(ROW(1) AS STRUCT<a integer>) AS s) AS t;

-- query 40
SELECT s.A FROM (SELECT CAST(ROW(1) AS STRUCT<a integer>) AS s) AS t;
