-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Value-form IN reduces ordinary comparisons, not null-safe equality.
-- A TRUE pair wins; otherwise any UNKNOWN pair yields NULL. A definite
-- leaf mismatch wins over a NULL leaf within one composite comparison.

-- query 1
SELECT [1, CAST(NULL AS INT)] IN (SELECT [1,2]) AS unknown_pair,
       [9, CAST(NULL AS INT)] IN (SELECT [1,2]) AS definite_mismatch;

-- query 2
SELECT [1,2] IN (SELECT x FROM (VALUES ROW([1,2]), ROW([1,CAST(NULL AS INT)]), ROW([1,2])) t(x)) AS match_wins,
       [1,2] NOT IN (SELECT x FROM (VALUES ROW([1,2]), ROW([1,CAST(NULL AS INT)])) t(x)) AS not_match_wins;

-- query 3
SELECT [9,7] IN (SELECT x FROM (VALUES ROW([1,2]), ROW(CAST(NULL AS ARRAY<INT>))) t(x)) AS outer_null;

-- query 4
SELECT CAST(NULL AS ARRAY<INT>) IN (SELECT [1,2] WHERE FALSE) AS empty_in,
       CAST(NULL AS ARRAY<INT>) NOT IN (SELECT [1,2] WHERE FALSE) AS empty_not_in;

-- query 5
-- @order_sensitive=true
-- Build duplicates must not multiply outer rows, and outer duplicates stay.
SELECT pk, x IN (SELECT v FROM (VALUES ROW([1,2]),ROW([1,2])) b(v)) AS member,
       x NOT IN (SELECT v FROM (VALUES ROW([1,2]),ROW([1,2])) b(v)) AS nonmember
FROM (VALUES ROW(1,[1,CAST(NULL AS INT)]), ROW(1,[1,CAST(NULL AS INT)]),
             ROW(2,[9,7]), ROW(3,[1,2]), ROW(4,CAST(NULL AS ARRAY<INT>))) t(pk,x)
ORDER BY pk;

-- query 6
SELECT row(1,CAST(NULL AS INT)) IN (SELECT row(1,2)) AS unknown_pair,
       row(9,CAST(NULL AS INT)) IN (SELECT row(1,2)) AS definite_mismatch;

-- query 7
SELECT map(1,CAST(NULL AS INT)) IN (SELECT map(1,2)) AS unknown_pair,
       map(9,CAST(NULL AS INT)) IN (SELECT map(1,2)) AS definite_mismatch;

-- query 8
-- Compare decimal values after exact common precision/scale coercion.
SELECT [CAST('1' AS DECIMAL(26,2)), CAST(NULL AS DECIMAL(26,2))]
         NOT IN (SELECT [CAST('1' AS DECIMAL(4,3)), CAST('2' AS DECIMAL(4,3))]) AS unknown_pair,
       [CAST('1' AS DECIMAL(26,2)), CAST('2' AS DECIMAL(26,2))]
         IN (SELECT [CAST('1' AS DECIMAL(4,3)), CAST('2' AS DECIMAL(4,3))]) AS match;

-- query 9
-- Preserve the FE's frozen STRING promotion for nested string/numeric pairs.
SELECT ['1','2'] IN (SELECT [CAST('1' AS DECIMAL(4,2)),CAST('2' AS DECIMAL(4,2))]) AS text_differs;

-- query 10
-- @order_sensitive=true
SELECT pk FROM (VALUES ROW(1,[1,CAST(NULL AS INT)]), ROW(2,[9,7]),
                       ROW(3,[1,2]), ROW(4,CAST(NULL AS ARRAY<INT>))) t(pk,x)
WHERE pk = 2 OR x IN (SELECT [1,2]) ORDER BY pk;

-- query 11
SELECT [[1,CAST(NULL AS INT)]] IN (SELECT [[1,2]]) AS unknown_pair,
       [[9,CAST(NULL AS INT)]] IN (SELECT [[1,2]]) AS definite_mismatch;

-- query 12
-- Nested HAVING probes belong to the grouped row, not the input FROM row.
SELECT x, count() AS n FROM (VALUES ROW(1),ROW(1),ROW(2)) t(x)
GROUP BY x HAVING [x] IN (SELECT [1]) OR count() = 0 ORDER BY x;

-- query 13
SELECT x, [count()] IN (SELECT [2]) AS membership
FROM (VALUES ROW(1),ROW(1),ROW(2)) t(x) GROUP BY x ORDER BY x;

-- query 14
SELECT x, count() AS n FROM (VALUES ROW(1),ROW(1),ROW(2)) t(x)
GROUP BY x HAVING [count()] IN (SELECT [2]) OR x = 3 ORDER BY x;

-- query 15
-- A collection attached after a scalar aggregate must preserve its empty-input row.
SELECT [count()] IN (SELECT [0]) AS membership
FROM (SELECT 1 AS x WHERE FALSE) t;

-- query 16
-- A probe inside an aggregate argument belongs before aggregation instead.
SELECT SUM(CASE WHEN [x] IN (SELECT [1]) THEN 1 ELSE 0 END) AS n
FROM (VALUES ROW(1),ROW(1),ROW(2)) t(x);
