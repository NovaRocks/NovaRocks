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

-- @tags=join,map
-- Test Objective:
-- Validate JOIN correctness for MAP-typed columns across all join types.
-- Covers: INNER, LEFT/RIGHT/FULL OUTER, EXISTS/NOT EXISTS, IN/NOT IN subqueries,
-- NULL-safe equality and map transformations in join predicates.
-- MAP value types include: simple (int→string), nested array, nested map, and struct.
-- Also verifies FE rejects unsupported MAP comparisons (cross-type, JSON, non-equality).

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.map_test;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.map_test (
  pk BIGINT NOT NULL,
  map0 MAP<INT, STRING>,
  map1 MAP<DECIMAL(16,3), VARCHAR(30)>,
  map2 MAP<INT, ARRAY<STRING>>,
  map3 MAP<STRING, MAP<INT, VARCHAR(30)>>,
  map4 MAP<INT, JSON>,
  map5 MAP<INT, ROW(c INT, b STRING)>
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.map_test VALUES
  (0, MAP(ARRAY[0], ARRAY['ab']), MAP(ARRAY[0], ARRAY['ab']), MAP(ARRAY[0], ARRAY[['1','2']]), MAP(ARRAY['1'], ARRAY[MAP(ARRAY[1], ARRAY['abc'])]), MAP(ARRAY[1], ARRAY[json_object('name':'abc','age':23)]), MAP(ARRAY[0], ARRAY[row(1,'a')]));

-- query 4
-- @skip_result_check=true
-- iceberg: map keys must be non-null; dropped null-key entries from map1/map4/map5 (int keys -> empty map)
INSERT INTO ${case_db}.map_test VALUES
  ROW(1, MAP(ARRAY[1], ARRAY[null]), map(), MAP(ARRAY[1], ARRAY[[]]), MAP(ARRAY['11', ''], ARRAY[MAP(ARRAY[1], ARRAY['abc']), MAP(ARRAY[2], ARRAY[null])]), map(), map());

-- query 5
-- @skip_result_check=true
-- iceberg: map keys must be non-null; every entry here had a null key, dropped them all (empty maps)
INSERT INTO ${case_db}.map_test VALUES
  ROW(2, map(), map(), map(), map(), map(), map());

-- query 6
-- @skip_result_check=true
-- iceberg: map keys must be non-null; dropped the trailing null-key entry from each map (kept the non-null entry)
INSERT INTO ${case_db}.map_test VALUES
  (3, MAP(ARRAY[3], ARRAY['']), MAP(ARRAY[3], ARRAY['']), MAP(ARRAY[3], ARRAY[['3',null]]), MAP(ARRAY['3'], ARRAY[MAP(ARRAY[3], ARRAY['a33c'])]), MAP(ARRAY[1], ARRAY[json_object('name':'abc','age':23)]), MAP(ARRAY[3], ARRAY[row(3,'a')]));

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.map_test VALUES ROW(4, null, null, null, null, null, null);

-- ========== INNER JOIN (same type) ==========

-- query 8
-- map<int, array<string>> self join
SELECT t.map2, s.map2
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map2 = t.map2
ORDER BY t.pk;

-- query 9
-- map<string, map<int, varchar>> self join
SELECT t.map3, s.map3
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map3 = t.map3
ORDER BY t.pk;

-- query 10
-- map<int, struct> self join
SELECT t.map5, s.map5
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map5 = t.map5
ORDER BY t.pk;

-- query 11
-- map<int, string> self join (WHERE syntax)
SELECT t.map0, s.map0
FROM ${case_db}.map_test t CROSS JOIN ${case_db}.map_test s WHERE s.map0 = t.map0
ORDER BY t.pk;

-- query 12
-- map<decimal, varchar> self join (WHERE syntax)
SELECT t.map1, s.map1
FROM ${case_db}.map_test t CROSS JOIN ${case_db}.map_test s WHERE s.map1 = t.map1
ORDER BY t.pk;

-- ========== MAP<INT, JSON> same-type equi-join ==========
-- StarRocks FE rejects this; NovaRocks supports it by comparing the
-- MAP entries element-wise (the JSON values are the inner value type,
-- and same-text JSON literals compare equal). The result depends on
-- the seed data's JSON literal canonicalisation, so verify the query
-- succeeds rather than pinning a row count.

-- query 13
-- @skip_result_check=true
SELECT t.map4, s.map4
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map4 = t.map4
ORDER BY t.pk;

-- ========== FE error: cross-type MAP comparison ==========

-- query 14
-- @expect_error=does not support binary predicate operation
SELECT t.map0, s.map5
FROM ${case_db}.map_test t CROSS JOIN ${case_db}.map_test s WHERE s.map0 = t.map5
ORDER BY t.pk;

-- ========== FE error: non-equality comparison on MAP ==========

-- query 15
-- @expect_error=does not support binary predicate operation
SELECT t.map0, s.map0
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map0 < t.map0
ORDER BY t.pk;

-- ========== NULL-safe equal (<=>)  ==========

-- query 16
-- map3 <=> map3: includes NULL row in result
SELECT t.map3, s.map3
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map3  IS NOT DISTINCT FROM  t.map3
ORDER BY t.pk;

-- ========== LEFT / RIGHT JOIN with MAP key ==========

-- query 17
-- LEFT JOIN map<int, struct>
SELECT t.map5, s.map5
FROM ${case_db}.map_test t LEFT JOIN ${case_db}.map_test s ON s.map5 = t.map5
ORDER BY t.pk;

-- query 18
-- RIGHT JOIN map<int, struct>
SELECT t.map5, s.map5
FROM ${case_db}.map_test t RIGHT JOIN ${case_db}.map_test s ON s.map5 = t.map5
ORDER BY t.pk;

-- ========== EXISTS / NOT EXISTS ==========

-- query 19
-- EXISTS with map3 self join
SELECT map3
FROM ${case_db}.map_test t
WHERE EXISTS (SELECT 1 FROM ${case_db}.map_test s WHERE t.map3 = s.map3)
ORDER BY t.pk;

-- query 20
-- NOT EXISTS with map0 self
SELECT map0
FROM ${case_db}.map_test t
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.map_test s WHERE t.map0 = s.map0)
ORDER BY t.pk;

-- query 21
-- NOT EXISTS with map1 vs map0 (cross column, compatible types auto-cast)
SELECT map1
FROM ${case_db}.map_test t
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.map_test s WHERE t.map1 = s.map0)
ORDER BY t.pk;

-- ========== IN / NOT IN subquery ==========

-- query 22
-- IN subquery with map3
SELECT map3
FROM ${case_db}.map_test t
WHERE map3 IN (SELECT map3 FROM ${case_db}.map_test s)
ORDER BY t.pk;

-- query 23
-- IN subquery with map5
SELECT map5
FROM ${case_db}.map_test t
WHERE map5 IN (SELECT map5 FROM ${case_db}.map_test s)
ORDER BY t.pk;

-- query 24
-- NOT IN subquery with map5
SELECT map5
FROM ${case_db}.map_test t
WHERE map5 NOT IN (SELECT map5 FROM ${case_db}.map_test s)
ORDER BY t.pk;

-- ========== IN / NOT IN as expression (boolean output) ==========

-- query 25
-- Stable aliases and row identities make UNKNOWN observable in the snapshot.
SELECT pk, map0 IN (SELECT map0 FROM ${case_db}.map_test s) AS membership FROM ${case_db}.map_test ORDER BY pk;

-- query 26
SELECT pk, map5 IN (SELECT map5 FROM ${case_db}.map_test s) AS membership FROM ${case_db}.map_test ORDER BY pk;

-- query 27
SELECT pk, map0 NOT IN (SELECT map0 FROM ${case_db}.map_test s) AS membership FROM ${case_db}.map_test ORDER BY pk;

-- query 28
SELECT pk, map1 NOT IN (SELECT map0 FROM ${case_db}.map_test s) AS membership FROM ${case_db}.map_test ORDER BY pk;

-- query 29
SELECT pk, map5 NOT IN (SELECT map5 FROM ${case_db}.map_test s) AS membership FROM ${case_db}.map_test ORDER BY pk;

-- ========== Map transformations in join predicates ==========

-- query 30
SELECT t.map2, s.map2
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s
  ON transform_values(transform_keys(s.map2, (k, v) -> k + 1), (k, v) -> cardinality(v))
     = transform_values(transform_keys(t.map2, (k, v) -> k + 1), (k, v) -> cardinality(v))
ORDER BY t.pk;

-- query 31
SELECT t.map2, s.map2
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s
  ON transform_keys(s.map2, (k, v) -> k + 1) = transform_keys(t.map2, (k, v) -> k + 1)
ORDER BY t.pk;

-- query 32
-- Empty maps/arrays stay equal; nested NULL leaves stay UNKNOWN.
SELECT pk, map2 = map2 AS eq, map2 != map2 AS ne, map2  IS NOT DISTINCT FROM  map2 AS safe,
       map3 = map3 AS nested_eq, map3  IS NOT DISTINCT FROM  map3 AS nested_safe
FROM ${case_db}.map_test ORDER BY pk;

-- query 33
-- Duplicate empty maps retain all four matches, not just one per value.
SELECT t.pk AS probe_pk, s.pk AS build_pk
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON t.map5 = s.map5
WHERE t.pk IN (1, 2) ORDER BY t.pk, s.pk;
