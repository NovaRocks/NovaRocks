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

-- Recursive conversion and comparison on actual relation-produced values.
-- @order_sensitive=true

-- query 1
WITH input AS (
    SELECT id,
           CASE id
               WHEN 1 THEN CAST('7' AS VARCHAR)
               WHEN 2 THEN CAST('bad' AS VARCHAR)
               ELSE CAST(NULL AS VARCHAR)
           END AS token
    FROM TABLE(sequence(1, 3)) AS s(id)
)
SELECT id,
       TRY_CAST(ROW(token, ARRAY[token], MAP(ARRAY[1], ARRAY[token]))
                AS ROW(n INTEGER, a ARRAY(INTEGER), m MAP(INTEGER, INTEGER))) AS converted
FROM input
ORDER BY id;

-- query 2
-- Both names exist on the source, with their positions reversed.
SELECT CAST(
           CAST(ROW(CAST('17' AS VARCHAR), 9) AS ROW(old_name VARCHAR, new_name INTEGER))
           AS ROW(new_name BIGINT, old_name VARCHAR)
       ) AS converted;

-- query 3
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(ROW('bad') AS ROW(n INTEGER));

-- query 4
SELECT CAST(JSON '[1,2,null]' AS ARRAY(INTEGER)) AS numbers,
       CAST(JSON '{"n":7,"flag":true,"note":null}' AS ROW(n BIGINT, flag BOOLEAN, note VARCHAR)) AS record_value,
       CAST(JSON '{"2":4,"1":null}' AS MAP(INTEGER, BIGINT)) AS mapped,
       CAST(JSON '[null]' AS ARRAY(JSON)) AS json_leaf;

-- query 5
SELECT TRY_CAST(JSON '[1]' AS ROW(n INTEGER, flag BOOLEAN)) AS wrong_arity,
       TRY_CAST(JSON '{"1":7,"01":8}' AS MAP(INTEGER, BIGINT)) AS collided_keys,
       TRY_CAST(JSON '"[1]"' AS ARRAY(INTEGER)) AS scalar_json;

-- query 6
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(JSON '[1]' AS ROW(n INTEGER, flag BOOLEAN));

-- query 7
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(JSON '{"1":7,"01":8}' AS MAP(INTEGER, BIGINT));

-- query 8
WITH input AS (
    SELECT id, IF(id = 1, CAST(NULL AS BIGINT), BIGINT '2') AS tail
    FROM TABLE(sequence(1, 3)) AS s(id)
), values_by_id AS (
    SELECT id, tail,
           ARRAY[id, tail] AS array_value,
           ROW(id, tail) AS row_value,
           MAP(ARRAY[1, 2], ARRAY[id, tail]) AS map_value
    FROM input
)
SELECT id,
       array_value = ARRAY[BIGINT '1', BIGINT '2'] AS array_equal,
       row_value <> ROW(BIGINT '2', BIGINT '2') AS row_not_equal,
       map_value = MAP(ARRAY[2, 1], ARRAY[BIGINT '2', BIGINT '2']) AS map_equal,
       row_value IN (ROW(BIGINT '1', BIGINT '2'), ROW(BIGINT '2', BIGINT '2')) AS row_in,
       array_value IS NOT DISTINCT FROM ARRAY[id, tail] AS array_identical,
       map_value IS DISTINCT FROM MAP(ARRAY[2, 1], ARRAY[tail, id]) AS map_distinct
FROM values_by_id
ORDER BY id;

-- query 9
-- The second WHEN would fail on id 2 if evaluated after the first match.
WITH input AS (
    SELECT id, IF(id = 1, CAST(NULL AS BIGINT), BIGINT '2') AS tail
    FROM TABLE(sequence(1, 3)) AS s(id)
)
SELECT id,
       NULLIF(CAST(ROW(id, tail) AS ROW(label BIGINT, tail BIGINT)), ROW(2, BIGINT '2')) AS original_row,
       CASE ROW(id, tail)
           WHEN ROW(BIGINT '2', BIGINT '2') THEN true
           WHEN ROW(CAST(IF(id = 2, 'bad', '99') AS BIGINT), BIGINT '2') THEN true
           ELSE false
       END AS matched
FROM input
ORDER BY id;

-- query 10
-- MAP_AGG can produce a valid key with a NULL member. A definite value
-- mismatch wins over key uncertainty regardless of which MAP is on the left.
WITH grouped AS (
    SELECT map_agg(ARRAY[id, IF(id = 1, CAST(NULL AS BIGINT), BIGINT '2')], id) AS m
    FROM TABLE(sequence(1, 2)) AS s(id)
), pairs AS (
    SELECT m,
           MAP(ARRAY[ARRAY[BIGINT '2', BIGINT '2'], ARRAY[BIGINT '1', BIGINT '7']],
               ARRAY[BIGINT '99', BIGINT '1']) AS other
    FROM grouped
)
SELECT m = other AS forward,
       other = m AS reverse,
       m IS NOT DISTINCT FROM m AS identical
FROM pairs;

-- query 11
-- Both values are evaluated. After a hit, the second comparison is not demanded.
WITH grouped AS (
    SELECT map_agg(ARRAY[id, CAST(NULL AS BIGINT)], id) AS uncertain
    FROM TABLE(sequence(1, 1)) AS s(id)
), pairs AS (
    SELECT uncertain,
           MAP(ARRAY[ARRAY[BIGINT '1', BIGINT '2']], ARRAY[BIGINT '1']) AS exact_value
    FROM grouped
)
SELECT exact_value IN (exact_value, uncertain) AS matched
FROM pairs;

-- query 12
-- The installed contains_sequence coercion profile requires matching nested MAP key declarations.
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT contains_sequence(
    ARRAY[MAP(ARRAY['c', 'd'], ARRAY[3, 4])],
    CAST(ARRAY[MAP(ARRAY['c', 'd'], ARRAY[3, 4])] AS ARRAY(MAP(VARCHAR, INTEGER)))
);

-- query 13
WITH doc AS (
    SELECT CAST(ARRAY[json_scalar(DATE '2024-02-29')] AS JSON) AS v
)
SELECT CAST(v AS ARRAY(JSON))[1] IN (json_scalar(DATE '2024-02-29')) AS same_date,
       CAST(v AS ARRAY(JSON))[1] IN (JSON '"2024-02-29"') AS same_string
FROM doc;
