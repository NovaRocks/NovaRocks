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
-- @tags=complex,array,map
-- Exercise exact collection lambda overloads on stored parent rows.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.b7c_small (
    id INTEGER,
    a ARRAY(INTEGER),
    groups_input ARRAY(ARRAY(INTEGER)),
    labels ARRAY(VARCHAR),
    cutoff INTEGER
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.b7c_small VALUES
    ROW(0, ARRAY[0, 1], ARRAY[ARRAY[8, 2, 5], ARRAY[9, 6]], ARRAY['a', 'b'], 4),
    ROW(1, ARRAY[1], ARRAY[ARRAY[4, 3], ARRAY[7]], ARRAY['z'], 2);

-- query 3
SELECT id, transform(a, x -> x + cutoff) AS transformed,
       filter(a, x -> x > 0) AS filtered,
       any_match(a, x -> x = 0) AS has_zero,
       all_match(a, x -> x > 0) AS all_positive,
       array_sort(a, (x, y) -> IF(x = y, 0, IF((x < y) = (id = 0), 1, -1))) AS compared,
       array_sort(a) AS natural_order
FROM ${case_db}.b7c_small ORDER BY id;

-- query 4
WITH input AS (
    SELECT id, MAP(ARRAY[1, 2], ARRAY[cutoff, cutoff + 1]) AS m
    FROM ${case_db}.b7c_small
)
SELECT id, map_filter(m, (k, v) -> k <= id + 1) AS filtered,
       transform_keys(m, (k, v) -> k + id + 10) AS keyed,
       transform_values(m, (k, v) -> IF(k = 1, v + id, CAST(NULL AS INTEGER))) AS valued,
       TRY(transform_keys(m, (k, v) -> IF(id = 0, k, 1))) AS guarded
FROM input ORDER BY id;

-- query 5
SELECT TRY(transform_keys(MAP(ARRAY[1], ARRAY[5]), (k, v) -> CAST(NULL AS INTEGER))) AS null_key,
       TRY(transform_keys(CAST(map() AS MAP(INTEGER, INTEGER)),
                          (k, v) -> CAST(NULL AS INTEGER))) AS empty_map,
       transform_keys(CAST(map() AS MAP(INTEGER, INTEGER)), (k, v) -> NULL) AS unknown_empty,
       cardinality(transform_keys(MAP(ARRAY[1, 2], ARRAY[10, 20]),
                                  (k, v) -> ROW(k, CAST(NULL AS INTEGER)))) AS composite_keys;

-- query 6
SELECT id, any_match(a, x -> any_match(a, y -> y > id)) AS nested_match,
       transform(groups_input, g -> filter(g, x -> x > cutoff AND x < g[1])) AS nested_filter,
       transform(groups_input, g -> transform(g, x -> cardinality(concat(labels, labels)))) AS shared_top,
       transform(groups_input, g -> transform(a, x -> transform(a, y -> y + x + g[1] + cutoff)))
           AS shared_ancestor
FROM ${case_db}.b7c_small ORDER BY id;

-- query 7
SELECT id, filter(a, x -> id = 0 OR 1 / x > 0) AS early_true
FROM ${case_db}.b7c_small ORDER BY id;

-- query 8
-- NovaRocks' ordinary Boolean region retains the deciding TRUE after the buffered row error.
SELECT id, filter(a, x -> 1 / x > 0 OR id = 0) AS late_true
FROM ${case_db}.b7c_small ORDER BY id;

-- query 9
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT filter(a, x -> 1 / x > 0) FROM ${case_db}.b7c_small;

-- query 10
SELECT id,
       TRY(any_match(a, x -> 1 / x > 0)) AS any_original,
       TRY(any_match(reverse(a), x -> 1 / x > 0)) AS any_reversed,
       TRY(all_match(a, x -> 1 / x < 0)) AS all_original,
       TRY(all_match(reverse(a), x -> 1 / x < 0)) AS all_reversed
FROM ${case_db}.b7c_small ORDER BY id;

-- query 11
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT filter(a, x -> CAST(NULL AS BOOLEAN) AND 1 / x > 0)
FROM ${case_db}.b7c_small WHERE id = 0;

-- query 12
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT map_filter(MAP(a, a), (k, v) -> CAST(NULL AS BOOLEAN) AND 1 / k > 0)
FROM ${case_db}.b7c_small WHERE id = 0;

-- query 13
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT any_match(a, x -> CAST(NULL AS BOOLEAN) AND 1 / x > 0)
FROM ${case_db}.b7c_small WHERE id = 0;

-- query 14
SELECT id, TRY(filter(a, x -> CAST(NULL AS BOOLEAN) AND 1 / x > 0)) AS caught,
       filter(a, x -> FALSE AND 1 / x > 0) AS skipped,
       transform(a, x -> IF(x >= 0, x, 1 / (id - id))) AS guarded
FROM ${case_db}.b7c_small ORDER BY id;

-- query 15
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT transform_keys(MAP(ARRAY[1, 2], ARRAY[10, 20]),
                      (k, v) -> ROW(1, CAST(NULL AS INTEGER)));

-- query 16
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT transform_keys(MAP(ARRAY[1], ARRAY[10]), (k, v) -> NULL);

-- query 17
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT transform_keys(MAP(ARRAY[1, 2, 3], ARRAY[10, 20, 0]),
                      (k, v) -> CASE WHEN k < 3 THEN 1 ELSE 1 / v END);

-- query 18
-- @skip_result_check=true
CREATE TABLE ${case_db}.b7c_wide (
    id INTEGER,
    a ARRAY(INTEGER),
    b ARRAY(INTEGER),
    c ARRAY(INTEGER),
    d ARRAY(INTEGER)
) WITH (format_version = 3);

-- query 19
-- @skip_result_check=true
INSERT INTO ${case_db}.b7c_wide VALUES
    ROW(1, flatten(repeat(ARRAY[1, 2], 10000)),
        concat(ARRAY[1], repeat(-1, 19999)), CAST(NULL AS ARRAY(INTEGER)),
        concat(ARRAY[2], repeat(-1, 19999))),
    ROW(2, flatten(repeat(ARRAY[2, 1], 10000)),
        CAST(NULL AS ARRAY(INTEGER)), concat(ARRAY[2], repeat(-1, 19999)),
        concat(ARRAY[1], repeat(-1, 19999))),
    ROW(3, repeat(1, 20000), concat(ARRAY[1], repeat(2, 19999)),
        concat(ARRAY[-1, 1], repeat(0, 20000)), CAST(NULL AS ARRAY(INTEGER)));

-- query 20
SELECT id, cardinality(filter(a, x -> contains(b, x))) AS retained,
       any_match(c, x -> 1 / x > 0) AS match_before_tail_error
FROM ${case_db}.b7c_wide WHERE id = 3;

-- query 21
WITH filtered_rows AS (
    SELECT id, a, filter(a, x -> contains(COALESCE(IF(x = 1, b, c), d), x)) AS kept
    FROM ${case_db}.b7c_wide WHERE id IN (1, 2)
)
SELECT id, cardinality(kept) AS retained, kept[1] AS first_item, kept[2] AS second_item,
       all_match(zip(a, kept), pair -> pair[1] = pair[2]) AS same_order
FROM filtered_rows ORDER BY id;

-- query 22
WITH sorted_rows AS (
    SELECT array_sort(
        zip(a, concat(repeat(1, 5000), repeat(2, 5000), repeat(3, 5000), repeat(4, 5000))),
        (x, y) -> IF(x[1] < y[1], -1, IF(x[1] = y[1], 0, 1))) AS sorted_items
    FROM ${case_db}.b7c_wide WHERE id = 1
), expected_tags AS (
    SELECT concat(repeat(1, 2500), repeat(2, 2500), repeat(3, 2500), repeat(4, 2500)) AS tags
)
SELECT cardinality(sorted_items) AS sorted_count,
       all_match(
           zip(sorted_items, concat(repeat(1, 10000), repeat(2, 10000)), concat(tags, tags)),
           entry -> entry[1][1] = entry[2] AND entry[1][2] = entry[3]) AS stable_order
FROM sorted_rows CROSS JOIN expected_tags;

-- query 23
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.b7c_small;
DROP TABLE IF EXISTS ${case_db}.b7c_wide;
