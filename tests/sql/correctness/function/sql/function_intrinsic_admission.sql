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

-- Independent admission and authoritative lowering controls; no storage dependency.
-- query 1
SELECT lower('A') AS lowercase, TRY(json_parse('{')) IS NULL AS caught_parse;

-- query 2
SELECT every(TRUE) AS all_true, max_by(1,2) AS winning_value;

-- query 3
SELECT row_number() OVER () AS row_number_value;

-- query 4
SELECT -CAST(1 AS SMALLINT) AS negated, typeof(CAST(1 AS TINYINT)) AS narrow_type;

-- query 5
SELECT reverse(ARRAY[1]) AS reversed_array WHERE FALSE;

-- query 6
-- Invalid arity must be rejected even when the row predicate is false.
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT next_day('2020-01-01') WHERE FALSE;

-- query 7
-- @expect_error=Unknown function
SELECT intrinsic_definitely_unknown(1) WHERE FALSE;

-- query 8
-- A valid higher-order call still has its declared result type in an empty relation.
SELECT map_filter(map(ARRAY[1], ARRAY[10]), (k, v) -> TRUE) AS filtered_map
WHERE FALSE;

-- query 9
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, ordinal) AS null_key_order
FROM (VALUES (1, ARRAY[2], NULL), (2, ARRAY[1], NULL))
    AS input_rows(ordinal, payload_value, k1);

-- query 10
SELECT array_sort(ARRAY[]) AS result;

-- query 11
SELECT array_min(ARRAY[NULL]) AS result;

-- query 12
SELECT array_max(ARRAY[NULL]) AS result;

-- query 13
-- A NULL left value skips the right expression on that row.
SELECT NULLIF(x, 10 / divisor) AS result
FROM (VALUES (CAST(NULL AS SMALLINT), 0), (SMALLINT '7', 2)) AS t(x, divisor);

-- query 14
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT NULLIF(x, 10 / divisor)
FROM (VALUES (CAST(NULL AS SMALLINT), 0), (SMALLINT '7', 0)) AS t(x, divisor);

-- query 15
-- UNKNOWN equality retains the original left array and its element type.
SELECT NULLIF(ARRAY[1, NULL], ARRAY[BIGINT '1', NULL]) AS result;

-- query 16
SELECT typeof(JSON_OBJECT('k' VALUE 1)) AS default_json_type,
       typeof(JSON_OBJECT('k' VALUE 1 RETURNING JSON)) AS native_json_type,
       JSON_ARRAY(1, CAST(NULL AS BIGINT)) AS array_text,
       JSON_OBJECT('k' VALUE CAST(NULL AS BIGINT)) AS object_text;

-- query 17
SELECT JSON_SERIALIZE(JSON '"abc"' RETURNING VARCHAR(3)) AS shortened,
       to_hex(JSON_SERIALIZE(JSON '1' RETURNING VARBINARY FORMAT JSON ENCODING UTF16)) AS encoded;

-- query 18
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT JSON_OBJECT('k' VALUE JSON '{"a":1,"a":2}' WITH UNIQUE KEYS);

-- query 19
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT parse_json('1');

-- query 20
-- A malformed predicate body is rejected before row pruning.
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT map_filter(map(ARRAY[1], ARRAY[10]), (k, v) -> NULL) WHERE FALSE;
