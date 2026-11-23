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
SELECT lower('A') AS lowercase, parse_json('{') IS NULL AS caught_parse;

-- query 2
SELECT every(TRUE) AS all_true, max_by(1,2) AS winning_value;

-- query 3
SELECT row_number() OVER () AS row_number_value;

-- query 4
SELECT -CAST(1 AS SMALLINT) AS negated, typeof(CAST(1 AS TINYINT)) AS narrow_type;

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT reverse(ARRAY[1]) WHERE FALSE;

-- query 6
-- Invalid arity must be rejected even when the row predicate is false.
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT next_day('2020-01-01') WHERE FALSE;

-- query 7
-- @expect_error=Unknown function
SELECT intrinsic_definitely_unknown(1) WHERE FALSE;

-- query 8
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT map_filter(map(ARRAY[1], ARRAY[10]), (k, v) -> TRUE) WHERE FALSE;

-- query 9
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, ordinal) AS null_key_order
FROM (VALUES (1, ARRAY[2], NULL), (2, ARRAY[1], NULL))
    AS input_rows(ordinal, payload_value, k1);

-- query 10
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT array_sort(ARRAY[]);

-- query 11
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT array_min(ARRAY[NULL]);

-- query 12
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT array_max(ARRAY[NULL]);
