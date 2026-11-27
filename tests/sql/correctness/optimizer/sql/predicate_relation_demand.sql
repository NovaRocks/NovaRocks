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


-- @tags=optimizer,predicate_pushdown,relation_demand
-- @order_sensitive=true
-- A TruthOnly filter can eliminate rows before a selected wait. A Value
-- projection and surviving rows still observe the original row-error scope.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.predicate_demand_rows (
    id BIGINT, d BIGINT, delay BIGINT, enabled BOOLEAN, dt INTEGER, txt VARCHAR
) WITH (partitioning = ARRAY['dt']);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.predicate_demand_rows VALUES
    ROW(1, 0, 0, false, 10, 'bad'),
    ROW(2, 0, 0, NULL, 10, 'bad'),
    ROW(3, 3, 0, true, 10, '7'),
    ROW(4, 0, 0, true, 20, 'bad'),
    ROW(5, 3, 0, NULL, 10, '13');

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.predicate_demand_groups (k BIGINT, v BIGINT, safe_v BIGINT);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.predicate_demand_groups VALUES
    ROW(1, 10, 10),
    ROW(2, BIGINT '9223372036854775807', 20),
    ROW(2, 1, 5);

-- query 5
-- @explain_contains=predicates: enabled
-- @explain_contains=sleep(delay)
SELECT id FROM ${case_db}.predicate_demand_rows
WHERE 12 / d > 3 AND sleep(delay) AND enabled AND dt = 10 ORDER BY id;

-- query 6
-- The original wait must not run for FALSE or NULL rows. Passing the Scan
-- through the engine still succeeds when the connector offer is disabled.
-- @skip_result_check=true
SET SESSION enable_connector_static_predicate_pushdown = false;

-- query 7
SELECT id FROM ${case_db}.predicate_demand_rows
WHERE 12 / d > 3 AND sleep(delay) AND enabled AND dt = 10 ORDER BY id;

-- query 8
-- @skip_result_check=true
SET SESSION enable_connector_static_predicate_pushdown = true;

-- query 9
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT id FROM ${case_db}.predicate_demand_rows
WHERE 12 / d > 3 AND sleep(delay) AND enabled AND dt = 20;

-- query 10
-- @skip_result_check=true
SET SESSION disable_optimizer_rules = 'PushDownPredicateScan';

-- query 11
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT id FROM ${case_db}.predicate_demand_rows
WHERE 12 / d > 3 AND sleep(delay) AND enabled AND dt = 10;

-- query 12
-- @skip_result_check=true
SET SESSION disable_optimizer_rules = '';

-- query 13
-- @skip_result_check=true
-- @result_contains=provider-guarantee root=
-- @result_contains=builtin.scalar/rand/v1
EXPLAIN WITH SESSION explain_detail = 'CONTRACT'
SELECT id FROM ${case_db}.predicate_demand_rows WHERE rand() < 0.1 AND dt = 10;

-- query 14
SELECT id FROM ${case_db}.predicate_demand_rows
WHERE 12 / d > 3 AND enabled AND id IN (2, 5) ORDER BY id;

-- query 15
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT (12 / d > 3 AND enabled) AS retained FROM ${case_db}.predicate_demand_rows WHERE id = 2;

-- query 16
SELECT (12 / d > 3 AND enabled) AS retained FROM ${case_db}.predicate_demand_rows WHERE id = 5;

-- query 17
SELECT id, n FROM (
    SELECT id, CAST(txt AS BIGINT) AS n FROM ${case_db}.predicate_demand_rows
) t WHERE id = 3;

-- query 18
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT id, n FROM (
    SELECT id, CAST(txt AS BIGINT) AS n FROM ${case_db}.predicate_demand_rows
) t WHERE id = 1;

-- query 19
-- @skip_result_check=true
SET SESSION disable_optimizer_rules = 'PushDownPredicateProject';

-- query 20
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT id, n FROM (
    SELECT id, CAST(txt AS BIGINT) AS n FROM ${case_db}.predicate_demand_rows
) t WHERE id = 3;

-- query 21
-- @skip_result_check=true
SET SESSION disable_optimizer_rules = '';

-- query 22
SELECT k, SUM(v) AS total FROM ${case_db}.predicate_demand_groups GROUP BY k HAVING k = 1;

-- query 23
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT k, SUM(v) AS total FROM ${case_db}.predicate_demand_groups GROUP BY k HAVING k = 2;

-- query 24
SELECT k, SUM(safe_v) AS total FROM ${case_db}.predicate_demand_groups
GROUP BY GROUPING SETS ((k), ()) HAVING k IS NULL;

-- query 25
SELECT id, TRY_CAST(txt AS BIGINT) AS caught
FROM ${case_db}.predicate_demand_rows WHERE id = 1;

-- query 26
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT TRY_CAST(CAST(txt AS BIGINT) AS DOUBLE)
FROM ${case_db}.predicate_demand_rows WHERE id = 1;
