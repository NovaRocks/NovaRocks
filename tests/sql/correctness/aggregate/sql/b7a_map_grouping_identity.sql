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
-- Keep the two equivalent keys in separate Iceberg writes. The positional
-- exchange hash sends them to different ordinary V1 partitions with three BEs.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.b7a_map_grouping (id INTEGER, m MAP(INTEGER, INTEGER))
WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.b7a_map_grouping
VALUES ROW(1, MAP(ARRAY[1, 2], ARRAY[11, CAST(NULL AS INTEGER)]));

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.b7a_map_grouping
VALUES ROW(2, MAP(ARRAY[2, 1], ARRAY[CAST(NULL AS INTEGER), 11]));

-- query 4
-- @skip_result_check=true
SET SESSION pipeline_dop = 3;

-- query 5
-- @skip_result_check=true
SET SESSION disable_optimizer_rules = 'AggToHashAgg';

-- query 6
-- @explain_contains=HASH EXCHANGE
SELECT element_at(m, 1) AS map_value, count(*) AS row_count,
       element_at(map_keys(m), 1) IN (1, 2) AS original_order
FROM ${case_db}.b7a_map_grouping
GROUP BY m
ORDER BY map_value, row_count;

-- query 7
SELECT count(*) AS distinct_maps
FROM (SELECT DISTINCT m FROM ${case_db}.b7a_map_grouping);

-- query 8
-- @explain_contains=HASH EXCHANGE
SELECT count(*) AS row_count
FROM ${case_db}.b7a_map_grouping
GROUP BY ARRAY[m]
ORDER BY row_count;

-- query 9
-- @explain_contains=HASH AGGREGATE (LOCAL,
-- @explain_contains=HASH AGGREGATE (GLOBAL,
WITH collected AS (
    SELECT map_agg(m, id) AS by_map FROM ${case_db}.b7a_map_grouping
)
SELECT cardinality(by_map) AS map_count,
       element_at(map_keys(element_at(map_keys(by_map), 1)), 1) IN (1, 2) AS original_order
FROM collected;

-- query 10
-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_contains=Profile: tasks=
-- @result_contains=HASH EXCHANGE
EXPLAIN ANALYZE SELECT count(*) FROM ${case_db}.b7a_map_grouping GROUP BY m;

-- query 11
-- @skip_result_check=true
RESET SESSION disable_optimizer_rules;

-- query 12
-- @skip_result_check=true
RESET SESSION pipeline_dop;

-- query 13
SELECT id, element_at(map_keys(m), 1) AS first_key
FROM ${case_db}.b7a_map_grouping ORDER BY id;
