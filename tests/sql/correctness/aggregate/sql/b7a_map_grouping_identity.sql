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
-- Keep the ordered arrays in separate Iceberg writes and construct the MAPs
-- after reading them. The V1 hash sends these orders to different partitions
-- with three BEs.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.b7a_map_grouping (
    id INTEGER, entry_keys ARRAY(INTEGER), entry_values ARRAY(INTEGER)
)
WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.b7a_map_grouping
VALUES ROW(1, ARRAY[1, 2], ARRAY[11, CAST(NULL AS INTEGER)]);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.b7a_map_grouping
VALUES ROW(2, ARRAY[2, 1], ARRAY[CAST(NULL AS INTEGER), 11]);

-- query 4
-- @skip_result_check=true
SET SESSION pipeline_dop = 3;

-- query 5
SELECT id, element_at(map_entries(MAP(entry_keys, entry_values)), 1)[1] AS first_key
FROM ${case_db}.b7a_map_grouping ORDER BY id;

-- query 6
-- @be_log_be_count_at_least=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK,2
SELECT element_at(m, 1) AS map_value, count(*) AS row_count,
       element_at(map_entries(m), 1)[1] IN (1, 2) AS original_order
FROM (SELECT MAP(entry_keys, entry_values) AS m FROM ${case_db}.b7a_map_grouping) AS input_maps
GROUP BY m
ORDER BY map_value, row_count;

-- query 7
-- @explain_contains=HASH EXCHANGE
SELECT count(*) AS distinct_maps
FROM (SELECT DISTINCT m FROM (SELECT MAP(entry_keys, entry_values) AS m FROM ${case_db}.b7a_map_grouping) AS input_maps) AS unique_maps;

-- query 8
SELECT count(*) AS row_count
FROM (SELECT MAP(entry_keys, entry_values) AS m FROM ${case_db}.b7a_map_grouping) AS input_maps
GROUP BY ARRAY[m]
ORDER BY row_count;

-- query 9
WITH collected AS (
    SELECT map_agg(MAP(entry_keys, entry_values), id) AS by_map FROM ${case_db}.b7a_map_grouping
), representative AS (
    SELECT cardinality(by_map) AS map_count, element_at(map_entries(by_map), 1)[1] AS original
    FROM collected
)
SELECT map_count, element_at(map_entries(original), 1)[1] IN (1, 2) AS original_order
FROM representative;

-- query 10
-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_contains=Profile: tasks=
-- @result_contains=HASH EXCHANGE
-- @result_contains=algorithm=novarocks.native-exchange.v2
EXPLAIN ANALYZE SELECT DISTINCT MAP(entry_keys, entry_values) AS m
FROM ${case_db}.b7a_map_grouping;

-- query 11
-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_contains=Profile: tasks=
-- @result_contains=HASH EXCHANGE
-- @result_contains=algorithm=novarocks.native-exchange.v2
EXPLAIN ANALYZE SELECT DISTINCT ARRAY[MAP(entry_keys, entry_values)] AS m
FROM ${case_db}.b7a_map_grouping;

-- query 12
-- @skip_result_check=true
RESET SESSION pipeline_dop;
