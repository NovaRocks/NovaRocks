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
-- @tags=aggregate,map_agg
-- Test Objective:
-- 1. Validate MAP_AGG materialization and key extraction.
-- 2. Prevent regressions in map aggregate state merge/finalization.
-- Test Flow:
-- 1. Create/reset key-value source table.
-- 2. Insert deterministic grouped key-value rows.
-- 3. Aggregate to map and extract keys for assertions.
CREATE TABLE ${case_db}.t_agg_map_agg_extract (
    g INT,
    k VARCHAR(10),
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_map_agg_extract VALUES
    ROW(1, 'a', 10),
    ROW(1, 'b', 20),
    ROW(2, 'a', 30);

SELECT
    g,
    ELEMENT_AT(MAP_AGG(k, v), 'a') AS v_a,
    ELEMENT_AT(MAP_AGG(k, v), 'b') AS v_b,
    CARDINALITY(MAP_AGG(k, v)) AS map_size
FROM ${case_db}.t_agg_map_agg_extract
GROUP BY g
ORDER BY g;
