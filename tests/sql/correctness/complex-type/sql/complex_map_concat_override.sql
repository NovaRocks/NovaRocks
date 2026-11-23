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
-- @tags=complex,map,map_concat
-- Test Objective:
-- 1. Validate MAP_CONCAT key-conflict override semantics.
-- 2. Prevent regressions where right-map values fail to override duplicate keys.
-- Test Flow:
-- 1. Build two deterministic MAP literals with overlapping keys.
-- 2. Concatenate maps.
-- 3. Assert element lookup and cardinality outputs.
SELECT
    ELEMENT_AT(
        MAP_CONCAT(MAP(ARRAY['a', 'b'], ARRAY[1, 2]), MAP(ARRAY['b', 'c'], ARRAY[9, 3])),
        'a'
    ) AS a_v,
    ELEMENT_AT(
        MAP_CONCAT(MAP(ARRAY['a', 'b'], ARRAY[1, 2]), MAP(ARRAY['b', 'c'], ARRAY[9, 3])),
        'b'
    ) AS b_v,
    ELEMENT_AT(
        MAP_CONCAT(MAP(ARRAY['a', 'b'], ARRAY[1, 2]), MAP(ARRAY['b', 'c'], ARRAY[9, 3])),
        'c'
    ) AS c_v,
    cardinality(
        MAP_CONCAT(MAP(ARRAY['a', 'b'], ARRAY[1, 2]), MAP(ARRAY['b', 'c'], ARRAY[9, 3]))
    ) AS map_sz;
