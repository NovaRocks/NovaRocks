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
-- @tags=complex,map
-- Test Objective:
-- 1. Validate MAP scalar access/size functions.
-- 2. Prevent regressions in MAP element lookup semantics.
-- Test Flow:
-- 1. Build constant MAP expressions.
-- 2. Apply size and element access functions.
-- 3. Assert scalar outputs.
SELECT
    cardinality(MAP(ARRAY['a', 'b'], ARRAY[1, 2])) AS map_size_v,
    CARDINALITY(MAP(ARRAY['a', 'b'], ARRAY[1, 2])) AS map_cardinality,
    ELEMENT_AT(MAP(ARRAY['a', 'b'], ARRAY[1, 2]), 'b') AS value_b;
