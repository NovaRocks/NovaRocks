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
-- @tags=complex,map,array
-- Test Objective:
-- 1. Validate nested MAP<key,ARRAY> access and downstream array aggregation.
-- 2. Prevent regressions in nested complex-type expression evaluation.
-- Test Flow:
-- 1. Build nested MAP and ARRAY expressions.
-- 2. Extract nested array by key.
-- 3. Aggregate extracted array and assert scalar outputs.
SELECT
    ELEMENT_AT(MAP(ARRAY['a', 'b'], ARRAY[[1, 2, 3], [4]]), 'a') AS arr_a,
    concat(ELEMENT_AT(MAP(ARRAY['x'], ARRAY[[1, 2]]), 'x'), [3]) AS concat_arr;
