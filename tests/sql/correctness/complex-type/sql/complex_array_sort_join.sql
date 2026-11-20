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
-- @tags=complex,array
-- Test Objective:
-- 1. Validate ARRAY_SORT and ARRAY_JOIN outputs.
-- 2. Prevent regressions in array ordering/stringification helpers.
-- Test Flow:
-- 1. Build deterministic array literals.
-- 2. Apply sort and join helpers.
-- 3. Assert scalar output.
SELECT
    ARRAY_SORT([3, 1, 2, NULL]) AS sorted_arr,
    ARRAY_JOIN(['a', 'b', NULL], ',') AS joined_arr;
