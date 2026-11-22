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
-- 1. Validate ARRAY_SLICE offset=0 semantics match StarRocks (empty result).
-- 2. Prevent regressions where offset=0 is treated as start-from-first-element.
-- Test Flow:
-- 1. Build deterministic array literals.
-- 2. Evaluate ARRAY_SLICE with zero, positive, and negative offsets.
-- 3. Assert projected arrays.
SELECT
    slice([1, 2, 3], 0, 2) AS sliced_zero_with_len,
    slice([1, 2, 3], 0) AS sliced_zero_no_len,
    slice([1, 2, 3], -2, 2) AS sliced_negative,
    slice([1, 2, 3], 1, 2) AS sliced_positive;
