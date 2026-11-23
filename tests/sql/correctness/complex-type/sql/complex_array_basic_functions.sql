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
-- 1. Validate core ARRAY scalar functions.
-- 2. Prevent regressions in ARRAY length/sum/contains/position semantics.
-- Test Flow:
-- 1. Build constant ARRAY expressions.
-- 2. Evaluate core ARRAY functions.
-- 3. Assert scalar output.
SELECT
    cardinality([1, 2, 3]) AS len_a,
    contains([1, 2, 3], 2) AS has_2,
    ARRAY_POSITION([1, 2, 3], 3) AS pos_3;
