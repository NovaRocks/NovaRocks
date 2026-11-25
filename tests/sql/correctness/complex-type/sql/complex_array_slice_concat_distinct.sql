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
-- 1. Validate ARRAY_SLICE/ARRAY_CONCAT/ARRAY_DISTINCT behavior.
-- 2. Prevent regressions in array-shape transformation functions.
-- Test Flow:
-- 1. Build deterministic array expressions.
-- 2. Apply slice/concat/distinct.
-- 3. Assert scalar output arrays.
SELECT
    slice([1, 2, 3, 4], 2, 2) AS sliced,
    concat([1, 2], [3, NULL]) AS concated,
    ARRAY_DISTINCT([1, 2, 2, NULL, 1, NULL]) AS deduped;

-- query 2
-- This result fits the query host. Trino 484 charges its object representation.
SELECT cardinality(repeat(rpad(CAST(x AS VARCHAR), 1020, 'x'), 4096)) AS repeat_count
FROM TABLE(sequence(1, 1)) AS input(x);
