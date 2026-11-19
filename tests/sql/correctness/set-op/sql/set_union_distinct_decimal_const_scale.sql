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
-- @tags=set_op,union,decimal,const,scale
-- Test Objective:
-- 1. Validate UNION DISTINCT handles mixed decimal literal scales in const rows.
-- 2. Prevent regressions where const row decimals are materialized with mismatched precision.
-- Test Flow:
-- 1. Build two constant decimal expressions with different inferred precision.
-- 2. Apply UNION (DISTINCT) to force set-op grouping on the unified output slot.
-- 3. Assert that only one distinct row remains.
SELECT COUNT() AS dedup_count
FROM (
    SELECT -(1.0) * 0.0 AS v
    UNION
    SELECT 0.0 AS v
) t;
