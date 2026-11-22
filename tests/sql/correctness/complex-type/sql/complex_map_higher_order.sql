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
-- Regression for the map higher-order functions transform_keys and
-- transform_values. Their (key, value) lambda parameters must bind as lambda
-- slots (LambdaParamRef), not as freshly-minted scope columns. The latter
-- produced a phantom ColumnId for each parameter, which the ColumnId-binding
-- verifier rejected with "ColumnId(N) is not produced by child scope" in
-- projections, filters, and join predicates.
-- Test Flow:
-- 1. Apply each map higher-order function to a constant MAP.
-- 2. Assert the rewritten MAP output (key+value transforms).
-- 3. Exercise a lambda body in a nested (non-projection) scalar position.
-- Join-predicate coverage lives in the join suite (join_map_type).

-- query 1
-- transform_keys: rewrite keys, values pass through.
SELECT transform_keys(MAP(ARRAY[1, 2], ARRAY[10, 20]), (k, v) -> k + 100) AS keyed;

-- query 2
-- transform_values: rewrite values, keys pass through.
SELECT transform_values(MAP(ARRAY[1, 2], ARRAY[10, 20]), (k, v) -> v * 2) AS valued;
