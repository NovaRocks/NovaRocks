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
-- @tags=project,function,abs,largeint
-- Test Objective:
-- 1. Validate ABS on LARGEINT boundary values follows FE planned LARGEINT semantics.
-- 2. Prevent BIGINT-overflow style regression where ABS(min_bigint_literal) wraps to negative.
-- Test Flow:
-- 1. Execute deterministic projection-only query (no table dependency).
-- 2. Assert ABS result, comparison against BIGINT max, and +1 arithmetic on the same row.
SELECT
  CAST(ABS(-9223372036854775808) AS VARCHAR) AS abs_largeint_min,
  ABS(-9223372036854775808) > 9223372036854775807 AS gt_bigint_max,
  CAST(ABS(-9223372036854775808) + 1 AS VARCHAR) AS abs_largeint_min_plus_one;
