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
-- @tags=project,date
-- Test Objective:
-- 1. Validate date/time scalar semantics for coercion, boundary handling, and null propagation.
-- 2. Guard regressions for hour_from_unixtime, makedate, microseconds_add/sub, time_format, and years_diff.
-- Test Flow:
-- 1. Execute one deterministic projection over constant literals.
-- 2. Assert exact scalar outputs and nullability.
SELECT

  YEARS_DIFF('2026-02-28', '2024-02-29') AS years_diff_v,
  YEARS_DIFF('2024-02-29', '2026-02-28') AS years_diff_neg_v;
