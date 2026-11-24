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
-- @tags=project,bit,bitnot
-- Test Objective:
-- 1. Validate unary bitwise NOT operator semantics.
-- 2. Prevent regressions where BITWISE_NOT is lowered as binary arithmetic.
-- Test Flow:
-- 1. Evaluate BITWISE_NOT on negative/zero/positive BIGINT literals.
-- 2. Assert scalar output matches Trino semantics.
SELECT
  bitwise_not(CAST(-1 AS BIGINT)) AS n_neg1,
  bitwise_not(CAST(0 AS BIGINT)) AS n_zero,
  bitwise_not(CAST(1 AS BIGINT)) AS n_pos1,
  bitwise_not(CAST(1024 AS BIGINT)) AS n_1024;
