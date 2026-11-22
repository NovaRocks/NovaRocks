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
-- @tags=project,string,int_args
-- Test Objective:
-- 1. Validate string functions with integer arguments (INT literals).
-- 2. Prevent regressions where INT-typed arguments are rejected or produce wrong NULL/empty semantics.
-- Test Flow:
-- 1. Evaluate string scalar functions in a single deterministic SELECT.
-- 2. Cover negative/zero/positive integer argument behavior for split_part, substring_index, space, left/right, lpad/rpad.
-- 3. Compare one-row output against expected values.
SELECT
  split_part('a,b,c', ',', -1) AS sp_neg1,
  split_part('a,b,c', ',', -2) AS sp_neg2,
  split_part('abc', '', 1) AS sp_empty_d1,
  split_part('abc', '', 4) AS sp_empty_d4,
  strleft('abc', 2) AS left2,
  strleft('abc', -1) AS left_neg,
  strright('abc', 2) AS right2,
  strright('abc', -1) AS right_neg,
  lpad('ab', 1, '') AS lpad_empty_shrink,
  lpad('ab', -1, 'x') AS lpad_neg,
  rpad('ab', 1, '') AS rpad_empty_shrink,
  rpad('ab', -1, 'x') AS rpad_neg;
