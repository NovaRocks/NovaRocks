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
-- @tags=project,string,length_limit
-- Test Objective:
-- 1. Validate OLAP string max-length boundary behavior for LPAD/RPAD/CONCAT/CONCAT_WS.
-- 2. Prevent regressions where over-limit string builders return oversized values instead of NULL.
-- Test Flow:
-- 1. Evaluate boundary-sized outputs at 1048576 bytes.
-- 2. Evaluate over-limit outputs at 1048577 bytes.
-- 3. Assert boundary lengths and NULL markers in a deterministic single-row projection.
SELECT
  LENGTH(LPAD('x', 1048576, 'ab')) AS lpad_limit_len,
  LPAD('x', 1048577, 'ab') IS NULL AS lpad_over_limit_is_null,
  LENGTH(RPAD('x', 1048576, 'ab')) AS rpad_limit_len,
  RPAD('x', 1048577, 'ab') IS NULL AS rpad_over_limit_is_null,
  LENGTH(CONCAT(rpad('', 600000, 'a'), rpad('', 448576, 'b'))) AS concat_limit_len,
  CONCAT(rpad('', 600000, 'a'), rpad('', 448577, 'b')) IS NULL AS concat_over_limit_is_null,
  LENGTH(CONCAT_WS('-', rpad('', 600000, 'a'), rpad('', 448575, 'b'))) AS concat_ws_limit_len,
  CONCAT_WS('-', rpad('', 600000, 'a'), rpad('', 448576, 'b')) IS NULL AS concat_ws_over_limit_is_null;
