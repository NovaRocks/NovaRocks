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
-- @tags=join,float,null_semantics
-- Test Objective:
-- 1. Validate non-finite floating-point expressions are normalized to NULL.
-- 2. Prevent regressions where NaN/Infinity keys are matched by equality hash join.
-- Test Flow:
-- 1. Build deterministic left/right inline datasets containing non-finite and finite keys.
-- 2. Assert non-finite keys become NULL on both sides.
-- 3. Assert INNER JOIN on equality only matches the finite key.
WITH
left_keys AS (
  SELECT CAST('NaN' AS DOUBLE) AS k
  UNION ALL SELECT CAST('Infinity' AS DOUBLE)
  UNION ALL SELECT SQRT(-DECIMAL '1.0')
  UNION ALL SELECT 1.5
),
right_keys AS (
  SELECT CAST('NaN' AS DOUBLE) AS k
  UNION ALL SELECT LOG(-DECIMAL '1.0')
  UNION ALL SELECT ACOS(2.0)
  UNION ALL SELECT 1.5
)
SELECT
  (SELECT COUNT() FROM left_keys WHERE k IS NULL) AS left_null_keys,
  (SELECT COUNT() FROM right_keys WHERE k IS NULL) AS right_null_keys,
  (SELECT COUNT() FROM left_keys l INNER JOIN right_keys r ON l.k = r.k) AS join_matches;
