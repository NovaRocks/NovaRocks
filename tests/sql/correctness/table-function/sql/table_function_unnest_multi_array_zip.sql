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
-- @tags=table_function,unnest,multi_array
-- Test Objective:
-- 1. Validate multi-argument UNNEST zip semantics.
-- 2. Prevent regressions in positional alignment across arrays.
-- Test Flow:
-- 1. Build two same-length arrays.
-- 2. Expand both via LATERAL UNNEST.
-- 3. Assert ordered zipped rows.
SELECT i, s
FROM (
    SELECT [1, 2, 3] AS ai, ['x', 'y', 'z'] AS as1
) t
CROSS JOIN  UNNEST(t.ai, t.as1) AS u(i, s)
ORDER BY i;
