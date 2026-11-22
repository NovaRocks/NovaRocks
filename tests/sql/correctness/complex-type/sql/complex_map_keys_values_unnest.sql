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
-- @tags=complex,map,unnest
-- Test Objective:
-- 1. Validate MAP_KEYS/MAP_VALUES feeding UNNEST.
-- 2. Prevent regressions in zipped expansion of key/value arrays.
-- Test Flow:
-- 1. Build key/value arrays from MAP.
-- 2. Expand rows via CROSS JOIN LATERAL UNNEST.
-- 3. Assert ordered key/value rows.
SELECT k, v
FROM (
    SELECT
        MAP_KEYS(MAP(ARRAY['a', 'b'], ARRAY[1, 2])) AS ks,
        MAP_VALUES(MAP(ARRAY['a', 'b'], ARRAY[1, 2])) AS vs
) t
CROSS JOIN  UNNEST(t.ks, t.vs) AS u(k, v)
ORDER BY k;
