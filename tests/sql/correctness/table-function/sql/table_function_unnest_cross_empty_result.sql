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
-- @tags=table_function,unnest,empty_result
-- Test Objective:
-- 1. Validate CROSS JOIN LATERAL UNNEST behavior for empty and NULL arrays.
-- 2. Prevent regressions in empty-result handling while preserving output schema header.
-- Test Flow:
-- 1. Build deterministic rows with per-row array expressions.
-- 2. Expand using CROSS JOIN LATERAL UNNEST.
-- 3. Assert stable empty output (no rows).
SELECT b.id, x
FROM (
    SELECT CAST(1 AS BIGINT) AS id
    UNION ALL
    SELECT CAST(2 AS BIGINT)
) b
CROSS JOIN LATERAL UNNEST(
    IF(b.id = 1, [CAST(1 AS BIGINT)], CAST(NULL AS ARRAY<BIGINT>))
) AS u(x)
WHERE b.id = 2
ORDER BY b.id, x;
