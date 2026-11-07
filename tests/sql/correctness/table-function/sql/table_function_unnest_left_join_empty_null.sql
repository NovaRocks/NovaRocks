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
-- @tags=table_function,unnest,left_join
-- Test Objective:
-- 1. Validate LEFT JOIN LATERAL UNNEST for empty/NULL arrays.
-- 2. Prevent regressions where unmatched outer rows are dropped.
-- Test Flow:
-- 1. Build deterministic outer rows.
-- 2. Produce non-empty, empty, and NULL arrays per row.
-- 3. LEFT JOIN LATERAL UNNEST and assert ordered output.
SELECT b.id, x
FROM (
    SELECT CAST(1 AS BIGINT) AS id
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(3 AS BIGINT)
) b
LEFT JOIN LATERAL UNNEST(
    IF(b.id = 1, [CAST(10 AS BIGINT), 20], IF(b.id = 2, [], CAST(NULL AS ARRAY<BIGINT>)))
) AS u(x) ON TRUE
ORDER BY b.id, x;
