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
-- @tags=table_function,unnest,multi_array,left_join
-- Test Objective:
-- 1. Validate LEFT JOIN LATERAL UNNEST zip behavior for uneven multi-array lengths.
-- 2. Prevent regressions in NULL-padding semantics for shorter array arguments.
-- Test Flow:
-- 1. Build deterministic outer rows.
-- 2. Derive per-row multi-array arguments with uneven lengths and NULL arrays.
-- 3. LEFT JOIN LATERAL UNNEST and assert ordered padded output.
SELECT b.id, x, y
FROM (
    SELECT CAST(1 AS BIGINT) AS id
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(3 AS BIGINT)
) b
LEFT JOIN LATERAL UNNEST(
    IF(
        b.id = 1,
        [CAST(1 AS BIGINT), 2, 3],
        IF(b.id = 2, [CAST(4 AS BIGINT)], CAST(NULL AS ARRAY<BIGINT>))
    ),
    IF(
        b.id = 1,
        ['a'],
        IF(b.id = 2, ['b', 'c'], ['z'])
    )
) AS u(x, y) ON TRUE
ORDER BY b.id, IFNULL(x, -1), y;
