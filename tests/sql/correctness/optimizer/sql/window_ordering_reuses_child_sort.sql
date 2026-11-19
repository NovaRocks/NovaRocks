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

-- @tags=optimizer,window_ordering
-- Test Objective:
-- 1. Lock in physical ordering reuse for standalone window planning.
-- 2. A child Sort that already provides the required ORDER BY for a
--    non-partitioned window must not be wrapped in another equivalent Sort.
EXPLAIN VERBOSE
SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
FROM (
    SELECT id, v
    FROM (
        SELECT 2 AS id, 20 AS v
        UNION ALL SELECT 1 AS id, 10 AS v
        UNION ALL SELECT 3 AS id, 30 AS v
    ) t
    ORDER BY id
) s;
