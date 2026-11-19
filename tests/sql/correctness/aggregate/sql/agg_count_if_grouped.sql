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
-- @tags=aggregate,count_if
-- Test Objective:
-- 1. Validate grouped count_if semantics with deterministic ordering.
-- 2. Prevent regressions in partial/final merge handling for count_if.
-- Test Flow:
-- 1. Create/reset grouped source table.
-- 2. Insert rows that exercise true/false/null predicate outcomes.
-- 3. Group by key and assert ordered count_if outputs.
CREATE TABLE ${case_db}.t_agg_count_if_grouped (
    k INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_count_if_grouped VALUES
    ROW(1, 10),
    ROW(2, 20),
    ROW(3, NULL),
    ROW(4, 30),
    ROW(5, 15);

SELECT
    MOD(k, 2) AS g,
    count_if(v > 15) AS c_gt_15
FROM ${case_db}.t_agg_count_if_grouped
GROUP BY g
ORDER BY g;
