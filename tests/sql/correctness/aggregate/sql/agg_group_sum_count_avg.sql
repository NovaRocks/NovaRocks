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
-- @tags=aggregate,basic
-- Test Objective:
-- 1. Validate grouped COUNT/SUM/AVG semantics with nullable inputs.
-- 2. Prevent regressions where NULL handling changes aggregate outputs.
-- Test Flow:
-- 1. Create/reset aggregate source table.
-- 2. Insert deterministic rows across groups with NULLs.
-- 3. Aggregate by group and assert ordered output.
CREATE TABLE ${case_db}.t_agg_group_sum_count_avg (
    g INT,
    v INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t_agg_group_sum_count_avg VALUES
    (1, 10),
    (1, 20),
    (1, NULL),
    (2, 5),
    (2, 15),
    (3, NULL);

SELECT
    g,
    COUNT() AS c_all,
    COUNT(v) AS c_not_null,
    SUM(v) AS s_v,
    AVG(v) AS avg_v
FROM ${case_db}.t_agg_group_sum_count_avg
GROUP BY g
ORDER BY g;
