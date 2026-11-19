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
-- @tags=analytic,count,null
-- Test Objective:
-- 1. Validate COUNT(*) vs COUNT(expr) in window context.
-- 2. Prevent regressions in NULL-aware window counting.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with NULL values.
-- 3. Compute window counts and assert ordered output.
CREATE TABLE ${case_db}.t_analytic_count_window_nulls (
    grp VARCHAR(10),
    ts INT,
    v INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t_analytic_count_window_nulls VALUES
    ('A', 1, 10),
    ('A', 2, NULL),
    ('A', 3, 30),
    ('B', 1, NULL);

SELECT
    grp,
    ts,
    v,
    COUNT() OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt_all,
    COUNT(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt_v
FROM ${case_db}.t_analytic_count_window_nulls
ORDER BY grp, ts;
