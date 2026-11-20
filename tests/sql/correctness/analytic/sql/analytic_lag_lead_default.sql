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
-- @tags=analytic,lag,lead
-- Test Objective:
-- 1. Validate LAG/LEAD default-value behavior.
-- 2. Prevent regressions in offset window navigation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered rows.
-- 3. Compute LAG/LEAD and assert ordered output.
CREATE TABLE ${case_db}.t_analytic_lag_lead_default (
    grp VARCHAR(10),
    ts INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_lag_lead_default VALUES
    ROW('A', 1, 10),
    ROW('A', 2, 20),
    ROW('A', 3, 30),
    ROW('B', 1, 7),
    ROW('B', 2, NULL);

SELECT
    grp,
    ts,
    v,
    LAG(v, 1, -1) OVER (PARTITION BY grp ORDER BY ts) AS prev_v,
    LEAD(v, 1, -1) OVER (PARTITION BY grp ORDER BY ts) AS next_v
FROM ${case_db}.t_analytic_lag_lead_default
ORDER BY grp, ts;
