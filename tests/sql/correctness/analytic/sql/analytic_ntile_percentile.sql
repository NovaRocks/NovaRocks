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
-- @tags=analytic,ntile,percent_rank,cume_dist
-- Test Objective:
-- 1. Validate NTILE/PERCENT_RANK/CUME_DIST outputs.
-- 2. Prevent regressions in distribution-based window functions.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered values.
-- 3. Compute distribution windows and assert ordered output.
CREATE TABLE ${case_db}.t_analytic_ntile_percentile (
    grp VARCHAR(10),
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_ntile_percentile VALUES
    ROW('A', 10),
    ROW('A', 20),
    ROW('A', 30),
    ROW('A', 40);

SELECT
    grp,
    v,
    NTILE(2) OVER (PARTITION BY grp ORDER BY v) AS nt,
    ROUND(PERCENT_RANK() OVER (PARTITION BY grp ORDER BY v), 6) AS pr,
    ROUND(CUME_DIST() OVER (PARTITION BY grp ORDER BY v), 6) AS cd
FROM ${case_db}.t_analytic_ntile_percentile
ORDER BY grp, v;
