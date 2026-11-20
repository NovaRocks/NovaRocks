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
-- @tags=analytic,avg,rows_frame
-- Test Objective:
-- 1. Validate AVG over sliding ROWS frame.
-- 2. Prevent regressions in bounded frame aggregation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered rows.
-- 3. Compute AVG with 1-preceding/1-following frame and assert output.
CREATE TABLE ${case_db}.t_analytic_avg_rows_sliding (
    grp VARCHAR(10),
    ts INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_avg_rows_sliding VALUES
    ROW('A', 1, 10),
    ROW('A', 2, 20),
    ROW('A', 3, 40),
    ROW('A', 4, 80);

SELECT
    grp,
    ts,
    v,
    AVG(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS avg_win
FROM ${case_db}.t_analytic_avg_rows_sliding
ORDER BY grp, ts;
