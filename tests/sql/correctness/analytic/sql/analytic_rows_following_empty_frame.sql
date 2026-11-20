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
-- @tags=analytic,rows_frame,empty_frame
-- Test Objective:
-- 1. Validate ROWS FOLLOWING window-frame behavior when tail rows produce empty frames.
-- 2. Prevent regressions where empty frames return non-NULL SUM or non-zero COUNT(expr).
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered rows with nullable values.
-- 3. Compute SUM/COUNT over FOLLOWING frame and assert ordered output.
CREATE TABLE ${case_db}.t_analytic_rows_following_empty_frame (
    grp VARCHAR(10),
    ts INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_rows_following_empty_frame VALUES
    ROW('A', 1, 10),
    ROW('A', 2, 20),
    ROW('A', 3, NULL),
    ROW('A', 4, 40);

SELECT
    grp,
    ts,
    v,
    SUM(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING
    ) AS sum_follow,
    COUNT(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING
    ) AS cnt_follow
FROM ${case_db}.t_analytic_rows_following_empty_frame
ORDER BY grp, ts;
