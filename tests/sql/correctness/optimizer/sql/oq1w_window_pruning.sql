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

-- @tags=optimizer,oq1w,window_pruning
-- Test Objective:
-- 1. An unused window result is pruned from the Window node.
-- 2. A Window node is removed when no window result is required by its parent.
DROP TABLE IF EXISTS ${case_db}.oq1w_sales;
CREATE TABLE ${case_db}.oq1w_sales (region VARCHAR(20), amount INT);
INSERT INTO ${case_db}.oq1w_sales VALUES
    ROW('A', 10), ROW('A', 20), ROW('B', 15), ROW('B', 30);
ANALYZE ${case_db}.oq1w_sales;

-- The outer query reads rn but never reads rk, so rank() must be pruned.
-- @explain_contains=WINDOW [row_number
-- @explain_not_contains=rank(
SELECT region, rn
FROM (
    SELECT
        region,
        row_number() OVER (PARTITION BY region ORDER BY amount) AS rn,
        rank() OVER (PARTITION BY region ORDER BY amount) AS rk
    FROM ${case_db}.oq1w_sales
) t
ORDER BY region, rn;

-- The outer query reads only region, so the entire Window node is removable.
-- @explain_not_contains=WINDOW [
SELECT region
FROM (
    SELECT
        region,
        row_number() OVER (PARTITION BY region ORDER BY amount) AS rn
    FROM ${case_db}.oq1w_sales
) t
ORDER BY region;
