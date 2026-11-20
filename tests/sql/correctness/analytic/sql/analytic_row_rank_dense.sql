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
-- @tags=analytic,row_number,rank,dense_rank
-- Test Objective:
-- 1. Validate ROW_NUMBER/RANK/DENSE_RANK semantics with ties.
-- 2. Prevent regressions in ranking gap behavior under duplicate sort keys.
-- Test Flow:
-- 1. Create/reset analytic source table.
-- 2. Insert deterministic rows with tie scores.
-- 3. Compute ranking functions and assert ordered output.
CREATE TABLE ${case_db}.t_analytic_row_rank_dense (
    grp VARCHAR(10),
    id INT,
    score INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_row_rank_dense VALUES
    ROW('A', 1, 100),
    ROW('A', 2, 100),
    ROW('A', 3, 90),
    ROW('B', 4, 80),
    ROW('B', 5, 70);

SELECT
    grp,
    id,
    score,
    ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC, id) AS rn,
    RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS rnk,
    DENSE_RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS drnk
FROM ${case_db}.t_analytic_row_rank_dense
ORDER BY grp, rn;
