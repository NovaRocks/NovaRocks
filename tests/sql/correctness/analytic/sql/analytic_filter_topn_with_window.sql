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
-- @tags=analytic,row_number,filter
-- Test Objective:
-- 1. Validate filtering over window outputs via subquery.
-- 2. Prevent regressions in window + outer predicate integration.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows per partition.
-- 3. Apply ROW_NUMBER in subquery and keep top-2 rows per partition.
CREATE TABLE ${case_db}.t_analytic_filter_topn_with_window (
    grp VARCHAR(10),
    id INT,
    score INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_filter_topn_with_window VALUES
    ROW('A', 1, 70),
    ROW('A', 2, 90),
    ROW('A', 3, 80),
    ROW('B', 4, 60),
    ROW('B', 5, 50),
    ROW('B', 6, 40);

SELECT grp, id, score, rn
FROM (
    SELECT
        grp,
        id,
        score,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC, id) AS rn
    FROM ${case_db}.t_analytic_filter_topn_with_window
) t
WHERE rn <= 2
ORDER BY grp, rn;
