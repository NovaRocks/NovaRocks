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
-- @tags=aggregate,covar,corr
-- Test Objective:
-- 1. Validate covariance/correlation aggregates.
-- 2. Prevent regressions in pairwise statistical aggregations.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic (x,y) pairs by group.
-- 3. Compute rounded covar/corr metrics and assert ordered output.
CREATE TABLE ${case_db}.t_agg_covar_corr (
    g INT,
    x INT,
    y INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_covar_corr VALUES
    ROW(1, 1, 2),
    ROW(1, 2, 4),
    ROW(1, 3, 6),
    ROW(2, 1, 9),
    ROW(2, 2, 7),
    ROW(2, 3, 5);

SELECT
    g,
    ROUND(COVAR_POP(x, y), 6) AS cov_pop,
    ROUND(COVAR_SAMP(x, y), 6) AS cov_samp,
    ROUND(CORR(x, y), 6) AS corr_xy
FROM ${case_db}.t_agg_covar_corr
GROUP BY g
ORDER BY g;
