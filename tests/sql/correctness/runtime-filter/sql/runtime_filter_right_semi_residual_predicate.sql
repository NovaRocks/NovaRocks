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
-- @tags=runtime_filter,right_semi,residual
-- Test Objective:
-- 1. Validate RIGHT SEMI JOIN behavior with additional residual predicate.
-- 2. Prevent regressions where runtime-filter application or match marking ignores residual checks.
-- Test Flow:
-- 1. Create/reset left/right tables.
-- 2. Insert deterministic rows with passing and failing residual conditions.
-- 3. Execute RIGHT SEMI JOIN and assert right-side output rows.
DROP TABLE IF EXISTS ${case_db}.t_rf_right_semi_residual_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_right_semi_residual_r;
CREATE TABLE ${case_db}.t_rf_right_semi_residual_l (
    k INT,
    score INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_right_semi_residual_r (
    k INT,
    threshold INT,
    tag VARCHAR(20)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_rf_right_semi_residual_l VALUES
    ROW(1, 10),
    ROW(1, 2),
    ROW(2, 7),
    ROW(3, 5);

INSERT INTO ${case_db}.t_rf_right_semi_residual_r VALUES
    ROW(1, 5, 'r1_pass'),
    ROW(1, 15, 'r1_fail'),
    ROW(2, 8, 'r2_fail'),
    ROW(3, 3, 'r3_pass'),
    ROW(4, 1, 'r4_nomatch'),
    ROW(NULL, 1, 'rnull');

-- @explain_contains=LEFT SEMI
-- @explain_contains=producer binding
SELECT r.k, r.threshold, r.tag
FROM ${case_db}.t_rf_right_semi_residual_r r
WHERE EXISTS (SELECT 1 FROM ${case_db}.t_rf_right_semi_residual_l l WHERE l.k = r.k AND l.score > r.threshold)
ORDER BY r.k, r.threshold;
