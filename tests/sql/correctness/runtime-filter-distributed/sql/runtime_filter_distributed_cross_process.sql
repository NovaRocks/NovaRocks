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
-- @tags=runtime_filter,cross_process,distributed
-- Test Objective:
-- 1. Validate distributed runtime filter transport with coord + multiple BE processes.
-- 2. Cover inner, multi-column, string, decimal, and empty build-side filters.
--    The left-semi string scenario is kept as a result-equivalence guard.
-- 3. Each scenario returns the same fingerprint with RuntimeFilterPushDown off and on.
-- 4. The suite is intended to run with --cluster-mode cross-process; standalone
--    SQL parsing strips StarRocks join hints, so plan assertions verify RF descriptors.

CREATE TABLE ${case_db}.rf_dist_probe (
    id INT NOT NULL,
    k INT,
    s VARCHAR(32),
    d DECIMAL(18, 2)
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_dist_build (
    k INT,
    s VARCHAR(32),
    d DECIMAL(18, 2),
    flag VARCHAR(8)
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_dist_probe VALUES
    (1, 10, 'aa', 1.10),
    (2, 20, 'bb', 2.20),
    (3, 30, 'cc', 3.30),
    (4, 40, 'dd', 4.40),
    (5, 50, 'ee', 5.50),
    (6, NULL, 'zz', NULL);

INSERT INTO ${case_db}.rf_dist_build VALUES
    (10, 'aa', 1.10, 'N'),
    (20, 'bb', 2.20, 'Y'),
    (30, 'cc', 3.30, 'Y'),
    (50, 'ee', 5.50, 'Y'),
    (60, 'ff', 6.60, 'Y');

ANALYZE TABLE ${case_db}.rf_dist_probe;
ANALYZE TABLE ${case_db}.rf_dist_build;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'inner_int' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
JOIN ${case_db}.rf_dist_build b ON p.k = b.k
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT 'inner_int' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
JOIN ${case_db}.rf_dist_build b ON p.k = b.k
WHERE b.flag = 'Y';

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'left_semi_string' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
WHERE EXISTS (SELECT 1 FROM ${case_db}.rf_dist_build b WHERE p.s = b.s AND b.flag = 'Y');

SET disable_optimizer_rules = '';
SELECT 'left_semi_string' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
WHERE EXISTS (SELECT 1 FROM ${case_db}.rf_dist_build b WHERE p.s = b.s AND b.flag = 'Y');

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'multi_column' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
JOIN ${case_db}.rf_dist_build b ON p.k = b.k AND p.s = b.s
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT 'multi_column' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
JOIN ${case_db}.rf_dist_build b ON p.k = b.k AND p.s = b.s
WHERE b.flag = 'Y';

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'decimal_key' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
JOIN ${case_db}.rf_dist_build b ON p.d = b.d
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT 'decimal_key' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
JOIN ${case_db}.rf_dist_build b ON p.d = b.d
WHERE b.flag = 'Y';

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'empty_build' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
JOIN ${case_db}.rf_dist_build b ON p.k = b.k
WHERE b.flag = 'NOPE';

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT 'empty_build' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_probe p
JOIN ${case_db}.rf_dist_build b ON p.k = b.k
WHERE b.flag = 'NOPE';
