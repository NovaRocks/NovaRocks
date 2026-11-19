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
-- @tags=runtime_filter,complete_only,cross_process,distributed
-- Test Objective:
-- 1. Validate complete-only RF correctness under cross-process 1FE+3BE execution.
-- 2. Result is identical with RF enabled and disabled.

CREATE TABLE ${case_db}.rf_co_dist_probe (
    id INT NOT NULL,
    k INT
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_co_dist_build (
    k INT,
    flag VARCHAR(8)
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_co_dist_probe VALUES
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40),
    (5, 50);

INSERT INTO ${case_db}.rf_co_dist_build VALUES
    (20, 'Y'),
    (30, 'Y'),
    (50, 'Y'),
    (60, 'Y');

ANALYZE TABLE ${case_db}.rf_co_dist_probe;
ANALYZE TABLE ${case_db}.rf_co_dist_build;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT COUNT() AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_co_dist_probe p
JOIN ${case_db}.rf_co_dist_build b ON p.k = b.k
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT COUNT() AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_co_dist_probe p
JOIN ${case_db}.rf_co_dist_build b ON p.k = b.k
WHERE b.flag = 'Y';
