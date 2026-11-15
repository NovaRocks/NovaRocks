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
-- @tags=runtime_filter,left_semi,null_key
-- Test Objective:
-- 1. Validate LEFT SEMI JOIN semantics for probe-side NULL keys.
-- 2. Prevent regressions where runtime-filter pruning changes NULL-key semi-join behavior.
-- Test Flow:
-- 1. Create/reset left/right tables.
-- 2. Insert deterministic rows including NULL keys.
-- 3. Execute LEFT SEMI JOIN and assert deterministic output.
DROP TABLE IF EXISTS ${case_db}.t_rf_left_semi_null_probe_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_left_semi_null_probe_r;
CREATE TABLE ${case_db}.t_rf_left_semi_null_probe_l (
    id INT,
    k INT
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.t_rf_left_semi_null_probe_r (
    k INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t_rf_left_semi_null_probe_l VALUES
    (1, 10),
    (2, NULL),
    (3, 30),
    (4, 40);

INSERT INTO ${case_db}.t_rf_left_semi_null_probe_r VALUES
    (10),
    (NULL),
    (50);

SELECT l.id, l.k
FROM ${case_db}.t_rf_left_semi_null_probe_l l
WHERE EXISTS (SELECT 1 FROM ${case_db}.t_rf_left_semi_null_probe_r r WHERE l.k = r.k)
ORDER BY l.id;
