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
-- @tags=join,not_in,correlated,null_aware
-- Test Objective:
-- 1. Validate correlated NOT IN semantics when FE lowers to NULL_AWARE_LEFT_ANTI_JOIN with an extra join conjunct.
-- 2. Prevent regressions where execution rejects or mis-evaluates plans containing both eq_join_conjunct and other_join_conjuncts.
-- Test Flow:
-- 1. Create/reset probe and build tables with nullable group/key columns.
-- 2. Insert deterministic rows covering empty-subquery, NULL-in-subquery, and direct-match cases.
-- 3. Execute correlated NOT IN and assert deterministic row ids.
DROP TABLE IF EXISTS ${case_db}.t_naaj_corr_not_in_l;
DROP TABLE IF EXISTS ${case_db}.t_naaj_corr_not_in_r;
CREATE TABLE ${case_db}.t_naaj_corr_not_in_l (
    id INT,
    g INT,
    k INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_naaj_corr_not_in_r (
    g INT,
    k INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_naaj_corr_not_in_l VALUES
    ROW(1, 1, 2),
    ROW(2, NULL, 0),
    ROW(3, NULL, 1),
    ROW(4, 3, 1),
    ROW(5, 3, -1),
    ROW(6, NULL, 1),
    ROW(7, NULL, NULL),
    ROW(8, 3, 2),
    ROW(9, 2, 2);

INSERT INTO ${case_db}.t_naaj_corr_not_in_r VALUES
    ROW(NULL, 1),
    ROW(1, 1),
    ROW(NULL, 2),
    ROW(3, 2),
    ROW(2, NULL);

SELECT l.id
FROM ${case_db}.t_naaj_corr_not_in_l l
WHERE l.k NOT IN (
    SELECT r.k
    FROM ${case_db}.t_naaj_corr_not_in_r r
    WHERE r.g = l.g
)
ORDER BY l.id;
