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
-- @tags=runtime_filter,inner_join
-- Test Objective:
-- 1. Validate inner-join semantics under selective build-side filter.
-- 2. Prevent regressions where runtime filter drops valid probe rows.
-- Test Flow:
-- 1. Create/reset probe/build tables.
-- 2. Insert deterministic keys with partial overlap.
-- 3. Join with build-side predicate and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_join_selective_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_join_selective_r;
CREATE TABLE ${case_db}.t_rf_inner_join_selective_l (
    id INT,
    k INT,
    v VARCHAR(20)
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_inner_join_selective_r (
    k INT,
    tag VARCHAR(20)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_rf_inner_join_selective_l VALUES
    ROW(1, 10, 'l1'),
    ROW(2, 20, 'l2'),
    ROW(3, 30, 'l3'),
    ROW(4, 40, 'l4');

INSERT INTO ${case_db}.t_rf_inner_join_selective_r VALUES
    ROW(10, 'drop'),
    ROW(20, 'keep'),
    ROW(30, 'keep'),
    ROW(50, 'keep');

SELECT l.id, l.k, r.tag
FROM ${case_db}.t_rf_inner_join_selective_l l
JOIN ${case_db}.t_rf_inner_join_selective_r r
  ON l.k = r.k
WHERE r.tag = 'keep'
ORDER BY l.id;
