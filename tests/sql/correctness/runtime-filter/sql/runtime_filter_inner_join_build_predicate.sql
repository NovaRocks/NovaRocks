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
-- @tags=runtime_filter,inner_join,predicate
-- Test Objective:
-- 1. Validate join results with additional build-side range predicate.
-- 2. Prevent regressions where runtime-filter pruning changes join semantics.
-- Test Flow:
-- 1. Create/reset probe/build tables.
-- 2. Insert deterministic rows.
-- 3. Apply join with build-side predicate and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_join_build_predicate_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_join_build_predicate_r;
CREATE TABLE ${case_db}.t_rf_inner_join_build_predicate_l (
    id INT,
    k INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_inner_join_build_predicate_r (
    k INT,
    score INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_rf_inner_join_build_predicate_l VALUES
    ROW(1, 1),
    ROW(2, 2),
    ROW(3, 3),
    ROW(4, 4);

INSERT INTO ${case_db}.t_rf_inner_join_build_predicate_r VALUES
    ROW(1, 10),
    ROW(2, 20),
    ROW(3, 30),
    ROW(4, 40);

SELECT l.id, l.k, r.score
FROM ${case_db}.t_rf_inner_join_build_predicate_l l
JOIN ${case_db}.t_rf_inner_join_build_predicate_r r
  ON l.k = r.k
WHERE r.score >= 25
ORDER BY l.id;
