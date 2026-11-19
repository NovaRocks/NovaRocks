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
-- @tags=runtime_filter,inner_join,null_key
-- Test Objective:
-- 1. Validate INNER JOIN key semantics with NULL keys under runtime-filter-enabled path.
-- 2. Prevent regressions where NULL-key rows are incorrectly matched or retained.
-- Test Flow:
-- 1. Create/reset probe/build tables.
-- 2. Insert deterministic rows including NULL keys on both sides.
-- 3. Execute INNER JOIN and assert only non-NULL key matches remain.
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_null_key_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_null_key_r;
CREATE TABLE ${case_db}.t_rf_inner_null_key_l (
    id INT,
    k INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_inner_null_key_r (
    k INT,
    tag VARCHAR(20)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_rf_inner_null_key_l VALUES
    ROW(1, 10),
    ROW(2, 20),
    ROW(3, NULL),
    ROW(4, 30),
    ROW(5, NULL);

INSERT INTO ${case_db}.t_rf_inner_null_key_r VALUES
    ROW(10, 'r10'),
    ROW(NULL, 'rnull'),
    ROW(30, 'r30');

-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT l.id, l.k, r.tag
FROM ${case_db}.t_rf_inner_null_key_l l
INNER JOIN ${case_db}.t_rf_inner_null_key_r r
  ON l.k = r.k
ORDER BY l.id;
