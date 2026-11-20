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
-- @tags=runtime_filter,inner_join,null_safe
-- Test Objective:
-- 1. Validate NULL-safe equality join (`<=>`) keeps NULL-key matches on runtime-filter-enabled hash-join path.
-- 2. Prevent regressions where runtime filter incorrectly prunes probe NULL keys under null-safe join semantics.
-- Test Flow:
-- 1. Create/reset probe and build tables with nullable join keys.
-- 2. Insert deterministic rows where only NULL keys can match across sides.
-- 3. Execute INNER JOIN with `<=>` and assert NULL-key matches are preserved.
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_null_safe_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_null_safe_r;
CREATE TABLE ${case_db}.t_rf_inner_null_safe_l (
    id INT,
    k INT,
    v VARCHAR(20)
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_inner_null_safe_r (
    k INT,
    tag VARCHAR(20)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_rf_inner_null_safe_l VALUES
    ROW(1, NULL, 'ln1'),
    ROW(2, NULL, 'ln2'),
    ROW(3, 10, 'l10');

INSERT INTO ${case_db}.t_rf_inner_null_safe_r VALUES
    ROW(NULL, 'rn1'),
    ROW(20, 'r20');

-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT l.id, l.v, r.tag
FROM ${case_db}.t_rf_inner_null_safe_l l
INNER JOIN ${case_db}.t_rf_inner_null_safe_r r
  ON l.k  IS NOT DISTINCT FROM  r.k
ORDER BY l.id, l.v, r.tag;
