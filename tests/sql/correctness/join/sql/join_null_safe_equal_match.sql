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
-- @tags=join,inner,null_safe
-- Test Objective:
-- 1. Validate NULL-safe equality join (`<=>`) matches NULL join keys in hash join paths.
-- 2. Prevent regressions where probe/build NULL keys are skipped as unmatched.
-- Test Flow:
-- 1. Create/reset left and right tables with nullable join keys.
-- 2. Insert deterministic rows including duplicated NULL keys on both sides.
-- 3. Execute INNER JOIN with `<=>` and assert NULL-key multiplicity is preserved.
DROP TABLE IF EXISTS ${case_db}.t_join_null_safe_equal_l;
DROP TABLE IF EXISTS ${case_db}.t_join_null_safe_equal_r;
CREATE TABLE ${case_db}.t_join_null_safe_equal_l (
  k INT,
  v VARCHAR
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_null_safe_equal_r (
  k INT,
  v VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_null_safe_equal_l VALUES
  ROW(1, 'l1'),
  ROW(NULL, 'ln1'),
  ROW(NULL, 'ln2');
INSERT INTO ${case_db}.t_join_null_safe_equal_r VALUES
  ROW(NULL, 'rn1'),
  ROW(NULL, 'rn2'),
  ROW(1, 'r1'),
  ROW(2, 'r2');
SELECT l.v AS lv, r.v AS rv
FROM ${case_db}.t_join_null_safe_equal_l l
INNER JOIN ${case_db}.t_join_null_safe_equal_r r
  ON l.k  IS NOT DISTINCT FROM  r.k
ORDER BY lv, rv;
