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
-- @tags=runtime_filter,inner_join,null_safe,composite_key
-- Test Objective:
-- 1. Validate mixed composite-key semantics: `<=>` on key1 and `=` on key2.
-- 2. Prevent regressions where mixed null-safe/non-null-safe keys produce false matches or false pruning.
-- Test Flow:
-- 1. Create/reset probe and build tables with composite join keys.
-- 2. Insert deterministic rows including NULLs on key1 and key2.
-- 3. Execute INNER JOIN with mixed predicates and assert expected matched rows only.
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_mixed_null_safe_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_inner_mixed_null_safe_r;
CREATE TABLE ${case_db}.t_rf_inner_mixed_null_safe_l (
    id INT,
    k1 VARCHAR(20),
    k2 INT,
    v VARCHAR(20)
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_inner_mixed_null_safe_r (
    k1 VARCHAR(20),
    k2 INT,
    tag VARCHAR(20)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_rf_inner_mixed_null_safe_l VALUES
    ROW(1, NULL, 1, 'ln1'),
    ROW(2, NULL, 2, 'ln2'),
    ROW(3, 'a', 1, 'la1'),
    ROW(4, 'a', 2, 'la2'),
    ROW(5, 'b', 1, 'lb1');

INSERT INTO ${case_db}.t_rf_inner_mixed_null_safe_r VALUES
    ROW(NULL, 1, 'rn1'),
    ROW(NULL, 2, 'rn2'),
    ROW('a', 1, 'ra1'),
    ROW('a', 3, 'ra3'),
    ROW('b', NULL, 'rbn'),
    ROW('b', 1, 'rb1');

-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT l.id, l.v, r.tag
FROM ${case_db}.t_rf_inner_mixed_null_safe_l l
INNER JOIN ${case_db}.t_rf_inner_mixed_null_safe_r r
  ON l.k1  IS NOT DISTINCT FROM  r.k1
 AND l.k2 = r.k2
ORDER BY l.id, l.v, r.tag;
