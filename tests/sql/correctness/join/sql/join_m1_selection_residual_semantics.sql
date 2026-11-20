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
-- @tags=join,m1,residual,selection
-- Test Objective:
-- 1. Validate M1 selection-path residual predicates after hash-key matching.
-- 2. Preserve duplicate build-row matches that pass the residual predicate.
-- 3. Prevent null-key rows from leaking into normal equality join output.
-- Test Flow:
-- 1. Create/reset probe and build tables.
-- 2. Insert duplicate keys, residual-pass/residual-fail rows, and NULL keys.
-- 3. Execute INNER JOIN with a residual predicate and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.m1_probe;
DROP TABLE IF EXISTS ${case_db}.m1_build;
CREATE TABLE ${case_db}.m1_probe (
  k INT,
  v INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.m1_build (
  k INT,
  threshold_v INT,
  tag INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.m1_probe VALUES
  ROW(1, 10),
  ROW(1, 30),
  ROW(2, 20),
  ROW(3, 40),
  ROW(NULL, 50);
INSERT INTO ${case_db}.m1_build VALUES
  ROW(1, 15, 100),
  ROW(1, 25, 200),
  ROW(2, 10, 300),
  ROW(NULL, 0, 400);
SELECT p.k, p.v, b.threshold_v, b.tag
FROM ${case_db}.m1_probe p
JOIN ${case_db}.m1_build b
  ON p.k = b.k AND p.v > b.threshold_v
ORDER BY p.k, p.v, b.threshold_v, b.tag;
