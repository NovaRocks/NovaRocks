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
-- @tags=join,inner,residual
-- Test Objective:
-- 1. Validate INNER JOIN with additional residual predicate in ON clause.
-- 2. Prevent regressions where residual filters are ignored after key matching.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows with both passing and failing residual conditions.
-- 3. Execute INNER JOIN with residual predicate and assert output.
DROP TABLE IF EXISTS ${case_db}.t_join_inner_residual_l;
DROP TABLE IF EXISTS ${case_db}.t_join_inner_residual_r;
CREATE TABLE ${case_db}.t_join_inner_residual_l (
  id INT,
  lv INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_inner_residual_r (
  id INT,
  rv INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_inner_residual_l VALUES
  ROW(1, 5),
  ROW(2, 20),
  ROW(3, 7);
INSERT INTO ${case_db}.t_join_inner_residual_r VALUES
  ROW(1, 10),
  ROW(2, 15),
  ROW(3, 7),
  ROW(4, 100);
SELECT l.id, l.lv, r.rv
FROM ${case_db}.t_join_inner_residual_l l
INNER JOIN ${case_db}.t_join_inner_residual_r r
  ON l.id = r.id AND l.lv < r.rv
ORDER BY l.id;
