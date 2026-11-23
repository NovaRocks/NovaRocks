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
-- @tags=join,right_semi,residual
-- Test Objective:
-- 1. Validate RIGHT SEMI JOIN with residual predicate over non-key columns.
-- 2. Prevent regressions where build-side match marking ignores residual filtering.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows with both passing and failing residual conditions.
-- 3. Execute RIGHT SEMI JOIN with residual predicate and assert right-side output rows.
DROP TABLE IF EXISTS ${case_db}.t_join_right_semi_residual_l;
DROP TABLE IF EXISTS ${case_db}.t_join_right_semi_residual_r;
CREATE TABLE ${case_db}.t_join_right_semi_residual_l (
  id INT,
  score INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_right_semi_residual_r (
  id INT,
  threshold INT,
  tag VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_right_semi_residual_l VALUES
  ROW(1, 10),
  ROW(1, 1),
  ROW(2, 5),
  ROW(3, 7),
  ROW(NULL, 100);
INSERT INTO ${case_db}.t_join_right_semi_residual_r VALUES
  ROW(1, 5, 'r1_pass'),
  ROW(1, 20, 'r1_fail'),
  ROW(2, 3, 'r2_pass'),
  ROW(3, 9, 'r3_fail'),
  ROW(4, 1, 'r4_nomatch'),
  ROW(NULL, 1, 'rnull');
SELECT r.id, r.threshold, r.tag
FROM ${case_db}.t_join_right_semi_residual_r r
WHERE EXISTS (SELECT 1 FROM ${case_db}.t_join_right_semi_residual_l l WHERE l.id = r.id AND l.score > r.threshold)
ORDER BY r.id, r.threshold;
