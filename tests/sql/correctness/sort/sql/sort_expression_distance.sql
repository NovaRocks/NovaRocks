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
-- @tags=sort,expression
-- Test Objective:
-- 1. Validate ORDER BY computed expression outputs.
-- 2. Prevent regressions in expression materialization before sorting.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic numeric rows.
-- 3. Sort by ABS distance expression and assert output order.
DROP TABLE IF EXISTS ${case_db}.t_sort_expression_distance;
CREATE TABLE ${case_db}.t_sort_expression_distance (
  id INT,
  v INT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_sort_expression_distance VALUES
  (1, 7),
  (2, 12),
  (3, 9),
  (4, 15);
SELECT trc2_output.id AS id, trc2_output.v AS v,
  CAST(trc2_output.dist AS VARCHAR) AS dist
FROM (
SELECT id, v, ABS(v - 10) AS dist
FROM ${case_db}.t_sort_expression_distance
ORDER BY dist ASC, id ASC
) AS trc2_output
ORDER BY trc2_output.dist ASC, trc2_output.id ASC;
