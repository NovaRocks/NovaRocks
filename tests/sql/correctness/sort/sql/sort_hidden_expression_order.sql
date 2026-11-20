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
-- @tags=sort,expression,projection
-- Test Objective:
-- 1. Validate ORDER BY on expressions that are not projected in output.
-- 2. Prevent regressions where hidden sort expressions are mis-evaluated with NULL handling.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows covering NULL and non-NULL operands.
-- 3. Sort by computed distance expression and assert ordered projected ids.
DROP TABLE IF EXISTS ${case_db}.t_sort_hidden_expression_order;
CREATE TABLE ${case_db}.t_sort_hidden_expression_order (
  id INT,
  base INT,
  delta INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_sort_hidden_expression_order VALUES
  ROW(1, 10, 1),
  ROW(2, 8, NULL),
  ROW(3, 5, 5),
  ROW(4, NULL, 2),
  ROW(5, 7, 9);
SELECT id
FROM ${case_db}.t_sort_hidden_expression_order
ORDER BY ABS(COALESCE(base, 0) - COALESCE(delta, 0)) ASC, id DESC
LIMIT 5;
