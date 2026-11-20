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
-- @tags=project,expression
-- Test Objective:
-- 1. Validate arithmetic projection with nullable inputs.
-- 2. Validate explicit cast projection in the same operator pipeline.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows including NULL arithmetic operands.
-- 3. Project computed columns and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_project_arithmetic_cast;
CREATE TABLE ${case_db}.t_project_arithmetic_cast (
  id INT,
  a INT,
  b INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_project_arithmetic_cast VALUES
  ROW(1, 2, 3),
  ROW(2, 5, NULL),
  ROW(3, -4, 10);
SELECT
  id,
  a + IFNULL(b, 0) AS sum_ab,
  a * IFNULL(b, 1) AS mul_ab,
  CAST(a AS BIGINT) AS a_big
FROM ${case_db}.t_project_arithmetic_cast
ORDER BY id;
