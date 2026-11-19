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
-- @tags=project,case
-- Test Objective:
-- 1. Validate CASE-WHEN projection with nullable branches.
-- 2. Validate COALESCE normalization in the same projected row set.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert rows covering grade boundaries and NULL values.
-- 3. Project CASE and COALESCE expressions, then assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_project_case_coalesce;
CREATE TABLE ${case_db}.t_project_case_coalesce (
  id INT,
  score INT,
  note STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_project_case_coalesce VALUES
  ROW(1, 95, 'top'),
  ROW(2, 82, NULL),
  ROW(3, NULL, 'missing'),
  ROW(4, 60, NULL);
SELECT
  id,
  CASE
    WHEN score >= 90 THEN 'A'
    WHEN score >= 80 THEN 'B'
    WHEN score IS NULL THEN 'N/A'
    ELSE 'C'
  END AS grade,
  COALESCE(note, 'EMPTY') AS note_norm
FROM ${case_db}.t_project_case_coalesce
ORDER BY id;
