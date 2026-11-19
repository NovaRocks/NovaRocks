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
-- @tags=project,string
-- Test Objective:
-- 1. Validate string projection functions (CONCAT/COALESCE/UPPER).
-- 2. Prevent regressions in nullable string expression evaluation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert rows with mixed-case and NULL string fields.
-- 3. Project normalized key expression and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_project_string_functions;
CREATE TABLE ${case_db}.t_project_string_functions (
  id INT,
  first_name STRING,
  last_name STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_project_string_functions VALUES
  ROW(1, 'alice', 'smith'),
  ROW(2, 'Bob', 'Lee'),
  ROW(3, NULL, 'Z');
SELECT
  id,
  UPPER(CONCAT(COALESCE(first_name, ''), '_', COALESCE(last_name, ''))) AS full_key
FROM ${case_db}.t_project_string_functions
ORDER BY id;
