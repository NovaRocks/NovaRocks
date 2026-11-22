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

-- @tags=iceberg_ddl,struct
-- Test Objective:
-- 1. Validate dropping then re-adding a STRUCT field with the same name but a different
--    type is accepted on Iceberg.
-- 2. Verify SELECT on the non-STRUCT column still works after the evolution sequence.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (
  c1 INT,
  c2 ROW(v2_1 INT)
);

-- query 2
-- @skip_result_check=true
ALTER TABLE ${case_db}.t ADD COLUMN c2.v2_2 VARCHAR;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.t DROP COLUMN c2.v2_2;

-- query 4
-- @skip_result_check=true
ALTER TABLE ${case_db}.t ADD COLUMN c2.v2_2 DATE;

-- query 5
-- @order_sensitive=true
SELECT c1 FROM ${case_db}.t ORDER BY c1;
