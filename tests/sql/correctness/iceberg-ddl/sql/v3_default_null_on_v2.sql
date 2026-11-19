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
-- Test Point: DEFAULT NULL is permitted on v2 Iceberg tables and produces NULL on omitted-column INSERTs (preserves pre-existing v2 behavior).
-- Method: CREATE v2 (default) table, ALTER ADD COLUMN b INT DEFAULT NULL, INSERT (a) VALUES (1), SELECT to confirm NULL.
-- Scope: D5 — DEFAULT NULL semantics never write metadata, work on v2.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_null_v2 ;
CREATE TABLE ${case_db}.t_v3_default_null_v2 (
  a INT
);
ALTER TABLE ${case_db}.t_v3_default_null_v2 ADD COLUMN b INT DEFAULT NULL;
INSERT INTO ${case_db}.t_v3_default_null_v2 (a) VALUES ROW(1);

-- query 2
SELECT a, b FROM ${case_db}.t_v3_default_null_v2 ORDER BY a;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_null_v2 ;
