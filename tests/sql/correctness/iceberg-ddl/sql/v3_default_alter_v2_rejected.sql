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
-- Test Point: ALTER ADD COLUMN with non-NULL DEFAULT on an existing v2 table is hard-rejected.
-- Method: CREATE v2 (default) table, ALTER ADD COLUMN b INT DEFAULT 5; expect format-version 3 error.
-- Scope: D5 — v2 ALTER ADD COLUMN gate.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_alter_v2 ;
CREATE TABLE ${case_db}.t_v3_default_alter_v2 (
  a INT
);

-- query 2
-- @expect_error=format-version 3
ALTER TABLE ${case_db}.t_v3_default_alter_v2 ADD COLUMN b INT DEFAULT 5;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_alter_v2 ;
