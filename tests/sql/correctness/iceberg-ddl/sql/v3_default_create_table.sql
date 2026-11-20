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
-- Test Point: Iceberg v3 CREATE TABLE with non-NULL DEFAULT persists initial/write-default and INSERT (col list) materializes write-default.
-- Method: CREATE v3 table with `b INT DEFAULT 5`, INSERT with explicit column list omitting b, INSERT including b, then SELECT both rows.
-- Scope: CREATE TABLE DEFAULT capture, write-default materialization on VALUES INSERT, read-back of physically materialized values.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_ct ;
CREATE TABLE ${case_db}.t_v3_default_ct (
  a INT,
  b INT DEFAULT 5
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_v3_default_ct (a) VALUES ROW(1);
INSERT INTO ${case_db}.t_v3_default_ct (a, b) VALUES ROW(2, 7);

-- query 2
SELECT a, b FROM ${case_db}.t_v3_default_ct ORDER BY a;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_ct ;
