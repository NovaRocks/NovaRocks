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

-- @tags=iceberg_ddl,default,json,validation
-- Test Objective:
-- 1. Validate JSON DEFAULT with an invalid JSON literal is rejected at ALTER time.
-- 2. Complement to default_json.sql (positive case for JSON DEFAULT).

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) WITH (format_version = 3);

-- @expect_error=invalid JSON DEFAULT literal
ALTER TABLE ${case_db}.t ADD COLUMN meta JSON DEFAULT 'not-a-json';
