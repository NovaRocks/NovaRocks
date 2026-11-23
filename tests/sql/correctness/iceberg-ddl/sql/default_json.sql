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
-- @tags=iceberg_ddl,default,json
-- Test Objective:
-- 1. Validate JSON DEFAULT — a type NOT covered by v3_default_primitive_types.sql.
-- 2. Positive companion to v3_default_complex_type_rejected.sql (negative).

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name VARCHAR) WITH (format_version = 3);
INSERT INTO ${case_db}.t VALUES ROW(1, 'alice'), ROW(2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN meta JSON DEFAULT '{"k":"v"}';

SELECT id, name, meta FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES ROW(3, 'charlie');
SELECT id, name, meta FROM ${case_db}.t ORDER BY id;
