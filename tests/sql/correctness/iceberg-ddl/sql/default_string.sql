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
-- @tags=iceberg_ddl,default,string
-- Test Objective:
-- 1. Validate STRING DEFAULT with empty-string, unicode, comma, and newline-escape
--    literals. v3_default_primitive_types.sql covers only the trivial 'hi' literal.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) WITH (format_version = 3);
INSERT INTO ${case_db}.t VALUES ROW(1, 'alice'), ROW(2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN tag STRING DEFAULT 'default';
ALTER TABLE ${case_db}.t ADD COLUMN empty_v STRING DEFAULT '';
ALTER TABLE ${case_db}.t ADD COLUMN unicode_v STRING DEFAULT '日本語';
ALTER TABLE ${case_db}.t ADD COLUMN special_v STRING DEFAULT U&'a,b\000Ac';

SELECT id, name, tag, empty_v, unicode_v, special_v FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES ROW(3, 'charlie');
SELECT id, name, tag, empty_v, unicode_v, special_v FROM ${case_db}.t ORDER BY id;
