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

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name VARCHAR) WITH (format_version = 3);
INSERT INTO ${case_db}.t VALUES ROW(1, 'alice'), ROW(2, 'bob');

-- query 2
-- @skip_result_check=true
CREATE TABLE IF NOT EXISTS ${case_db}.t (payload JSON DEFAULT 'malformed');

-- query 3
-- @expect_trino_error=TABLE_ALREADY_EXISTS,USER_ERROR
CREATE TABLE ${case_db}.t (payload JSON);

-- query 4
-- @expect_sql_code=query.execution.unsupported
-- @expect_error=column payload.leaf.element
CREATE TABLE ${case_db}.json_create_rejected (payload ROW(leaf ARRAY(JSON)));

-- query 5
-- @expect_error=column `id` already exists
ALTER TABLE ${case_db}.t ADD COLUMN id JSON;

-- query 6
-- @expect_error=column `missing` does not exist
ALTER TABLE ${case_db}.t ALTER COLUMN missing SET DATA TYPE JSON;

-- query 7
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[query.execution.unsupported] Iceberg JSON writes are not supported yet
ALTER TABLE ${case_db}.t ALTER COLUMN name SET DATA TYPE JSON;

-- query 8
-- @skip_result_check=true
ALTER TABLE ${case_db}.t ADD COLUMN meta VARCHAR DEFAULT '{"k":"v"}';

-- query 9
SELECT id, name, meta FROM ${case_db}.t ORDER BY id;

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.t (id, name) VALUES ROW(3, 'charlie');
INSERT INTO ${case_db}.t (id, name, meta)
VALUES ROW(4, 'dana', json_format(JSON '{"w":1,"w":2}'));

-- query 11
SELECT id, name, meta FROM ${case_db}.t ORDER BY id;

-- query 12
SELECT count(*) AS rejected_targets
FROM iceberg_ddl_cat_${suite_uuid0}.information_schema.tables
WHERE table_schema = '${case_db}' AND table_name = 'json_create_rejected';

-- query 13
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.json_create_rejected;
DROP TABLE IF EXISTS ${case_db}.t;
