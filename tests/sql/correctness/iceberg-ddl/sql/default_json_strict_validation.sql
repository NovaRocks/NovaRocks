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

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name VARCHAR) WITH (format_version = 2);

-- query 2
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[query.execution.unsupported] Iceberg JSON writes are not supported yet
ALTER TABLE ${case_db}.t ADD COLUMN meta JSON DEFAULT 'not-a-json';

-- query 3
-- @expect_sql_code=query.execution.unsupported
-- @expect_error=column meta.element
ALTER TABLE ${case_db}.t ADD COLUMN meta ARRAY(JSON) DEFAULT 'not-an-array';

-- query 4
SELECT count(*) AS rejected_columns
FROM iceberg_ddl_cat_${suite_uuid0}.information_schema.columns
WHERE table_schema = '${case_db}' AND table_name = 't' AND column_name = 'meta';

-- query 5
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
