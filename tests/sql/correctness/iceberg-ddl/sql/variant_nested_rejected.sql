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

-- query 1
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
CREATE TABLE ${case_db}.variant_nested_struct (s ROW(v VARIANT)) WITH (format_version = 3);

-- query 2
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
CREATE TABLE ${case_db}.variant_nested_list (a ARRAY<VARIANT>) WITH (format_version = 3);

-- query 3
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
CREATE TABLE ${case_db}.variant_nested_map (m MAP<VARCHAR,VARIANT>) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.variant_nested_alter (id INT) WITH (format_version = 3);

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
ALTER TABLE ${case_db}.variant_nested_alter ADD COLUMN s ROW(v VARIANT);

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.variant_nested_alter;
