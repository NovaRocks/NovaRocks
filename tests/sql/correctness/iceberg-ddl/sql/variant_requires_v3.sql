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

-- Variant definitions are refused independently of the target format version.
-- The legacy case name is retained so the existing targeted selection stays valid.
-- query 1
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
CREATE TABLE ${case_db}.variant_default_v2 (id INT,v VARIANT);

-- query 2
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
CREATE TABLE ${case_db}.variant_explicit_v2 (id INT,v VARIANT) WITH (format_version = 2);

-- query 3
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
CREATE TABLE ${case_db}.variant_explicit_v3 (id INT,v VARIANT) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.variant_alter_v2 (id INT) WITH (format_version = 2);
CREATE TABLE ${case_db}.variant_alter_v3 (id INT,s ROW(x INT)) WITH (format_version = 3);

-- query 5
-- An existing target makes IF NOT EXISTS finish before new-definition validation.
-- @skip_result_check=true
CREATE TABLE IF NOT EXISTS ${case_db}.variant_alter_v3 (id INT,v VARIANT) WITH (format_version = 3);

-- query 6
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
ALTER TABLE ${case_db}.variant_alter_v2 ADD COLUMN v VARIANT;

-- query 7
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
ALTER TABLE ${case_db}.variant_alter_v3 ADD COLUMN v VARIANT;

-- query 8
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
ALTER TABLE ${case_db}.variant_alter_v3 ADD COLUMN s.v VARIANT;

-- query 9
-- SQL type admission refuses Variant before Iceberg type evolution.
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
ALTER TABLE ${case_db}.variant_alter_v3 ALTER COLUMN id SET DATA TYPE VARIANT;

-- query 10
-- SQL type admission refuses Variant before partition-source validation.
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.admit.unsupported_statement] SQL type variant is not supported
CREATE TABLE ${case_db}.variant_partition_rejected (id INT,v VARIANT) WITH (partitioning = ARRAY['v'], format_version = 3);

-- query 11
-- Ordinary schema changes still work after the refusals.
-- @skip_result_check=true
ALTER TABLE ${case_db}.variant_alter_v2 ADD COLUMN extra INT;
ALTER TABLE ${case_db}.variant_alter_v3 ADD COLUMN extra INT;
DROP TABLE ${case_db}.variant_alter_v2;
DROP TABLE ${case_db}.variant_alter_v3;
