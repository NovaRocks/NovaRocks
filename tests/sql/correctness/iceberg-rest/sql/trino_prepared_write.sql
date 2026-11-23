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
-- Test Objective: Prepare write metadata independently of the actual INSERT.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.prepared_write_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items (id BIGINT);

-- query 3
-- @skip_result_check=true
PREPARE put_value FROM INSERT INTO iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items VALUES (?);

-- query 4
DESCRIBE INPUT put_value;

-- query 5
DESCRIBE OUTPUT put_value;

-- query 6
SELECT count(*) AS n FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items;

-- query 7
-- @skip_result_check=true
EXECUTE put_value USING CAST(31 AS BIGINT);

-- query 8
-- @skip_result_check=true
EXECUTE IMMEDIATE 'INSERT INTO iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items SELECT CAST(? AS BIGINT)' USING 32;

-- query 9
SELECT id FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items ORDER BY id;

-- query 10
-- @skip_result_check=true
DEALLOCATE PREPARE put_value;

-- query 11
-- @skip_result_check=true
PREPARE missing_target FROM INSERT INTO iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.missing_table VALUES (1);

-- query 12
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
DESCRIBE OUTPUT missing_target;

-- query 13
-- @skip_result_check=true
PREPARE missing_column FROM INSERT INTO iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items (unknown_id) VALUES (1);

-- query 14
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
DESCRIBE OUTPUT missing_column;

-- query 15
-- @skip_result_check=true
PREPARE invalid_assignment FROM INSERT INTO iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items SELECT TRUE;

-- query 16
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
DESCRIBE OUTPUT invalid_assignment;

-- query 17
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE VIEW iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.prepared_view AS SELECT ? AS id;

-- query 18
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
SELECT id FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.prepared_view;

-- query 19
-- @skip_result_check=true
CREATE VIEW iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.prepared_view AS SELECT 41 AS id;

-- query 20
SELECT id FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.prepared_view;

-- query 21
-- @skip_result_check=true
DROP VIEW iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.prepared_view;

-- query 22
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.prepared_mv AS SELECT ? AS id;

-- query 23
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items;

-- query 24
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.prepared_write_${uuid0};
