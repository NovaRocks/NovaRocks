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
-- Test Objective: Describe prepared write metadata without executing the writes.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.prepared_write_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items (id BIGINT) WITH (format_version = 3);

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
USE iceberg_rest_${suite_uuid0}.prepared_write_${uuid0};
EXECUTE IMMEDIATE 'INSERT INTO items SELECT CAST(? AS BIGINT)' USING 32;

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
PREPARE remove_value FROM DELETE FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items WHERE id IN (?, 37);

-- query 24
DESCRIBE INPUT remove_value;

-- query 25
DESCRIBE OUTPUT remove_value;

-- query 26
-- @skip_result_check=true
PREPARE remove_missing_column FROM DELETE FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items WHERE missing_id = ?;

-- query 27
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
DESCRIBE INPUT remove_missing_column;

-- query 28
-- @skip_result_check=true
PREPARE remove_missing_target FROM DELETE FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.missing_table WHERE id = ?;

-- query 29
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
DESCRIBE OUTPUT remove_missing_target;

-- query 30
SELECT id FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items ORDER BY id;

-- query 31
-- @skip_result_check=true
EXECUTE remove_value USING CAST(31 AS BIGINT);

-- query 32
SELECT id FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items ORDER BY id;

-- query 33
-- @skip_result_check=true
PREPARE update_value FROM UPDATE iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items SET id = ? WHERE id = ?;

-- query 34
DESCRIBE INPUT update_value;

-- query 35
DESCRIBE OUTPUT update_value;

-- query 36
-- @skip_result_check=true
PREPARE update_invalid FROM UPDATE iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items SET id = TRUE;

-- query 37
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
DESCRIBE OUTPUT update_invalid;

-- query 38
-- @skip_result_check=true
PREPARE merge_value FROM
MERGE INTO iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items AS t
USING (SELECT CAST(? AS BIGINT) AS id) AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET id = ?
WHEN NOT MATCHED THEN INSERT (id) VALUES (?);

-- query 39
DESCRIBE INPUT merge_value;

-- query 40
DESCRIBE OUTPUT merge_value;

-- query 41
-- @skip_result_check=true
PREPARE create_from_value FROM
CREATE TABLE iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.prepared_ctas AS SELECT CAST(? AS BIGINT) AS id;

-- query 42
DESCRIBE INPUT create_from_value;

-- query 43
DESCRIBE OUTPUT create_from_value;

-- query 44
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
SELECT id FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.prepared_ctas;

-- query 45
SELECT id FROM iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items ORDER BY id;

-- query 46
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.prepared_write_${uuid0}.items;

-- query 47
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.prepared_write_${uuid0};
