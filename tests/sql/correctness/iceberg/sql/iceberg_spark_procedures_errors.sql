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

-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg,procedures,errors
-- Reject unknown and retired CALL names while preserving ordinary v2 deletes.

-- query 1
-- @skip_result_check=true
CREATE CATALOG proc_err_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'hadoop', "iceberg.catalog.warehouse" = '${iceberg_test_warehouse}/proc_err_${uuid0}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA proc_err_${uuid0}.ns_${uuid0};
CREATE TABLE proc_err_${uuid0}.ns_${uuid0}.orders (
  id INT
) WITH (format_version = 3);
CREATE TABLE proc_err_${uuid0}.ns_${uuid0}.v2_orders (
  id INT
) WITH (format_version = 2);

-- query 2
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
INSERT INTO proc_err_${uuid0}.ns_${uuid0}.v2_orders VALUES ROW(1), ROW(2), ROW(3);

-- query 3
-- @skip_result_check=true
DELETE FROM proc_err_${uuid0}.ns_${uuid0}.v2_orders WHERE id = 2;

-- query 4
-- @expect_trino_error=PROCEDURE_NOT_FOUND,USER_ERROR
-- @expect_error=Procedure not registered: system.rewrite_manifests
CALL proc_err_${uuid0}.admin.rewrite_manifests("table" => 'ns_${uuid0}.orders');

-- query 5
-- @expect_trino_error=PROCEDURE_NOT_FOUND,USER_ERROR
-- @expect_error=Procedure not registered: system.unknown_proc
CALL proc_err_${uuid0}.system.unknown_proc("table" => 'ns_${uuid0}.orders');

-- query 6
-- @expect_trino_error=PROCEDURE_NOT_FOUND,USER_ERROR
-- @expect_error=Procedure not registered: system.rewrite_position_delete_files
CALL proc_err_${uuid0}.system.rewrite_position_delete_files("table" => 'ns_${uuid0}.orders', "where" => 'id = 1');

-- query 7
-- @expect_trino_error=PROCEDURE_NOT_FOUND,USER_ERROR
-- @expect_error=Procedure not registered: system.rewrite_position_delete_files
CALL proc_err_${uuid0}.system.rewrite_position_delete_files("table" => 'ns_${uuid0}.orders', options => MAP(ARRAY['partial-progress.enabled'], ARRAY['true']));

-- query 8
-- @expect_trino_error=PROCEDURE_NOT_FOUND,USER_ERROR
-- @expect_error=Procedure not registered: system.rewrite_position_delete_files
CALL proc_err_${uuid0}.system.rewrite_position_delete_files("table" => 'ns_${uuid0}.orders', options => MAP(ARRAY['target-file-size-bytes'], ARRAY['134217728']));

-- query 9
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- @expect_trino_error=PROCEDURE_NOT_FOUND,USER_ERROR
-- @expect_error=Procedure not registered: system.rewrite_position_delete_files
CALL proc_err_${uuid0}.system.rewrite_position_delete_files("table" => 'ns_${uuid0}.v2_orders', options => MAP(ARRAY['rewrite-all'], ARRAY['true']));

-- query 10
-- @db=proc_err_${uuid0}.ns_${uuid0}
SELECT COUNT() AS n FROM v2_orders;

-- query 11
-- @skip_result_check=true
DROP TABLE proc_err_${uuid0}.ns_${uuid0}.v2_orders ;
DROP TABLE proc_err_${uuid0}.ns_${uuid0}.orders ;
DROP SCHEMA proc_err_${uuid0}.ns_${uuid0};
DROP CATALOG proc_err_${uuid0};
