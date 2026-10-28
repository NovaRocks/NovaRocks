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
-- @tags=iceberg,procedures,spark
-- Test Spark-style Iceberg procedure CALL routing for supported maintenance
-- procedures and no-candidate position-delete rewrite.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG proc_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_test_warehouse}/proc_ice_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);
CREATE DATABASE proc_ice_${uuid0}.ns_${uuid0};
CREATE TABLE proc_ice_${uuid0}.ns_${uuid0}.orders (
  id INT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO proc_ice_${uuid0}.ns_${uuid0}.orders VALUES (1, 10);
INSERT INTO proc_ice_${uuid0}.ns_${uuid0}.orders VALUES (2, 20);

-- query 2
-- @db=proc_ice_${uuid0}.ns_${uuid0}
-- @be_log_not_contains=NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
CALL proc_ice_${uuid0}.system.rewrite_manifests(table => 'ns_${uuid0}.orders');

-- query 3
-- @db=proc_ice_${uuid0}.ns_${uuid0}
-- @be_log_not_contains=NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
CALL proc_ice_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders');

-- query 4
-- @db=proc_ice_${uuid0}.ns_${uuid0}
-- Scanned GC requires Provider safety proof even for an old cutoff.
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- @be_log_not_contains=NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
CALL proc_ice_${uuid0}.system.remove_orphan_files(table => 'ns_${uuid0}.orders', older_than => TIMESTAMP '2020-01-01 00:00:00');

-- query 5
-- @db=proc_ice_${uuid0}.ns_${uuid0}
SELECT id, amount FROM orders ORDER BY id;

-- query 6
-- @db=proc_ice_${uuid0}.ns_${uuid0}
-- @be_log_not_contains=NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
CALL proc_ice_${uuid0}.system.expire_snapshots(table => 'ns_${uuid0}.orders', retain_last => 1);

-- query 7
-- @db=proc_ice_${uuid0}.ns_${uuid0}
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- @skip_result_check=true
CALL proc_ice_${uuid0}.system.rewrite_data_files(table => 'ns_${uuid0}.orders', options => map('rewrite-all', 'true'));

-- query 8
-- @skip_result_check=true
DROP TABLE proc_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE proc_ice_${uuid0}.ns_${uuid0};
DROP CATALOG proc_ice_${uuid0};
