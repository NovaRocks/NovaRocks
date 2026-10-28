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
-- @tags=iceberg,procedures,rewrite_position_delete_files,v3,dv
-- Repack a V3 Puffin deletion vector through Spark-style CALL and
-- verify the visible row set is unchanged.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG dv_proc_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_test_warehouse}/dv_proc_${uuid0}",
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
CREATE DATABASE dv_proc_${uuid0}.ns_${uuid0};
CREATE TABLE dv_proc_${uuid0}.ns_${uuid0}.orders (
  id INT,
  user_id INT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO dv_proc_${uuid0}.ns_${uuid0}.orders VALUES (1, 10, 100), (2, 20, 200);
INSERT INTO dv_proc_${uuid0}.ns_${uuid0}.orders VALUES (3, 30, 300), (4, 40, 400);
INSERT INTO dv_proc_${uuid0}.ns_${uuid0}.orders VALUES (5, 50, 500), (6, 60, 600);
INSERT INTO dv_proc_${uuid0}.ns_${uuid0}.orders VALUES (7, 70, 700), (8, 80, 800);
DELETE FROM dv_proc_${uuid0}.ns_${uuid0}.orders WHERE id = 2;
DELETE FROM dv_proc_${uuid0}.ns_${uuid0}.orders WHERE id = 4;
DELETE FROM dv_proc_${uuid0}.ns_${uuid0}.orders WHERE id = 6;
DELETE FROM dv_proc_${uuid0}.ns_${uuid0}.orders WHERE id = 8;

-- query 2
-- @db=dv_proc_${uuid0}.ns_${uuid0}
SELECT id, user_id, amount FROM orders ORDER BY id;

-- query 3
-- @db=dv_proc_${uuid0}.ns_${uuid0}
-- @be_log_contains=NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- @result_contains=rewritten_delete_files_count	added_delete_files_count	rewritten_bytes_count	added_bytes_count
-- @skip_result_check=true
CALL dv_proc_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders', options => map('rewrite-all', 'true'));

-- query 4
-- @db=dv_proc_${uuid0}.ns_${uuid0}
SELECT id, user_id, amount FROM orders ORDER BY id;

-- query 5
-- @db=dv_proc_${uuid0}.ns_${uuid0}
SELECT count(*) AS puffin_delete_files
  FROM orders$files
  WHERE content = 1 AND file_format = 'PUFFIN';

-- query 6
-- @skip_result_check=true
DROP TABLE dv_proc_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE dv_proc_${uuid0}.ns_${uuid0};
DROP CATALOG dv_proc_${uuid0};
