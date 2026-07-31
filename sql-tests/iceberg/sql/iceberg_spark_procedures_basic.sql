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
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
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
CALL proc_ice_${uuid0}.system.rewrite_manifests(table => 'ns_${uuid0}.orders');

-- query 3
-- @db=proc_ice_${uuid0}.ns_${uuid0}
CALL proc_ice_${uuid0}.system.rewrite_position_delete_files(table => 'ns_${uuid0}.orders');

-- query 4
-- @db=proc_ice_${uuid0}.ns_${uuid0}
CALL proc_ice_${uuid0}.system.remove_orphan_files(table => 'ns_${uuid0}.orders', older_than => TIMESTAMP '2099-01-01 00:00:00');

-- query 5
-- @db=proc_ice_${uuid0}.ns_${uuid0}
CALL proc_ice_${uuid0}.system.expire_snapshots(table => 'ns_${uuid0}.orders', retain_last => 1);

-- query 6
-- @db=proc_ice_${uuid0}.ns_${uuid0}
-- @skip_result_check=true
CALL proc_ice_${uuid0}.system.rewrite_data_files(table => 'ns_${uuid0}.orders', options => map('rewrite-all', 'true'));

-- query 7
-- @skip_result_check=true
DROP TABLE proc_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE proc_ice_${uuid0}.ns_${uuid0};
DROP CATALOG proc_ice_${uuid0};
