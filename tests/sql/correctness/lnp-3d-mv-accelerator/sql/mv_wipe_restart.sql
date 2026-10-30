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
-- @tags=mv,iceberg,rest,minio,lnp-3d,accelerator,cold-restart
-- The wipe directive clears only the current StateStore MV Accelerator family
-- and immediately restarts the runner-owned FE. The subsequent read can only
-- succeed if startup rediscovers D/L/P/C from REST Catalog + MinIO.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG lnp3d_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "${oss_region}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);
CREATE DATABASE lnp3d_ice_${uuid0}.ns_${uuid0};
CREATE TABLE lnp3d_ice_${uuid0}.ns_${uuid0}.orders (k1 INT NOT NULL, v1 BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO lnp3d_ice_${uuid0}.ns_${uuid0}.orders VALUES (1, 10), (2, 20);
SET CATALOG lnp3d_ice_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PRIMARY KEY (k1)
REFRESH ASYNC EVERY INTERVAL 1 HOUR
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v1 FROM orders;
REFRESH MATERIALIZED VIEW orders_mv;

-- query 2
-- @retry_count=30
-- @retry_interval_ms=500
-- @skip_result_check=true
-- @result_contains=10
SELECT k1, v1 FROM orders_mv ORDER BY k1;

-- query 3
-- @imv_accelerator_wipe_restart=orders_mv,catalog=lnp3d_ice_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 4
-- @retry_count=30
-- @retry_interval_ms=500
-- @skip_result_check=true
-- @result_contains=10
SELECT k1, v1 FROM lnp3d_ice_${uuid0}.ns_${uuid0}.orders_mv ORDER BY k1;

-- query 5
-- The wiped Accelerator is rebuildable; the new process still needs a real
-- readmission declaration before its DROP can have an effect.
-- @retry_count=40
-- @retry_interval_ms=250
-- @skip_result_check=true
-- @result_contains=AWAITING_EFFECT_SETTLEMENT
CALL novarocks_mv_management_status('lnp3d_ice_${uuid0}', 'ns_${uuid0}', 'orders_mv');

-- query 6
-- @mv_resume_management=orders_mv,catalog=lnp3d_ice_${uuid0},database=ns_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 7
-- @cleanup=true
-- @skip_result_check=true
SET CATALOG lnp3d_ice_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE lnp3d_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE lnp3d_ice_${uuid0}.ns_${uuid0};
DROP CATALOG lnp3d_ice_${uuid0};
