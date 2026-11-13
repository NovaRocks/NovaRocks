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
-- @tags=mv,iceberg,ivm,storage_engine_iceberg,refresh_policy,scheduler
-- Test Objective:
-- 1. Validate Iceberg target MV refresh policy metadata is visible through SHOW MATERIALIZED VIEWS.
-- 2. Validate PAUSE/RESUME and ALTER SET REFRESH update user-facing scheduler state.

-- query 1
CREATE EXTERNAL CATALOG ice_ivm_policy_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.region" = "${oss_region}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);
CREATE DATABASE ice_ivm_policy_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_policy_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
-- Establish a real v3 source snapshot before testing MV policy metadata.
INSERT INTO ice_ivm_policy_${uuid0}.ns_${uuid0}.orders VALUES (1, 1);
USE ice_ivm_policy_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW orders_policy_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 1
REFRESH ASYNC EVERY INTERVAL 5 MINUTE
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders;

-- query 2
-- @result_contains=orders_policy_mv_
-- @result_rows_where=name=orders_policy_mv_${uuid0}
-- @result_rows_count=1
-- @result_rows_expect=refresh_policy=INTERVAL
-- @result_rows_expect=refresh_paused=false
-- @result_rows_expect=refresh_interval_ms=300000
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'ice_ivm_policy_${uuid0}';

-- query 3
ALTER MATERIALIZED VIEW orders_policy_mv_${uuid0} PAUSE REFRESH;

-- query 4
-- @result_contains=orders_policy_mv_
-- @result_contains=INTERVAL
-- @result_contains=true
-- @result_rows_where=name=orders_policy_mv_${uuid0}
-- @result_rows_count=1
-- @result_rows_expect=refresh_paused=true
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'ice_ivm_policy_${uuid0}';

-- query 5
ALTER MATERIALIZED VIEW orders_policy_mv_${uuid0} SET REFRESH ASYNC ON CHANGE;
ALTER MATERIALIZED VIEW orders_policy_mv_${uuid0} RESUME REFRESH;

-- query 6
-- @result_contains=orders_policy_mv_
-- @result_rows_where=name=orders_policy_mv_${uuid0}
-- @result_rows_count=1
-- @result_rows_expect=refresh_policy=ON_CHANGE
-- @result_rows_expect=refresh_paused=false
-- @result_rows_expect=refresh_interval_ms=NULL
-- @result_contains=false
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'ice_ivm_policy_${uuid0}';

-- query 7
-- @cleanup=true
DROP MATERIALIZED VIEW orders_policy_mv_${uuid0};
DROP TABLE ice_ivm_policy_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_policy_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_policy_${uuid0};
