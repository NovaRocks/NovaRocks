-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under
-- the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may
-- obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
-- BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language
-- governing permissions and limitations under the License.

-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,rest,minio,lnp-3d,accelerator,package-isolation
-- A corrupt document package quarantines only its exact target. The state table keeps
-- a diagnostic row with UNAVAILABLE; a fault-free sweep restores it.

-- query 1
-- @skip_result_check=true
CREATE CATALOG lnp3d_pkg_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA lnp3d_pkg_${uuid0}.ns_${uuid0};
CREATE TABLE lnp3d_pkg_${uuid0}.ns_${uuid0}.z_source (k1 INT NOT NULL, v1 BIGINT) WITH (format_version = 3);
INSERT INTO lnp3d_pkg_${uuid0}.ns_${uuid0}.z_source VALUES ROW(1, 10);
USE lnp3d_pkg_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW a_mv WITH (primary_key = ARRAY['k1'], refresh_policy = 'INTERVAL', refresh_interval = '1h') AS SELECT k1, v1 FROM z_source;
REFRESH MATERIALIZED VIEW a_mv;

-- query 2
-- Bind the fault to this case's exact MV package; startup request order
-- does not determine which table reaches the proxy first.
-- @publication_catalog_fault=table-load,corrupt-package
-- @publication_catalog_fault_target=namespace=ns_${uuid0},table=a_mv
-- @publication_catalog_fault_timing=after-frontend-exit
-- @restart_fe_after_step=true
-- @skip_result_check=true
SELECT 1;

-- query 3
-- @skip_result_check=true
-- @result_contains=a_mv
-- @result_contains=UNAVAILABLE
USE lnp3d_pkg_${uuid0}.ns_${uuid0};
SELECT name, manageability, manageability_reason FROM system.runtime.materialized_view_states WHERE catalog_name = 'lnp3d_pkg_${uuid0}' AND schema_name = 'ns_${uuid0}';

-- query 4
-- @restart_fe_after_step=true
-- @skip_result_check=true
SELECT 1;

-- query 5
-- @skip_result_check=true
-- @result_contains=a_mv
USE lnp3d_pkg_${uuid0}.ns_${uuid0};
SELECT name, manageability, manageability_reason FROM system.runtime.materialized_view_states WHERE catalog_name = 'lnp3d_pkg_${uuid0}' AND schema_name = 'ns_${uuid0}';

-- query 6
-- @retry_count=40
-- @retry_interval_ms=250
-- @skip_result_check=true
-- @result_contains=AWAITING_EFFECT_SETTLEMENT
CALL novarocks_mv_management_status('lnp3d_pkg_${uuid0}', 'ns_${uuid0}', 'a_mv');

-- query 7
-- @mv_resume_management=a_mv,catalog=lnp3d_pkg_${uuid0},database=ns_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
USE lnp3d_pkg_${uuid0}.ns_${uuid0};
DROP MATERIALIZED VIEW a_mv;
DROP TABLE lnp3d_pkg_${uuid0}.ns_${uuid0}.z_source ;
DROP SCHEMA lnp3d_pkg_${uuid0}.ns_${uuid0};
DROP CATALOG lnp3d_pkg_${uuid0};
