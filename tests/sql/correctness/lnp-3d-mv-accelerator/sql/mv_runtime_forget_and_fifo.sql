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
-- @tags=mv,iceberg,rest,minio,lnp-3d,process-runtime,fifo,cold-restart
-- This case exercises one canonical target through ALTER, manual REFRESH and
-- DROP surfaces.  The runner then replaces FE and asserts that SHOW exposes
-- only Ready desired facts (not a persisted RUNNING/error/backoff runtime),
-- before the new process obtains readmission and performs a fresh refresh.
--
-- The SQL runner intentionally has no second concurrent client directive.
-- Contended FIFO ticket ordering is covered by the focused activity-gate
-- matrix; this native case proves that the production owner paths remain
-- serializable and restart-empty on the real REST/MinIO topology.

-- query 1
-- @skip_result_check=true
CREATE CATALOG lnp3d_runtime_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA lnp3d_runtime_${uuid0}.ns_${uuid0};
CREATE TABLE lnp3d_runtime_${uuid0}.ns_${uuid0}.orders (k1 INT NOT NULL, v1 BIGINT) WITH (format_version = 3);
INSERT INTO lnp3d_runtime_${uuid0}.ns_${uuid0}.orders VALUES ROW(1, 10), ROW(2, 20);
USE lnp3d_runtime_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv WITH (primary_key = ARRAY['k1'], refresh_policy = 'INTERVAL', refresh_interval = '1h') AS SELECT k1, v1 FROM orders;

-- query 2
-- ALTER and manual refresh must use the same canonical-target activity gate.
-- @skip_result_check=true
ALTER MATERIALIZED VIEW orders_mv SET PROPERTIES refresh_paused = true;
ALTER MATERIALIZED VIEW orders_mv SET PROPERTIES refresh_paused = false;
REFRESH MATERIALIZED VIEW orders_mv;

-- query 3
-- @retry_count=30
-- @retry_interval_ms=500
-- @skip_result_check=true
-- @result_contains=10
SELECT k1, v1 FROM orders_mv ORDER BY k1;

-- query 4
-- The publication has completed; restarting FE must forget its ProcessRuntime
-- entries rather than restore an old attempt, queue, error or retry deadline.
-- @restart_fe_after_step=true
-- @skip_result_check=true
SELECT 1;

-- query 5
-- The replacement FE readmits this nonaggregate view without a resume command.
-- Its desired scheduling facts must survive this readmission.
-- @retry_count=40
-- @retry_interval_ms=250
-- @skip_result_check=true
-- @result_contains=orders_mv
-- @result_rows_where=name=orders_mv
-- @result_rows_count=1
-- @result_rows_expect=manageability=MANAGEABLE
-- @result_rows_expect=refresh_policy=INTERVAL
-- @result_rows_expect=refresh_interval_ms=3600000
-- @result_rows_expect=refresh_paused=false
USE lnp3d_runtime_${uuid0}.ns_${uuid0};
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'lnp3d_runtime_${uuid0}' AND schema_name = 'ns_${uuid0}';

-- query 6
-- No old nonaggregate effect barrier survives automatic readmission.
-- @skip_result_check=true
-- @retry_count=40
-- @retry_interval_ms=250
-- @result_rows_where=Property=UnsettledEffects
-- @result_rows_count=1
-- @result_rows_expect=Value=0
-- @result_not_contains=AWAITING_EFFECT_SETTLEMENT
CALL novarocks_mv_management_status('lnp3d_runtime_${uuid0}', 'ns_${uuid0}', 'orders_mv');

-- query 7
-- @skip_result_check=true
-- @result_not_contains=ExplicitRebuildRequired
EXPLAIN REFRESH MATERIALIZED VIEW lnp3d_runtime_${uuid0}.ns_${uuid0}.orders_mv;

-- query 8
-- The resulting data proves the fresh attempt is not a restored old slot.
-- @skip_result_check=true
INSERT INTO lnp3d_runtime_${uuid0}.ns_${uuid0}.orders VALUES ROW(3, 30);
USE lnp3d_runtime_${uuid0}.ns_${uuid0};
REFRESH MATERIALIZED VIEW orders_mv;

-- query 9
-- @retry_count=30
-- @retry_interval_ms=500
-- @skip_result_check=true
-- @result_contains=30
SELECT k1, v1 FROM lnp3d_runtime_${uuid0}.ns_${uuid0}.orders_mv ORDER BY k1;

-- query 10
-- DROP is the final same-target mutation owner.  It must run after the fresh
-- refresh and leave no process-local gate/runtime state to carry elsewhere.
-- @cleanup=true
-- @skip_result_check=true
USE lnp3d_runtime_${uuid0}.ns_${uuid0};
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE lnp3d_runtime_${uuid0}.ns_${uuid0}.orders ;
DROP SCHEMA lnp3d_runtime_${uuid0}.ns_${uuid0};
DROP CATALOG lnp3d_runtime_${uuid0};
