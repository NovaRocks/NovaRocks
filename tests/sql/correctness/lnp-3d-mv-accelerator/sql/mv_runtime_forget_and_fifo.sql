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
-- before the new process performs a fresh manual refresh.
--
-- The SQL runner intentionally has no second concurrent client directive.
-- Contended FIFO ticket ordering is covered by the focused activity-gate
-- matrix; this native case proves that the production owner paths remain
-- serializable and restart-empty on the real REST/MinIO topology.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG lnp3d_runtime_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE lnp3d_runtime_${uuid0}.ns_${uuid0};
CREATE TABLE lnp3d_runtime_${uuid0}.ns_${uuid0}.orders (k1 INT NOT NULL, v1 BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO lnp3d_runtime_${uuid0}.ns_${uuid0}.orders VALUES (1, 10), (2, 20);
SET CATALOG lnp3d_runtime_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PRIMARY KEY (k1)
REFRESH ASYNC EVERY INTERVAL 1 HOUR
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v1 FROM orders;

-- query 2
-- ALTER and manual refresh must use the same canonical-target activity gate.
-- @skip_result_check=true
ALTER MATERIALIZED VIEW orders_mv PAUSE REFRESH;
ALTER MATERIALIZED VIEW orders_mv RESUME REFRESH;
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
-- SHOW must reflect source-backed desired facts, never durable runtime.
-- @skip_result_check=true
-- @result_contains=orders_mv
-- @result_contains=PENDING
-- @result_not_contains=RUNNING
-- @result_not_contains=BLOCKED_RECOVERY
-- @result_not_contains=BACKOFF
SET CATALOG lnp3d_runtime_${uuid0};
USE ns_${uuid0};
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 6
-- The replacement FE is allowed to begin a new manual publication.  The
-- resulting data proves its fresh attempt is not a restored old runtime slot.
-- @skip_result_check=true
INSERT INTO lnp3d_runtime_${uuid0}.ns_${uuid0}.orders VALUES (3, 30);
SET CATALOG lnp3d_runtime_${uuid0};
USE ns_${uuid0};
REFRESH MATERIALIZED VIEW orders_mv;

-- query 7
-- @retry_count=30
-- @retry_interval_ms=500
-- @skip_result_check=true
-- @result_contains=30
SELECT k1, v1 FROM lnp3d_runtime_${uuid0}.ns_${uuid0}.orders_mv ORDER BY k1;

-- query 8
-- DROP is the final same-target mutation owner.  It must run after the fresh
-- refresh and leave no process-local gate/runtime state to carry elsewhere.
-- @skip_result_check=true
SET CATALOG lnp3d_runtime_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE lnp3d_runtime_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE lnp3d_runtime_${uuid0}.ns_${uuid0};
DROP CATALOG lnp3d_runtime_${uuid0};
