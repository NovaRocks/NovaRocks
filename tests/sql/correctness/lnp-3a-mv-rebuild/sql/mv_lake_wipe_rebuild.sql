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
-- @tags=mv,iceberg,rest,minio,lnp-3a,native-rebuild
-- This case is intentionally executed only as cross-process 1FE+3BE. It
-- proves that a published, paused, non-default async MV survives an
-- Accelerator wipe and later resumes with the same lake authority.
--
-- The wipe is the real one: this fixture's Accelerator is cleared and the
-- frontend is restarted, so everything below comes back through startup
-- rediscovery and the lake is the only source it can come from. That is also
-- why the paused state has to be read back rather than assumed -- it lives in
-- the view's own configuration document, and nothing local survives to
-- remember it.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG lnp3a_ice_${uuid0}
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
CREATE DATABASE lnp3a_ice_${uuid0}.ns_${uuid0};
CREATE TABLE lnp3a_ice_${uuid0}.ns_${uuid0}.orders (
  k1 INT NOT NULL,
  v1 BIGINT
)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO lnp3a_ice_${uuid0}.ns_${uuid0}.orders VALUES (1, 10), (2, 20);
SET CATALOG lnp3a_ice_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PRIMARY KEY (k1)
REFRESH ASYNC EVERY INTERVAL 1 SECOND
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v1 FROM orders;
REFRESH MATERIALIZED VIEW orders_mv;

-- query 2
-- @retry_count=30
-- @retry_interval_ms=500
SELECT k1, v1 FROM orders_mv ORDER BY k1;

-- query 3
-- @skip_result_check=true
ALTER MATERIALIZED VIEW orders_mv PAUSE REFRESH;

-- query 4
-- Clear this fixture's Accelerator and restart the frontend.
-- @imv_accelerator_wipe_restart=orders_mv,catalog=lnp3a_ice_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 5
-- The published result is still there, read from the lake by the new process.
-- @retry_count=40
-- @retry_interval_ms=250
SELECT k1, v1 FROM lnp3a_ice_${uuid0}.ns_${uuid0}.orders_mv ORDER BY k1;

-- query 6
-- The restart replaced the session, so name the catalog again.
-- @skip_result_check=true
SET CATALOG lnp3a_ice_${uuid0};
USE ns_${uuid0};

-- query 7
-- @skip_result_check=true
INSERT INTO lnp3a_ice_${uuid0}.ns_${uuid0}.orders VALUES (3, 30);

-- query 8
-- @skip_result_check=true
shell: sleep 2

-- query 9
-- The view is paused, and the restart did not un-pause it: the new row is not
-- picked up even though the schedule would otherwise have fired twice by now.
SELECT k1, v1 FROM orders_mv ORDER BY k1;

-- query 10
-- Writing to the view is this process's to do only after the restart barrier
-- is retired, which is the same route any restarted target takes.
-- @mv_resume_management=orders_mv,catalog=lnp3a_ice_${uuid0},database=ns_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 11
-- @skip_result_check=true
SET CATALOG lnp3a_ice_${uuid0};
USE ns_${uuid0};
ALTER MATERIALIZED VIEW orders_mv RESUME REFRESH;

-- query 12
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE lnp3a_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE lnp3a_ice_${uuid0}.ns_${uuid0};
DROP CATALOG lnp3a_ice_${uuid0};
