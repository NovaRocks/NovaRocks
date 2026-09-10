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
-- @tags=mv,iceberg,rest,minio,lnp-3d,accelerator,retain,cold-restart
-- This is the retained-Accelerator counterpart to mv_wipe_restart.sql.  The
-- runner restarts FE without deleting the MV Accelerator family, then reads
-- both SHOW metadata and the fully-qualified lake target.  A new refresh in
-- the replacement process must advance the same lake-backed MV normally.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG lnp3d_retain_${uuid0}
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
CREATE DATABASE lnp3d_retain_${uuid0}.ns_${uuid0};
CREATE TABLE lnp3d_retain_${uuid0}.ns_${uuid0}.orders (k1 INT NOT NULL, v1 BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO lnp3d_retain_${uuid0}.ns_${uuid0}.orders VALUES (1, 10), (2, 20);
SET CATALOG lnp3d_retain_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PRIMARY KEY (k1)
REFRESH ASYNC EVERY INTERVAL 1 HOUR
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v1 FROM orders;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 3
-- @retry_count=30
-- @retry_interval_ms=500
-- @skip_result_check=true
-- @result_contains=10
SELECT k1, v1 FROM orders_mv ORDER BY k1;

-- query 4
-- The runner-owned restart happens after the first publication is complete.
-- No wipe or rebuild directive is armed: the new FE must validate and retain
-- the persisted Accelerator projection against its exact lake source.
-- @restart_fe_after_step=true
-- @skip_result_check=true
SELECT 1;

-- query 5
-- SHOW is served from the replacement FE's readiness-filtered Accelerator.
-- @skip_result_check=true
-- @result_contains=orders_mv
-- @result_contains=iceberg
-- @result_contains=ASYNC_INTERVAL
SET CATALOG lnp3d_retain_${uuid0};
USE ns_${uuid0};
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 6
-- A fully-qualified target read avoids depending on the old session catalog
-- selection and proves the retained projection still resolves lake truth.
-- @retry_count=30
-- @retry_interval_ms=500
-- @skip_result_check=true
-- @result_contains=10
SELECT k1, v1 FROM lnp3d_retain_${uuid0}.ns_${uuid0}.orders_mv ORDER BY k1;

-- query 7
-- A new manual refresh belongs only to the new FE process.  It may publish a
-- fresh descriptor/snapshot over the retained lake-backed target.
-- @skip_result_check=true
INSERT INTO lnp3d_retain_${uuid0}.ns_${uuid0}.orders VALUES (3, 30);
SET CATALOG lnp3d_retain_${uuid0};
USE ns_${uuid0};
REFRESH MATERIALIZED VIEW orders_mv;

-- query 8
-- @retry_count=30
-- @retry_interval_ms=500
-- @skip_result_check=true
-- @result_contains=30
SELECT k1, v1 FROM lnp3d_retain_${uuid0}.ns_${uuid0}.orders_mv ORDER BY k1;

-- query 9
-- @skip_result_check=true
SET CATALOG lnp3d_retain_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE lnp3d_retain_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE lnp3d_retain_${uuid0}.ns_${uuid0};
DROP CATALOG lnp3d_retain_${uuid0};
