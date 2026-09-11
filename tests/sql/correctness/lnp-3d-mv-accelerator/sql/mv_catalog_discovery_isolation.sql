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
-- @tags=mv,iceberg,rest,minio,lnp-3d,accelerator,catalog-incomplete
-- An incomplete namespace enumeration hides every retained MV projection in
-- that catalog. A later complete enumeration is authoritative and restores
-- it; the final cleanup makes this safe for the shared REST fixture.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG lnp3d_disc_${uuid0}
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
CREATE DATABASE lnp3d_disc_${uuid0}.ns_${uuid0};
CREATE TABLE lnp3d_disc_${uuid0}.ns_${uuid0}.z_source (k1 INT NOT NULL, v1 BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO lnp3d_disc_${uuid0}.ns_${uuid0}.z_source VALUES (1, 10);
SET CATALOG lnp3d_disc_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW a_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PRIMARY KEY (k1)
REFRESH ASYNC EVERY INTERVAL 1 HOUR
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v1 FROM z_source;
REFRESH MATERIALIZED VIEW a_mv;

-- query 2
-- @publication_catalog_fault=namespace-list,incomplete-discovery
-- @restart_fe_after_step=true
-- @skip_result_check=true
SELECT 1;

-- query 3
-- @skip_result_check=true
-- @result_not_contains=a_mv
SET CATALOG lnp3d_disc_${uuid0};
USE ns_${uuid0};
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 4
-- @skip_result_check=true
-- @restart_fe_after_step=true
SELECT 1;

-- query 5
-- @skip_result_check=true
-- @result_contains=a_mv
SET CATALOG lnp3d_disc_${uuid0};
USE ns_${uuid0};
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 6
-- @skip_result_check=true
DROP MATERIALIZED VIEW a_mv;
DROP TABLE lnp3d_disc_${uuid0}.ns_${uuid0}.z_source FORCE;
DROP DATABASE lnp3d_disc_${uuid0}.ns_${uuid0};
DROP CATALOG lnp3d_disc_${uuid0};
