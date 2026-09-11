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
-- @tags=mv,iceberg,scheduler,manual
-- Test Objective:
-- Validate that REFRESH DEFERRED MANUAL is not auto-scheduled even when the
-- standalone MV refresh scheduler is enabled.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_mv_sched_manual_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_mv_sched_manual_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_mv_sched_manual_${uuid0}.ns_${uuid0};
CREATE TABLE ice_mv_sched_manual_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_mv_sched_manual_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_manual_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders;
INSERT INTO ice_mv_sched_manual_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (2, 20);

-- query 2
-- @skip_result_check=true
shell: sleep 2

-- query 3
SELECT k1, v2 FROM orders_manual_mv_${uuid0} ORDER BY k1;

-- query 4
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_manual_mv_${uuid0};

-- query 5
SELECT k1, v2 FROM orders_manual_mv_${uuid0} ORDER BY k1;

-- query 6
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_manual_mv_${uuid0};
DROP TABLE ice_mv_sched_manual_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_mv_sched_manual_${uuid0}.ns_${uuid0};
DROP CATALOG ice_mv_sched_manual_${uuid0};
