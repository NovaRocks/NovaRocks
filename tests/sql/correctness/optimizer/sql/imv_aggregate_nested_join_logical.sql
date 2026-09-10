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
-- @tags=optimizer,iceberg,imv,aggregate,join,nested,logical_cutover
-- Test Objective:
-- Validate optimizer-visible plan evidence for aggregate IMV incremental
-- refresh over a nested join after the refresh path has cut over to IMV
-- rewrite execution. The case mutates every base table after the initial
-- refresh so the nested join delta rewrite exposes multiple UNION and
-- IcebergVersionTable branches.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG imv_anj_cut_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/imv_anj_cut_${uuid0}",
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
CREATE DATABASE imv_anj_cut_${uuid0}.ns_${uuid0};
CREATE TABLE imv_anj_cut_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE imv_anj_cut_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region_id BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE imv_anj_cut_${uuid0}.ns_${uuid0}.dim2 (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO imv_anj_cut_${uuid0}.ns_${uuid0}.dim2 VALUES
  (100, 'east'),
  (200, 'west');
INSERT INTO imv_anj_cut_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 100),
  (20, 200);
INSERT INTO imv_anj_cut_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, 200),
  (3, 20, 50);
SET CATALOG imv_anj_cut_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_nested_join_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d2.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
JOIN dim2 AS d2 ON d.region_id = d2.id
GROUP BY d2.region;

-- query 2
-- Build the real previous snapshots required by incremental refresh planning.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_nested_join_mv_${uuid0};

-- query 3
-- Mutate each base table so nested join refresh planning expands both inner
-- and outer join-delta branches over pinned version scans.
-- @skip_result_check=true
INSERT INTO imv_anj_cut_${uuid0}.ns_${uuid0}.dim2 VALUES
  (300, 'north');
INSERT INTO imv_anj_cut_${uuid0}.ns_${uuid0}.dim VALUES
  (30, 300);
INSERT INTO imv_anj_cut_${uuid0}.ns_${uuid0}.fact VALUES
  (4, 20, 80),
  (5, 30, 40);

-- query 4
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=UNION
-- @explain_contains=IcebergVersionTable
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergMvTargetState
EXPLAIN REFRESH MATERIALIZED VIEW agg_nested_join_mv_${uuid0};

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_nested_join_mv_${uuid0};
DROP TABLE imv_anj_cut_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE imv_anj_cut_${uuid0}.ns_${uuid0}.dim FORCE;
DROP TABLE imv_anj_cut_${uuid0}.ns_${uuid0}.dim2 FORCE;
DROP DATABASE imv_anj_cut_${uuid0}.ns_${uuid0};
DROP CATALOG imv_anj_cut_${uuid0};
