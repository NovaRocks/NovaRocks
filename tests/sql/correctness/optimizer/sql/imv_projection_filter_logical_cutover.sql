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
-- @tags=optimizer,iceberg,imv,projection_filter,logical_cutover
-- Test Objective:
-- Plan-shape golden for projection/filter IMV incremental refresh. This locks
-- the default EXPLAIN REFRESH output and the VERBOSE rendering for the same
-- refresh rewrite pipeline.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG imv_pf_cut_${uuid0}
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
CREATE DATABASE imv_pf_cut_${uuid0}.ns_${uuid0};
CREATE TABLE imv_pf_cut_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO imv_pf_cut_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10), (1, 20), (2, 40), (3, 5);
SET CATALOG imv_pf_cut_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pf_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT k1, v2 FROM orders WHERE v2 > 0;

-- query 2
-- Build the real previous snapshot required by incremental refresh planning.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW pf_mv_${uuid0};

-- query 3
-- Delta with both a delete and an insert before inspecting the refresh-time plan.
-- @skip_result_check=true
DELETE FROM imv_pf_cut_${uuid0}.ns_${uuid0}.orders WHERE k1 = 2;
INSERT INTO imv_pf_cut_${uuid0}.ns_${uuid0}.orders VALUES (4, 7);

-- query 4
-- Default EXPLAIN REFRESH should print the actual refresh logical plan.
-- @skip_result_check=true
-- @result_contains=LEFT OUTER JOIN
-- @result_contains=predicate: v2 > 0
-- @result_contains=__nova_base_row_id
-- @result_not_contains=source:
EXPLAIN REFRESH MATERIALIZED VIEW pf_mv_${uuid0};

-- query 5
-- VERBOSE should include the same refresh plan shape plus refresh source detail.
-- @skip_result_check=true
-- @result_contains=LEFT OUTER JOIN
-- @result_contains=predicate: v2 > 0
-- @result_contains=__nova_base_row_id
-- @result_contains=source: IcebergDeltaTable
-- @result_contains=source: IcebergMvTargetLocator
EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW pf_mv_${uuid0};

-- query 6
-- @cleanup=true
-- @skip_result_check=true
SET CATALOG imv_pf_cut_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.pf_mv_${uuid0};
DROP TABLE IF EXISTS imv_pf_cut_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE IF EXISTS imv_pf_cut_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS imv_pf_cut_${uuid0};
