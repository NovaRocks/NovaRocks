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
-- @tags=pir8,optimizer,iceberg,mv,refresh,plan_shape
-- PIR-8 M3 guard: EXPLAIN REFRESH must still expose the IMV change-stream
-- rewrite shape after planner physical vocabulary is separated from optimizer
-- execution semantics.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG pir8_mv_cut_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/pir8_mv_cut_${uuid0}",
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
CREATE DATABASE pir8_mv_cut_${uuid0}.ns_${uuid0};
CREATE TABLE pir8_mv_cut_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO pir8_mv_cut_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (1, 20),
  (2, 40),
  (3, 5);
SET CATALOG pir8_mv_cut_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pir8_pf_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT k1, v2 FROM orders WHERE v2 > 0;

-- query 2
-- Build the previous snapshot required by incremental refresh planning.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW pir8_pf_mv_${uuid0};

-- query 3
-- Create both delete and insert deltas before inspecting refresh-time shape.
-- @skip_result_check=true
DELETE FROM pir8_mv_cut_${uuid0}.ns_${uuid0}.orders WHERE k1 = 2;
INSERT INTO pir8_mv_cut_${uuid0}.ns_${uuid0}.orders VALUES (4, 7);

-- query 4
-- @skip_result_check=true
-- @result_contains=LEFT OUTER JOIN
-- @result_contains=predicate: v2 > 0
-- @result_contains=__nova_base_row_id
-- @result_contains=source: IcebergDeltaTable
-- @result_contains=source: IcebergMvTargetLocator
EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW pir8_pf_mv_${uuid0};

-- query 5
SELECT 'pir8_mv_refresh_shape_ok' AS status;

-- query 6
-- @skip_result_check=true
DROP MATERIALIZED VIEW pir8_pf_mv_${uuid0};
DROP TABLE pir8_mv_cut_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE pir8_mv_cut_${uuid0}.ns_${uuid0};
DROP CATALOG pir8_mv_cut_${uuid0};
