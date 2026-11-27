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
CREATE CATALOG pir8_mv_cut_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA pir8_mv_cut_${uuid0}.ns_${uuid0};
CREATE TABLE pir8_mv_cut_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
) WITH (format_version = 3);
INSERT INTO pir8_mv_cut_${uuid0}.ns_${uuid0}.orders VALUES
  ROW(1, 10),
  ROW(1, 20),
  ROW(2, 40),
  ROW(3, 5);
USE pir8_mv_cut_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW pir8_pf_mv_${uuid0} AS SELECT k1, v2 FROM orders WHERE v2 > 0;

-- query 2
-- Build the previous snapshot required by incremental refresh planning.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW pir8_pf_mv_${uuid0};

-- query 3
-- Create both delete and insert deltas before inspecting refresh-time shape.
-- @skip_result_check=true
DELETE FROM pir8_mv_cut_${uuid0}.ns_${uuid0}.orders WHERE k1 = 2;
INSERT INTO pir8_mv_cut_${uuid0}.ns_${uuid0}.orders VALUES ROW(4, 7);

-- query 4
-- @skip_result_check=true
-- @result_contains=LEFT OUTER JOIN
-- @result_contains=predicate: CAST(0 AS Int64) < v2
-- @result_contains=__nova_base_row_id
-- @result_contains=source: IcebergDeltaTable
-- @result_contains=source: IcebergMvTargetLocator
SET SESSION explain_detail = 'VERBOSE'
;
EXPLAIN  REFRESH MATERIALIZED VIEW pir8_pf_mv_${uuid0}
;
RESET SESSION explain_detail;

-- query 5
SELECT 'pir8_mv_refresh_shape_ok' AS status;

-- query 6
-- @cleanup=true
-- @skip_result_check=true
USE pir8_mv_cut_${uuid0}.ns_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.pir8_pf_mv_${uuid0};
DROP TABLE IF EXISTS pir8_mv_cut_${uuid0}.ns_${uuid0}.orders ;
DROP SCHEMA IF EXISTS pir8_mv_cut_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS pir8_mv_cut_${uuid0};
