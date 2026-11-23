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
-- @tags=mv,iceberg,ivm,aggregate,min_max,detail_state,partitioned
-- Test Point: MIN/MAX detail-state IMV refresh combined with the
-- affected-partition planner (PR #145). Deltas that touch only a subset of
-- partitions should re-derive MIN/MAX only for those partitions; untouched
-- partitions remain bit-identical across REFRESH.
-- Method: 3 partitions ('SF', 'NY', 'LA') x 5 rows each. PARTITION BY region
-- MV with MIN(amount), MAX(amount), COUNT(*). DELETE the current MIN row in
-- 'SF' and 'NY' partitions only -- 'LA' must remain unchanged.
-- Scope: Iceberg target MV, partitioned aggregate, MIN/MAX detail-state,
-- partition-pruned touched-group lookup + boundary DELETE.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_minmax_p_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_minmax_p_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders (
  region VARCHAR,
  amount BIGINT
) WITH (format_version = 3);
USE ice_ivm_minmax_p_${uuid0}.ns_${uuid0};

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders VALUES
  ROW('SF', 10),
  ROW('SF', 20),
  ROW('SF', 30),
  ROW('SF', 40),
  ROW('SF', 50),
  ROW('NY', 11),
  ROW('NY', 22),
  ROW('NY', 33),
  ROW('NY', 44),
  ROW('NY', 55),
  ROW('LA', 7),
  ROW('LA', 17),
  ROW('LA', 27),
  ROW('LA', 37),
  ROW('LA', 47);
CREATE MATERIALIZED VIEW minmax_p_mv_${uuid0} WITH (partitioning = ARRAY['region']) AS SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders
GROUP BY region;
REFRESH MATERIALIZED VIEW minmax_p_mv_${uuid0};

-- query 3
SELECT region, mn, mx, c
FROM minmax_p_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
DELETE FROM ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders
WHERE region = 'SF' AND amount = 10;
DELETE FROM ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders
WHERE region = 'NY' AND amount = 11;
REFRESH MATERIALIZED VIEW minmax_p_mv_${uuid0};

-- query 5
SELECT region, mn, mx, c
FROM minmax_p_mv_${uuid0}
ORDER BY region;

-- query 6
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 7
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW minmax_p_mv_${uuid0};
DROP TABLE ice_ivm_minmax_p_${uuid0}.ns_${uuid0}.orders ;
DROP SCHEMA ice_ivm_minmax_p_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_minmax_p_${uuid0};
