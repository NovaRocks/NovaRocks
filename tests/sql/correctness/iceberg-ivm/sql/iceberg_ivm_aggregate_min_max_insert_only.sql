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
-- @tags=mv,iceberg,ivm,aggregate,min_max,detail_state
-- Test Point: Iceberg-backed aggregate MV with MIN/MAX detail-state refreshes
-- incrementally on INSERT-only deltas via the value-count map state path.
-- Method: Create v3 row-lineage base table, create storage_engine='iceberg'
-- aggregate MV with MIN(amount), MAX(amount), COUNT(*) GROUP BY region. Apply
-- successive INSERT batches and assert MV result matches plain GROUP BY query.
-- Scope: Iceberg target MV, single-base aggregate, MIN/MAX/COUNT, INSERT-only delta.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_minmax_ins_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_minmax_ins_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_minmax_ins_${uuid0}.ns_${uuid0}.orders (
  region VARCHAR,
  amount BIGINT
) WITH (format_version = 3);
USE ice_ivm_minmax_ins_${uuid0}.ns_${uuid0};

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_minmax_ins_${uuid0}.ns_${uuid0}.orders VALUES
  ROW('east', 30),
  ROW('east', 10),
  ROW('east', 20),
  ROW('west', 100),
  ROW('west', 200);
CREATE MATERIALIZED VIEW minmax_ins_mv_${uuid0} AS SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_ins_${uuid0}.ns_${uuid0}.orders
GROUP BY region;
REFRESH MATERIALIZED VIEW minmax_ins_mv_${uuid0};

-- query 3
SELECT region, mn, mx, c
FROM minmax_ins_mv_${uuid0}
ORDER BY region;

-- query 4
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_ins_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_minmax_ins_${uuid0}.ns_${uuid0}.orders VALUES
  ROW('east', 5),
  ROW('east', 40),
  ROW('west', 50);
REFRESH MATERIALIZED VIEW minmax_ins_mv_${uuid0};

-- query 6
SELECT region, mn, mx, c
FROM minmax_ins_mv_${uuid0}
ORDER BY region;

-- query 7
SELECT region,
       MIN(amount) AS mn,
       MAX(amount) AS mx,
       COUNT(*) AS c
FROM ice_ivm_minmax_ins_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW minmax_ins_mv_${uuid0};
DROP TABLE ice_ivm_minmax_ins_${uuid0}.ns_${uuid0}.orders ;
DROP SCHEMA ice_ivm_minmax_ins_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_minmax_ins_${uuid0};
