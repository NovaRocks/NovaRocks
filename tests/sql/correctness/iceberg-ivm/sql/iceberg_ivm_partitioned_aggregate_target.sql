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
-- @tags=mv,iceberg,ivm,aggregate,partitioned,target_state
-- Test Point: Iceberg-backed partitioned aggregate MV refreshes incrementally using
-- the partition-pruned touched-group lookup path.
-- Method: Create v3 row-lineage base table, create storage_engine='iceberg'
-- partitioned aggregate MV (PARTITION BY region), refresh after insert/update/delete
-- limited to a subset of regions, and verify result rows match the equivalent
-- aggregate over the base.
-- Scope: Iceberg target MV, single-base aggregate, COUNT/SUM/AVG, identity-partition
-- pruning + group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_pagg_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_pagg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders (
  region VARCHAR,
  amount BIGINT
) WITH (format_version = 3);
USE ice_ivm_pagg_${uuid0}.ns_${uuid0};

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders VALUES
  ROW('east', 10),
  ROW('east', 20),
  ROW('west', NULL),
  ROW('south', 5);
CREATE MATERIALIZED VIEW pagg_mv_${uuid0} WITH (partitioning = ARRAY['region']) AS SELECT region, COUNT(*) AS c, COUNT(amount) AS c_amount, SUM(amount) AS s, AVG(amount) AS a
FROM ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders
GROUP BY region;
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 3
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders VALUES
  ROW('east', 30),
  ROW('south', 15);
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 5
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 6
-- @skip_result_check=true
DELETE FROM ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders WHERE region = 'east' AND amount = 10;
UPDATE ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders SET amount = 100 WHERE region = 'east' AND amount = 20;
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 7
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 8
-- @skip_result_check=true
INSERT INTO ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders VALUES ROW('west', 42);
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 9
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 10
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

SELECT region,
       COUNT(*) AS c,
       COUNT(amount) AS c_amount,
       SUM(amount) AS s,
       AVG(amount) AS a
FROM ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW pagg_mv_${uuid0};
DROP TABLE ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders;
DROP SCHEMA ice_ivm_pagg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_pagg_${uuid0};
