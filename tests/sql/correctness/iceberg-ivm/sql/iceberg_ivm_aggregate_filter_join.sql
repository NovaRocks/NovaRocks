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
-- @tags=mv,iceberg,ivm,join,aggregate,filter,target_state
-- Test Point: aggregate over a FILTERED inner join (Aggregate(Filter(Join)))
-- refreshes incrementally. The WHERE sits between the aggregate and the join,
-- exercising the decomposed pure Delta(Join) rule + delta pushdown through Filter.
-- Method: fact JOIN dim, WHERE f.amount > 0, GROUP BY d.region. Initial REFRESH,
-- then INSERT into both bases and DELETE from fact; cross-check MV == full recompute.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_fjoin_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_fjoin_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) WITH (format_version = 3);
CREATE TABLE ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) WITH (format_version = 3);
USE ice_ivm_fjoin_${uuid0}.ns_${uuid0};

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim VALUES
  ROW(10, 'east'),
  ROW(20, 'west'),
  ROW(30, 'south');
INSERT INTO ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact VALUES
  ROW(1, 10, 100),
  ROW(2, 10, 200),
  ROW(3, 20, 50),
  ROW(4, 30, -5),
  ROW(5, 30, 70);
CREATE MATERIALIZED VIEW fjoin_mv_${uuid0} AS SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount > 0
GROUP BY d.region;
REFRESH MATERIALIZED VIEW fjoin_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM fjoin_mv_${uuid0}
ORDER BY region;

-- query 4
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount > 0
GROUP BY d.region
ORDER BY region;

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim VALUES
  ROW(40, 'north');
INSERT INTO ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact VALUES
  ROW(6, 20, 80),
  ROW(7, 40, 60),
  ROW(8, 10, -9);

-- query 6
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=FILTER
-- @explain_contains=UNION
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW fjoin_mv_${uuid0};

-- query 7
SELECT region, c, s
FROM fjoin_mv_${uuid0}
ORDER BY region;

-- query 8
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount > 0
GROUP BY d.region
ORDER BY region;

-- query 9
-- @skip_result_check=true
DELETE FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact WHERE id = 1;

-- query 10
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=FILTER
-- @explain_contains=UNION
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW fjoin_mv_${uuid0};

-- query 11
SELECT region, c, s
FROM fjoin_mv_${uuid0}
ORDER BY region;

-- query 12
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount > 0
GROUP BY d.region
ORDER BY region;

-- query 13
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW fjoin_mv_${uuid0};
DROP TABLE ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact ;
DROP TABLE ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim ;
DROP SCHEMA ice_ivm_fjoin_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_fjoin_${uuid0};
