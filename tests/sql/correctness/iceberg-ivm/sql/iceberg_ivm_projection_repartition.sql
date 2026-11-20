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
-- @tags=mv,iceberg,ivm,projection_filter,aggregate,join,partitioned,repartition
-- Test Point: Iceberg-backed MV repartition supports projection/filter,
-- aggregate, and join full rebuild shapes while preserving post-repartition refresh.
-- Method: Repartition a projection/filter MV, an aggregate MV, and a join MV
-- from one target partition spec to another, then verify visible results match
-- a full recompute. Keep one unsupported shape assertion with a concrete
-- UnsupportedRepartitionShape error.
-- Scope: Iceberg target MV, repartition, operation lifecycle, full rebuild,
-- post-repartition incremental refresh for projection/filter.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_repart_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_repart_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT,
  category STRING
) WITH (format_version = 3);
USE ice_ivm_repart_${uuid0}.ns_${uuid0};

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.orders VALUES
  ROW(1, 'east', 10, 'books'),
  ROW(2, 'west', 20, 'games'),
  ROW(3, 'east', 30, 'books'),
  ROW(4, 'north', 5, 'games');
CREATE MATERIALIZED VIEW pf_repart_mv_${uuid0} WITH (partitioning = ARRAY['bucket(id, 4)']) AS SELECT id, region, amount, category
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10;
REFRESH MATERIALIZED VIEW pf_repart_mv_${uuid0};

-- query 3
SELECT id, region, amount, category
FROM pf_repart_mv_${uuid0}
ORDER BY id;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.orders VALUES
  ROW(5, 'west', 40, 'toys'),
  ROW(6, 'south', 8, 'toys');
DELETE FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders WHERE id = 1;
REFRESH MATERIALIZED VIEW pf_repart_mv_${uuid0};

-- query 5
SELECT id, region, amount, category
FROM pf_repart_mv_${uuid0}
ORDER BY id;

-- query 6
-- @skip_result_check=true
ALTER MATERIALIZED VIEW pf_repart_mv_${uuid0} SET PROPERTIES partitioning = ARRAY['truncate(region, 2)'];

-- query 7
SELECT id, region, amount, category
FROM pf_repart_mv_${uuid0}
ORDER BY id;

-- query 8
-- @skip_result_check=true
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.orders VALUES
  ROW(7, 'east', 70, 'books'),
  ROW(8, 'north', 1, 'games');
DELETE FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders WHERE id = 3;
REFRESH MATERIALIZED VIEW pf_repart_mv_${uuid0};

-- query 9
SELECT id, region, amount, category
FROM pf_repart_mv_${uuid0}
ORDER BY id;

-- query 10
SELECT id, region, amount, category
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10
ORDER BY id;

-- query 11
-- @skip_result_check=true
CREATE MATERIALIZED VIEW agg_repart_mv_${uuid0} WITH (partitioning = ARRAY['region']) AS SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
GROUP BY region;
REFRESH MATERIALIZED VIEW agg_repart_mv_${uuid0};

-- query 12
-- @skip_result_check=true
ALTER MATERIALIZED VIEW agg_repart_mv_${uuid0} SET PROPERTIES partitioning = ARRAY['truncate(region, 2)'];

-- query 13
SELECT region, c, s
FROM agg_repart_mv_${uuid0}
ORDER BY region;

-- query 14
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 15
-- @skip_result_check=true
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.customers (
  customer_id BIGINT NOT NULL,
  region STRING
) WITH (format_version = 3);
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.customers VALUES
  ROW(10, 'east'),
  ROW(20, 'west'),
  ROW(30, 'north');
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders (
  id BIGINT NOT NULL,
  customer_id BIGINT,
  amount BIGINT
) WITH (format_version = 3);
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders VALUES
  ROW(101, 10, 100),
  ROW(102, 20, 200),
  ROW(103, 10, 300);
CREATE MATERIALIZED VIEW join_repart_mv_${uuid0} WITH (partitioning = ARRAY['region']) AS SELECT o.id, c.region, o.amount
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders o
JOIN ice_ivm_repart_${uuid0}.ns_${uuid0}.customers c
  ON o.customer_id = c.customer_id;
REFRESH MATERIALIZED VIEW join_repart_mv_${uuid0};
ALTER MATERIALIZED VIEW join_repart_mv_${uuid0} SET PROPERTIES partitioning = ARRAY['truncate(region, 2)'];

-- query 16
SELECT id, region, amount
FROM join_repart_mv_${uuid0}
ORDER BY id;

-- query 17
SELECT o.id, c.region, o.amount
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders o
JOIN ice_ivm_repart_${uuid0}.ns_${uuid0}.customers c
  ON o.customer_id = c.customer_id
ORDER BY o.id;

-- query 18
-- @expect_error=UnsupportedRepartitionShape
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT,
  category STRING
) WITH (format_version = 3);
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra VALUES
  ROW(201, 'east', 11, 'books'),
  ROW(202, 'west', 22, 'games');
CREATE MATERIALIZED VIEW unsupported_repart_mv_${uuid0} WITH (partitioning = ARRAY['region']) AS SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
GROUP BY region
UNION ALL
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra
GROUP BY region;
REFRESH MATERIALIZED VIEW unsupported_repart_mv_${uuid0};
ALTER MATERIALIZED VIEW unsupported_repart_mv_${uuid0} SET PROPERTIES partitioning = ARRAY['truncate(region, 2)'];

-- query 19
-- @cleanup=true
-- @skip_result_check=true
USE ice_ivm_repart_${uuid0}.ns_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.unsupported_repart_mv_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.join_repart_mv_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.agg_repart_mv_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.pf_repart_mv_${uuid0};
DROP TABLE IF EXISTS ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders;
DROP TABLE IF EXISTS ice_ivm_repart_${uuid0}.ns_${uuid0}.customers;
DROP TABLE IF EXISTS ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra;
DROP TABLE IF EXISTS ice_ivm_repart_${uuid0}.ns_${uuid0}.orders;
DROP SCHEMA IF EXISTS ice_ivm_repart_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS ice_ivm_repart_${uuid0};
