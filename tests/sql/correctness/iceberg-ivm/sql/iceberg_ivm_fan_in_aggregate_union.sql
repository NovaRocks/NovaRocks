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
-- @tags=mv,iceberg,ivm,union,aggregate,fan_in,target_state
-- Test Point: Iceberg aggregate-over-UNION-ALL IMV merges branch deltas into
-- one shared aggregate state by group key.
-- Method: Aggregate over a UNION ALL subquery, refresh after branch-local
-- insert/delete changes, and compare MV rows with the equivalent base query.
-- Scope: RefreshStrategy::FanInAggregate, group-row apply key.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_fanin_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_fanin_${uuid0}",
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
CREATE DATABASE ice_ivm_fanin_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_east (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_west (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_fanin_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW fanin_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM (
  SELECT region, amount
  FROM ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_east
  UNION ALL
  SELECT region, amount
  FROM ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_west
) u
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_east VALUES
  (1, 'east', 10),
  (2, 'west', 7);
INSERT INTO ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_west VALUES
  (3, 'east', 5),
  (4, 'north', 3);
REFRESH MATERIALIZED VIEW fanin_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM fanin_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_west VALUES
  (5, 'east', 100);

-- query 5
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW fanin_mv_${uuid0};

-- query 6
SELECT region, c, s
FROM fanin_mv_${uuid0}
ORDER BY region;

-- query 7
-- @skip_result_check=true
DELETE FROM ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_east WHERE id = 1;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW fanin_mv_${uuid0};

-- query 9
-- @imv_equivalence_check=fanin_mv_${uuid0}
SELECT region, c, s
FROM fanin_mv_${uuid0}
ORDER BY region;

-- query 10
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM (
  SELECT region, amount
  FROM ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_east
  UNION ALL
  SELECT region, amount
  FROM ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_west
) u
GROUP BY region
ORDER BY region;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW fanin_mv_${uuid0};
DROP TABLE ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_east FORCE;
DROP TABLE ice_ivm_fanin_${uuid0}.ns_${uuid0}.fact_west FORCE;
DROP DATABASE ice_ivm_fanin_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_fanin_${uuid0};
