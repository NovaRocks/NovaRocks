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
-- @tags=mv,iceberg,ivm,join,aggregate,nested_join,target_state
-- Test Point: aggregate over a three-table nested inner join refreshes incrementally.
-- Method: fact JOIN dim JOIN dim2, GROUP BY dim2.region. Initial REFRESH,
-- then INSERT into all three bases, DELETE from fact, and DELETE from dim; cross-check MV == full recompute.
-- Scope: nested join delta decomposition, aggregate state merge, version-table scans,
-- and Iceberg target-state apply for an aggregate over Join(Join(fact, dim), dim2).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_njoin_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_njoin_${uuid0}",
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
CREATE DATABASE ice_ivm_njoin_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region_id BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 (
  id BIGINT NOT NULL,
  region STRING
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_njoin_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW njoin_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d2.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 AS d2 ON d.region_id = d2.id
GROUP BY d2.region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 VALUES
  (100, 'east'),
  (200, 'west'),
  (300, 'south');
INSERT INTO ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 100),
  (20, 200),
  (30, 300);
INSERT INTO ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, 200),
  (3, 20, 50),
  (4, 30, 70);
REFRESH MATERIALIZED VIEW njoin_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM njoin_mv_${uuid0}
ORDER BY region;

-- query 4
SELECT d2.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 AS d2 ON d.region_id = d2.id
GROUP BY d2.region
ORDER BY region;

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 VALUES
  (400, 'north');
INSERT INTO ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim VALUES
  (40, 400);
INSERT INTO ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact VALUES
  (5, 20, 80),
  (6, 40, 60),
  (7, 10, 90);

-- query 6
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=UNION
-- @explain_contains=IcebergVersionTable
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW njoin_mv_${uuid0};

-- query 7
SELECT region, c, s
FROM njoin_mv_${uuid0}
ORDER BY region;

-- query 8
SELECT d2.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 AS d2 ON d.region_id = d2.id
GROUP BY d2.region
ORDER BY region;

-- query 9
-- @skip_result_check=true
DELETE FROM ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact WHERE id = 1;

-- query 10
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=UNION
-- @explain_contains=IcebergVersionTable
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW njoin_mv_${uuid0};

-- query 11
SELECT region, c, s
FROM njoin_mv_${uuid0}
ORDER BY region;

-- query 12
SELECT d2.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 AS d2 ON d.region_id = d2.id
GROUP BY d2.region
ORDER BY region;

-- query 13
-- @skip_result_check=true
DELETE FROM ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim WHERE id = 20;

-- query 14
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=UNION
-- @explain_contains=IcebergVersionTable
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW njoin_mv_${uuid0};

-- query 15
SELECT region, c, s
FROM njoin_mv_${uuid0}
ORDER BY region;

-- query 16
SELECT d2.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 AS d2 ON d.region_id = d2.id
GROUP BY d2.region
ORDER BY region;

-- query 17
-- @skip_result_check=true
DROP MATERIALIZED VIEW njoin_mv_${uuid0};
DROP TABLE ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim FORCE;
DROP TABLE ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 FORCE;
DROP DATABASE ice_ivm_njoin_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_njoin_${uuid0};
