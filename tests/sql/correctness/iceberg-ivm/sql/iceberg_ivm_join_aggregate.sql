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
-- @tags=mv,iceberg,ivm,join,aggregate,target_state
-- Test Point: Iceberg-backed join aggregate IMV supports two-sided base retract changes.
-- Method: Create fact/dim v3 row-lineage tables, refresh join aggregate MV, mutate both bases, and compare MV with base query.
-- Scope: Iceberg target MV, two-table inner equi-join aggregate, telescoping delta, group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_agg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_join_agg_${uuid0}",
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
CREATE DATABASE ice_ivm_join_agg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_join_agg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region;
CREATE MATERIALIZED VIEW join_agg_dim_amount_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(d.amount) AS s
FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 'east', 7),
  (20, 'west', 11),
  (30, 'south', 13);
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, 200),
  (3, 20, 50),
  (4, 30, 70);
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};
REFRESH MATERIALIZED VIEW join_agg_dim_amount_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM join_agg_mv_${uuid0}
ORDER BY region;

-- query 4
SELECT region, c, s
FROM join_agg_dim_amount_mv_${uuid0}
ORDER BY region;

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact VALUES (5, 20, 80);
UPDATE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact SET amount = 150 WHERE id = 1;
UPDATE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim SET region = 'north' WHERE id = 10;
DELETE FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact WHERE id = 4;
DELETE FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim WHERE id = 30;

-- query 6
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 7
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW join_agg_dim_amount_mv_${uuid0};

-- query 8
SELECT region, c, s
FROM join_agg_mv_${uuid0}
ORDER BY region;

-- query 9
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY d.region;

-- query 10
SELECT region, c, s
FROM join_agg_dim_amount_mv_${uuid0}
ORDER BY region;

-- query 11
SELECT d.region, COUNT(*) AS c, SUM(d.amount) AS s
FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY d.region;

-- query 12
-- @skip_result_check=true
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact VALUES (6, 20, 20);

-- query 13
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 14
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW join_agg_dim_amount_mv_${uuid0};

-- query 15
SELECT region, c, s
FROM join_agg_mv_${uuid0}
ORDER BY region;

-- query 16
SELECT region, c, s
FROM join_agg_dim_amount_mv_${uuid0}
ORDER BY region;

-- query 17
-- @skip_result_check=true
UPDATE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim SET region = 'coast', amount = 17 WHERE id = 20;

-- query 18
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 19
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW join_agg_dim_amount_mv_${uuid0};

-- query 20
SELECT region, c, s
FROM join_agg_mv_${uuid0}
ORDER BY region;

-- query 21
SELECT region, c, s
FROM join_agg_dim_amount_mv_${uuid0}
ORDER BY region;

-- query 22
SELECT d.region, COUNT(*) AS c, SUM(d.amount) AS s
FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY d.region;

-- query 23
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_agg_dim_amount_mv_${uuid0};
DROP MATERIALIZED VIEW join_agg_mv_${uuid0};
DROP TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_join_agg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_agg_${uuid0};
