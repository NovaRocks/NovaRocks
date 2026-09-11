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
-- @tags=mv,iceberg,ivm,aggregate,join,branch_union,incremental_apply,change_stream
-- Test Point: aggregate IMV change streams are consumed by the existing Iceberg MV merge sink.
-- Method: Refresh aggregate, join-aggregate, and branch-union aggregate MVs through incremental changes,
--         then compare each MV with its full base query.
-- Scope: relation aggregate merge cutover, __change_op merge sink apply, retraction delete, new group insert.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_mv_apply_agg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_mv_apply_agg_${uuid0}",
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
CREATE DATABASE ice_mv_apply_agg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.orders_${uuid0} (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.fact_${uuid0} (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.dim_${uuid0} (
  id BIGINT NOT NULL,
  region STRING
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t1_${uuid0} (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t2_${uuid0} (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_mv_apply_agg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_apply_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM orders_${uuid0}
GROUP BY region;
CREATE MATERIALIZED VIEW join_agg_apply_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact_${uuid0} AS f
JOIN dim_${uuid0} AS d ON f.dim_id = d.id
GROUP BY d.region;
CREATE MATERIALIZED VIEW branch_agg_apply_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM t1_${uuid0}
GROUP BY region
UNION ALL
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM t2_${uuid0}
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_mv_apply_agg_${uuid0}.ns_${uuid0}.orders_${uuid0} VALUES
  (1, 'east', 10),
  (2, 'east', -10),
  (3, 'west', 5);
INSERT INTO ice_mv_apply_agg_${uuid0}.ns_${uuid0}.dim_${uuid0} VALUES
  (10, 'east'),
  (20, 'west');
INSERT INTO ice_mv_apply_agg_${uuid0}.ns_${uuid0}.fact_${uuid0} VALUES
  (1, 10, 100),
  (2, 20, 50);
INSERT INTO ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t1_${uuid0} VALUES
  (1, 'k1', 10),
  (2, 'k2', 5);
INSERT INTO ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t2_${uuid0} VALUES
  (3, 'k1', 100),
  (4, 'k3', 7);
REFRESH MATERIALIZED VIEW agg_apply_mv_${uuid0};
REFRESH MATERIALIZED VIEW join_agg_apply_mv_${uuid0};
REFRESH MATERIALIZED VIEW branch_agg_apply_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM agg_apply_mv_${uuid0}
ORDER BY region;

-- query 4
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.orders_${uuid0}
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
INSERT INTO ice_mv_apply_agg_${uuid0}.ns_${uuid0}.orders_${uuid0} VALUES
  (4, 'east', 0),
  (5, 'north', 7);
DELETE FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.orders_${uuid0}
WHERE region = 'west';

-- query 6
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergMvTargetState
-- @explain_contains=__change_op
REFRESH MATERIALIZED VIEW agg_apply_mv_${uuid0};

-- query 7
SELECT region, c, s
FROM agg_apply_mv_${uuid0}
ORDER BY region;

-- query 8
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.orders_${uuid0}
GROUP BY region
ORDER BY region;

-- query 9
SELECT region, c, s
FROM join_agg_apply_mv_${uuid0}
ORDER BY region;

-- query 10
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.fact_${uuid0} AS f
JOIN ice_mv_apply_agg_${uuid0}.ns_${uuid0}.dim_${uuid0} AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY d.region;

-- query 11
-- @skip_result_check=true
UPDATE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.dim_${uuid0}
SET region = 'north'
WHERE id = 10;
DELETE FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.fact_${uuid0}
WHERE id = 2;
INSERT INTO ice_mv_apply_agg_${uuid0}.ns_${uuid0}.dim_${uuid0} VALUES
  (30, 'south');
INSERT INTO ice_mv_apply_agg_${uuid0}.ns_${uuid0}.fact_${uuid0} VALUES
  (3, 30, 70);

-- query 12
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW join_agg_apply_mv_${uuid0};

-- query 13
SELECT region, c, s
FROM join_agg_apply_mv_${uuid0}
ORDER BY region;

-- query 14
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.fact_${uuid0} AS f
JOIN ice_mv_apply_agg_${uuid0}.ns_${uuid0}.dim_${uuid0} AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY d.region;

-- query 15
SELECT region, c, s
FROM branch_agg_apply_mv_${uuid0}
ORDER BY region, s;

-- query 16
SELECT region, c, s
FROM (
  SELECT region, COUNT(*) AS c, SUM(amount) AS s
  FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t1_${uuid0}
  GROUP BY region
  UNION ALL
  SELECT region, COUNT(*) AS c, SUM(amount) AS s
  FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t2_${uuid0}
  GROUP BY region
) u
ORDER BY region, s;

-- query 17
-- @skip_result_check=true
DELETE FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t2_${uuid0}
WHERE region = 'k1';
INSERT INTO ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t1_${uuid0} VALUES
  (5, 'k1', 50);

-- query 18
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW branch_agg_apply_mv_${uuid0};

-- query 19
SELECT region, c, s
FROM branch_agg_apply_mv_${uuid0}
ORDER BY region, s;

-- query 20
SELECT region, c, s
FROM (
  SELECT region, COUNT(*) AS c, SUM(amount) AS s
  FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t1_${uuid0}
  GROUP BY region
  UNION ALL
  SELECT region, COUNT(*) AS c, SUM(amount) AS s
  FROM ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t2_${uuid0}
  GROUP BY region
) u
ORDER BY region, s;

-- query 21
-- @skip_result_check=true
DROP MATERIALIZED VIEW branch_agg_apply_mv_${uuid0};
DROP MATERIALIZED VIEW join_agg_apply_mv_${uuid0};
DROP MATERIALIZED VIEW agg_apply_mv_${uuid0};
DROP TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t2_${uuid0} FORCE;
DROP TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.t1_${uuid0} FORCE;
DROP TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.dim_${uuid0} FORCE;
DROP TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.fact_${uuid0} FORCE;
DROP TABLE ice_mv_apply_agg_${uuid0}.ns_${uuid0}.orders_${uuid0} FORCE;
DROP DATABASE ice_mv_apply_agg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_mv_apply_agg_${uuid0};
