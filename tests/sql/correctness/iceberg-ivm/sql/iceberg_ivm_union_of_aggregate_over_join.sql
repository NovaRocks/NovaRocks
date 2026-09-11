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
-- @tags=mv,iceberg,ivm,union,aggregate,join,branch_union,composed,target_state
-- Test Point: Iceberg UNION ALL of aggregate-over-join branches (composed
-- branch-union aggregate) creates and refreshes incrementally when every
-- branch shares the same base set + join structure (homogeneous-base case).
-- Method: Two base tables (fact/dim). The MV is a UNION ALL of two aggregate-
-- over-join branches that share the same fact JOIN dim and GROUP BY d.region,
-- differing only by the SUM input (SUM(f.amount) vs SUM(d.amount)) so the two
-- branches stay homogeneous (same bases, same inner equi-join, same group key,
-- same aggregate arity). Each branch is an aggregate OVER A JOIN, so the
-- per-branch delta runs through the join-aggregate delta rewrite under a
-- branch scope. Initial REFRESH populates the MV; cross-check via full
-- recompute. Mutate both bases (INSERT + DELETE); REFRESH again; cross-check
-- again (exercises the composed incremental delta path).
-- Scope: BranchUnionAggregate contract with BranchShape::Composed branches,
-- BranchUtf8 apply key, join-aggregate delta + relation aggregate merge +
-- IcebergMvTargetState refresh.
-- Note: branches are differentiated by aggregate input, NOT by a WHERE filter:
-- the join-aggregate delta rule matches Aggregate(Join) directly, so a filter
-- between the aggregate and the join (Aggregate(Filter(Join))) is a separate,
-- pre-existing join-delta limitation independent of branch-union composition.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_ujoin_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_ujoin_${uuid0}",
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
CREATE DATABASE ice_ivm_ujoin_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_ujoin_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW ujoin_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
UNION ALL
SELECT d.region, COUNT(*) AS c, SUM(d.amount) AS s
FROM ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 'east', 7),
  (20, 'west', 11),
  (30, 'south', 13);
INSERT INTO ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, 200),
  (3, 20, 50),
  (4, 30, 70);
REFRESH MATERIALIZED VIEW ujoin_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM ujoin_mv_${uuid0}
ORDER BY region, s;

-- query 4
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
UNION ALL
SELECT d.region, COUNT(*) AS c, SUM(d.amount) AS s
FROM ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY region, s;

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim VALUES
  (40, 'north', 5);
INSERT INTO ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact VALUES
  (5, 20, 80),
  (6, 40, 60),
  (7, 10, 90);

-- query 6
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW ujoin_mv_${uuid0};

-- query 7
SELECT region, c, s
FROM ujoin_mv_${uuid0}
ORDER BY region, s;

-- query 8
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
UNION ALL
SELECT d.region, COUNT(*) AS c, SUM(d.amount) AS s
FROM ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY region, s;

-- query 9
-- @skip_result_check=true
DELETE FROM ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact WHERE id = 1;

-- query 10
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW ujoin_mv_${uuid0};

-- query 11
SELECT region, c, s
FROM ujoin_mv_${uuid0}
ORDER BY region, s;

-- query 12
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
UNION ALL
SELECT d.region, COUNT(*) AS c, SUM(d.amount) AS s
FROM ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY region, s;

-- query 13
-- @skip_result_check=true
DROP MATERIALIZED VIEW ujoin_mv_${uuid0};
DROP TABLE ice_ivm_ujoin_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_ujoin_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_ujoin_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_ujoin_${uuid0};
