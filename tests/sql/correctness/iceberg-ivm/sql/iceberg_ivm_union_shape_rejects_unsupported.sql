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
-- @tags=mv,iceberg,ivm,union,negative,shape_validation
-- Test Point: Iceberg IMV UNION shape validation rejects unsupported
-- neighboring shapes at CREATE time.
-- Scope: UNION DISTINCT, mixed projection/aggregate branches, incompatible
-- aggregate branches, duplicate base refs, reserved branch-id output names, and
-- heterogeneous-base composed branch-union aggregates (aggregate-over-join
-- branches whose two branches join DIFFERENT base sets). Also pins the
-- join-of-aggregate boundary: joins with aggregate subquery sides remain
-- outside this phase.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_union_reject_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_union_reject_${uuid0}",
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
CREATE DATABASE ice_ivm_union_reject_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1 (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2 (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t3 (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_union_reject_${uuid0};
USE ns_${uuid0};

-- query 2
-- @expect_error=Iceberg IMV refresh contract only supports UNION ALL set operations
CREATE MATERIALIZED VIEW union_distinct_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT id, region
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
UNION
SELECT id, region
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2;

-- query 3
-- @expect_error=requires homogeneous UNION ALL branches
CREATE MATERIALIZED VIEW union_mixed_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, amount
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
UNION ALL
SELECT region, SUM(amount) AS amount
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2
GROUP BY region;

-- query 4
-- @expect_error=Iceberg IMV refresh contract requires compatible aggregate branch contracts
CREATE MATERIALIZED VIEW union_incompatible_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
GROUP BY region
UNION ALL
SELECT region, amount, COUNT(*) AS c
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2
GROUP BY region, amount;

-- query 5
-- @expect_error=requires 2 distinct Iceberg base table refs
CREATE MATERIALIZED VIEW union_duplicate_base_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
GROUP BY region
UNION ALL
SELECT region, COUNT(*) AS c
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
GROUP BY region;

-- query 6
-- @expect_error=reserved for internal branch id
CREATE MATERIALIZED VIEW union_reserved_branch_id_mv_${uuid0}
DISTRIBUTED BY HASH(__branch_id__) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT id AS __branch_id__, region
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
UNION ALL
SELECT id AS __branch_id__, region
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2;

-- query 7
-- @expect_error=a composed UNION ALL of aggregates is only supported when every branch shares the same base tables and structure
CREATE MATERIALIZED VIEW union_hetero_composed_join_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT t1.region, COUNT(*) AS c, SUM(t2.amount) AS s
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1 t1
JOIN ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2 t2 ON t1.id = t2.id
GROUP BY t1.region
UNION ALL
SELECT t1.region, COUNT(*) AS c, SUM(t3.amount) AS s
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1 t1
JOIN ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t3 t3 ON t1.id = t3.id
GROUP BY t1.region;

-- query 8
-- @expect_error=Iceberg IMV refresh contract supports join keys only over direct scan inputs
CREATE MATERIALIZED VIEW union_join_of_aggregate_reject_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, SUM(g.sv) AS total
FROM (
    SELECT id, SUM(amount) AS sv
    FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
    GROUP BY id
) AS g
JOIN ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2 AS d ON g.id = d.id
GROUP BY d.region;

-- query 9
-- @skip_result_check=true
DROP TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1 FORCE;
DROP TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2 FORCE;
DROP TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t3 FORCE;
DROP DATABASE ice_ivm_union_reject_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_union_reject_${uuid0};
