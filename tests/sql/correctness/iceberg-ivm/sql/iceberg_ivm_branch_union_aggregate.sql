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
-- @tags=mv,iceberg,ivm,union,aggregate,branch_union,target_state
-- Test Point: Iceberg UNION ALL of aggregate branches keeps same group keys
-- independent across branches.
-- Method: Build a UNION ALL MV whose two aggregate branches both output
-- region='k1'. Delete/insert in one branch and verify the other branch's
-- aggregate row is not merged or retracted.
-- Scope: RefreshStrategy::BranchUnionAggregate, BranchUtf8 apply key.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_bunion_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_bunion_${uuid0}",
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
CREATE DATABASE ice_ivm_bunion_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1 (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2 (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_bunion_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW branch_union_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1
GROUP BY region
UNION ALL
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1 VALUES
  (1, 'k1', 10),
  (2, 'k2', 5);
INSERT INTO ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2 VALUES
  (3, 'k1', 100),
  (4, 'k3', 7);
REFRESH MATERIALIZED VIEW branch_union_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM branch_union_mv_${uuid0}
ORDER BY region, s;

-- query 4
-- @skip_result_check=true
DELETE FROM ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2 WHERE region = 'k1';

-- query 5
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW branch_union_mv_${uuid0};

-- query 6
SELECT region, c, s
FROM branch_union_mv_${uuid0}
ORDER BY region, s;

-- query 7
-- @skip_result_check=true
INSERT INTO ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1 VALUES
  (5, 'k1', 50);

-- query 8
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW branch_union_mv_${uuid0};

-- query 9
SELECT region, c, s
FROM branch_union_mv_${uuid0}
ORDER BY region, s;

-- query 10
-- @expect_error=Column '__agg_state_c' cannot be resolved
SELECT __agg_state_c FROM branch_union_mv_${uuid0};

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW branch_union_mv_${uuid0};
DROP TABLE ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1 FORCE;
DROP TABLE ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2 FORCE;
DROP DATABASE ice_ivm_bunion_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_bunion_${uuid0};
