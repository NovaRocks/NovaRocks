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
-- @tags=mv,iceberg,ivm,union,projection_filter,branch_apply_key
-- Test Point: Iceberg UNION ALL projection/filter IMV preserves branch-local
-- row identity when different bases produce colliding row ids.
-- Method: Create two v3 row-lineage bases with the same visible id, refresh a
-- UNION ALL projection MV, delete/update only one branch, and verify the other
-- branch's row remains.
-- Scope: RefreshStrategy::UnionProjectionFilter, BranchInt64 apply key.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_union_pf_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_union_pf_${uuid0}",
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
CREATE DATABASE ice_ivm_union_pf_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_live (
  id INT NOT NULL,
  name STRING,
  amount INT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_archive (
  id INT NOT NULL,
  name STRING,
  amount INT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_union_pf_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW union_pf_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT id, name, amount
FROM ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_live
WHERE amount >= 0
UNION ALL
SELECT id, name, amount
FROM ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_archive
WHERE amount >= 0;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_live VALUES
  (10, 'same', 11),
  (20, 'live-only', 20);
INSERT INTO ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_archive VALUES
  (10, 'same', 99),
  (30, 'archive-only', 30);
REFRESH MATERIALIZED VIEW union_pf_mv_${uuid0};

-- query 3
SELECT id, name, amount
FROM union_pf_mv_${uuid0}
ORDER BY id, amount;

-- query 4
-- @skip_result_check=true
DELETE FROM ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_live WHERE id = 10;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW union_pf_mv_${uuid0};

-- query 6
SELECT id, name, amount
FROM union_pf_mv_${uuid0}
ORDER BY id, amount;

-- query 7
-- @skip_result_check=true
INSERT INTO ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_live VALUES
  (10, 'live-new', 15);
UPDATE ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_archive
SET amount = 120
WHERE id = 10;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW union_pf_mv_${uuid0};

-- query 9
SELECT id, name, amount
FROM union_pf_mv_${uuid0}
ORDER BY id, amount;

-- query 10
-- @skip_result_check=true
DROP MATERIALIZED VIEW union_pf_mv_${uuid0};
DROP TABLE ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_live FORCE;
DROP TABLE ice_ivm_union_pf_${uuid0}.ns_${uuid0}.orders_archive FORCE;
DROP DATABASE ice_ivm_union_pf_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_union_pf_${uuid0};
