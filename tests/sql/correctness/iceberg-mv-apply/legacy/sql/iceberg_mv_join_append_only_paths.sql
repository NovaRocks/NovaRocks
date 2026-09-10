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
-- @tags=mv,iceberg,ivm,join,append_only,target_apply
-- Test Point: join MV relational coalesce handles append-only branches and later retractions.
-- Method: Exercise left-only append, right-only append, both-side append, zero-effective append, payload update, and delete.
-- Scope: Join delta append-only fast path, relational coalesce payload grouping, framework target apply-key locator.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_mv_join_paths_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_mv_join_paths_${uuid0}",
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
CREATE DATABASE ice_mv_join_paths_${uuid0}.ns_${uuid0};
CREATE TABLE ice_mv_join_paths_${uuid0}.ns_${uuid0}.fact_${uuid0} (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount INT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_mv_join_paths_${uuid0}.ns_${uuid0}.dim_${uuid0} (
  id BIGINT NOT NULL,
  label STRING
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_mv_join_paths_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_paths_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT f.id, f.dim_id, f.amount, d.label
FROM fact_${uuid0} AS f
JOIN dim_${uuid0} AS d ON f.dim_id = d.id
WHERE f.amount >= 10;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_mv_join_paths_${uuid0}.ns_${uuid0}.dim_${uuid0} VALUES
  (10, 'A'),
  (20, 'B');
INSERT INTO ice_mv_join_paths_${uuid0}.ns_${uuid0}.fact_${uuid0} VALUES
  (1, 10, 100),
  (2, 20, 200),
  (4, 40, 400);
REFRESH MATERIALIZED VIEW join_paths_mv_${uuid0};

-- query 3
SELECT id, dim_id, amount, label
FROM join_paths_mv_${uuid0}
ORDER BY id;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_mv_join_paths_${uuid0}.ns_${uuid0}.fact_${uuid0} VALUES
  (3, 10, 300);
REFRESH MATERIALIZED VIEW join_paths_mv_${uuid0};

-- query 5
SELECT id, dim_id, amount, label
FROM join_paths_mv_${uuid0}
ORDER BY id;

-- query 6
-- @skip_result_check=true
INSERT INTO ice_mv_join_paths_${uuid0}.ns_${uuid0}.dim_${uuid0} VALUES
  (40, 'D');
REFRESH MATERIALIZED VIEW join_paths_mv_${uuid0};

-- query 7
SELECT id, dim_id, amount, label
FROM join_paths_mv_${uuid0}
ORDER BY id;

-- query 8
-- @skip_result_check=true
INSERT INTO ice_mv_join_paths_${uuid0}.ns_${uuid0}.dim_${uuid0} VALUES
  (50, 'E');
INSERT INTO ice_mv_join_paths_${uuid0}.ns_${uuid0}.fact_${uuid0} VALUES
  (5, 50, 500);
REFRESH MATERIALIZED VIEW join_paths_mv_${uuid0};

-- query 9
SELECT id, dim_id, amount, label
FROM join_paths_mv_${uuid0}
ORDER BY id;

-- query 10
-- @skip_result_check=true
INSERT INTO ice_mv_join_paths_${uuid0}.ns_${uuid0}.dim_${uuid0} VALUES
  (60, 'F');
REFRESH MATERIALIZED VIEW join_paths_mv_${uuid0};

-- query 11
SELECT COUNT(*) AS c, SUM(id) AS sum_id, SUM(amount) AS sum_amount
FROM join_paths_mv_${uuid0};

-- query 12
-- @skip_result_check=true
UPDATE ice_mv_join_paths_${uuid0}.ns_${uuid0}.dim_${uuid0}
SET label = 'B2'
WHERE id = 20;
DELETE FROM ice_mv_join_paths_${uuid0}.ns_${uuid0}.fact_${uuid0}
WHERE id = 3;

-- query 13
-- @skip_result_check=true
-- @explain_contains=IcebergVersionTable
REFRESH MATERIALIZED VIEW join_paths_mv_${uuid0};

-- query 14
-- @imv_equivalence_check=join_paths_mv_${uuid0}
SELECT id, dim_id, amount, label
FROM join_paths_mv_${uuid0}
ORDER BY id;

-- query 15
SELECT f.id, f.dim_id, f.amount, d.label
FROM ice_mv_join_paths_${uuid0}.ns_${uuid0}.fact_${uuid0} AS f
JOIN ice_mv_join_paths_${uuid0}.ns_${uuid0}.dim_${uuid0} AS d ON f.dim_id = d.id
WHERE f.amount >= 10
ORDER BY f.id;

-- query 16
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_paths_mv_${uuid0};
DROP TABLE ice_mv_join_paths_${uuid0}.ns_${uuid0}.fact_${uuid0} FORCE;
DROP TABLE ice_mv_join_paths_${uuid0}.ns_${uuid0}.dim_${uuid0} FORCE;
DROP DATABASE ice_mv_join_paths_${uuid0}.ns_${uuid0};
DROP CATALOG ice_mv_join_paths_${uuid0};
