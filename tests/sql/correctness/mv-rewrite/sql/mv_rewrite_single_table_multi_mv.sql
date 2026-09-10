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
-- @tags=optimizer,mv,rewrite,iceberg
-- Test Objective (migrated from materialized-view/test_single_table_mv_rewrite):
-- Single-table aggregate rewrite across several rollup-shaped MVs:
-- 1. COUNT(1) direct 1:1 hit when query group-by equals the MV group-by.
-- 2. COUNT(1) -> SUM rollup hit when the query groups by a strict subset.
-- 3. A direct hit on an MV that materializes MORE aggregates than the query
--    uses (agg-output subset).
-- 4. With two competing valid MVs (coarse + fine group-by) the CBO picks the
--    smaller (coarse) one for the coarse query.
--
-- Data scale: ~3000 rows via generate_series so the pre-aggregated MVs are a
-- genuine cost win (see mv_rewrite_hit_basic.sql for the rationale).
-- user_id has 5 distinct values, user_name 25 -> coarse MV 5 rows, fine MV
-- 25 (user_id,user_name) pairs.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvrw_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/mvrw_${uuid0}",
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

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.user_tags (
  dt STRING,
  user_id INT,
  user_name STRING,
  tag_id INT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.user_tags
SELECT
  '2023-04-13',
  CAST(n % 5 AS INT) AS user_id,
  concat('u', CAST(n % 25 AS STRING)) AS user_name,
  CAST(n % 7 AS INT) AS tag_id
FROM TABLE(generate_series(1, 3000)) t(n);

-- query 5
-- @skip_result_check=true
SET CATALOG mvrw_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- fine-grained MV: 25 (user_id, user_name) groups, extra sum aggregate
-- @skip_result_check=true
CREATE MATERIALIZED VIEW fine_mv
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT user_id, user_name, COUNT(1) AS cnt, SUM(tag_id) AS total
FROM user_tags GROUP BY user_id, user_name;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW fine_mv WITH SYNC MODE;

-- query 9
-- rollup hit: query group-by {user_id} is a strict subset of the fine MV's
-- {user_id, user_name}; COUNT(1) rolls up via SUM
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: fine_mv
SELECT user_id, COUNT(1) FROM user_tags GROUP BY user_id;

-- query 10
SELECT user_id, COUNT(1) AS cnt FROM user_tags GROUP BY user_id ORDER BY user_id;

-- query 11
-- direct 1:1 hit with agg-output subset: the query uses only COUNT(1), the MV
-- also materializes SUM(tag_id)
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: fine_mv
SELECT user_id, user_name, COUNT(1) FROM user_tags GROUP BY user_id, user_name;

-- query 12
SELECT user_id, user_name, COUNT(1) AS cnt FROM user_tags GROUP BY user_id, user_name ORDER BY user_id, user_name;

-- query 13
-- now add the coarse MV: 5 user_id groups
-- @skip_result_check=true
CREATE MATERIALIZED VIEW coarse_mv
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT user_id, COUNT(1) AS cnt FROM user_tags GROUP BY user_id;

-- query 14
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW coarse_mv WITH SYNC MODE;

-- query 15
-- both MVs are valid for the coarse query; the CBO must pick the smaller
-- coarse MV (5 rows, direct mapping) over the fine MV (25 rows, rollup)
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: coarse_mv
-- @explain_not_contains=rewritten with mv: fine_mv
SELECT user_id, COUNT(1) FROM user_tags GROUP BY user_id;

-- query 16
SELECT user_id, COUNT(1) AS cnt FROM user_tags GROUP BY user_id ORDER BY user_id;

-- query 17
-- SUM-only query: the fine MV still serves it via rollup (coarse MV has no sum)
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: fine_mv
SELECT user_id, SUM(tag_id) FROM user_tags GROUP BY user_id;

-- query 18
SELECT user_id, SUM(tag_id) AS total FROM user_tags GROUP BY user_id ORDER BY user_id;

-- query 19
-- @skip_result_check=true
DROP MATERIALIZED VIEW coarse_mv;

-- query 20
-- @skip_result_check=true
DROP MATERIALIZED VIEW fine_mv;

-- query 21
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.user_tags FORCE;

-- query 22
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 23
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
