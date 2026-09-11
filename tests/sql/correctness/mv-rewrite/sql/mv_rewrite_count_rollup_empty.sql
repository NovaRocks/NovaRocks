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
-- Test Objective (migrated from materialized-view/test_count_rollup_with_empty_table):
-- COUNT rollup correctness over an EMPTY (zero-row, but snapshot-bearing) base
-- table and across the first insert:
-- 1. With an empty refreshed MV, scalar COUNT(col) must return one row of 0
--    (COALESCE rollup semantics), group-by rollups return the empty set, and
--    direct 1:1 mapping returns the empty set — whether or not the CBO picks
--    the (also empty) MV alternative, results must be identical.
-- 2. COUNT(user_id) has no matching materialized aggregate -> never rewrites.
-- 3. After inserting one row WITHOUT refresh, strict snapshot freshness must
--    prevent the rewrite, and results must reflect the base table.
-- 4. After REFRESH the rewrite is eligible again and results stay correct.
--
-- NOTE on EXPLAIN assertions: with a zero-row MV and zero/one-row base both
-- alternatives cost ~nothing, so hit assertions would be cost-flaky; this case
-- asserts only the deterministic MISS sides (freshness, no matching agg) and
-- pins everything else through result goldens.

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
-- Give the empty table a real snapshot so the MV refresh records a pin the
-- strict freshness check can compare against.
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.user_tags
SELECT '2023-04-13', 1, 'a', 1 FROM TABLE(generate_series(1, 1)) t(n) WHERE n < 0;

-- query 5
-- @skip_result_check=true
SET CATALOG mvrw_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW cnt_mv
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT user_id, dt, COUNT(tag_id) AS cnt FROM user_tags GROUP BY user_id, dt;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW cnt_mv WITH SYNC MODE;

-- query 9
-- scalar COUNT over the empty table: exactly one row of 0
SELECT COUNT(tag_id) AS c FROM user_tags;

-- query 10
-- group-by rollup over the empty table: empty set
SELECT user_id, COUNT(tag_id) AS c FROM user_tags GROUP BY user_id ORDER BY user_id;

-- query 11
-- direct 1:1 mapping over the empty table: empty set
SELECT user_id, dt, COUNT(tag_id) AS c FROM user_tags GROUP BY user_id, dt ORDER BY user_id, dt;

-- query 12
-- COUNT(user_id) is not materialized -> deterministic MISS regardless of cost
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT COUNT(user_id) FROM user_tags;

-- query 13
SELECT COUNT(user_id) AS c FROM user_tags;

-- query 14
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.user_tags VALUES ('2023-04-13', 1, 'a', 1);

-- query 15
-- base advanced past the MV pin -> strict freshness MISS
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT COUNT(tag_id) FROM user_tags;

-- query 16
-- result must come from the base table (1, not the stale MV's 0)
SELECT COUNT(tag_id) AS c FROM user_tags;

-- query 17
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT user_id, COUNT(tag_id) FROM user_tags GROUP BY user_id;

-- query 18
SELECT user_id, COUNT(tag_id) AS c FROM user_tags GROUP BY user_id ORDER BY user_id;

-- query 19
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW cnt_mv WITH SYNC MODE;

-- query 20
-- fresh again: results stay correct (rewrite eligibility restored; the CBO
-- choice over one row is not asserted)
SELECT COUNT(tag_id) AS c FROM user_tags;

-- query 21
SELECT user_id, dt, COUNT(tag_id) AS c FROM user_tags GROUP BY user_id, dt ORDER BY user_id, dt;

-- query 22
-- compensation predicates on group-by keys with empty results
SELECT user_id, COUNT(tag_id) AS c FROM user_tags WHERE user_id = 2 GROUP BY user_id ORDER BY user_id;

-- query 23
SELECT user_id, COUNT(tag_id) AS c FROM user_tags WHERE user_id > 2 GROUP BY user_id ORDER BY user_id;

-- query 24
-- @skip_result_check=true
DROP MATERIALIZED VIEW cnt_mv;

-- query 25
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.user_tags FORCE;

-- query 26
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 27
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
