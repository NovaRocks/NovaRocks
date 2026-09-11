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
-- @tags=mv,iceberg,ivm,row_lineage,join,a11,base_drop,referenced,error
-- Test Point: Join IMV blocks refresh when a referenced column on either base is dropped.
-- Method: Create a two-base join MV, drop a right-base projected column through Spark, then refresh.
-- Scope: Multi-base A11 schema contract, current Iceberg schema validation, metadata-only evolution.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_a11_drop_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_a11_drop_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_a11_drop_${uuid0}.ns_${uuid0}.join_left_${uuid0} (
  id INT NOT NULL,
  rid INT,
  amount INT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
CREATE TABLE ice_ivm_join_a11_drop_${uuid0}.ns_${uuid0}.join_right_${uuid0} (
  rid INT NOT NULL,
  label STRING
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_ivm_join_a11_drop_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES
  (1, 10, 100),
  (2, 20, 200);
INSERT INTO ice_ivm_join_a11_drop_${uuid0}.ns_${uuid0}.join_right_${uuid0} VALUES
  (10, 'old-a'),
  (20, 'old-b');

-- query 2
-- @skip_result_check=true
SET CATALOG ice_ivm_join_a11_drop_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT l.id, l.amount, r.label
FROM join_left_${uuid0} AS l
JOIN join_right_${uuid0} AS r ON l.rid = r.rid;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 4
SELECT id, amount, label
FROM join_mv_${uuid0}
ORDER BY id, label;

-- query 5
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-join-a11-drop-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.join_right_${uuid0} DROP COLUMN label;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- @skip_result_check=true
INSERT INTO ice_ivm_join_a11_drop_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES
  (3, 20, 300);

-- query 7
-- @expect_error=was dropped from
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_mv_${uuid0};
DROP TABLE ice_ivm_join_a11_drop_${uuid0}.ns_${uuid0}.join_left_${uuid0} FORCE;
DROP TABLE ice_ivm_join_a11_drop_${uuid0}.ns_${uuid0}.join_right_${uuid0} FORCE;
DROP DATABASE ice_ivm_join_a11_drop_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_a11_drop_${uuid0};
