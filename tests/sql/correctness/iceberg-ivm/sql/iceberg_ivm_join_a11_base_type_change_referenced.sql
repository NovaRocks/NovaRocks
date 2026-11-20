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
-- @tags=mv,iceberg,ivm,row_lineage,join,a11,base_type_change,referenced,error
-- Test Point: Join IMV blocks refresh when a referenced column on either base changes type.
-- Method: Create a two-base join MV, widen a left-base projected column through Spark, then refresh.
-- Scope: Multi-base A11 schema contract, current Iceberg schema validation, metadata-only evolution.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_join_a11_type_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_join_a11_type_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_left_${uuid0} (
  id INT NOT NULL,
  rid INT,
  amount INT
) WITH (format_version = 3);
CREATE TABLE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_right_${uuid0} (
  rid INT NOT NULL,
  label STRING
) WITH (format_version = 3);
INSERT INTO ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES
  ROW(1, 10, 100),
  ROW(2, 20, 200);
INSERT INTO ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_right_${uuid0} VALUES
  ROW(10, 'old-a'),
  ROW(20, 'old-b');

-- query 2
-- @skip_result_check=true
USE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW join_mv_${uuid0} AS SELECT l.id, l.amount, r.label
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
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-join-a11-type-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.join_left_${uuid0} ALTER COLUMN amount TYPE bigint;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- @skip_result_check=true
INSERT INTO ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES
  ROW(3, 20, 300);

-- query 7
-- The refusal names the occurrence whose reference changed shape, which
-- is what tells the two sides of a join apart.
-- @expect_error=MV source field type or nullability changed for relation occurrence
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_mv_${uuid0};
DROP TABLE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_left_${uuid0} ;
DROP TABLE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_right_${uuid0} ;
DROP SCHEMA ice_ivm_join_a11_type_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_a11_type_${uuid0};
