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
-- @tags=mv,iceberg,ivm,a11,aggregate,base_rename
-- A referenced rename makes the original definition stale and preserves the published bag.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_agg_a11_measure_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0}.orders (
  region VARCHAR,
  amount BIGINT
) WITH (format_version = 3);
INSERT INTO ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0}.orders VALUES
  ROW('east', 10),
  ROW('east', 20),
  ROW('west', 5);
USE ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv_${uuid0} AS SELECT region, COUNT(amount) AS c_amount, SUM(amount) AS s
FROM orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
SELECT region, c_amount, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 4
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-agg-measure-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.orders RENAME COLUMN amount TO gross_amount;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0}.orders VALUES ROW('east', 30), ROW('north', 7);

-- query 6
-- @expect_trino_error=VIEW_IS_STALE,USER_ERROR
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 7
SELECT region, c_amount, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 8
SELECT region, COUNT(gross_amount) AS c_amount, SUM(gross_amount) AS s
FROM orders
GROUP BY region
ORDER BY region;

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0}.orders ;
DROP SCHEMA ice_ivm_agg_a11_measure_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_a11_measure_${uuid0};
