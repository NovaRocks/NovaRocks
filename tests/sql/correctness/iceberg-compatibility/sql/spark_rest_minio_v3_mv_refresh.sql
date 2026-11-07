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

-- @order_sensitive=true
-- @sequential=true
-- Validate Iceberg MV refresh sees Spark-written base-table snapshots.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-mv-refresh-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db};

DROP TABLE IF EXISTS ice_rest.${case_db}.spark_v3_mv_base_${uuid0};

CREATE TABLE ice_rest.${case_db}.spark_v3_mv_base_${uuid0} (
  id BIGINT,
  metric INT
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.row-lineage' = 'true',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.${case_db}.spark_v3_mv_base_${uuid0} VALUES
  (1, 10),
  (2, 20),
  (3, 30);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
-- @skip_result_check=true
USE iceberg_compat_${suite_uuid0}.${case_db};
CREATE MATERIALIZED VIEW mv_spark_refresh_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, metric
FROM spark_v3_mv_base_${uuid0}
WHERE metric >= 20;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_spark_refresh_${uuid0};

-- query 4
SELECT id, metric
FROM mv_spark_refresh_${uuid0}
ORDER BY id;

-- query 5
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-mv-refresh-append-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
INSERT INTO ice_rest.${case_db}.spark_v3_mv_base_${uuid0} VALUES
  (4, 40),
  (5, 5),
  (6, 60);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_spark_refresh_${uuid0};

-- query 7
SELECT id, metric
FROM mv_spark_refresh_${uuid0}
ORDER BY id;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.mv_spark_refresh_${uuid0};
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_mv_base_${uuid0} FORCE;
