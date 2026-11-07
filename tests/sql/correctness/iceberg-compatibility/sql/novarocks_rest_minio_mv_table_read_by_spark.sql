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
-- Validate the external-engine read interop contract for Iceberg MVs (W5 of
-- the IMV lake-native umbrella): after NovaRocks creates and refreshes an
-- Iceberg MV in a REST catalog, an external engine (Spark) can read the MV
-- table's visible materialized columns directly. The same Iceberg table also
-- carries NovaRocks internal columns, which are visible at schema level and
-- intentionally outside the public read-column contract. See
-- docker/iceberg-rest/README.md#external-engine-mv-read-interop for the
-- narrative walkthrough of this contract.

-- query 1
CREATE DATABASE IF NOT EXISTS iceberg_compat_${suite_uuid0}.${case_db};

-- query 2
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.mv_interop_base_${uuid0} (
  id BIGINT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);

-- query 3
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.mv_interop_base_${uuid0}
VALUES
  (1, 10),
  (2, 20),
  (3, 30);

-- query 4
-- @skip_result_check=true
USE iceberg_compat_${suite_uuid0}.${case_db};
CREATE MATERIALIZED VIEW mv_nr_interop_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, amount
FROM mv_interop_base_${uuid0}
WHERE amount >= 20;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_nr_interop_${uuid0};

-- query 6
SELECT id, amount
FROM mv_nr_interop_${uuid0}
ORDER BY id;

-- query 7
-- External read of the MV table's visible columns. Spark reads the
-- already-materialized rows from the Iceberg MV table; it is not re-running
-- the base query.
-- @result_contains=SPARK_MV_TABLE_READ_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-mv-table-read-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
SELECT id, amount
FROM ice_rest.${case_db}.mv_nr_interop_${uuid0}
ORDER BY id;
SPARK_SQL
view_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql")"
echo "$view_out" | tr -s '[:space:]' ' ' | grep -F "2 20" >/dev/null
echo "$view_out" | tr -s '[:space:]' ' ' | grep -F "3 30" >/dev/null
# A visible-column read must not return the internal apply-key column that the
# MV table physically carries.
if echo "$view_out" | grep -F "__nova_base_row_id" >/dev/null; then
  echo "visible-column MV read leaked internal column __nova_base_row_id" >&2
  exit 1
fi
printf 'SPARK_MV_TABLE_READ_OK\n'

-- query 8
-- Contrast: the same MV table exposes the internal apply-key
-- column at schema level. Uses DESCRIBE rather than SELECT * so the assertion
-- is stable against column ordering/formatting rather than parsing data rows.
-- @result_contains=SPARK_MV_SCHEMA_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-mv-schema-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
DESCRIBE ice_rest.${case_db}.mv_nr_interop_${uuid0};
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql" \
  | grep -F "__nova_base_row_id" >/dev/null
printf 'SPARK_MV_SCHEMA_OK\n'

-- query 9
-- @cleanup=true
-- @skip_result_check=true
USE iceberg_compat_${suite_uuid0}.${case_db};
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.mv_nr_interop_${uuid0};
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.mv_interop_base_${uuid0} FORCE;
