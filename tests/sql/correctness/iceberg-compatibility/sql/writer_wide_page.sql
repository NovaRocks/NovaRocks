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

-- query 1
-- @skip_result_check=true
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-wide-source-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db};
CREATE TABLE ice_rest.${case_db}.rwm13_wide_source_${uuid0}
USING iceberg TBLPROPERTIES ('format-version' = '3') AS
SELECT CAST(1 AS BIGINT) AS id, repeat('x', 17825639) AS payload;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm13_wide_${uuid0}
    (id BIGINT, payload VARCHAR);
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm13_wide_${uuid0}
SELECT id, payload FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm13_wide_source_${uuid0};

-- query 3
-- Spark reads the stored value independently of NovaRocks' scan batch budget.
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-wide-write-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
SELECT concat('RWM13_WIDE=', CAST(count(*) AS STRING), ':',
             COALESCE(CAST(max(id) AS STRING), 'NULL'), ':', COALESCE(CAST(max(length(payload)) AS STRING), 'NULL'))
FROM ice_rest.${case_db}.rwm13_wide_${uuid0};
SPARK_SQL
if ! spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql" 2>&1)"; then
  printf '%s\n' "$spark_out"
  exit 1
fi
if ! printf '%s\n' "$spark_out" | grep '^RWM13_WIDE='; then
  printf '%s\n' "$spark_out" >&2
  exit 1
fi

-- query 4
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm13_wide_${uuid0} ;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm13_wide_source_${uuid0};
