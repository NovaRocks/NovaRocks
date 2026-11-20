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
CREATE SCHEMA IF NOT EXISTS iceberg_compat_${suite_uuid0}.${case_db};
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm13_wide_${uuid0}
  (id BIGINT, payload VARCHAR);
-- Each repeat fits the scalar function's 1 MiB limit. The aggregate produces
-- one string of 17 * 1048576 - (1 + ... + 17) bytes for a single input row.
SET SESSION group_concat_max_len = 17825792;
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm13_wide_${uuid0}
SELECT CAST(1 AS BIGINT),
       string_agg(repeat('x', 1048576 - generate_series), '')
FROM TABLE(generate_series(1, 17));

-- query 2
-- Spark reads the stored value independently of NovaRocks' scan batch budget.
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-wide-write-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
SELECT concat('RWM13_WIDE=', CAST(count(*) AS STRING), ':',
             CAST(max(id) AS STRING), ':', CAST(max(length(payload)) AS STRING))
FROM ice_rest.${case_db}.rwm13_wide_${uuid0};
SPARK_SQL
if ! spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql" 2>&1)"; then
  printf '%s\n' "$spark_out"
  exit 1
fi
printf '%s\n' "$spark_out" | grep '^RWM13_WIDE='

-- query 3
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm13_wide_${uuid0} ;
