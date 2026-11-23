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
-- Each pad fits its 1 MiB limit. CONCAT produces one payload of
-- 17 * 1048576 - (1 + ... + 17) bytes for the single input row.
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm13_wide_${uuid0}
SELECT BIGINT '1',
       concat(
           rpad('', 1048576 - n, 'x'),
           rpad('', 1048575 - n, 'x'),
           rpad('', 1048574 - n, 'x'),
           rpad('', 1048573 - n, 'x'),
           rpad('', 1048572 - n, 'x'),
           rpad('', 1048571 - n, 'x'),
           rpad('', 1048570 - n, 'x'),
           rpad('', 1048569 - n, 'x'),
           rpad('', 1048568 - n, 'x'),
           rpad('', 1048567 - n, 'x'),
           rpad('', 1048566 - n, 'x'),
           rpad('', 1048565 - n, 'x'),
           rpad('', 1048564 - n, 'x'),
           rpad('', 1048563 - n, 'x'),
           rpad('', 1048562 - n, 'x'),
           rpad('', 1048561 - n, 'x'),
           rpad('', 1048560 - n, 'x')
       )
FROM TABLE(sequence(1, 1)) AS offsets(n);

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
