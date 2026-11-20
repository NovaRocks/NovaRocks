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
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm13_affine_${uuid0}
  (id BIGINT, region VARCHAR) WITH (partitioning = ARRAY['region']);
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm13_affine_${uuid0}
VALUES ROW(1, 'east'), ROW(2, 'east'), ROW(3, 'west'), ROW(4, 'west');

-- query 2
-- @result_contains=SPARK_AFFINE_WRITE_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-affine-write-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
SELECT CASE
  WHEN COUNT(*) = 4
    AND SUM(CASE WHEN id IN (1, 2) AND region = 'east' THEN 1 ELSE 0 END) = 2
    AND SUM(CASE WHEN id IN (3, 4) AND region = 'west' THEN 1 ELSE 0 END) = 2
    AND COUNT(DISTINCT id) = 4
  THEN 'SPARK_AFFINE_WRITE_OK'
  ELSE 'SPARK_AFFINE_WRITE_BAD'
END AS row_bag_check
FROM ice_rest.${case_db}.rwm13_affine_${uuid0};
SELECT CASE
  WHEN COUNT(*) = 2 AND MIN(record_count) = 2 AND MAX(record_count) = 2
    AND MIN(file_size_in_bytes) > 0
  THEN 'SPARK_AFFINE_FILES_OK'
  ELSE 'SPARK_AFFINE_FILES_BAD'
END AS file_check
FROM ice_rest.${case_db}.rwm13_affine_${uuid0}.files WHERE content = 0;
SPARK_SQL
if ! spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql" 2>&1)"; then
  printf '%s\n' "$spark_out"
  exit 1
fi
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -Fx SPARK_AFFINE_WRITE_OK >/dev/null
printf '%s\n' "$spark_out" | grep -Fx SPARK_AFFINE_FILES_OK >/dev/null
printf 'SPARK_AFFINE_WRITE_OK\n'

-- query 3
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm13_affine_${uuid0} ;
