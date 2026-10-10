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
-- Test Point: Spark compares changed row lineage to the actual committed Java
-- snapshot/entry sequences; updated IDs survive, new IDs remain disjoint.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0};
CREATE TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.mor_lineage_${uuid0} (id BIGINT, value INT)
TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true",
  "novarocks.update.mode"="merge-on-read");
INSERT INTO iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.mor_lineage_${uuid0} VALUES (1,10),(2,20),(3,30),(4,40);
UPDATE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.mor_lineage_${uuid0} SET value = 220 WHERE id = 2;

-- query 2
-- @result_contains=SPARK_MOR_UPDATE_LINEAGE_OK
shell: set -eu
interop_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/interop-${uuid0}"
mkdir -p "$interop_dir"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-iru5-interop-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_MOR_UPDATE_LINEAGE_OK") { Iru5SparkCommitInterop.verifyMor(spark, "ice_rest.nr_compat_${suite_uuid0}.mor_lineage_${uuid0}", "update") }
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)" || {
  printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_MOR_UPDATE_LINEAGE_OK.log"
  exit 1
}
printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_MOR_UPDATE_LINEAGE_OK.log"
printf '%s\n' "$spark_out" | grep -Fx 'SPARK_MOR_UPDATE_LINEAGE_OK'

-- query 3
-- @skip_result_check=true
MERGE INTO iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.mor_lineage_${uuid0} AS target
USING (SELECT 3 AS id, 330 AS value UNION ALL SELECT 5 AS id, 500 AS value) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET value = source.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value);

-- query 4
-- @result_contains=SPARK_MOR_MERGE_LINEAGE_OK
shell: set -eu
interop_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/interop-${uuid0}"
mkdir -p "$interop_dir"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-iru5-interop-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_MOR_MERGE_LINEAGE_OK") { Iru5SparkCommitInterop.verifyMor(spark, "ice_rest.nr_compat_${suite_uuid0}.mor_lineage_${uuid0}", "merge") }
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)" || {
  printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_MOR_MERGE_LINEAGE_OK.log"
  exit 1
}
printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_MOR_MERGE_LINEAGE_OK.log"
printf '%s\n' "$spark_out" | grep -Fx 'SPARK_MOR_MERGE_LINEAGE_OK'

-- query 5
-- @skip_result_check=true
INSERT INTO iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.mor_lineage_${uuid0} VALUES (6,600);

-- query 6
-- @result_contains=SPARK_MOR_APPEND_LINEAGE_OK
shell: set -eu
interop_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/interop-${uuid0}"
mkdir -p "$interop_dir"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-iru5-interop-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_MOR_APPEND_LINEAGE_OK") { Iru5SparkCommitInterop.verifyMor(spark, "ice_rest.nr_compat_${suite_uuid0}.mor_lineage_${uuid0}", "append") }
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)" || {
  printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_MOR_APPEND_LINEAGE_OK.log"
  exit 1
}
printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_MOR_APPEND_LINEAGE_OK.log"
printf '%s\n' "$spark_out" | grep -Fx 'SPARK_MOR_APPEND_LINEAGE_OK'

-- query 7
SELECT id, value FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.mor_lineage_${uuid0} ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.mor_lineage_${uuid0} FORCE;
