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
-- Test Point: replacing one logical DV must retain another blob in the same
-- physical Puffin. Independent Java/Spark APIs inspect complete identities.

-- query 1
-- @result_contains=SPARK_SHARED_PUFFIN_READY
shell: set -eu
interop_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/interop-${uuid0}"
mkdir -p "$interop_dir"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-iru5-interop-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_SHARED_PUFFIN_READY") { Iru5SparkCommitInterop.createSharedPuffin(spark, "ice_rest.nr_compat_${suite_uuid0}.shared_puffin_${uuid0}") }
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)" || {
  printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_SHARED_PUFFIN_READY.log"
  exit 1
}
printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_SHARED_PUFFIN_READY.log"
printf '%s\n' "$spark_out" | grep -Fx 'SPARK_SHARED_PUFFIN_READY'

-- query 2
-- @skip_result_check=true
DELETE FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.shared_puffin_${uuid0} WHERE id = 2;

-- query 3
-- @result_contains=SPARK_SHARED_PUFFIN_DELETE_OK
shell: set -eu
interop_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/interop-${uuid0}"
mkdir -p "$interop_dir"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-iru5-interop-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_SHARED_PUFFIN_DELETE_OK") { Iru5SparkCommitInterop.verifySharedPuffin(spark, "ice_rest.nr_compat_${suite_uuid0}.shared_puffin_${uuid0}", false) }
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)" || {
  printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_SHARED_PUFFIN_DELETE_OK.log"
  exit 1
}
printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_SHARED_PUFFIN_DELETE_OK.log"
printf '%s\n' "$spark_out" | grep -Fx 'SPARK_SHARED_PUFFIN_DELETE_OK'

-- query 4
-- @skip_result_check=true
UPDATE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.shared_puffin_${uuid0} SET value = 130 WHERE id = 3;

-- query 5
-- @result_contains=SPARK_SHARED_PUFFIN_UPDATE_OK
shell: set -eu
interop_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/interop-${uuid0}"
mkdir -p "$interop_dir"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-iru5-interop-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_SHARED_PUFFIN_UPDATE_OK") { Iru5SparkCommitInterop.verifySharedPuffin(spark, "ice_rest.nr_compat_${suite_uuid0}.shared_puffin_${uuid0}", true) }
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)" || {
  printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_SHARED_PUFFIN_UPDATE_OK.log"
  exit 1
}
printf '%s\n' "$spark_out" | tee "$interop_dir/SPARK_SHARED_PUFFIN_UPDATE_OK.log"
printf '%s\n' "$spark_out" | grep -Fx 'SPARK_SHARED_PUFFIN_UPDATE_OK'

-- query 6
SELECT id, value FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.shared_puffin_${uuid0} ORDER BY id;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.shared_puffin_${uuid0} FORCE;
