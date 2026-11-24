-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @sequential=true
-- Spark writes two small files in one append and verifies the rewrite
-- through the committed manifest list, raw Avro entries, and physical rows.

-- query 1
-- @result_contains=SPARK_OPTIMIZE_LINEAGE_READY
shell: set -eu
repo="${NOVAROCKS_WORKSPACE_ROOT:-.}"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-optimize-prepare-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "$repo/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_OPTIMIZE_LINEAGE_READY") {
  Iru5SparkCommitInterop.createOptimizeLineage(spark, "ice_rest.${case_db}.optimize_assigned_lineage_${uuid0}", false)
}
SPARK_SCALA
"$repo/docker/iceberg-rest/spark-shell.sh" "$tmp_scala"

-- query 2
-- @skip_result_check=true
ALTER TABLE iceberg_compat_${suite_uuid0}.${case_db}.optimize_assigned_lineage_${uuid0} EXECUTE optimize;

-- query 3
-- @result_contains=SPARK_OPTIMIZE_LINEAGE_VERIFIED
shell: set -eu
repo="${NOVAROCKS_WORKSPACE_ROOT:-.}"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-optimize-verify-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "$repo/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_OPTIMIZE_LINEAGE_VERIFIED") {
  Iru5SparkCommitInterop.verifyOptimizeLineage(spark, "ice_rest.${case_db}.optimize_assigned_lineage_${uuid0}", false)
}
SPARK_SCALA
"$repo/docker/iceberg-rest/spark-shell.sh" "$tmp_scala"

-- query 4
SELECT count(*) AS rewritten_rows
FROM iceberg_compat_${suite_uuid0}.${case_db}.optimize_assigned_lineage_${uuid0};

-- query 5
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.optimize_assigned_lineage_${uuid0};
