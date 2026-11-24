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
-- The first snapshot remains in history without a branch or tag. Current
-- and historical reads have different schemas, and CTAS writes the old one.

-- query 1
-- @result_contains=SPARK_UNREFERENCED_HISTORY_READY
shell: set -eu
repo="${NOVAROCKS_WORKSPACE_ROOT:-.}"
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-unreferenced-history-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "$repo/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_UNREFERENCED_HISTORY_READY") {
  Iru5SparkCommitInterop.createUnreferencedHistory(spark, "ice_rest.${case_db}.history_${uuid0}")
}
SPARK_SCALA
"$repo/docker/iceberg-rest/spark-shell.sh" "$tmp_scala"

-- query 2
SELECT current_rows.added, historical_rows.*
FROM iceberg_rest_${suite_uuid0}.${case_db}.history_${uuid0} current_rows
CROSS JOIN iceberg_rest_${suite_uuid0}.${case_db}.history_${uuid0}
  FOR VERSION AS OF 42 historical_rows
WHERE current_rows.id = 2;

-- query 3
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.${case_db}.historical_copy_${uuid0} AS
SELECT * FROM iceberg_rest_${suite_uuid0}.${case_db}.history_${uuid0}
  FOR VERSION AS OF 42;

-- query 4
SELECT * FROM iceberg_rest_${suite_uuid0}.${case_db}.historical_copy_${uuid0};

-- query 5
-- @skip_result_check=true
MERGE INTO iceberg_rest_${suite_uuid0}.${case_db}.history_${uuid0} target
USING (
  SELECT id + 2 AS id, value
  FROM iceberg_rest_${suite_uuid0}.${case_db}.history_${uuid0} FOR VERSION AS OF 42
) source ON target.id = source.id
WHEN NOT MATCHED THEN INSERT (id, value, added) VALUES (source.id, source.value, 9);

-- query 6
SELECT id, value, added FROM iceberg_rest_${suite_uuid0}.${case_db}.history_${uuid0} WHERE id = 3;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.${case_db}.historical_copy_${uuid0};
DROP TABLE iceberg_rest_${suite_uuid0}.${case_db}.history_${uuid0};
