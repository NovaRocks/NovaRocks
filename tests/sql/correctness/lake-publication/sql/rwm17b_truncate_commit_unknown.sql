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

-- query 1
-- @skip_result_check=true
CREATE SCHEMA lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test (id BIGINT)
WITH (format_version = 3);
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test
VALUES (1), (2), (3);

-- query 2
-- One commit succeeds at the real REST Catalog. The proxy withholds the
-- response and exact marker readback until the original SQL statement exits.
-- The fault guard requires a readback attempt and rejects any second dispatch.
-- @publication_catalog_fault=table-commit,after-commit-before-response-hide-table-load
-- @publication_catalog_fault_target=namespace=ns_${uuid0},table=rows_under_test
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit during truncate: [CommitUnknown]
TRUNCATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test;

-- query 3
SELECT COUNT(*) AS rows_after_truncate
FROM lake_publication_${suite_uuid0}.ns_${uuid0}.rows_under_test;

-- query 4
-- Spark reads the real Catalog directly after the proxy guard has cleared.
-- Read both actual snapshots to prove the published and historical objects
-- remained usable after the statement returned unknown.
-- @result_contains=RWM17B_TRUNCATE_COMMIT_UNKNOWN_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/rwm17b-truncate-commit-unknown-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("RWM17B_TRUNCATE_COMMIT_UNKNOWN_OK") {
  val qualified = "ice_rest.ns_${uuid0}.rows_under_test"
  val table = Iru5SparkCommitInterop.table(spark, qualified)
  val current = table.currentSnapshot()
  val parent = table.snapshot(current.parentId())
  require(current.operation() == "delete" && parent.operation() == "append",
    "TRUNCATE did not publish a delete on the seed append")
  require(table.snapshots().asScala.size == 2, "TRUNCATE created an extra snapshot")
  require(current.summary().get("deleted-records") == "3" &&
    current.summary().get("total-records") == "0" &&
    current.summary().get("total-data-files") == "0", "TRUNCATE summary differs from its committed effect")
  for (snapshot <- Seq(parent, current)) {
    require(table.io().newInputFile(snapshot.manifestListLocation()).getLength() > 0L,
      "A published manifest list lost its storage object")
    Iru5SparkCommitInterop.assertManifestLengths(table, snapshot)
  }
  require(spark.sql(s"SELECT id FROM $qualified").count() == 0L,
    "Spark sees rows after the committed TRUNCATE")
  val historic = spark.sql(s"SELECT id FROM $qualified VERSION AS OF ${parent.snapshotId()} ORDER BY id")
    .collect().toVector.map(_.getLong(0))
  require(historic == Vector(1L, 2L, 3L), "The seed snapshot lost its historical data")
}
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)" || {
  printf '%s\n' "$spark_out"
  exit 1
}
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | tr '\r' '\n' | grep -Fx 'RWM17B_TRUNCATE_COMMIT_UNKNOWN_OK'

-- query 5
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA lake_publication_${suite_uuid0}.ns_${uuid0} CASCADE;
