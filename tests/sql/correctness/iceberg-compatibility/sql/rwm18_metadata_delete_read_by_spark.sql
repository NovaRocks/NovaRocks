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
-- An external Spark reader validates both NovaRocks delete snapshots on v2 and v3.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA IF NOT EXISTS iceberg_compat_${suite_uuid0}.${case_db};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v2_${uuid0} (id BIGINT, part INTEGER, score INTEGER)
WITH (partitioning = ARRAY['part'], format_version = 2);
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v2_${uuid0} VALUES ROW(1, 10, 10), ROW(2, 10, 20), ROW(3, 30, 30);

-- query 3
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v2_${uuid0} WHERE id = 1;

-- query 4
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v2_${uuid0} WHERE part = 10;

-- query 5
SELECT id, part, score FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v2_${uuid0} ORDER BY id;

-- query 6
-- @skip_result_check=true
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v3_${uuid0} (id BIGINT, part INTEGER, score INTEGER)
WITH (partitioning = ARRAY['part'], format_version = 3);
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v3_${uuid0} VALUES ROW(1, 10, 10), ROW(2, 10, 20), ROW(3, 30, 30);

-- query 7
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v3_${uuid0} WHERE id = 1;

-- query 8
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v3_${uuid0} WHERE part = 10;

-- query 9
SELECT id, part, score FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v3_${uuid0} ORDER BY id;

-- query 10
-- @skip_result_check=true
-- @result_contains=RWM18_SPARK_METADATA_DELETE_OK
shell: set -eu
evidence="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/rwm18/sql-interop-${uuid0}"
mkdir -p "$evidence"
tmp_scala="$(mktemp "$evidence/readback-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("RWM18_SPARK_METADATA_DELETE_OK") {
  for ((version, deleteFormat) <- Seq((2, "PARQUET"), (3, "PUFFIN"))) {
    val qualified = "ice_rest.${case_db}.rwm18_spark_v" + version + "_${uuid0}"
    val t = Iru5SparkCommitInterop.table(spark, qualified)
    t.refresh()
    require(Iru5SparkCommitInterop.metadata(t).formatVersion() == version)
    val latest = t.currentSnapshot()
    require(latest != null && latest.operation() == "delete" && latest.parentId() != null)
    val prior = t.snapshot(latest.parentId().longValue())
    require(prior != null && prior.operation() == "delete")
    Iru5SparkCommitInterop.assertManifestLengths(t, prior)
    require(latest.summary().get("deleted-records") == "2")
    require(latest.summary().get("removed-position-deletes") == "1")
    if (version == 3) require(latest.summary().get("removed-dvs") == "1")

    def liveDeleteFiles(snapshotId: Long) = {
      val tasks = t.newScan().useSnapshot(snapshotId).planFiles()
      try tasks.asScala.flatMap(_.deletes().asScala)
        .map(file => (file.location().toString, file.format().name(), file.recordCount())).toSet
      finally tasks.close()
    }
    val oldDeletes = liveDeleteFiles(prior.snapshotId())
    require(oldDeletes.map(_._2) == Set(deleteFormat) && oldDeletes.toSeq.map(_._3).sum == 1L,
      s"Unexpected prior delete files for v$version: $oldDeletes")
    require(liveDeleteFiles(latest.snapshotId()).isEmpty,
      s"A removed partition retains a delete file on v$version")
    require(spark.sql(s"SELECT count(*) FROM $qualified.delete_files").head().getLong(0) == 0L,
      s"The final snapshot still lists a live delete entry on v$version")

    def rows(snapshotId: Long) =
      spark.sql(s"SELECT id, part, score FROM $qualified VERSION AS OF $snapshotId ORDER BY id")
        .collect().toVector.map(r => (r.getLong(0), r.getInt(1), r.getInt(2)))
    require(rows(prior.snapshotId()) == Vector((2L, 10, 20), (3L, 30, 30)))
    require(rows(latest.snapshotId()) == Vector((3L, 30, 30)))
    println("RWM18_SPARK_VERSION " + version + " prior=" + prior.snapshotId() + " latest=" + latest.snapshotId())
  }
}
SPARK_SCALA
if ! "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" > "$evidence/spark-readback.log" 2>&1; then
  cat "$evidence/spark-readback.log"
  exit 1
fi
grep -Fx "RWM18_SPARK_METADATA_DELETE_OK" "$evidence/spark-readback.log"

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v2_${uuid0};
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm18_spark_v3_${uuid0};
