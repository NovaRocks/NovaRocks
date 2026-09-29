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
-- @tags=mv,iceberg,ivm,delete_applicability,equality_delete,deletion_vector,endpoint_bag
-- Official Iceberg 1.11 writers construct immutable endpoint fixtures. Independent
-- integer/string row bags below are fixed before the NovaRocks verification run.
-- Each mutation is followed by an explicit incremental-plan assertion and compares
-- the incremental MV, a separately rebuilt MV, and a direct base read (rewrite off).
-- Stages: same-commit data+DV+equality; cumulative DV in the same immutable Puffin
-- plus equality replacement; equivalent artifact replacement; whole-file removal.
-- The expected signed bags preserve duplicate old and new row values.

-- query 1
-- @skip_result_check=true
-- @result_contains=UEA4G_IVM_STAGE_0_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea4g/ivm-delete-applicability/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/stage-0.scala"
uea_log="$uea_receipts/stage-0.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "uea4g_ivm_${uuid0}", "ivm")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  session.sql("CREATE TABLE ice_rest.uea4g_ivm_${uuid0}.base (id BIGINT NOT NULL, p INT NOT NULL, value STRING NOT NULL) USING iceberg TBLPROPERTIES ('format-version'='3', 'write.row-lineage'='true')")
  val t = Spark3Util.loadIcebergTable(session, "ice_rest.uea4g_ivm_${uuid0}.base")
  t.refresh()
  val rows = Seq(Seq(1L, 1, "one"), Seq(2L, 1, "dup"), Seq(2L, 1, "dup"), Seq(3L, 1, "three"), Seq(4L, 1, "four"))
  t.newAppend().appendFile(data(t, rows)).commit(); t.refresh()
  observe("ivm_initial", t, obj("writer_commit" -> "success", "independent_added_rows" -> rows, "independent_removed_rows" -> Seq.empty), Some(rows))
  println("UEA4G_IVM_STAGE_0_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^UEA4G_IVM_STAGE_0_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/stage-0.jsonl"
printf 'UEA4G_IVM_STAGE_0_OK\n'

-- query 2
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_4g_ivm_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
SET CATALOG ice_4g_ivm_${uuid0};
USE uea4g_ivm_${uuid0};
SET enable_materialized_view_rewrite = false;
CREATE MATERIALIZED VIEW incremental_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT id, p, value FROM base;
CREATE MATERIALIZED VIEW full_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT id, p, value FROM base;
REFRESH MATERIALIZED VIEW incremental_${uuid0};
REFRESH MATERIALIZED VIEW full_${uuid0} FULL;

-- query 3
SELECT 'base' AS source, id, p, value FROM base
UNION ALL SELECT 'full' AS source, id, p, value FROM full_${uuid0}
UNION ALL SELECT 'incremental' AS source, id, p, value FROM incremental_${uuid0}
ORDER BY source, id, p, value;

-- query 4
-- @skip_result_check=true
-- @result_contains=UEA4G_IVM_STAGE_1_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea4g/ivm-delete-applicability/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/stage-1.scala"
uea_log="$uea_receipts/stage-1.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "uea4g_ivm_${uuid0}", "ivm")
  import DeleteApplicabilityFixture._
  val t = Spark3Util.loadIcebergTable(org.apache.spark.sql.SparkSession.active, "ice_rest.uea4g_ivm_${uuid0}.base")
  t.refresh()
  val from = t.currentSnapshot().snapshotId()
  val fresh = data(t, Seq(Seq(2L, 1, "new-two"), Seq(5L, 1, "five"), Seq(6L, 1, "six")))
  val eq = equality(t, Seq(2L))
  val indexes = Seq(Seq(1L), Seq(1L, 2L)).map(ps => org.apache.iceberg.deletes.Deletes.toPositionIndex(org.apache.iceberg.io.CloseableIterable.withNoopClose[java.lang.Long](ps.map(Long.box).asJava)))
  val destination = output(t, ".puffin")
  val writer = org.apache.iceberg.puffin.Puffin.write(destination).createdBy(IcebergBuild.fullVersion()).build()
  val blobs = try indexes.map(ix => writer.write(new org.apache.iceberg.puffin.Blob("deletion-vector-v1", Seq(Int.box(MetadataColumns.ROW_POSITION.fieldId())).asJava, -1L, -1L, ix.serialize(), null, Map("referenced-data-file" -> fresh.location(), "cardinality" -> ix.cardinality().toString).asJava))) finally writer.close()
  val vectors = indexes.zip(blobs).map { case (ix, blob) => FileMetadata.deleteFileBuilder(t.spec()).ofPositionDeletes().withFormat(FileFormat.PUFFIN).withPath(destination.location()).withPartition(partition(t, 1)).withFileSizeInBytes(writer.fileSize()).withReferencedDataFile(fresh.location()).withContentOffset(blob.offset()).withContentSizeInBytes(blob.length()).withRecordCount(ix.cardinality()).build() }
  val immutableHash = java.security.MessageDigest.getInstance("SHA-256").digest(bytes(t.io(), destination.location())).map(b => f"${b & 0xff}%02x").mkString
  val next = vectors(1)
  t.updateProperties().set("uea4g.next-dv.path", next.location()).set("uea4g.next-dv.offset", next.contentOffset().toString).set("uea4g.next-dv.length", next.contentSizeInBytes().toString).set("uea4g.next-dv.file-size", next.fileSizeInBytes().toString).set("uea4g.next-dv.target", fresh.location()).set("uea4g.next-dv.sha256", immutableHash).commit()
  t.newRowDelta().addRows(fresh).addDeletes(vectors(0)).addDeletes(eq).commit(); t.refresh()
  val planned = t.newScan().planFiles(); val tasks = try planned.asScala.toVector finally planned.close()
  val newTask = tasks.find(_.file().location() == fresh.location()).get
  val oldTask = tasks.find(_.file().location() != fresh.location()).get
  require(newTask.file().dataSequenceNumber() == Long.box(2L))
  require(newTask.deletes().asScala.size == 1 && newTask.deletes().get(0).format() == FileFormat.PUFFIN && newTask.deletes().get(0).dataSequenceNumber() == Long.box(2L))
  require(oldTask.deletes().asScala.exists(d => d.content() == FileContent.EQUALITY_DELETES && d.dataSequenceNumber() == Long.box(2L)))
  observe("ivm_same_commit", t, obj("writer_commit" -> "success", "from_snapshot" -> from, "immutable_puffin_sha256" -> immutableHash, "next_blob" -> describe(next), "independent_removed_rows" -> Seq(Seq(2L, 1, "dup"), Seq(2L, 1, "dup")), "independent_added_rows" -> Seq(Seq(2L, 1, "new-two"), Seq(6L, 1, "six"))), Some(Seq(Seq(1L, 1, "one"), Seq(2L, 1, "new-two"), Seq(3L, 1, "three"), Seq(4L, 1, "four"), Seq(6L, 1, "six"))))
  println("UEA4G_IVM_STAGE_1_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^UEA4G_IVM_STAGE_1_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/stage-1.jsonl"
printf 'UEA4G_IVM_STAGE_1_OK\n'

-- query 5
-- @skip_result_check=true
-- @result_contains=source: IcebergDeltaTable
EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW incremental_${uuid0};

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW incremental_${uuid0};
REFRESH MATERIALIZED VIEW full_${uuid0} FULL;

-- query 7
SELECT 'base' AS source, id, p, value FROM base
UNION ALL SELECT 'full' AS source, id, p, value FROM full_${uuid0}
UNION ALL SELECT 'incremental' AS source, id, p, value FROM incremental_${uuid0}
ORDER BY source, id, p, value;

-- query 8
-- @skip_result_check=true
-- @result_contains=UEA4G_IVM_STAGE_2_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea4g/ivm-delete-applicability/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/stage-2.scala"
uea_log="$uea_receipts/stage-2.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "uea4g_ivm_${uuid0}", "ivm")
  import DeleteApplicabilityFixture._
  val t = Spark3Util.loadIcebergTable(org.apache.spark.sql.SparkSession.active, "ice_rest.uea4g_ivm_${uuid0}.base")
  t.refresh()
  val from = t.currentSnapshot().snapshotId()
  val planned = t.newScan().planFiles(); val tasks = try planned.asScala.toVector finally planned.close()
  val deletes = tasks.flatMap(_.deletes().asScala).groupBy(d => (d.location(), d.contentOffset())).values.map(_.head).toVector
  val previousDv = deletes.find(_.format() == FileFormat.PUFFIN).get
  val previousEq = deletes.find(_.content() == FileContent.EQUALITY_DELETES).get
  val properties = t.properties()
  val path = properties.get("uea4g.next-dv.path")
  val digest = java.security.MessageDigest.getInstance("SHA-256").digest(bytes(t.io(), path)).map(b => f"${b & 0xff}%02x").mkString
  require(digest == properties.get("uea4g.next-dv.sha256"), "Published Puffin bytes changed")
  val next = FileMetadata.deleteFileBuilder(t.spec()).ofPositionDeletes().withFormat(FileFormat.PUFFIN).withPath(path).withPartition(partition(t, 1)).withFileSizeInBytes(properties.get("uea4g.next-dv.file-size").toLong).withReferencedDataFile(properties.get("uea4g.next-dv.target")).withContentOffset(properties.get("uea4g.next-dv.offset").toLong).withContentSizeInBytes(properties.get("uea4g.next-dv.length").toLong).withRecordCount(2L).build()
  require(previousDv.location() == next.location() && previousDv.contentOffset() != next.contentOffset())
  t.newRowDelta().validateFromSnapshot(from).removeDeletes(previousDv).removeDeletes(previousEq).addDeletes(next).addDeletes(equality(t, Seq(2L))).commit(); t.refresh()
  observe("ivm_cumulative_and_equality_replacement", t, obj("writer_commit" -> "success", "from_snapshot" -> from, "immutable_puffin_sha256" -> digest, "independent_removed_rows" -> Seq(Seq(2L, 1, "new-two"), Seq(6L, 1, "six")), "independent_added_rows" -> Seq.empty), Some(Seq(Seq(1L, 1, "one"), Seq(3L, 1, "three"), Seq(4L, 1, "four"))))
  println("UEA4G_IVM_STAGE_2_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^UEA4G_IVM_STAGE_2_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/stage-2.jsonl"
printf 'UEA4G_IVM_STAGE_2_OK\n'

-- query 9
-- @skip_result_check=true
-- @result_contains=source: IcebergDeltaTable
EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW incremental_${uuid0};

-- query 10
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW incremental_${uuid0};
REFRESH MATERIALIZED VIEW full_${uuid0} FULL;

-- query 11
SELECT 'base' AS source, id, p, value FROM base
UNION ALL SELECT 'full' AS source, id, p, value FROM full_${uuid0}
UNION ALL SELECT 'incremental' AS source, id, p, value FROM incremental_${uuid0}
ORDER BY source, id, p, value;

-- query 12
-- @skip_result_check=true
-- @result_contains=UEA4G_IVM_STAGE_3_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea4g/ivm-delete-applicability/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/stage-3.scala"
uea_log="$uea_receipts/stage-3.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "uea4g_ivm_${uuid0}", "ivm")
  import DeleteApplicabilityFixture._
  val t = Spark3Util.loadIcebergTable(org.apache.spark.sql.SparkSession.active, "ice_rest.uea4g_ivm_${uuid0}.base")
  t.refresh()
  val from = t.currentSnapshot().snapshotId()
  val planned = t.newScan().planFiles(); val tasks = try planned.asScala.toVector finally planned.close()
  val deletes = tasks.flatMap(_.deletes().asScala).groupBy(d => (d.location(), d.contentOffset())).values.map(_.head).toVector
  val previousDv = deletes.find(_.format() == FileFormat.PUFFIN).get
  val previousEq = deletes.find(_.content() == FileContent.EQUALITY_DELETES).get
  val replacement = dv(t, Seq((previousDv.referencedDataFile(), 1L), (previousDv.referencedDataFile(), 2L))).head
  t.newRowDelta().validateFromSnapshot(from).removeDeletes(previousDv).removeDeletes(previousEq).addDeletes(replacement).addDeletes(equality(t, Seq(2L))).commit(); t.refresh()
  observe("ivm_equivalent_delete_replacement", t, obj("writer_commit" -> "success", "from_snapshot" -> from, "independent_removed_rows" -> Seq.empty, "independent_added_rows" -> Seq.empty), Some(Seq(Seq(1L, 1, "one"), Seq(3L, 1, "three"), Seq(4L, 1, "four"))))
  println("UEA4G_IVM_STAGE_3_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^UEA4G_IVM_STAGE_3_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/stage-3.jsonl"
printf 'UEA4G_IVM_STAGE_3_OK\n'

-- query 13
-- @skip_result_check=true
-- @result_contains=source: IcebergDeltaTable
EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW incremental_${uuid0};

-- query 14
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW incremental_${uuid0};
REFRESH MATERIALIZED VIEW full_${uuid0} FULL;

-- query 15
SELECT 'base' AS source, id, p, value FROM base
UNION ALL SELECT 'full' AS source, id, p, value FROM full_${uuid0}
UNION ALL SELECT 'incremental' AS source, id, p, value FROM incremental_${uuid0}
ORDER BY source, id, p, value;

-- query 16
-- @skip_result_check=true
-- @result_contains=UEA4G_IVM_STAGE_4_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea4g/ivm-delete-applicability/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/stage-4.scala"
uea_log="$uea_receipts/stage-4.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "uea4g_ivm_${uuid0}", "ivm")
  import DeleteApplicabilityFixture._
  val t = Spark3Util.loadIcebergTable(org.apache.spark.sql.SparkSession.active, "ice_rest.uea4g_ivm_${uuid0}.base")
  t.refresh()
  val from = t.currentSnapshot().snapshotId()
  val planned = t.newScan().planFiles(); val tasks = try planned.asScala.toVector finally planned.close()
  val original = tasks.find(_.file().recordCount() == 5L).get.file()
  val added = Seq(Seq(7L, 1, "seven"), Seq(8L, 1, "eight"), Seq(8L, 1, "eight"))
  t.newOverwrite().deleteFile(original).addFile(data(t, added)).commit(); t.refresh()
  observe("ivm_removed_and_added_files", t, obj("writer_commit" -> "success", "from_snapshot" -> from, "independent_removed_rows" -> Seq(Seq(1L, 1, "one"), Seq(3L, 1, "three"), Seq(4L, 1, "four")), "independent_added_rows" -> added), Some(added))
  println("UEA4G_IVM_STAGE_4_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^UEA4G_IVM_STAGE_4_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/stage-4.jsonl"
printf 'UEA4G_IVM_STAGE_4_OK\n'

-- query 17
-- @skip_result_check=true
-- @result_contains=source: IcebergDeltaTable
EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW incremental_${uuid0};

-- query 18
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW incremental_${uuid0};
REFRESH MATERIALIZED VIEW full_${uuid0} FULL;

-- query 19
SELECT 'base' AS source, id, p, value FROM base
UNION ALL SELECT 'full' AS source, id, p, value FROM full_${uuid0}
UNION ALL SELECT 'incremental' AS source, id, p, value FROM incremental_${uuid0}
ORDER BY source, id, p, value;

-- query 20
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW incremental_${uuid0};
DROP MATERIALIZED VIEW full_${uuid0};
DROP TABLE ice_4g_ivm_${uuid0}.uea4g_ivm_${uuid0}.base FORCE;
DROP DATABASE ice_4g_ivm_${uuid0}.uea4g_ivm_${uuid0};
DROP CATALOG ice_4g_ivm_${uuid0};
