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
-- This native 1FE+3BE case reads REST-catalog metadata through Spark so it
-- proves the StatisticsFile attachment, not merely a frontend presentation.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0};

-- query 2
-- Start with maintenance disabled so this nonempty parent deliberately has no
-- Puffin. Enabling it for the second append must not publish an NDV that only
-- describes the second row.
-- @skip_result_check=true
CREATE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} (
    id BIGINT,
    k BIGINT
) TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');

-- query 3
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} VALUES (1, 10);

-- query 4
-- @skip_result_check=true
ALTER TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}
SET TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'true');

-- query 5
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} VALUES (2, 20);

-- query 6
-- @result_contains=COLLECT_ON_WRITE_MISSING_PARENT_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-collect-on-write-missing-parent-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
require(table.statisticsFiles().asScala.forall(_.snapshotId() != current), "nonempty parent without statistics gained a partial current-snapshot StatisticsFile")
println("COLLECT_ON_WRITE_MISSING_PARENT_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "COLLECT_ON_WRITE_MISSING_PARENT_OK"

-- query 7
-- Seed one authoritative parent through the ordinary aggregate ANALYZE path;
-- Spark replaces it below with an independently produced standard Puffin
-- parent before Nova performs the incremental union.
-- @skip_result_check=true
ANALYZE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0};

-- query 8
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 9
-- Replace the Nova-produced parent with a Spark-produced standard Puffin file
-- for the same snapshot. The next Nova INSERT must consume and union this
-- cross-engine parent rather than treating it as provider-private state.
-- @result_contains=SPARK_CROSS_ENGINE_PARENT_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-parent-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import java.nio.ByteBuffer
import java.util.UUID
import scala.jdk.CollectionConverters._
import org.apache.iceberg.{GenericBlobMetadata, GenericStatisticsFile}
import org.apache.iceberg.puffin.{Blob, Puffin, PuffinCompressionCodec, StandardBlobTypes}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.shaded.org.apache.datasketches.theta.UpdateSketch

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val snapshot = table.currentSnapshot()
val path = table.location() + "/metadata/spark-parent-" + UUID.randomUUID().toString + ".stats"
val writer = Puffin.write(table.io().newOutputFile(path)).createdBy("ncp-8-spark-parent").build()
def addTheta(fieldId: Int, values: Seq[Long]): Unit = {
  val sketch = UpdateSketch.builder().setNominalEntries(4096).build()
  values.foreach(sketch.update)
  val compact = sketch.compact(true, null)
  writer.add(new Blob(
    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
    Seq(Int.box(fieldId)).asJava,
    snapshot.snapshotId(),
    snapshot.sequenceNumber(),
    ByteBuffer.wrap(compact.toByteArray),
    PuffinCompressionCodec.NONE,
    Map("ndv" -> compact.getEstimate.toLong.toString).asJava
  ))
}
addTheta(1, Seq(1L, 2L))
addTheta(2, Seq(10L, 20L))
writer.close()
val statistics = new GenericStatisticsFile(
  snapshot.snapshotId(),
  path,
  writer.fileSize(),
  writer.footerSize(),
  GenericBlobMetadata.from(writer.writtenBlobsMetadata())
)
table.updateStatistics().setStatistics(statistics).commit()
table.refresh()
val current = table.statisticsFiles().asScala.filter(_.snapshotId() == snapshot.snapshotId()).toSeq
require(current.size == 1 && current.head.path() == path, "Spark did not publish the cross-engine parent")
println("SPARK_CROSS_ENGINE_PARENT_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "SPARK_CROSS_ENGINE_PARENT_OK"

-- query 10
-- With a compatible parent sketch, collect-on-write must publish the union for
-- the same newly committed snapshot. Duplicate values exercise Theta union
-- instead of making the expected NDV equal to the number of written rows.
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}
VALUES (2, 20), (3, 30);

-- query 11
-- @result_contains=COLLECT_ON_WRITE_INCREMENTAL_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-collect-on-write-incremental-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.puffin.StandardBlobTypes
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
val currentStatistics = table.statisticsFiles().asScala.filter(_.snapshotId() == current).toSeq
require(currentStatistics.size == 1, "collect-on-write did not publish exactly one current-snapshot StatisticsFile")
val theta = currentStatistics.flatMap(_.blobMetadata().asScala).filter(
  _.`type`() == StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1
)
require(theta.size == 2, "collect-on-write did not publish one Theta blob for each supported field")
require(theta.forall(_.properties().get("ndv") == "3"), "collect-on-write parent union did not publish NDV 3")
println("COLLECT_ON_WRITE_INCREMENTAL_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "COLLECT_ON_WRITE_INCREMENTAL_OK"

-- query 12
-- Repair from the full table and then append again. This alternation exercises
-- both producers without changing the published Puffin contract.
-- @skip_result_check=true
ANALYZE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0};

-- query 13
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 14
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}
VALUES (4, 40);

-- query 15
-- @result_contains=COLLECT_ON_WRITE_ALTERNATING_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-collect-on-write-alternating-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.puffin.StandardBlobTypes
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
val theta = table.statisticsFiles().asScala
  .filter(_.snapshotId() == current)
  .flatMap(_.blobMetadata().asScala)
  .filter(_.`type`() == StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
  .toSeq
require(theta.size == 2, "alternating ANALYZE and INSERT did not publish both Theta blobs")
require(theta.forall(_.properties().get("ndv") == "4"), "alternating ANALYZE and INSERT did not publish NDV 4")
println("COLLECT_ON_WRITE_ALTERNATING_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "COLLECT_ON_WRITE_ALTERNATING_OK"

-- query 16
-- DELETE must not reseat this current StatisticsFile onto its new snapshot.
-- @skip_result_check=true
DELETE FROM statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} WHERE id = 1;

-- query 17
-- @result_contains=COLLECT_ON_WRITE_DELETE_BASIS_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-collect-on-write-delete-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
require(table.statisticsFiles().asScala.forall(_.snapshotId() != current), "DELETE reseated a StatisticsFile onto the new snapshot")
require(table.statisticsFiles().asScala.exists(_.snapshotId() != current), "DELETE discarded the ancestor StatisticsFile")
println("COLLECT_ON_WRITE_DELETE_BASIS_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "COLLECT_ON_WRITE_DELETE_BASIS_OK"

-- query 18
-- A full ANALYZE after an ineligible mutation must restore exact evidence for
-- the mutation snapshot instead of leaving the optimizer on ancestor data.
-- @skip_result_check=true
ANALYZE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0};

-- query 19
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 20
-- @result_contains=COLLECT_ON_WRITE_MUTATION_REPAIR_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-collect-on-write-repair-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.puffin.StandardBlobTypes
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
val theta = table.statisticsFiles().asScala
  .filter(_.snapshotId() == current)
  .flatMap(_.blobMetadata().asScala)
  .filter(_.`type`() == StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
  .toSeq
require(theta.size == 2, "full ANALYZE did not repair both fields after DELETE")
require(theta.forall(_.properties().get("ndv") == "3"), "full ANALYZE did not publish repaired NDV 3")
println("COLLECT_ON_WRITE_MUTATION_REPAIR_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "COLLECT_ON_WRITE_MUTATION_REPAIR_OK"

-- query 21
-- @skip_result_check=true
DROP TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} FORCE;
