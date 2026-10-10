// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import scala.collection.JavaConverters._
import org.apache.iceberg.{DataFile, FileFormat, HasTableOperations, IcebergBuild,
  PartitionData, Snapshot, Table, TableMetadata}
import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord}
import org.apache.iceberg.deletes.{BaseDVFileWriter, PositionDeleteIndex}
import org.apache.iceberg.encryption.EncryptedFiles
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession

object Iru5SparkCommitInterop {
  case class Dv(path: String, offset: Long, size: Long, reference: String)
  case class Lineage(id: Long, value: Int, rowId: Long, sequence: Long, file: String)

  // A REPL error must not become a successful shell marker.
  def checked(marker: String)(body: => Unit): Unit = {
    try { body; println(marker) }
    catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
  }

  def table(spark: SparkSession, qualified: String): Table = {
    require(IcebergBuild.version() == "1.11.0", "The fixture oracle requires pinned Iceberg 1.11.0")
    val t = Spark3Util.loadIcebergTable(spark, qualified)
    t.refresh()
    t
  }

  def metadata(t: Table): TableMetadata =
    t.asInstanceOf[HasTableOperations].operations().current()

  def output(t: Table, label: String, suffix: String): OutputFile =
    t.io().newOutputFile(t.location() + "/data/iru5-" + label + "-" +
      java.util.UUID.randomUUID().toString + suffix)

  def data(t: Table, label: String, rows: Seq[(Long, Int)]): DataFile = {
    val writer = new GenericAppenderFactory(t.schema(), t.spec())
      .set("write.metadata.metrics.default", "full")
      .newDataWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, label, ".parquet")),
        FileFormat.PARQUET, new PartitionData(t.spec().partitionType()))
    try rows.foreach { case (id, value) =>
      val record = GenericRecord.create(t.schema())
      record.set(0, Long.box(id)); record.set(1, Int.box(value)); writer.write(record)
    } finally writer.close()
    writer.toDataFile()
  }

  def dvs(t: Table, snapshot: Snapshot): Set[Dv] = {
    val tasks = t.newScan().useSnapshot(snapshot.snapshotId()).planFiles()
    try tasks.asScala.toVector.flatMap(_.deletes().asScala).map { d =>
      require(d.format() == FileFormat.PUFFIN && d.contentOffset() != null &&
        d.contentSizeInBytes() != null && d.referencedDataFile() != null,
        "Expected complete deletion-vector facts")
      Dv(d.location(), d.contentOffset().longValue(), d.contentSizeInBytes().longValue(),
        d.referencedDataFile())
    }.toSet finally tasks.close()
  }

  def lineage(spark: SparkSession, qualified: String, snapshot: Option[Long] = None): Vector[Lineage] = {
    val reader = spark.read.format("iceberg")
    snapshot.foreach(id => reader.option("snapshot-id", id.toString))
    reader.load(qualified).selectExpr("id", "value", "_row_id",
      "_last_updated_sequence_number", "_file").orderBy("id").collect().toVector.map { r =>
      require(!r.isNullAt(2) && !r.isNullAt(3), "Spark must resolve actual inherited row lineage")
      Lineage(r.getLong(0), r.getInt(1), r.getLong(2), r.getLong(3), r.getString(4))
    }
  }

  def assertRows(spark: SparkSession, qualified: String, expected: Seq[(Long, Int)]): Unit = {
    val rows = spark.sql(s"SELECT id, value FROM $qualified ORDER BY id").collect()
      .toVector.map(r => (r.getLong(0), r.getInt(1)))
    require(rows == expected, "Independent Spark rows differ: " + rows)
  }

  def createSharedPuffin(spark: SparkSession, qualified: String): Unit = {
    require(IcebergBuild.version() == "1.11.0", "The fixture oracle requires pinned Iceberg 1.11.0")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS ${qualified.split("\\.").dropRight(1).mkString(".")}")
    spark.sql(s"CREATE TABLE $qualified (id BIGINT, value INT) USING iceberg " +
      "TBLPROPERTIES ('format-version'='3', 'write.row-lineage'='true', " +
      "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', " +
      "'novarocks.update.mode'='merge-on-read')")
    val t = table(spark, qualified)
    val a = data(t, "a", (1L to 5L).map(id => (id, (id * 10).toInt)))
    val b = data(t, "b", (11L to 15L).map(id => (id, (id * 10).toInt)))
    t.newAppend().appendFile(a).appendFile(b).commit(); t.refresh()
    val writer = new BaseDVFileWriter(new java.util.function.Supplier[OutputFile] {
      override def get(): OutputFile = output(t, "shared", ".puffin")
    }, new java.util.function.Function[String, PositionDeleteIndex] {
      override def apply(path: String): PositionDeleteIndex = null
    })
    try {
      writer.delete(a.location(), 0L, t.spec(), a.partition())
      writer.delete(b.location(), 0L, t.spec(), b.partition())
    } finally writer.close()
    val files = writer.result().deleteFiles().asScala.toVector
    require(files.size == 2 && files.map(_.location()).distinct.size == 1 &&
      files.map(d => (d.contentOffset(), d.contentSizeInBytes())).distinct.size == 2,
      "BaseDVFileWriter must produce two distinct blobs in one physical Puffin")
    val change = t.newRowDelta().set("iru5.fixture", "shared-puffin")
    files.foreach(change.addDeletes); change.commit(); t.refresh()
    spark.catalog.refreshTable(qualified)
    val exposed = spark.sql(s"SELECT file_path, content_offset, content_size_in_bytes, " +
      s"referenced_data_file FROM $qualified.delete_files").collect().toVector.map { r =>
      Dv(r.getString(0), r.getLong(1), r.getLong(2), r.getString(3))
    }.toSet
    require(exposed == dvs(t, t.currentSnapshot()) && exposed.size == 2 &&
      exposed.map(_.path).size == 1, "Spark delete_files must expose the shared physical path")
    assertRows(spark, qualified, (2L to 5L).map(id => (id, (id * 10).toInt)) ++
      (12L to 15L).map(id => (id, (id * 10).toInt)))
    println("IRU5_SHARED_PUFFIN_FACTS " + exposed.toVector.sortBy(_.reference).mkString(" "))
  }

  def verifySharedPuffin(spark: SparkSession, qualified: String, afterUpdate: Boolean): Unit = {
    val t = table(spark, qualified)
    val seed = t.snapshots().asScala.find(_.summary().get("iru5.fixture") == "shared-puffin").get
    val original = dvs(t, seed)
    val preserved = original.find(_.reference.contains("/iru5-b-")).get
    val replaced = original.find(_.reference.contains("/iru5-a-")).get
    val current = dvs(t, t.currentSnapshot())
    require(current.size == 2 && current.contains(preserved) && !current.contains(replaced),
      "Only the targeted logical DV may be replaced")
    require(current.count(_.reference == replaced.reference) == 1,
      "One live DV per referenced data file must remain")
    require(t.io().newInputFile(preserved.path).exists(),
      "The original shared Puffin must remain physically reachable")
    val parent = t.snapshot(t.currentSnapshot().parentId())
    if (afterUpdate) {
      require(t.currentSnapshot().operation() == "overwrite" && parent.parentId() != null && parent.parentId().longValue() == seed.snapshotId(),
        "MoR UPDATE must follow the Nova DELETE with an overwrite snapshot")
    } else {
      require(t.currentSnapshot().operation() == "delete" && parent.snapshotId() == seed.snapshotId(),
        "Nova DELETE must follow the shared-Puffin baseline")
    }
    val a = (3L to 5L).map(id => (id, if (afterUpdate && id == 3L) 130 else (id * 10).toInt))
    assertRows(spark, qualified, a ++ (12L to 15L).map(id => (id, (id * 10).toInt)))
    println("IRU5_SHARED_PUFFIN_RETAINED " + preserved)
  }

  def verifyMor(spark: SparkSession, qualified: String, phase: String): Unit = {
    val t = table(spark, qualified)
    val snapshot = t.currentSnapshot()
    val parent = t.snapshot(snapshot.parentId())
    val prior = lineage(spark, qualified, Some(parent.snapshotId())).map(r => r.id -> r).toMap
    val now = lineage(spark, qualified)
    val changed = phase match {
      case "update" => Set(2L)
      case "merge" => Set(3L, 5L)
      case "append" => Set(6L)
      case _ => throw new IllegalArgumentException("Unknown MoR verification phase")
    }
    val expected = phase match {
      case "update" => Seq(1L -> 10, 2L -> 220, 3L -> 30, 4L -> 40)
      case "merge" => Seq(1L -> 10, 2L -> 220, 3L -> 330, 4L -> 40, 5L -> 500)
      case "append" => Seq(1L -> 10, 2L -> 220, 3L -> 330, 4L -> 40, 5L -> 500, 6L -> 600)
    }
    assertRows(spark, qualified, expected)
    require(now.map(_.rowId).distinct.size == now.size, "Spark row IDs must be globally unique")
    val liveDvs = dvs(t, snapshot)
    if (phase != "append") {
      require(t.properties().get("novarocks.update.mode") == "merge-on-read",
        "The Nova provider must admit the configured MoR update/merge strategy")
      val addedDvs = spark.sql(s"SELECT data_file.file_path, data_file.content_offset, " +
        s"data_file.content_size_in_bytes, data_file.referenced_data_file FROM $qualified.entries " +
        s"WHERE status=1 AND snapshot_id=${snapshot.snapshotId()} AND data_file.content=1 " +
        "AND data_file.file_format='PUFFIN'").collect().toVector.map { r =>
        require((0 to 3).forall(i => !r.isNullAt(i)), "New DV entries require full logical identity")
        Dv(r.getString(0), r.getLong(1), r.getLong(2), r.getString(3))
      }.toSet
      require(addedDvs.nonEmpty && addedDvs.subsetOf(liveDvs),
        "MoR UPDATE/MERGE must add actual live Puffin DV entries in this committed snapshot")
      println("IRU5_MOR_ADDED_DVS phase=" + phase + " identities=" + addedDvs)
    } else {
      require(liveDvs == dvs(t, parent), "Following INSERT must carry the exact existing logical DVs")
    }
    val sequence = snapshot.sequenceNumber()
    val added = spark.sql(s"SELECT sequence_number, file_sequence_number, data_file.file_path " +
      s"FROM $qualified.entries WHERE status=1 AND snapshot_id=${snapshot.snapshotId()} " +
      "AND data_file.content=0").collect().toVector
    require(added.nonEmpty && added.forall(r => r.getLong(0) == sequence && r.getLong(1) == sequence),
      "New logical data must inherit the actual Java snapshot data/file sequence")
    val addedPaths = added.map(_.getString(2)).toSet
    val tasks = t.newScan().planFiles()
    val actualFiles = try tasks.asScala.toVector.map(_.file().copy()) finally tasks.close()
    val javaFiles = actualFiles.map(f => f.location() -> f).toMap
    require(addedPaths.forall { path =>
      val file = javaFiles(path)
      file.dataSequenceNumber() == Long.box(sequence) &&
        file.fileSequenceNumber() == Long.box(sequence)
    }, "Java manifest inheritance must agree with Spark entries for newly added files")
    now.foreach { row =>
      if (changed(row.id)) {
        require(row.sequence == sequence && addedPaths(row.file),
          "Changed/new row must inherit its actual committed data-file sequence")
        prior.get(row.id).foreach(old => require(old.rowId == row.rowId,
          "Updated row identity must be retained"))
      } else {
        val old = prior(row.id)
        require(old.rowId == row.rowId && old.sequence == row.sequence,
          "Untouched row lineage must retain its actual source values")
      }
    }
    require(snapshot.operation() == (if (phase == "append") "append" else "overwrite"),
      "Snapshot operation tag must match logical mutation")
    println("IRU5_MOR_ACTUAL_SEQUENCE phase=" + phase + " snapshot=" + snapshot.snapshotId() +
      " sequence=" + sequence + " added_paths=" + addedPaths.mkString(",") + " rows=" + now)
  }

  def verifyFirstHistoricalAssignment(spark: SparkSession, qualified: String, expectedRows: Long): Unit = {
    val t = table(spark, qualified)
    val snapshot = t.currentSnapshot()
    val md = metadata(t)
    require(md.formatVersion() == 3 && snapshot.firstRowId() == Long.box(0L) &&
      snapshot.addedRows() == Long.box(expectedRows) && md.nextRowId() == expectedRows,
      "The first Nova V3 commit must record actual historical plus new-row allocation")
    require(t.snapshots().asScala.count(_.firstRowId() != null) == 1,
      "No intervening Spark V3 snapshot may hide Nova first assignment")
    val manifests = snapshot.dataManifests(t.io()).asScala
    require(manifests.nonEmpty && manifests.forall(_.firstRowId() != null),
      "Every carried V3 data manifest must expose an assigned row range")
    val tasks = t.newScan().planFiles()
    val files = try tasks.asScala.toVector.map(_.file().copy()) finally tasks.close()
    require(files.nonEmpty && files.forall(_.firstRowId() != null),
      "Java manifest inheritance must assign every historical data entry")
    val assigned = files.flatMap(f => (f.firstRowId().longValue() until
      (f.firstRowId().longValue() + f.recordCount())))
    require(assigned.distinct.size == expectedRows && assigned.sorted == (0L until expectedRows),
      "Assigned physical file ranges must cover the actual highwater without overlap")
    val rows = spark.sql(s"SELECT id, _row_id FROM $qualified").collect().toVector
    require(rows.size == expectedRows && rows.forall(r => !r.isNullAt(1)) &&
      rows.map(_.getLong(1)).distinct.size == expectedRows,
      "Spark must resolve every historical row ID without duplication")
    println("IRU5_HISTORICAL_ALLOCATION snapshot=" + snapshot.snapshotId() + " first_row_id=" +
      snapshot.firstRowId() + " added_rows=" + snapshot.addedRows() + " next_row_id=" + md.nextRowId())
  }
}
