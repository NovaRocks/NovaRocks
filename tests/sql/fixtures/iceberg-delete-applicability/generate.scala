// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import scala.collection.JavaConverters._
import org.apache.iceberg._
import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord, IcebergGenerics, Record}
import org.apache.iceberg.deletes.{BaseDVFileWriter, PositionDelete, PositionDeleteIndex}
import org.apache.iceberg.encryption.EncryptedFiles
import org.apache.iceberg.io.{FileIO, OutputFile}
import org.apache.iceberg.spark.Spark3Util
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode

object DeleteApplicabilityFixture {
  val mapper = new ObjectMapper()
  var spark: org.apache.spark.sql.SparkSession = _
  var namespace = ""
  var prefix = ""
  val exported = scala.collection.mutable.Set.empty[String]
  val tables = scala.collection.mutable.ArrayBuffer.empty[(String, Table)]
  type Row = Seq[Any]

  def json(value: Any): JsonNode = value match {
    case null => mapper.getNodeFactory.nullNode()
    case n: JsonNode => n
    case s: String => mapper.getNodeFactory.textNode(s)
    case b: Boolean => mapper.getNodeFactory.booleanNode(b)
    case n: Int => mapper.getNodeFactory.numberNode(n)
    case n: Long => mapper.getNodeFactory.numberNode(n)
    case n: Number => mapper.getNodeFactory.numberNode(n.doubleValue())
    case xs: Seq[_] => val a = mapper.createArrayNode(); xs.foreach(x => a.add(json(x))); a
    case xs: java.util.List[_] => json(xs.asScala.toSeq)
    case m: Map[_, _] => obj(m.toSeq.map { case (k, v) => (k.toString, v) }: _*)
    case other => mapper.getNodeFactory.textNode(other.toString)
  }
  def obj(fields: (String, Any)*): ObjectNode = {
    val n = mapper.createObjectNode()
    fields.foreach { case (k, v) => n.set[JsonNode](k, json(v)) }
    n
  }
  def emit(n: JsonNode): Unit = println("UEA4G_RECEIPT " + mapper.writeValueAsString(n))
  def error(t: Throwable): JsonNode = obj("class" -> t.getClass.getName,
    "message" -> Option(t.getMessage).getOrElse(""),
    "cause" -> Option(t.getCause).filter(_ ne t).map(error).orNull)
  def bytes(io: FileIO, path: String): Array[Byte] = {
    val in = io.newInputFile(path).newStream()
    try { val out = new java.io.ByteArrayOutputStream(); val b = new Array[Byte](8192)
      var n = in.read(b); while (n >= 0) { if (n > 0) out.write(b, 0, n); n = in.read(b) }
      out.toByteArray
    } finally in.close()
  }
  def export(io: FileIO, path: String, kind: String): Unit = if (exported.add(path)) {
    val content = bytes(io, path)
    val digest = java.security.MessageDigest.getInstance("SHA-256").digest(content)
      .map(b => f"${b & 0xff}%02x").mkString
    emit(obj("record" -> "artifact", "kind" -> kind, "path" -> path,
      "size" -> content.length, "sha256" -> digest,
      "base64" -> java.util.Base64.getEncoder.encodeToString(content)))
  }
  def initialize(session: org.apache.spark.sql.SparkSession, ns: String, namePrefix: String): Unit = {
    require(ns.matches("[a-zA-Z0-9_]+") && namePrefix.matches("[a-zA-Z0-9_]+"))
    spark = session; namespace = ns; prefix = namePrefix
    require(IcebergBuild.version() == "1.11.0", "The oracle requires Iceberg 1.11.0")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS ice_rest.$namespace")
    emit(obj("record" -> "runtime", "iceberg" -> IcebergBuild.fullVersion(),
      "spark" -> spark.version, "java" -> System.getProperty("java.version"),
      "requested_warehouse" -> spark.conf.get("spark.sql.catalog.ice_rest.warehouse", ""),
      "namespace" -> namespace, "prefix" -> prefix))
  }
  def create(name: String, version: Int, partitioned: Boolean = true): Table = {
    val qualified = s"ice_rest.$namespace.${prefix}_$name"
    val partition = if (partitioned) "PARTITIONED BY (p)" else ""
    spark.sql(s"CREATE TABLE $qualified (id BIGINT, p INT, value STRING) USING iceberg $partition " +
      s"TBLPROPERTIES ('format-version'='$version', 'write.metadata.metrics.default'='full')")
    val t = Spark3Util.loadIcebergTable(spark, qualified)
    tables += ((qualified, t)); t
  }
  def partition(t: Table, p: Int): StructLike = {
    val v = new PartitionData(t.spec().partitionType())
    if (!t.spec().isUnpartitioned()) v.set(0, Int.box(p))
    v
  }
  def output(t: Table, suffix: String): OutputFile = t.io().newOutputFile(
    t.location() + "/data/g02-" + java.util.UUID.randomUUID().toString + suffix)
  def record(schema: Schema, values: Row): Record = {
    val r = GenericRecord.create(schema)
    values.zipWithIndex.foreach { case (v, i) => r.set(i, v.asInstanceOf[AnyRef]) }; r
  }
  def data(t: Table, rows: Seq[Row], p: Int = 1): DataFile = {
    val w = new GenericAppenderFactory(t.schema(), t.spec())
      .set("write.metadata.metrics.default", "full")
      .newDataWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, ".parquet")),
        FileFormat.PARQUET, partition(t, p))
    try rows.foreach(r => w.write(record(t.schema(), r))) finally w.close()
    w.toDataFile()
  }
  def equality(t: Table, ids: Seq[Long], p: Int = 1, wide: Boolean = false): DeleteFile = {
    val schema = if (wide) t.schema().select("id", "p") else t.schema().select("id")
    val w = new GenericAppenderFactory(t.schema(), t.spec(), Array(1), schema, null)
      .set("write.metadata.metrics.default", "full")
      .newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, ".parquet")),
        FileFormat.PARQUET, partition(t, p))
    try ids.foreach(id => w.write(record(schema, if (wide) Seq[Any](id, p) else Seq[Any](id)))) finally w.close()
    w.toDeleteFile()
  }
  def positionDelete(t: Table, positions: Seq[(String, Long)], p: Int = 1): DeleteFile = {
    val w = new GenericAppenderFactory(t.schema(), t.spec())
      .set("write.metadata.metrics.default", "full")
      .newPosDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, ".parquet")),
        FileFormat.PARQUET, partition(t, p))
    try positions.sortBy(x => (x._1, x._2)).foreach { case (path, pos) =>
      w.write(PositionDelete.create[Record]().set(path, pos))
    } finally w.close()
    w.toDeleteFile()
  }
  def dv(t: Table, positions: Seq[(String, Long)], p: Int = 1): Seq[DeleteFile] = {
    val w = new BaseDVFileWriter(new java.util.function.Supplier[OutputFile] {
      override def get(): OutputFile = output(t, ".puffin")
    }, new java.util.function.Function[String, PositionDeleteIndex] {
      override def apply(path: String): PositionDeleteIndex = null
    })
    try positions.foreach { case (path, pos) => w.delete(path, pos, t.spec(), partition(t, p)) }
    finally w.close()
    w.result().deleteFiles().asScala.toVector
  }
  def describe(f: ContentFile[_]): JsonNode = {
    def counts(values: java.util.Map[Integer, java.lang.Long]): Any =
      Option(values).map(_.asScala.map { case (k, v) => k.toString -> v }.toMap).orNull
    def bounds(values: java.util.Map[Integer, java.nio.ByteBuffer]): Any =
      Option(values).map(_.asScala.map { case (k, v) =>
        val source = v.duplicate(); val content = new Array[Byte](source.remaining()); source.get(content)
        k.toString -> java.util.Base64.getEncoder.encodeToString(content)
      }.toMap).orNull
    val n = obj("path" -> f.location(), "content" -> f.content().toString,
      "format" -> f.format().toString, "spec_id" -> f.specId(), "partition" -> f.partition().toString,
      "data_sequence" -> f.dataSequenceNumber(), "file_sequence" -> f.fileSequenceNumber(),
      "record_count" -> f.recordCount(), "file_size" -> f.fileSizeInBytes(),
      "value_counts" -> counts(f.valueCounts()), "null_value_counts" -> counts(f.nullValueCounts()),
      "nan_value_counts" -> counts(f.nanValueCounts()),
      "lower_bounds_base64" -> bounds(f.lowerBounds()), "upper_bounds_base64" -> bounds(f.upperBounds()))
    f match { case d: DeleteFile =>
      n.set[JsonNode]("equality_ids", json(d.equalityFieldIds()))
      n.set[JsonNode]("referenced_data_file", json(d.referencedDataFile()))
      n.set[JsonNode]("content_offset", json(d.contentOffset()))
      n.set[JsonNode]("content_size", json(d.contentSizeInBytes()))
      case _ => ()
    }; n
  }
  def metadata(t: Table): TableMetadata = t.asInstanceOf[HasTableOperations].operations().current()
  def observe(name: String, t: Table, generation: JsonNode, expected: Option[Seq[Row]],
      projectCurrentSchema: Boolean = false, enforceExpected: Boolean = true): Unit = {
    val snapshot = t.currentSnapshot().snapshotId()
    val baseScan = t.newScan().useSnapshot(snapshot).includeColumnStats()
    val scan = if (projectCurrentSchema) baseScan.project(t.schema()) else baseScan
    val planned = scala.collection.mutable.ArrayBuffer.empty[JsonNode]
    var stage = "plan-open"
    val plan = try {
      val tasks = scan.planFiles()
      try { stage = "plan-iterate"; tasks.asScala.foreach { task =>
        planned += obj("data" -> describe(task.file()),
          "deletes" -> task.deletes().asScala.toSeq.map(describe),
          "partition_constants" -> org.apache.iceberg.util.PartitionUtil.constantsMap(task).asScala.toSeq.map {
            case (id, value) => obj("field_id" -> id, "value" -> value,
              "java_class" -> Option(value).map(_.getClass.getName).orNull)
          })
      }; stage = "plan-close" } finally { tasks.close() }
      obj("status" -> "success", "tasks" -> planned.toVector)
    } catch { case t: Throwable => obj("status" -> "error", "stage" -> stage,
      "error" -> error(t), "partial_tasks" -> planned.toVector) }
    stage = "row-open"
    val rows = scala.collection.mutable.ArrayBuffer.empty[Row]
    val rowRead = try {
      val builder = IcebergGenerics.read(t).useSnapshot(snapshot)
      val reader = (if (projectCurrentSchema) builder.project(t.schema()) else builder).build()
      try { stage = "row-iterate"; reader.asScala.foreach { r =>
        rows += Seq(r.getField("id"), r.getField("p"), Option(r.getField("value")).map(_.toString).orNull)
      }; stage = "row-close" } finally { reader.close() }
      val sorted = rows.toVector.sortBy(r => json(r).toString)
      obj("status" -> "success", "rows" -> sorted,
        "row_java_types" -> sorted.map(_.map(v => Option(v).map(_.getClass.getName).orNull)))
    } catch { case failure: Throwable => obj("status" -> "error", "stage" -> stage, "error" -> error(failure)) }
    val matched = expected.map(e => rowRead.get("status").asText() == "success" &&
      rowRead.get("rows").toString == json(e.sortBy(r => json(r).toString)).toString)
    emit(obj("record" -> "case", "case" -> name, "table" -> t.name(),
      "actual_table_location" -> t.location(),
      "metadata" -> metadata(t).metadataFileLocation(), "snapshot" -> snapshot,
      "scan" -> obj("filter" -> "alwaysTrue", "include_column_stats" -> true,
        "project_current_schema" -> projectCurrentSchema,
        "snapshot_schema_id" -> t.currentSnapshot().schemaId(),
        "table_schema" -> mapper.readTree(SchemaParser.toJson(t.schema())),
        "scan_schema" -> mapper.readTree(SchemaParser.toJson(scan.schema()))),
      "generation" -> generation, "java_planFiles" -> plan, "java_row_read" -> rowRead,
      "independent_expected_rows" -> expected.map(json).orNull,
      "independent_oracle_matched" -> matched.map(json).orNull))
    if (enforceExpected) expected.foreach(_ => require(plan.get("status").asText() == "success" &&
      matched.contains(true), s"$name: independent compliant-fixture oracle failed"))
  }
  case class Seed(table: Table, old: DataFile, fresh: DataFile, delete: DeleteFile)
  def sameCommit(kind: String, suffix: String = "", wideEquality: Boolean = false): Seed = {
    val caseName = "same_commit_" + kind + suffix
    val t = create(caseName, if (kind == "dv") 3 else 2)
    val old = data(t, Seq(Seq(7L, 1, "old7"), Seq(10L, 1, "old10")))
    t.newAppend().appendFile(old).commit()
    val fresh = data(t, Seq(Seq(7L, 1, "new7"), Seq(8L, 1, "new8"), Seq(9L, 1, "new9")))
    val d = kind match {
      case "position" => positionDelete(t, Seq((fresh.location(), 0L)))
      case "equality" => equality(t, Seq(7L), wide = wideEquality)
      case "dv" => dv(t, Seq((fresh.location(), 0L))).head
    }
    t.newRowDelta().addRows(fresh).addDeletes(d).commit()
    t.refresh()
    export(t.io(), metadata(t).metadataFileLocation(), "metadata")
    export(t.io(), t.currentSnapshot().manifestListLocation(), "manifest-list")
    t.currentSnapshot().allManifests(t.io()).asScala.foreach(m => export(t.io(), m.path(), "manifest"))
    export(t.io(), old.location(), "data-content"); export(t.io(), fresh.location(), "data-content")
    export(t.io(), d.location(), "delete-content")
    val expected = if (kind == "equality") Seq(Seq(10L,1,"old10"), Seq(7L,1,"new7"),Seq(8L,1,"new8"),Seq(9L,1,"new9"))
      else Seq(Seq(7L,1,"old7"),Seq(10L,1,"old10"),Seq(8L,1,"new8"),Seq(9L,1,"new9"))
    observe(caseName, t, obj("method" -> "official RowDelta.addRows+addDeletes, one commit",
      "writer_commit" -> "success"), Some(expected))
    Seed(t, old, fresh, d)
  }
  def versionOne(): Unit = {
    val t = create("v1_sequence_zero", 1)
    val rows = Seq(Seq(7L, 1, "v1-a"), Seq(10L, 1, "v1-b"))
    val f = data(t, rows); t.newAppend().appendFile(f).commit(); t.refresh()
    export(t.io(), metadata(t).metadataFileLocation(), "metadata")
    export(t.io(), t.currentSnapshot().manifestListLocation(), "manifest-list")
    t.currentSnapshot().allManifests(t.io()).asScala.foreach(m => export(t.io(), m.path(), "manifest"))
    export(t.io(), f.location(), "data-content")
    observe("v1_sequence_zero", t, obj("method" -> "official v1 append", "writer_commit" -> "success"), Some(rows))
  }
}
