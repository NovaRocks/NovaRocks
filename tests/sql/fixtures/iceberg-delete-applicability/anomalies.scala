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

import org.apache.avro.{Schema => AvroSchema}
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord => AvroRecord}

object DeleteApplicabilityAnomalies {
  import DeleteApplicabilityFixture._
  case class Container(schema: AvroSchema, metadata: Vector[(String, Array[Byte])], rows: Vector[AvroRecord])
  def copy(r: AvroRecord): AvroRecord = GenericData.get().deepCopy(r.getSchema, r)
  def readAvro(io: FileIO, path: String): Container = {
    val reader = new DataFileStream[AvroRecord](io.newInputFile(path).newStream(), new GenericDatumReader[AvroRecord]())
    try Container(reader.getSchema, reader.getMetaKeys.asScala.toVector
      .filterNot(_.startsWith("avro.")).map(k => (k, reader.getMeta(k))),
      reader.iterator().asScala.map(copy).toVector)
    finally reader.close()
  }
  def writeAvro(io: FileIO, path: String, c: Container): Unit = {
    val writer = new DataFileWriter[AvroRecord](new GenericDatumWriter[AvroRecord](c.schema))
    c.metadata.foreach { case (k, v) => writer.setMeta(k, v) }
    writer.setCodec(CodecFactory.nullCodec())
    writer.create(c.schema, io.newOutputFile(path).create())
    try c.rows.foreach(writer.append) finally writer.close()
  }
  def file(r: AvroRecord): AvroRecord = r.get("data_file").asInstanceOf[AvroRecord]
  def patch(r: AvroRecord)(f: AvroRecord => Unit): AvroRecord = { val c = copy(r); f(c); c }
  def withSeq(r: AvroRecord, seq: Long): AvroRecord = patch(r)(_.put("sequence_number", Long.box(seq)))
  def withPartition(r: AvroRecord, p: Int): AvroRecord = patch(r) { c =>
    file(c).get("partition").asInstanceOf[AvroRecord].put(0, Int.box(p))
  }
  def rawEntry(r: AvroRecord): JsonNode = mapper.readTree(r.toString)
  def unique(t: Table, name: String, suffix: String): String =
    t.location() + "/metadata/g02-" + name + "-" + java.util.UUID.randomUUID().toString + suffix

  // The public content writers provide real bytes. This layer changes only metadata,
  // preserving raw duplicates for the reader instead of normalizing them beforehand.
  def variant(seed: Seed, name: String, transform: Vector[AvroRecord] => Vector[AvroRecord],
      duplicateManifest: Boolean = false, duplicateReference: Boolean = false,
      otherSpec: Boolean = false, replacement: Option[Container] = None,
      dataTransform: Vector[AvroRecord] => Vector[AvroRecord] = identity): Table = {
    val t = seed.table; val io = t.io(); val snapshot = t.currentSnapshot()
    val oldList = readAvro(io, snapshot.manifestListLocation())
    val listRows = scala.collection.mutable.ArrayBuffer.empty[AvroRecord]
    val raw = scala.collection.mutable.ArrayBuffer.empty[JsonNode]
    oldList.rows.foreach { original =>
      val descriptor = copy(original)
      val isDelete = descriptor.get("content").asInstanceOf[Number].intValue() == 1
      val originalPath = descriptor.get("manifest_path").toString
      val source = if (isDelete) replacement.getOrElse(readAvro(io, originalPath)) else readAvro(io, originalPath)
      val inherited = descriptor.get("sequence_number").asInstanceOf[Number].longValue()
      val explicit = source.rows.map { r => patch(r) { c =>
        if (c.get("sequence_number") == null) c.put("sequence_number", Long.box(inherited))
        if (c.get("file_sequence_number") == null) c.put("file_sequence_number", Long.box(inherited))
      }}
      val changed = if (isDelete) transform(explicit) else dataTransform(explicit)
      val metadata = if (isDelete && otherSpec) source.metadata.map { case (k, v) =>
        (k, if (k == "partition-spec-id") "1".getBytes("UTF-8") else v)
      } else source.metadata
      val container = source.copy(metadata = metadata, rows = changed)
      val path = unique(t, name, ".avro")
      writeAvro(io, path, container)
      descriptor.put("manifest_path", path)
      descriptor.put("manifest_length", Long.box(io.newInputFile(path).getLength()))
      descriptor.put("sequence_number", Long.box(4L))
      val live = changed.filter(_.get("status").asInstanceOf[Number].intValue() != 2)
      val explicitLiveSequences = live.flatMap(r => Option(r.get("sequence_number")))
        .map(_.asInstanceOf[Number].longValue())
      descriptor.put("min_sequence_number", Long.box(if (explicitLiveSequences.isEmpty) 4L else explicitLiveSequences.min))
      Seq((0, "existing"), (1, "added"), (2, "deleted")).foreach { case (status, label) =>
        val entries = changed.filter(_.get("status").asInstanceOf[Number].intValue() == status)
        descriptor.put(label + "_files_count", Int.box(entries.size))
        descriptor.put(label + "_rows_count", Long.box(entries.map(r => file(r).get("record_count")
          .asInstanceOf[Number].longValue()).sum))
      }
      // The experiment scans without predicates; absent summaries are conservative.
      descriptor.put("partitions", null)
      if (isDelete && otherSpec) descriptor.put("partition_spec_id", Int.box(1))
      listRows += descriptor
      raw += obj("manifest" -> path, "list_descriptor" -> rawEntry(descriptor),
        "entries" -> changed.map(rawEntry))
      export(io, path, "manifest")
      changed.foreach(r => export(io, file(r).get("file_path").toString, if (isDelete) "delete-content" else "data-content"))
      if (isDelete && duplicateManifest) {
        val second = unique(t, name + "-second", ".avro")
        writeAvro(io, second, container)
        val d = copy(descriptor); d.put("manifest_path", second)
        d.put("manifest_length", Long.box(io.newInputFile(second).getLength()))
        listRows += d; raw += obj("manifest" -> second, "list_descriptor" -> rawEntry(d), "entries" -> changed.map(rawEntry))
        export(io, second, "manifest")
      }
      if (isDelete && duplicateReference) listRows += copy(descriptor)
    }
    val listPath = unique(t, name, ".manifest-list.avro")
    writeAvro(io, listPath, oldList.copy(rows = listRows.toVector))
    val root = mapper.readTree(TableMetadataParser.toJson(metadata(t))).asInstanceOf[ObjectNode]
    root.put("last-sequence-number", 4L)
    root.get("snapshots").elements().asScala.foreach { snapshotNode =>
      if (snapshotNode.get("snapshot-id").asLong() == snapshot.snapshotId()) {
        val n = snapshotNode.asInstanceOf[ObjectNode]
        n.put("manifest-list", listPath); n.put("sequence-number", 4L)
      }
    }
    if (otherSpec) {
      val specs = root.get("partition-specs").asInstanceOf[com.fasterxml.jackson.databind.node.ArrayNode]
      val extra = specs.get(0).deepCopy[ObjectNode](); extra.put("spec-id", 1); specs.add(extra)
    }
    val metadataPath = unique(t, name, ".metadata.json")
    val stream = io.newOutputFile(metadataPath).create()
    try stream.write(mapper.writeValueAsBytes(root)) finally stream.close()
    export(io, listPath, "manifest-list"); export(io, metadataPath, "metadata")
    val result = new BaseTable(new StaticTableOperations(metadataPath, io), name)
    observe(name, result, obj("method" -> "controlled Avro rewrite of official writer output",
      "public_commit" -> "not_attempted_for_metadata_variant", "source_table" -> t.name(),
      "raw_manifests" -> raw.toVector, "raw_manifest_list" -> listRows.toVector.map(rawEntry)), None)
    result
  }
  def publicManifest(seed: Seed, delete: DeleteFile): Container = {
    val t = seed.table
    val out = t.io().newOutputFile(unique(t, "public-delete-writer", ".avro"))
    val writer = ManifestFiles.writeDeleteManifest(3, t.spec(), out, Long.box(t.currentSnapshot().snapshotId()))
    try writer.add(delete, 2L) finally writer.close()
    export(t.io(), out.location(), "public-delete-manifest")
    readAvro(t.io(), out.location())
  }
  def run(seeds: Seq[Seed]): Unit = {
    seeds.foreach { seed =>
      val kind = if (seed.delete.format() == FileFormat.PUFFIN) "dv"
        else if (seed.delete.content() == FileContent.EQUALITY_DELETES) "equality" else "position"
      val duplicated = variant(seed, kind + "_duplicate_same_manifest", rs => rs ++ rs.take(1))
      if (kind == "position") {
        val identifier = org.apache.iceberg.catalog.TableIdentifier.of(namespace, prefix + "_registered_anomaly")
        val path = metadata(duplicated).metadataFileLocation()
        val registration = try {
          val catalog = Spark3Util.loadIcebergCatalog(DeleteApplicabilityFixture.spark, "ice_rest")
          val registered = catalog.registerTable(identifier, path)
          require(metadata(registered).metadataFileLocation() == path, "Registration changed the fixture metadata")
          obj("status" -> "success", "table" -> identifier.toString, "metadata" -> path)
        } catch { case t: Throwable => obj("status" -> "error", "table" -> identifier.toString,
          "metadata" -> path, "error" -> error(t)) }
        emit(obj("record" -> "registration", "result" -> registration))
      }
      variant(seed, kind + "_duplicate_cross_manifest", identity, duplicateManifest = true)
      variant(seed, kind + "_duplicate_manifest_reference", identity, duplicateReference = true)
      variant(seed, kind + "_same_address_sequence", rs => rs ++ rs.take(1).map(r => withSeq(r, 3L)))
      variant(seed, kind + "_same_address_record_count", rs => rs ++ rs.take(1).map(r => patch(r)(c =>
        file(c).put("record_count", Long.box(99L)))))
      variant(seed, kind + "_same_address_scope", rs => rs ++ rs.take(1).map(r => withPartition(r, 2)))
      variant(seed, kind + "_added_null_sequence", rs => rs.map(r => patch(r)(_.put("sequence_number", null))))
      variant(seed, kind + "_existing_null_sequence", rs => rs.map(r => patch(r) { c =>
        c.put("status", Int.box(0)); c.put("sequence_number", null)
      }))
      variant(seed, kind + "_deleted_null_sequence", rs => rs ++ rs.take(1).map(r => patch(r) { c =>
        c.put("status", Int.box(2)); c.put("sequence_number", null)
      }))
      if (kind != "equality") {
        variant(seed, kind + "_cross_partition", rs => rs.map(r => withPartition(r, 2)))
        variant(seed, kind + "_cross_spec", identity, otherSpec = true)
      }
      if (kind == "dv") {
        variant(seed, "dv_older_than_exact_target", rs => rs.map(r => withSeq(r, 1L)))
        val second = dv(seed.table, Seq((seed.fresh.location(), 1L))).head
        val secondRows = publicManifest(seed, second).rows.map(r => patch(r)(_.put("file_sequence_number", Long.box(2L))))
        variant(seed, "dv_distinct_same_target", rs => rs ++ secondRows)
        variant(seed, "dv_same_address_reference", rs => rs ++ rs.take(1).map(r => patch(r)(c =>
          file(c).put("referenced_data_file", seed.old.location()))))
        variant(seed, "dv_unmatched_target_control", rs => rs.map(r => patch(r)(c =>
          file(c).put("referenced_data_file", seed.fresh.location() + ".absent"))))
      }
      if (kind == "position") {
        seed.table.updateProperties().set("format-version", "3").commit(); seed.table.refresh()
        val explicit = FileMetadata.deleteFileBuilder(seed.table.spec()).copy(seed.delete)
          .withReferencedDataFile(seed.fresh.location()).build()
        val replacement = Some(publicManifest(seed, explicit))
        variant(seed, "position_explicit_reference_control", identity, replacement = replacement)
        variant(seed, "position_explicit_cross_partition", rs => rs.map(r => withPartition(r, 2)), replacement = replacement)
        variant(seed, "position_explicit_cross_spec", identity, otherSpec = true, replacement = replacement)
      }
    }
    val positionSeed = seeds.find(_.delete.format() == FileFormat.PARQUET).get
    variant(positionSeed, "data_existing_null_sequence", identity, dataTransform = rs => rs.map(r => patch(r) { c =>
      c.put("status", Int.box(0)); c.put("sequence_number", null)
    }))
    versionOne()
    val wide = sameCommit("equality", "_wide", wideEquality = true)
    def extraField(rs: Vector[AvroRecord]): Vector[AvroRecord] = rs ++ rs.take(1).map(r => patch(r)(c =>
      file(c).put("equality_ids", java.util.Arrays.asList(Int.box(2)))))
    variant(wide, "equality_same_address_fields", extraField)
    wide.table.updateSchema().updateColumn("p", org.apache.iceberg.types.Types.LongType.get()).commit()
    wide.table.refresh()
    variant(wide, "equality_promoted_type_fields", extraField)
  }
}
