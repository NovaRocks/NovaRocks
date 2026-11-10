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

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.avro.generic.{GenericRecord => AvroRecord}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.types.Types

object ManifestLineageFixture {
  import DeleteApplicabilityFixture.{spark => fixtureSpark, _}
  import DeleteApplicabilityAvro._

  private def name(label: String): String = s"ice_rest.$namespace.${prefix}_$label"

  private def load(label: String): Table = {
    val table = Spark3Util.loadIcebergTable(fixtureSpark, name(label))
    require(ownedLocation(table.location()), "Table is outside this fixture's storage scope")
    table
  }

  private def mergePolicy(table: Table, enabled: Boolean): Unit = {
    table.updateProperties().set("commit.manifest-merge.enabled", enabled.toString)
      .set("commit.manifest.min-count-to-merge", "2").commit()
    table.refresh()
  }

  private def append(table: Table, file: DataFile): Snapshot = {
    table.newFastAppend().appendFile(file).commit()
    table.refresh()
    table.currentSnapshot()
  }

  private def manifestPath(descriptor: AvroRecord): String = descriptor.get("manifest_path").toString

  private def capture(table: Table, snapshot: Snapshot): JsonNode = {
    val io = table.io()
    val listPath = snapshot.manifestListLocation()
    val list = readAvro(io, listPath)
    export(io, listPath, "manifest-list")
    val manifests = list.rows.map { descriptor =>
      val path = manifestPath(descriptor)
      val raw = readAvro(io, path)
      export(io, path, "manifest")
      raw.rows.foreach(r => export(io, file(r).get("file_path").toString, "data-content"))
      obj("path" -> path, "descriptor" -> rawEntry(descriptor),
        "header_schema" -> mapper.readTree(header(raw, "schema")),
        "header_spec" -> mapper.readTree(header(raw, "partition-spec")),
        "entries" -> raw.rows.map(rawEntry))
    }
    obj("snapshot" -> snapshot.snapshotId(), "manifest_list" -> listPath, "raw_manifests" -> manifests)
  }

  private def header(container: Container, field: String): String =
    new String(container.metadata.toMap.apply(field), UTF_8)

  private def lineageRows(label: String, projection: String, where: String = ""): Vector[Row] = {
    fixtureSpark.catalog.refreshTable(name(label))
    fixtureSpark.sql(s"SELECT $projection, _row_id, _last_updated_sequence_number FROM ${name(label)} $where ORDER BY id")
      .collect().toVector.map(_.toSeq)
  }

  private def publishEvidence(label: String, table: Table, method: String,
      snapshots: Seq[Snapshot], observed: Vector[Row], expected: Seq[Row],
      newRowIdPosition: Option[Int], extra: (String, Any)*): Unit = {
    val comparable = newRowIdPosition.fold(observed) { position =>
      val row = observed.last
      require(row(position).asInstanceOf[Long] >= 2L, "New row identity overlaps existing rows")
      observed.init :+ row.updated(position, null)
    }
    require(json(comparable) == json(expected), s"$label: independent Spark row oracle failed: $observed")
    val meta = metadata(table)
    val metadataPath = meta.metadataFileLocation()
    export(table.io(), metadataPath, "metadata")
    emit(obj((Seq("record" -> "lineage", "case" -> label, "table" -> table.name(),
      "actual_table_location" -> table.location(), "table_uuid" -> meta.uuid(),
      "metadata" -> metadataPath,
      "method" -> method, "snapshots" -> snapshots.map(capture(table, _)),
      "spark_rows" -> observed, "independent_expected_rows" -> expected) ++ extra): _*))
  }

  def mixed(): Unit = {
    val label = "manifest_mixed"
    val table = create(label, 3, partitioned = false)
    val io = table.io()
    def write(ids: Range, value: String): DataFile =
      data(table, ids.map(id => Seq[Any](id.toLong, 1, value)))

    // Three ordinary appends establish the original lineage of X and a real
    // allocation waterline of 1000. The two padding sources stay in the child.
    append(table, write(-800 until 0, "padding"))
    val x = write(1 to 25, "x")
    val xSnapshot = append(table, x)
    val parent = append(table, write(-975 until -800, "padding"))
    val a = write(26 to 75, "a")
    val b = write(76 to 85, "b")
    val ops = table.asInstanceOf[HasTableOperations].operations()
    val base = ops.current()
    require(base.nextRowId() == 1000L && xSnapshot.firstRowId() == 800L)
    val id = ops.newSnapshotId()
    val sequence = parent.sequenceNumber() + 1
    val manifestPathname = unique(table, "mixed", ".avro")
    val writer = ManifestFiles.write(3, table.spec(), io.newOutputFile(manifestPathname), Long.box(id))
    try {
      val xWithRowId = DataFiles.builder(table.spec()).copy(x).withFirstRowId(800L).build()
      writer.existing(xWithRowId, xSnapshot.snapshotId(), xSnapshot.sequenceNumber(),
        Long.box(xSnapshot.sequenceNumber()))
      writer.add(a)
      writer.add(b)
    } finally writer.close()
    val written = writer.toManifestFile()

    // Publish the publicly authored mixed manifest via a snapshot with a v3
    // descriptor. appendManifest and rewriteManifests constrain entry status.
    val oldList = readAvro(io, parent.manifestListLocation())
    val xPath = xSnapshot.allManifests(io).asScala.find(_.snapshotId() == xSnapshot.snapshotId()).get.path()
    val xDescriptor = oldList.rows.find(row => manifestPath(row) == xPath).get
    val mixed = patch(xDescriptor) { row =>
      row.put("manifest_path", written.path())
      row.put("manifest_length", Long.box(written.length()))
      row.put("added_snapshot_id", Long.box(id))
      row.put("sequence_number", Long.box(sequence))
      row.put("min_sequence_number", Long.box(written.minSequenceNumber()))
      row.put("first_row_id", Long.box(1000L))
      row.put("added_files_count", written.addedFilesCount())
      row.put("added_rows_count", written.addedRowsCount())
      row.put("existing_files_count", written.existingFilesCount())
      row.put("existing_rows_count", written.existingRowsCount())
    }
    val listPath = unique(table, "mixed", ".manifest-list.avro")
    val listHeader = oldList.metadata.toMap ++ Map(
      "snapshot-id" -> id.toString.getBytes(UTF_8),
      "parent-snapshot-id" -> parent.snapshotId().toString.getBytes(UTF_8),
      "sequence-number" -> sequence.toString.getBytes(UTF_8),
      "first-row-id" -> "1000".getBytes(UTF_8))
    writeAvro(io, listPath, oldList.copy(metadata = listHeader.toVector,
      rows = mixed +: oldList.rows.filterNot(row => manifestPath(row) == xPath)))
    // V3 lists reserve for existing + added rows in a manifest without a range.
    val snapshot = SnapshotParser.fromJson(obj(
      "snapshot-id" -> id, "parent-snapshot-id" -> parent.snapshotId(),
      "sequence-number" -> sequence, "timestamp-ms" -> System.currentTimeMillis(),
      "summary" -> obj("operation" -> "append", "added-data-files" -> "2", "added-records" -> "60",
        "total-data-files" -> "5", "total-records" -> "1060"),
      "manifest-list" -> listPath, "schema-id" -> table.schema().schemaId(),
      "first-row-id" -> 1000L, "added-rows" -> 85L).toString)
    ops.commit(base, TableMetadata.buildFrom(base).setBranchSnapshot(snapshot, "main").build())
    table.refresh()
    require(table.currentSnapshot().snapshotId() == id, "REST did not publish the mixed snapshot")

    val raw = readAvro(io, manifestPathname).rows
    require(raw.map(_.get("status")) == Vector(0, 1, 1), "Expected raw EXISTING, ADDED, ADDED")
    require(raw.map(r => file(r).get("first_row_id")) == Vector(800L, null, null),
      "Expected one explicit and two unassigned raw file row IDs")
    val expected = (1 to 25).map(id => Seq[Any](id.toLong, "x", 799L + id, 2L)) ++
      (26 to 75).map(id => Seq[Any](id.toLong, "a", 974L + id, 4L)) ++
      (76 to 85).map(id => Seq[Any](id.toLong, "b", 974L + id, 4L))
    publishEvidence(label, table, "ManifestFiles.write/existing/add, v3 list and snapshot commit",
      Seq(table.currentSnapshot()), lineageRows(label, "id, value", "WHERE id > 0"), expected, None,
      "mixed_manifest" -> manifestPathname, "source_raw_first_row_ids" -> raw.map(r => file(r).get("first_row_id")),
      "expected_first_row_ids" -> Seq(800L, 1000L, 1050L))
  }

  private val bucketLabel = "manifest_old_bucket"
  private val identityLabel = "manifest_promoted_identity"
  private val bucketIds = Vector(1L, 2L)
  private val identityOld = Vector[Row](Seq[Any](11L, 11L, 1.25D), Seq[Any](12L, 3000000001L, 2.5D))
  private val identityNew = Seq[Any](13L, 3000000002L, 3.75D)

  private def singleFile(table: Table, values: Row, tuple: Seq[Any]): DataFile = {
    val partition = new PartitionData(table.spec().partitionType())
    tuple.zipWithIndex.foreach { case (value, index) => partition.set(index, value.asInstanceOf[AnyRef]) }
    val writer = new GenericAppenderFactory(table.schema(), table.spec())
      .set("write.metadata.metrics.default", "full")
      .newDataWriter(EncryptedFiles.plainAsEncryptedOutput(output(table, ".parquet")),
        FileFormat.PARQUET, partition)
    try writer.write(record(table.schema(), values)) finally writer.close()
    writer.toDataFile()
  }

  def historical(): Unit = {
    val bucket = create(bucketLabel, 3, partitioned = false)
    mergePolicy(bucket, false)
    bucket.updateSpec().addField(Expressions.bucket("p", 8)).commit()
    bucket.refresh()
    val bucketField = bucket.spec().fields().get(0).name()
    // Iceberg hashes integer 1 as little-endian int64 with Murmur3, yielding bucket 4 of 8.
    bucketIds.foreach(id => append(bucket, singleFile(bucket, Seq(id, 1, "old"), Seq(4))))
    bucket.updateSpec().removeField(bucketField).commit()
    bucket.updateSchema().deleteColumn("p").commit()
    mergePolicy(bucket, true)
    inspectHistory(bucketLabel, bucket, afterAppend = false)

    fixtureSpark.sql(s"CREATE TABLE ${name(identityLabel)} (id BIGINT, p INT, q FLOAT) USING iceberg " +
      "PARTITIONED BY (p,q) TBLPROPERTIES ('format-version'='3')")
    val identity = load(identityLabel)
    mergePolicy(identity, false)
    val specId = identity.spec().specId()
    append(identity, singleFile(identity, Seq[Any](11L, 11, 1.25F), Seq[Any](11, 1.25F)))
    identity.updateSchema().updateColumn("p", Types.LongType.get())
      .updateColumn("q", Types.DoubleType.get()).commit()
    identity.refresh()
    require(identity.spec().specId() == specId, "Promotion must preserve identity spec ID")
    append(identity, singleFile(identity, identityOld(1), identityOld(1).tail))
    mergePolicy(identity, true)
    inspectHistory(identityLabel, identity, afterAppend = false)
  }

  private case class RawEntry(source: AvroRecord, manifest: Container, entry: AvroRecord) {
    def path: String = file(entry).get("file_path").toString
    def partition: Vector[Any] = {
      val tuple = file(entry).get("partition").asInstanceOf[AvroRecord]
      tuple.getSchema.getFields.asScala.indices.map(tuple.get).toVector
    }
    def schema: Schema = SchemaParser.fromJson(header(manifest, "schema"))
  }

  private def entries(table: Table, snapshot: Snapshot): Vector[RawEntry] =
    readAvro(table.io(), snapshot.manifestListLocation()).rows.flatMap { descriptor =>
      val manifest = readAvro(table.io(), manifestPath(descriptor))
      manifest.rows.map(RawEntry(descriptor, manifest, _))
    }

  private def metricBound(entry: RawEntry, bound: String, id: Int): ByteBuffer = {
    val bounds = file(entry.entry).get(bound).asInstanceOf[java.util.List[AvroRecord]]
    bounds.asScala.find(_.get("key") == id).get.get("value").asInstanceOf[ByteBuffer]
  }

  private def partitionBounds(entry: RawEntry): Vector[(Number, Number)] = {
    val summaries = entry.source.get("partitions").asInstanceOf[java.util.List[AvroRecord]]
    require(summaries.size() == entry.partition.size)
    summaries.asScala.zip(entry.partition).map { case (summary, value) =>
      def decode(field: String): Number = {
        val bytes = summary.get(field).asInstanceOf[ByteBuffer].duplicate().order(ByteOrder.LITTLE_ENDIAN)
        value match {
          case _: java.lang.Integer => require(bytes.remaining() == 4); Int.box(bytes.getInt())
          case _: java.lang.Long => require(bytes.remaining() == 8); Long.box(bytes.getLong())
          case _: java.lang.Float => require(bytes.remaining() == 4); Float.box(bytes.getFloat())
          case _: java.lang.Double => require(bytes.remaining() == 8); Double.box(bytes.getDouble())
        }
      }
      (decode("lower_bound"), decode("upper_bound"))
    }.toVector
  }

  private def inspectHistory(label: String, table: Table, afterAppend: Boolean): Unit = {
    val bucket = label == bucketLabel
    val snapshot = table.currentSnapshot()
    val seed = if (afterAppend) table.snapshot(snapshot.parentId()) else snapshot
    val historical = entries(table, seed).sortBy(_.source.get("sequence_number").asInstanceOf[Long])
    require(historical.size == 2 && historical.map(r => manifestPath(r.source)).distinct.size == 2,
      "Fixture must retain two separate historical manifests before the Nova append")
    val typedPartitions = if (bucket) Vector(Vector(4), Vector(4))
      else Vector(Vector[Any](11, 1.25F), Vector[Any](3000000001L, 2.5D))
    historical.zip(typedPartitions).foreach { case (entry, expected) =>
      require(entry.partition == expected && entry.partition.zip(expected).forall { case (a,b) =>
        a.getClass == b.getClass
      }, s"Unexpected raw historical partition: ${entry.partition}")
    }

    if (afterAppend) {
      val completed = entries(table, snapshot)
      val originals = historical.map { before =>
        require(!completed.exists(e => manifestPath(e.source) == manifestPath(before.source)),
          "Ordinary append retained a historical manifest that should have been merged")
        val matches = completed.filter(_.path == before.path)
        require(matches.size == 1, "Merged manifest must retain each historical file exactly once")
        matches.head
      }
      require(originals.map(e => manifestPath(e.source)).distinct.size == 1,
        "The historical manifests did not merge into a common source")
      originals.zipWithIndex.foreach { case (entry, index) =>
        val expectedTuple = if (bucket) Vector[Any](4) else identityOld(index).tail
        require(entry.partition == expectedTuple && entry.partition.zip(expectedTuple).forall { case (a,b) =>
          a.getClass == b.getClass
        }, s"Merged entry has wrong partition encoding: ${entry.partition}")
        require(entry.entry.get("status") == 0 &&
          entry.entry.get("snapshot_id") == Long.box(if (index == 0) seed.parentId().longValue() else seed.snapshotId()) &&
          entry.entry.get("sequence_number") == Long.box(index + 1L) &&
          entry.entry.get("file_sequence_number") == Long.box(index + 1L) &&
          file(entry.entry).get("first_row_id") == Long.box(index.toLong),
          "Merged entry must preserve original snapshot, sequence and row identity")
        val meta = metadata(table)
        require(Option(meta.schemasById().get(entry.schema.schemaId())).exists(_.sameSchema(entry.schema)),
          "Output header must use a real retained schema")
        val specId = entry.source.get("partition_spec_id").asInstanceOf[Int]
        val spec = meta.specsById().get(specId)
        require(specId == historical(index).source.get("partition_spec_id") &&
          mapper.readTree(header(entry.manifest, "partition-spec")) ==
            mapper.readTree(PartitionSpecParser.toJson(spec)).get("fields"),
          "Output header must preserve the historical partition spec")
        val p = entry.schema.findField("p")
        require(p != null, "Output schema lost the historical partition source")
        if (bucket) require(p.`type`() == Types.IntegerType.get() && table.schema().findField("p") == null)
        else require(p.`type`() == Types.LongType.get() && entry.schema.findType("q") == Types.DoubleType.get())
        val width = if (bucket || index == 0) 4 else 8
        val fields = if (bucket) Seq(p) else Seq(p, entry.schema.findField("q"))
        for (bound <- Seq("lower_bounds", "upper_bounds"); field <- fields) {
          val actual = metricBound(entry, bound, field.fieldId())
          require(actual.remaining() == width && actual == metricBound(historical(index), bound, field.fieldId()),
            s"Merged entry changed stored $bound for ${field.name()}")
        }
      }
      val expectedBounds = if (bucket) Vector((4, 4))
        else Vector[(Any, Any)]((11L, 3000000002L), (1.25D, 3.75D))
      require(partitionBounds(originals.head) == expectedBounds, "Merged partition summaries are incorrect")
    }

    val oldRows = if (bucket) bucketIds.map(id => Seq[Any](id, "old")) else identityOld
    val observed = lineageRows(label, if (bucket) "id, value" else "id, p, q")
    val oldExpected = oldRows.zipWithIndex.map { case (row, index) => row ++ Seq(index.toLong, index + 1L) }
    val newRowIdPosition = if (afterAppend) Some(oldRows.head.size) else None
    val expected = if (afterAppend) {
      val added = if (bucket) Seq[Any](3L, "new") else identityNew
      oldExpected :+ (added ++ Seq(null, 3L))
    } else oldExpected
    publishEvidence(label, table, if (afterAppend) "Nova ordinary append, Spark and raw Avro verification"
        else "Iceberg fast append and public schema/spec evolution",
      if (afterAppend) Seq(seed, snapshot) else Seq(seed), observed, expected, newRowIdPosition,
      "after_nova_append" -> afterAppend,
      "source_manifests" -> historical.map(e => manifestPath(e.source)))
  }

  def verifyHistorical(): Unit = {
    inspectHistory(bucketLabel, load(bucketLabel), afterAppend = true)
    inspectHistory(identityLabel, load(identityLabel), afterAppend = true)
  }
}
