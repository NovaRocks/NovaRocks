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

object DeleteApplicabilityCorpus {
  import DeleteApplicabilityFixture._
  import org.apache.iceberg.deletes.Deletes
  import org.apache.iceberg.io.CloseableIterable
  import org.apache.iceberg.puffin.{Blob, Puffin}

  def capture(t: Table): Unit = {
    export(t.io(), metadata(t).metadataFileLocation(), "metadata")
    t.snapshots().asScala.foreach { s =>
      export(t.io(), s.manifestListLocation(), "manifest-list")
      s.allManifests(t.io()).asScala.foreach(m => export(t.io(), m.path(), "manifest"))
      val tasks = t.newScan().useSnapshot(s.snapshotId()).planFiles()
      try tasks.asScala.foreach { task =>
        export(t.io(), task.file().location(), "data-content")
        task.deletes().asScala.foreach(d => export(t.io(), d.location(), "delete-content"))
      } finally tasks.close()
    }
  }
  def check(name: String, t: Table, rows: Seq[Row], extra: (String, Any)*): Long = {
    t.refresh(); capture(t)
    observe(name, t, obj((Seq("method" -> "official Java content writers and snapshot commit",
      "writer_commit" -> "success") ++ extra): _*), Some(rows))
    t.currentSnapshot().snapshotId()
  }
  def delta(name: String, t: Table, from: Long, to: Long,
      removed: Seq[Row], added: Seq[Row]): Unit = emit(obj("record" -> "endpoint-oracle",
    "case" -> name, "table" -> t.name(), "metadata" -> metadata(t).metadataFileLocation(),
    "from_snapshot" -> from, "to_snapshot" -> to,
    "independent_removed_rows" -> removed, "independent_added_rows" -> added))

  def sameCommitDvEquality(): Unit = {
    val t = create("same_commit_dv_equality", 3)
    val old = data(t, Seq(Seq(7L, 1, "old7"), Seq(10L, 1, "old10")))
    t.newAppend().appendFile(old).commit()
    val fresh = data(t, Seq(Seq(7L, 1, "new7"), Seq(8L, 1, "new8"), Seq(9L, 1, "new9")))
    val vector = dv(t, Seq((fresh.location(), 1L))).head
    val eq = equality(t, Seq(7L))
    t.newRowDelta().addRows(fresh).addDeletes(vector).addDeletes(eq).commit()
    check("same_commit_dv_equality", t, Seq(Seq(10L, 1, "old10"), Seq(7L, 1, "new7"),
      Seq(9L, 1, "new9")), "same_commit_deletes" -> Seq(describe(vector), describe(eq)))
  }

  def sameCommitPositionEquality(): Unit = {
    val t = create("same_commit_position_equality", 2)
    val old = data(t, Seq(Seq(7L, 1, "old7"), Seq(10L, 1, "old10")))
    t.newAppend().appendFile(old).commit()
    val fresh = data(t, Seq(Seq(7L, 1, "new7"), Seq(8L, 1, "new8"), Seq(9L, 1, "new9")))
    val pos = positionDelete(t, Seq((fresh.location(), 1L)))
    val eq = equality(t, Seq(7L))
    t.newRowDelta().addRows(fresh).addDeletes(pos).addDeletes(eq).commit()
    check("same_commit_position_equality", t, Seq(Seq(10L, 1, "old10"), Seq(7L, 1, "new7"),
      Seq(9L, 1, "new9")), "same_commit_deletes" -> Seq(describe(pos), describe(eq)))
  }

  def multiTargetPosition(): Unit = {
    val t = create("multi_target_position", 2)
    val a = data(t, Seq(Seq(1L, 1, "a1"), Seq(2L, 1, "a2")))
    val b = data(t, Seq(Seq(3L, 1, "b3"), Seq(4L, 1, "b4")))
    t.newAppend().appendFile(a).appendFile(b).commit()
    val pos = positionDelete(t, Seq((a.location(), 0L), (b.location(), 1L)))
    t.newRowDelta().addDeletes(pos).commit()
    check("multi_target_position", t, Seq(Seq(2L, 1, "a2"), Seq(3L, 1, "b3")),
      "targets" -> Seq(a.location(), b.location()))
  }

  def multiBlobDv(): Unit = {
    val t = create("multi_blob_dv", 3)
    val a = data(t, Seq(Seq(1L, 1, "a1"), Seq(2L, 1, "a2")))
    val b = data(t, Seq(Seq(3L, 1, "b3"), Seq(4L, 1, "b4")))
    t.newAppend().appendFile(a).appendFile(b).commit()
    val ds = dv(t, Seq((a.location(), 0L), (b.location(), 1L)))
    require(ds.size == 2 && ds.map(_.location()).distinct.size == 1 &&
      ds.map(d => (d.contentOffset(), d.contentSizeInBytes())).distinct.size == 2,
      "The fixture must contain two distinct blobs in one immutable Puffin")
    val commit = t.newRowDelta(); ds.foreach(commit.addDeletes); commit.commit()
    check("multi_blob_dv", t, Seq(Seq(2L, 1, "a2"), Seq(3L, 1, "b3")),
      "immutable_puffin_blobs" -> ds.map(describe))
  }

  def legacyUpgrade(): Unit = {
    val t = create("legacy_position_dv_equality", 2)
    val a = data(t, Seq(Seq(1L, 1, "a1"), Seq(2L, 1, "a2")))
    val b = data(t, Seq(Seq(3L, 1, "b3"), Seq(4L, 1, "b4"), Seq(5L, 1, "b5")))
    t.newAppend().appendFile(a).appendFile(b).commit()
    val legacy = positionDelete(t, Seq((a.location(), 0L), (b.location(), 0L)))
    t.newRowDelta().addDeletes(legacy).commit()
    check("legacy_position_before_upgrade", t,
      Seq(Seq(2L, 1, "a2"), Seq(4L, 1, "b4"), Seq(5L, 1, "b5")))
    t.updateProperties().set("format-version", "3").commit(); t.refresh()
    val vector = dv(t, Seq((a.location(), 0L), (a.location(), 1L))).head
    val eq = equality(t, Seq(4L))
    t.newRowDelta().addDeletes(vector).addDeletes(eq).commit()
    check("legacy_position_dv_equality", t, Seq(Seq(5L, 1, "b5")),
      "retained_legacy_position" -> describe(legacy), "new_dv" -> describe(vector))
  }

  def cumulativeDv(): Unit = {
    val t = create("cumulative_dv", 3)
    val f = data(t, (1L to 4L).map(i => Seq(i, 1, "r" + i)))
    t.newAppend().appendFile(f).commit()
    val first = dv(t, Seq((f.location(), 0L))).head
    t.newRowDelta().addDeletes(first).commit()
    val from = check("cumulative_dv_from", t, (2L to 4L).map(i => Seq(i, 1, "r" + i)))
    val second = dv(t, Seq((f.location(), 0L), (f.location(), 2L))).head
    t.newRowDelta().validateFromSnapshot(from).removeDeletes(first).addDeletes(second).commit()
    val to = check("cumulative_dv_to", t, Seq(Seq(2L, 1, "r2"), Seq(4L, 1, "r4")),
      "previous_dv" -> describe(first), "replacement_dv" -> describe(second))
    delta("cumulative_dv", t, from, to, Seq(Seq(3L, 1, "r3")), Seq.empty)
  }

  def retainedDataSequence(): Unit = {
    val t = create("data_sequence_rewrite", 2)
    val original = data(t, Seq(Seq(1L, 1, "r1"), Seq(2L, 1, "r2")))
    t.newAppend().appendFile(original).commit()
    val rewritten = data(t, Seq(Seq(1L, 1, "r1"), Seq(2L, 1, "r2")))
    t.newRewrite().rewriteFiles(Set(original).asJava, Set(rewritten).asJava, 1L).commit()
    val tasks = t.newScan().planFiles()
    try {
      val actual = tasks.asScala.toVector.map(_.file())
      require(actual.size == 1 && actual.head.dataSequenceNumber() == Long.box(1L) &&
        actual.head.fileSequenceNumber() == Long.box(2L), "Rewrite must retain data sequence 1 with file sequence 2")
    } finally tasks.close()
    t.newRowDelta().addDeletes(equality(t, Seq(1L))).commit()
    check("data_sequence_rewrite", t, Seq(Seq(2L, 1, "r2")),
      "rewrite_data_sequence" -> 1L, "rewrite_file_sequence" -> 2L)
  }

  def eqFor(t: Table, spec: PartitionSpec, fields: Seq[String], keys: Seq[Row], p: Int): DeleteFile = {
    val schema = t.schema().select(fields: _*)
    val ids = fields.map(t.schema().findField(_).fieldId()).toArray
    val tuple = new PartitionData(spec.partitionType())
    if (!spec.isUnpartitioned()) tuple.set(0, Int.box(p))
    val writer = new GenericAppenderFactory(t.schema(), spec, ids, schema, null)
      .set("write.metadata.metrics.default", "full")
      .newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, ".parquet")), FileFormat.PARQUET, tuple)
    try keys.foreach(k => writer.write(record(schema, k))) finally writer.close()
    writer.toDeleteFile()
  }

  def globalPartitionGroups(): Unit = {
    val t = create("global_partition_field_groups", 2, false)
    val globalSpec = t.spec()
    val oldRows = Seq(Seq(1L, 1, "old1"), Seq(2L, 1, "drop"), Seq(3L, 2, "old3"))
    val old = data(t, oldRows)
    t.newAppend().appendFile(old).commit()
    t.updateSpec().addField("p").commit(); t.refresh()
    val a = data(t, Seq(Seq(1L, 1, "new1"), Seq(2L, 1, "drop"), Seq(3L, 1, "keep")), 1)
    val b = data(t, Seq(Seq(1L, 2, "new1"), Seq(2L, 2, "drop"), Seq(3L, 2, "keep")), 2)
    t.newAppend().appendFile(a).appendFile(b).commit()
    val global = eqFor(t, globalSpec, Seq("id"), Seq(Seq(1L)), 1)
    val part = eqFor(t, t.spec(), Seq("value"), Seq(Seq("drop")), 1)
    t.newRowDelta().addDeletes(global).addDeletes(part).commit()
    check("global_partition_field_groups", t, Seq(Seq(2L, 1, "drop"), Seq(3L, 2, "old3"),
      Seq(3L, 1, "keep"), Seq(2L, 2, "drop"), Seq(3L, 2, "keep")),
      "global_delete" -> describe(global), "partition_delete" -> describe(part))
  }

  def index(positions: Seq[Long]): PositionDeleteIndex = Deletes.toPositionIndex(
    CloseableIterable.withNoopClose[java.lang.Long](positions.map(Long.box).asJava))

  // Finish all bytes before publishing either reference. No published object is rewritten.
  def immutableEndpointBlobs(): Unit = {
    val t = create("same_puffin_endpoint_blobs", 3)
    val f = data(t, (10L to 13L).map(i => Seq(i, 1, "r" + i)))
    t.newAppend().appendFile(f).commit()
    val outputFile = output(t, ".puffin")
    val writer = Puffin.write(outputFile).createdBy(IcebergBuild.fullVersion()).build()
    val indexes = Seq(index(Seq(0L)), index(Seq(0L, 2L)))
    val blobs = try indexes.map { ix => writer.write(new Blob("deletion-vector-v1",
      Seq(Int.box(MetadataColumns.ROW_POSITION.fieldId())).asJava, -1L, -1L, ix.serialize(), null,
      Map("referenced-data-file" -> f.location(), "cardinality" -> ix.cardinality().toString).asJava))
    } finally writer.close()
    val vectors = indexes.zip(blobs).map { case (ix, blob) =>
      FileMetadata.deleteFileBuilder(t.spec()).ofPositionDeletes().withFormat(FileFormat.PUFFIN)
        .withPath(outputFile.location()).withPartition(partition(t, 1)).withFileSizeInBytes(writer.fileSize())
        .withReferencedDataFile(f.location()).withContentOffset(blob.offset()).withContentSizeInBytes(blob.length())
        .withRecordCount(ix.cardinality()).build()
    }
    require(vectors.map(_.location()).distinct.size == 1 && vectors.map(_.contentOffset()).distinct.size == 2)
    val before = bytes(t.io(), outputFile.location())
    t.newRowDelta().addDeletes(vectors(0)).commit()
    val from = check("same_puffin_endpoint_from", t,
      Seq(Seq(11L, 1, "r11"), Seq(12L, 1, "r12"), Seq(13L, 1, "r13")),
      "method_detail" -> "official Puffin.write with complete immutable container before S1",
      "all_container_blobs" -> vectors.map(describe))
    t.newRowDelta().validateFromSnapshot(from).removeDeletes(vectors(0)).addDeletes(vectors(1)).commit()
    val to = check("same_puffin_endpoint_to", t, Seq(Seq(11L, 1, "r11"), Seq(13L, 1, "r13")),
      "method_detail" -> "official RowDelta replaces exact blob reference, container already complete")
    require(java.util.Arrays.equals(before, bytes(t.io(), outputFile.location())), "Published Puffin changed")
    delta("same_puffin_endpoint_blobs", t, from, to, Seq(Seq(12L, 1, "r12")), Seq.empty)
  }

  def equivalentPositionToDv(): Unit = {
    val t = create("equivalent_position_to_dv", 2)
    val f = data(t, Seq(Seq(1L, 1, "r1"), Seq(2L, 1, "r2"), Seq(3L, 1, "r3")))
    t.newAppend().appendFile(f).commit()
    val pos = positionDelete(t, Seq((f.location(), 0L)))
    t.newRowDelta().addDeletes(pos).commit()
    val from = check("equivalent_position_from", t, Seq(Seq(2L, 1, "r2"), Seq(3L, 1, "r3")))
    t.updateProperties().set("format-version", "3").commit(); t.refresh()
    val vector = dv(t, Seq((f.location(), 0L))).head
    t.newRowDelta().removeDeletes(pos).addDeletes(vector).commit()
    val to = check("equivalent_dv_to", t, Seq(Seq(2L, 1, "r2"), Seq(3L, 1, "r3")))
    delta("equivalent_position_to_dv", t, from, to, Seq.empty, Seq.empty)
  }

  def parquetLayout(t: Table, f: DeleteFile): JsonNode = {
    val content = bytes(t.io(), f.location())
    val input = new org.apache.iceberg.shaded.org.apache.parquet.io.InputFile {
      override def getLength(): Long = content.length.toLong
      override def newStream(): org.apache.iceberg.shaded.org.apache.parquet.io.SeekableInputStream =
        new org.apache.iceberg.shaded.org.apache.parquet.io.SeekableInputStream {
          private var pos = 0
          override def getPos(): Long = pos.toLong
          override def seek(p: Long): Unit = {
            require(p >= 0 && p <= content.length); pos = p.toInt
          }
          override def read(): Int = if (pos == content.length) -1 else {
            val v = content(pos) & 0xff; pos += 1; v
          }
          override def read(b: Array[Byte], offset: Int, length: Int): Int = {
            if (length == 0) 0 else if (pos == content.length) -1 else {
              val n = math.min(length, content.length - pos)
              System.arraycopy(content, pos, b, offset, n); pos += n; n
            }
          }
          override def readFully(b: Array[Byte]): Unit = readFully(b, 0, b.length)
          override def readFully(b: Array[Byte], offset: Int, length: Int): Unit = {
            if (content.length - pos < length) throw new java.io.EOFException()
            System.arraycopy(content, pos, b, offset, length); pos += length
          }
          override def read(b: java.nio.ByteBuffer): Int = {
            if (!b.hasRemaining()) 0 else if (pos == content.length) -1 else {
              val n = math.min(b.remaining(), content.length - pos)
              b.put(content, pos, n); pos += n; n
            }
          }
          override def readFully(b: java.nio.ByteBuffer): Unit = {
            if (content.length - pos < b.remaining()) throw new java.io.EOFException()
            read(b)
          }
        }
    }
    val reader = org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(input)
    try obj("path" -> f.location(), "row_groups" -> reader.getRowGroups().asScala.toSeq.map { rg =>
      obj("row_count" -> rg.getRowCount(), "columns" -> rg.getColumns().asScala.toSeq.map { c =>
        val stats = c.getStatistics()
        obj("column" -> c.getPath().toDotString(), "offset" -> c.getStartingPos(),
          "compressed_size" -> c.getTotalSize(), "statistics_empty" -> stats.isEmpty(),
          "min" -> (if (stats.hasNonNullValue()) stats.genericGetMin().toString else null),
          "max" -> (if (stats.hasNonNullValue()) stats.genericGetMax().toString else null))
      })
    }) finally reader.close()
  }

  def positionRanges(mode: String): Unit = {
    val t = create("position_ranges_" + mode, 2)
    val files = (0 until 4).map { n =>
      val rows = Seq(Seq(n.toLong * 2L, 1, "remove" + n), Seq(n.toLong * 2L + 1L, 1, "keep" + n))
      if (mode != "long_prefix") data(t, rows) else {
        val longPrefix = Seq.fill(3)("long-common-prefix-" * 8).mkString("/")
        val out = t.io().newOutputFile(t.location() + "/data/" + longPrefix + "/" + n + ".parquet")
        val writer = new GenericAppenderFactory(t.schema(), t.spec())
          .newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, partition(t, 1))
        try rows.foreach(r => writer.write(record(t.schema(), r))) finally writer.close()
        writer.toDataFile()
      }
    }
    val append = t.newAppend(); files.foreach(append.appendFile); append.commit()
    val positionSchema = new Schema(MetadataColumns.DELETE_FILE_PATH, MetadataColumns.DELETE_FILE_POS)
    val factory = new GenericAppenderFactory(positionSchema)
      .set("write.metadata.metrics.default", if (mode == "missing_stats") "none" else "full")
      .set("write.parquet.row-group-size-bytes", "8192")
      .set("write.parquet.row-group-check-min-record-count", "100")
      .set("write.parquet.row-group-check-max-record-count", "100")
      .set("write.parquet.page-size-bytes", "4096")
      .set("write.parquet.compression-codec", "uncompressed")
      .set("parquet.enable.dictionary", "false")
    if (mode == "missing_stats") factory.set("write.parquet.stats-enabled.column.file_path", "false")
    if (mode == "long_prefix") factory.set("write.metadata.metrics.column.file_path", "truncate(16)")
    // The position-writer context drops per-column statistics configuration in
    // Java 1.11.0. A public appender with the official position schema exposes it.
    val outputFile = output(t, ".parquet")
    val writer = factory.newAppender(outputFile, FileFormat.PARQUET)
    // Repeated positions are legal and preserve a small independent row bag while
    // forcing multiple physical row groups with disjoint target path ranges.
    try files.sortBy(_.location()).foreach(f => (0 until 400).foreach(_ =>
      writer.add(record(positionSchema, Seq(f.location(), 0L))))) finally writer.close()
    val pos = FileMetadata.deleteFileBuilder(t.spec()).ofPositionDeletes().withFormat(FileFormat.PARQUET)
      .withPath(outputFile.location()).withPartition(partition(t, 1))
      .withFileSizeInBytes(t.io().newInputFile(outputFile.location()).getLength()).withMetrics(writer.metrics()).build()
    val layout = parquetLayout(t, pos)
    require(layout.get("row_groups").size() > 1, "Position fixture must have multiple physical row groups")
    if (mode == "missing_stats") require(layout.get("row_groups").elements().asScala.forall(rg =>
      rg.get("columns").elements().asScala.filter(_.get("column").asText() == "file_path")
        .forall(_.get("statistics_empty").asBoolean())), "Missing path statistics were not produced")
    t.newRowDelta().addDeletes(pos).commit()
    check("position_ranges_" + mode, t, (0 until 4).map(n => Seq(n.toLong * 2L + 1L, 1, "keep" + n)),
      "physical_layout" -> layout, "statistics_mode" -> mode,
      "method_detail" -> "official GenericAppenderFactory.newAppender with MetadataColumns position schema and FileMetadata builder",
      "position_content" -> describe(pos), "target_paths" -> files.map(_.location()))
  }

  def createKey(name: String, keyType: String): Table = {
    val qualified = s"ice_rest.$namespace.${prefix}_$name"
    DeleteApplicabilityFixture.spark.sql(s"CREATE TABLE $qualified (id BIGINT, p INT, value STRING, key $keyType) " +
      "USING iceberg PARTITIONED BY (p) TBLPROPERTIES ('format-version'='2', 'write.metadata.metrics.default'='full')")
    val t = Spark3Util.loadIcebergTable(DeleteApplicabilityFixture.spark, qualified); tables += ((qualified, t)); t
  }
  def keyEquality(t: Table, keys: Seq[Any], stats: Boolean = true): DeleteFile = {
    val schema = t.schema().select("key")
    val writer = new GenericAppenderFactory(t.schema(), t.spec(), Array(4), schema, null)
      .set("write.metadata.metrics.default", if (stats) "full" else "none")
      .newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, ".parquet")),
        FileFormat.PARQUET, partition(t, 1))
    try keys.foreach(k => writer.write(record(schema, Seq(k)))) finally writer.close()
    writer.toDeleteFile()
  }
  def metrics(f: ContentFile[_]): JsonNode = {
    def counts(m: java.util.Map[java.lang.Integer, java.lang.Long]): Any =
      if (m == null) null else m.asScala.toSeq.map { case (id, count) => obj("field_id" -> id.intValue(), "count" -> count) }
    def bounds(m: java.util.Map[java.lang.Integer, java.nio.ByteBuffer]): Any =
      if (m == null) null else m.asScala.toSeq.map { case (id, value) =>
        val copy = value.duplicate(); val b = new Array[Byte](copy.remaining()); copy.get(b)
        obj("field_id" -> id.intValue(), "hex" -> b.map(v => f"${v & 0xff}%02x").mkString)
      }
    obj("file" -> describe(f), "value_counts" -> counts(f.valueCounts()),
      "null_counts" -> counts(f.nullValueCounts()), "nan_counts" -> counts(f.nanValueCounts()),
      "lower_bounds" -> bounds(f.lowerBounds()), "upper_bounds" -> bounds(f.upperBounds()))
  }

  def nullEquality(): Unit = {
    val t = createKey("equality_null", "INT")
    val f = data(t, Seq(Seq(1L, 1, "null", null), Seq(2L, 1, "one", 1), Seq(3L, 1, "two", 2)))
    t.newAppend().appendFile(f).commit()
    val eq = keyEquality(t, Seq(null))
    t.newRowDelta().addDeletes(eq).commit()
    check("equality_null", t, Seq(Seq(2L, 1, "one"), Seq(3L, 1, "two")),
      "data_metrics" -> metrics(f), "delete_metrics" -> metrics(eq), "key_type" -> "INT")
  }

  def nanAndSignedZero(): Unit = {
    val t = createKey("equality_nan_signed_zero", "DOUBLE")
    val nan1 = java.lang.Double.longBitsToDouble(0x7ff8000000000001L)
    val nan2 = java.lang.Double.longBitsToDouble(0x7ff8000000000042L)
    val f = data(t, Seq(Seq(1L, 1, "null", null), Seq(2L, 1, "nan1", nan1), Seq(3L, 1, "nan2", nan2),
      Seq(4L, 1, "negative_zero", -0.0d), Seq(5L, 1, "positive_zero", 0.0d), Seq(6L, 1, "finite", 2.0d)))
    t.newAppend().appendFile(f).commit()
    val initial = IcebergGenerics.read(t).build()
    val actualBits = try initial.asScala.toVector.filter(r => r.getField("key") != null).map { r =>
      obj("row_label" -> r.getField("value").toString, "written_double_bits" ->
        java.lang.Long.toHexString(java.lang.Double.doubleToRawLongBits(r.getField("key").asInstanceOf[java.lang.Double])))
    } finally initial.close()
    val eq = keyEquality(t, Seq(null, Double.NaN, -0.0d))
    t.newRowDelta().addDeletes(eq).commit()
    check("equality_nan_signed_zero", t, Seq(Seq(5L, 1, "positive_zero"), Seq(6L, 1, "finite")),
      "data_metrics" -> metrics(f), "delete_metrics" -> metrics(eq), "key_type" -> "DOUBLE",
      "source_nan_bits" -> Seq("7ff8000000000001", "7ff8000000000042"), "actual_written_key_bits" -> actualBits)
  }

  def promotedKey(fromType: String): Unit = {
    val integral = fromType == "INT"
    val t = createKey("equality_promoted_" + fromType.toLowerCase, fromType)
    val first: Any = if (integral) Int.box(7) else Float.box(7.5f)
    val second: Any = if (integral) Int.box(8) else Float.box(8.25f)
    val f = data(t, Seq(Seq(1L, 1, "remove_old", first), Seq(2L, 1, "keep_old", second)))
    t.newAppend().appendFile(f).commit()
    val promoted = if (integral) org.apache.iceberg.types.Types.LongType.get()
      else org.apache.iceberg.types.Types.DoubleType.get()
    t.updateSchema().updateColumn("key", promoted).commit(); t.refresh()
    val freshKey: Any = if (integral) Long.box(9L) else Double.box(9.5d)
    val fresh = data(t, Seq(Seq(3L, 1, "keep_new", freshKey)))
    t.newAppend().appendFile(fresh).commit()
    val deletedKey: Any = if (integral) Long.box(7L) else Double.box(7.5d)
    val eq = keyEquality(t, Seq(deletedKey))
    t.newRowDelta().addDeletes(eq).commit()
    check("equality_promoted_" + fromType.toLowerCase, t,
      Seq(Seq(2L, 1, "keep_old"), Seq(3L, 1, "keep_new")),
      "historical_key_type" -> fromType, "resolved_key_type" -> (if (integral) "LONG" else "DOUBLE"),
      "unchanged_identity_partition_field" -> "p: INT",
      "historical_data_metrics" -> metrics(f), "current_data_metrics" -> metrics(fresh),
      "delete_metrics" -> metrics(eq))
  }

  def disjointAndMissingBounds(): Unit = {
    val t = createKey("equality_bounds", "BIGINT")
    val a = data(t, Seq(Seq(1L, 1, "a10", 10L), Seq(2L, 1, "a11", 11L)))
    val b = data(t, Seq(Seq(3L, 1, "b100", 100L), Seq(4L, 1, "b101", 101L)))
    t.newAppend().appendFile(a).appendFile(b).commit()
    val full = keyEquality(t, Seq(100L))
    t.newRowDelta().addDeletes(full).commit()
    val prunedTasks = t.newScan().includeColumnStats().planFiles()
    try require(prunedTasks.asScala.find(_.file().location() == a.location()).get.deletes().isEmpty(),
      "Reliable disjoint key bounds must exclude the equality delete") finally prunedTasks.close()
    val expected = Seq(Seq(1L, 1, "a10"), Seq(2L, 1, "a11"), Seq(4L, 1, "b101"))
    check("equality_disjoint_bounds", t, expected, "data_metrics" -> Seq(metrics(a), metrics(b)),
      "delete_metrics" -> metrics(full), "independent_disjoint_data_path" -> a.location())
    val missing = keyEquality(t, Seq(100L), false)
    t.newRowDelta().removeDeletes(full).addDeletes(missing).commit()
    val retainedTasks = t.newScan().includeColumnStats().planFiles()
    try require(retainedTasks.asScala.forall(_.deletes().size() == 1),
      "Missing equality bounds must conservatively retain both candidates") finally retainedTasks.close()
    check("equality_missing_bounds", t, expected, "data_metrics" -> Seq(metrics(a), metrics(b)),
      "delete_metrics" -> metrics(missing), "stats_missing_retains_candidate" -> a.location())
  }

  def run(): Unit = {
    sameCommitDvEquality()
    sameCommitPositionEquality()
    multiTargetPosition()
    multiBlobDv()
    legacyUpgrade()
    cumulativeDv()
    retainedDataSequence()
    globalPartitionGroups()
    immutableEndpointBlobs()
    equivalentPositionToDv()
    Seq("full", "missing_stats", "long_prefix").foreach(positionRanges)
    nullEquality()
    nanAndSignedZero()
    Seq("INT", "FLOAT").foreach(promotedKey)
    disjointAndMissingBounds()
  }
}
