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

// Compose after iceberg-delete-applicability/generate.scala. The original
// fixture run owns storage, the Spark process, deadline and final cleanup.
// Expected bags come from these fixed SDK inputs, never NovaRocks output.
object MetadataHistoryFixture {
  import DeleteApplicabilityFixture._
  import org.apache.iceberg.types.{Type, Types, TypeUtil}
  import org.apache.iceberg.catalog.TableIdentifier

  val DomainProperty = "novarocks.field_domains.v1"
  val DeepPath = "deep." + Vector.fill(63)("element").mkString(".")
  val FutureSourceName = "future_partition_source"
  val FutureSourceTag = "future_before_partition"
  val Names = Set("metadata_depth", "metadata_depth_copy", FutureSourceName)

  def catalog = Spark3Util.loadIcebergCatalog(DeleteApplicabilityFixture.spark, "ice_rest")
  def load(ns: String, name: String): Table = {
    require(ns == namespace && Names.contains(name), "Unexpected exact fixture table")
    val t = catalog.loadTable(TableIdentifier.of(ns, name)); t.refresh()
    require(ownedLocation(t.location()), "Table leaves the original fixture run storage")
    t
  }
  def create(ns: String, name: String, schema: Schema, properties: Map[String, String]): Table = {
    require(ns == namespace && Names.contains(name), "Unexpected exact fixture table")
    val t = catalog.createTable(TableIdentifier.of(ns, name), schema,
      PartitionSpec.unpartitioned(), properties.asJava)
    require(ownedLocation(t.location()), "CREATE leaves the original fixture run storage")
    require(t.currentSnapshot() == null && t.spec().isUnpartitioned())
    require(t.schema().sameSchema(schema), "CREATE changed the exact schema field IDs or types")
    t
  }
  def append(t: Table, rows: Vector[Record], label: String): Unit = {
    val before = Option(t.currentSnapshot()).map(_.snapshotId())
    val output = t.io().newOutputFile(t.location().stripSuffix("/") +
      "/data/read-policy-" + label + "-" + java.util.UUID.randomUUID() + ".parquet")
    val writer = new GenericAppenderFactory(t.schema(), t.spec()).newDataWriter(
      EncryptedFiles.plainAsEncryptedOutput(output), FileFormat.PARQUET, null)
    try rows.foreach(writer.write) finally writer.close()
    require(writer.toDataFile().recordCount() == rows.size)
    t.newAppend().appendFile(writer.toDataFile()).commit(); t.refresh()
    require(Option(t.currentSnapshot().parentId()).map(_.longValue()) == before)
    require(t.currentSnapshot().schemaId().intValue() == t.schema().schemaId())
  }
  def tag(t: Table, name: String): Long = {
    val ref = t.refs().get(name)
    require(ref != null && ref.isTag(), "Expected SDK tag is absent")
    ref.snapshotId()
  }
  def createTag(t: Table, name: String): Unit = {
    t.manageSnapshots().createTag(name, t.currentSnapshot().snapshotId()).commit(); t.refresh()
    require(tag(t, name) == t.currentSnapshot().snapshotId())
  }
  def snapshotSchema(t: Table, snapshot: Long): Schema = {
    val s = t.snapshot(snapshot)
    require(s != null && s.schemaId() != null && t.schemas().containsKey(s.schemaId()))
    t.schemas().get(s.schemaId())
  }
  def depth(t: Type): Int = t.typeId() match {
    case Type.TypeID.STRUCT => 1 + t.asStructType().fields().asScala.map(f => depth(f.`type`())).max
    case Type.TypeID.LIST => 1 + depth(t.asListType().elementType())
    case Type.TypeID.MAP => 1 + math.max(depth(t.asMapType().keyType()), depth(t.asMapType().valueType()))
    case _ => 1
  }
  def fieldDepth(schema: Schema): Int = schema.columns().asScala.map(f => depth(f.`type`())).max
  def value(t: Type, v: Any): JsonNode = {
    if (v == null) mapper.getNodeFactory.nullNode()
    else if (t.typeId() == Type.TypeID.STRUCT) {
      val n = mapper.createObjectNode(); val row = v.asInstanceOf[Record]
      t.asStructType().fields().asScala.foreach { f =>
        n.set[JsonNode](f.name(), value(f.`type`(), row.getField(f.name())))
      }; n
    } else if (t.typeId() == Type.TypeID.LIST) {
      val n = mapper.createArrayNode()
      v.asInstanceOf[java.util.List[AnyRef]].asScala.foreach(e => n.add(value(t.asListType().elementType(), e)))
      n
    } else json(v)
  }
  def content(schema: Schema, row: Record): String = {
    val n = mapper.createObjectNode()
    schema.columns().asScala.foreach(f => n.set[JsonNode](f.name(), value(f.`type`(), row.getField(f.name()))))
    mapper.writeValueAsString(n)
  }
  def expected(schema: Schema, rows: Vector[Record]): Map[String, Int] =
    rows.map(content(schema, _)).groupBy(identity).map { case(k, v) => k -> v.size }
  def bag(t: Table, snapshot: Long, schema: Schema): Map[String, Int] = {
    val reader = IcebergGenerics.read(t).useSnapshot(snapshot).project(schema).build()
    try reader.asScala.map(content(schema, _)).toVector.groupBy(identity)
      .map { case(k, v) => k -> v.size }
    finally reader.close()
  }
  def bagFact(values: Map[String, Int]): Vector[JsonNode] =
    values.toVector.sortBy(_._1).map { case(k, v) => obj("content" -> k, "count" -> v) }
  def facts(t: Table): JsonNode = {
    val m = metadata(t)
    // The Java SDK reads the actual Parquet schema without Arrow hints. Check
    // every physical semantic ID against a complete retained table schema.
    val scan = t.newScan().planFiles()
    val files = try scan.asScala.map { task =>
      val local = java.nio.file.Files.createTempFile("iceberg-read-policy-", ".parquet")
      try {
        val input = t.io().newInputFile(task.file().location()).newStream()
        try java.nio.file.Files.copy(input, local, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
        finally input.close()
        val reader = org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
          org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(local.toUri()), new org.apache.hadoop.conf.Configuration()))
        try {
          val raw = reader.getFooter().getFileMetaData().getSchema()
          val converted = org.apache.iceberg.parquet.ParquetSchemaUtil.convertAndPrune(raw)
          val dataSchema = new Schema(converted.columns().asScala.filterNot(f =>
            Set("_row_id", "_last_updated_sequence_number").contains(f.name())).asJava)
          val ids = TypeUtil.getProjectedIds(dataSchema).asScala.map(_.intValue()).toSet
          require(ids.nonEmpty && t.schemas().values().asScala.exists { schema =>
            ids == TypeUtil.getProjectedIds(schema).asScala.map(_.intValue()).toSet
          }, "Raw Parquet IDs do not belong to one exact retained table schema")
          obj("path" -> task.file().location(), "record_count" -> task.file().recordCount(),
            "schema" -> raw.toString, "field_ids" -> ids.toVector.sorted)
        } finally reader.close()
      } finally java.nio.file.Files.deleteIfExists(local)
    }.toVector finally scan.close()
    obj("table_uuid" -> m.uuid().toString, "metadata_file" -> m.metadataFileLocation(),
      "snapshot" -> t.currentSnapshot().snapshotId(), "current_schema" -> SchemaParser.toJson(t.schema()),
      "field_domains_json" -> t.properties().get(DomainProperty),
      "files" -> files,
      "retained_schemas" -> t.schemas().asScala.toVector.sortBy(_._1.intValue()).map { case(id, s) =>
        obj("schema_id" -> id.intValue(), "depth" -> fieldDepth(s), "schema" -> SchemaParser.toJson(s)) })
  }
  def depthSchema: Schema = {
    var t: Type = Types.IntegerType.get()
    for (i <- (1 to 63).reverse) t = Types.ListType.ofOptional(i + 2, t)
    new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "deep", t))
  }
  def depthRow(schema: Schema, id: Long, leaf: Option[Int]): Record = {
    val row = GenericRecord.create(schema); row.setField("id", Long.box(id))
    leaf.foreach { v =>
      var t = schema.findType("deep"); var levels = 0
      while (t.typeId() == Type.TypeID.LIST) { levels += 1; t = t.asListType().elementType() }
      require(levels == 63 && t == Types.IntegerType.get())
      var child: AnyRef = Int.box(v)
      for (_ <- 1 to levels) child = java.util.Collections.singletonList(child)
      row.setField("deep", child)
    }; row
  }
  def depthRows(schema: Schema): Vector[Record] = Vector(
    depthRow(schema, 1, Some(-128)), depthRow(schema, 1, Some(-128)), depthRow(schema, 2, None))
  def depthInitialize(ns: String): Unit = {
    val requested = depthSchema
    val t = create(ns, "metadata_depth", requested, Map("format-version" -> "3",
      "write.row-lineage" -> "true"))
    require(fieldDepth(t.schema()) == 64)
    append(t, depthRows(t.schema()), "initial"); createTag(t, "depth_initial")
    val rows = expected(t.schema(), depthRows(t.schema()))
    require(bag(t, t.currentSnapshot().snapshotId(), t.schema()) == rows)
    emit(obj("record" -> "metadata_depth_initialized", "facts" -> facts(t), "bag" -> bagFact(rows)))
    println("METADATA_DEPTH_READY")
  }
  def depthObserveAppend(ns: String): Unit = {
    val t = load(ns, "metadata_depth"); val initial = tag(t, "depth_initial")
    require(fieldDepth(t.schema()) == 64 && t.snapshots().asScala.size == 3)
    require(t.snapshot(t.currentSnapshot().parentId()).parentId().longValue() == initial)
    val rows = expected(t.schema(), depthRows(t.schema()) ++ Vector(depthRow(t.schema(), 3, None), depthRow(t.schema(), 5, Some(7))))
    require(bag(t, t.currentSnapshot().snapshotId(), t.schema()) == rows)
    val copy = load(ns, "metadata_depth_copy")
    require(metadata(copy).uuid() != metadata(t).uuid() && fieldDepth(copy.schema()) == 64)
    require(bag(copy, copy.currentSnapshot().snapshotId(), copy.schema()) == rows)
    emit(obj("record" -> "metadata_depth_native_committed", "source" -> facts(t),
      "ctas" -> facts(copy), "bag" -> bagFact(rows)))
    println("METADATA_DEPTH_NATIVE_COMMIT_OBSERVED")
  }
  def depthRetainHistory(ns: String): Unit = {
    val t = load(ns, "metadata_depth"); val schema = t.schema()
    createTag(t, "depth_before_drop")
    t.updateSchema().deleteColumn("deep").commit(); t.refresh()
    require(t.schemas().containsKey(Int.box(schema.schemaId())) && fieldDepth(schema) == 64 &&
      fieldDepth(t.schema()) == 1)
    val row = GenericRecord.create(t.schema()); row.setField("id", Long.box(4))
    append(t, Vector(row), "shallow")
    val rows = bag(t, t.currentSnapshot().snapshotId(), t.schema())
    require(rows == Map("{\"id\":1}" -> 2, "{\"id\":2}" -> 1, "{\"id\":3}" -> 1, "{\"id\":4}" -> 1, "{\"id\":5}" -> 1))
    val old = snapshotSchema(t, tag(t, "depth_initial"))
    require(fieldDepth(old) == 64 && bag(t, tag(t, "depth_initial"), old) == expected(old, depthRows(old)))
    emit(obj("record" -> "metadata_depth_retained_history", "facts" -> facts(t), "current_bag" -> bagFact(rows)))
    println("METADATA_DEPTH_HISTORY_READY")
  }
  def futureSourceFacts(t: Table, updated: Boolean = false): Unit = {
    val s0 = tag(t, FutureSourceTag); val schema0 = snapshotSchema(t, s0)
    val future = t.schema().findField("future")
    require(schema0.columns().size() == 1 && schema0.findField("future") == null &&
      t.schema().columns().size() == 2 && future.isOptional() && future.`type`() == Types.IntegerType.get())
    if (updated) require(t.currentSnapshot().snapshotId() != s0 && t.snapshots().asScala.size == 3 &&
      t.history().size() == 3 && t.history().get(0).snapshotId() == s0)
    else require(t.currentSnapshot().snapshotId() == s0 && t.snapshots().asScala.size == 1 &&
      t.history().size() == 1 && t.history().get(0).snapshotId() == s0)
    require(t.spec().fields().size() == 1 && t.spec().fields().get(0).sourceId() == future.fieldId() &&
      t.spec().fields().get(0).transform().isIdentity() && t.specs().values().asScala.exists(_.isUnpartitioned()))
    require(bag(t, s0, schema0) == Map("{\"id\":1}" -> 1))
    val current = if (updated) "{\"id\":1,\"future\":9}" else "{\"id\":1,\"future\":null}"
    require(bag(t, t.currentSnapshot().snapshotId(), t.schema()) == Map(current -> 1))
  }
  def futureSourceReceipt(t: Table, record: String, updated: Boolean = false): Unit = {
    futureSourceFacts(t, updated)
    emit(obj("record" -> record, "table_uuid" -> metadata(t).uuid().toString,
      "metadata_file" -> metadata(t).metadataFileLocation(), "snapshot" -> t.currentSnapshot().snapshotId(),
      "historical_snapshot" -> tag(t, FutureSourceTag),
      "snapshot_schema" -> SchemaParser.toJson(snapshotSchema(t, tag(t, FutureSourceTag))),
      "current_schema" -> SchemaParser.toJson(t.schema()), "current_spec" -> PartitionSpecParser.toJson(t.spec())))
  }
  def futureSourceInitialize(ns: String): Unit = {
    val t = create(ns, FutureSourceName,
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
      Map("format-version" -> "3", "write.row-lineage" -> "true", "novarocks.update.mode" -> "merge-on-read"))
    val row = GenericRecord.create(t.schema()); row.setField("id", Long.box(1))
    append(t, Vector(row), "future-s0"); createTag(t, FutureSourceTag)
    t.updateSchema().addColumn("future", Types.IntegerType.get()).commit(); t.refresh()
    t.updateSpec().addField("future").commit(); t.refresh()
    futureSourceReceipt(t, "future_partition_source_evolved")
    println("FUTURE_PARTITION_SOURCE_READY")
  }
  def futureSourceObserveCurrent(ns: String): Unit = {
    val t = load(ns, FutureSourceName)
    futureSourceReceipt(t, "future_partition_source_current", updated = true)
    val tasks = t.newScan().planFiles()
    try require(tasks.asScala.exists { task =>
      task.spec().fields().size() == 1 && task.spec().fields().get(0).sourceId() ==
        t.schema().findField("future").fieldId() && task.file().partition().get(0, classOf[java.lang.Integer]).intValue() == 9
    }, "Updated output did not use the current identity(future) partition")
    finally tasks.close()
    println("FUTURE_PARTITION_SOURCE_CURRENT")
  }
  def hadoopCatalog(ns: String, warehouse: String): org.apache.iceberg.catalog.Catalog = {
    require(ns == namespace && ownedLocation(warehouse), "Hadoop warehouse leaves the original run")
    val session = DeleteApplicabilityFixture.spark
    val prefix = "spark.sql.catalog.nr_hadoop_read"
    session.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    session.conf.set(prefix + ".type", "hadoop")
    session.conf.set(prefix + ".warehouse", warehouse.replaceFirst("^s3://", "s3a://"))
    session.conf.set(prefix + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    Seq("s3.endpoint", "s3.region", "s3.path-style-access", "s3.access-key-id", "s3.secret-access-key").foreach { key =>
      session.conf.getOption("spark.sql.catalog.ice_rest." + key).foreach(v => session.conf.set(prefix + "." + key, v))
    }
    Spark3Util.loadIcebergCatalog(session, "nr_hadoop_read")
  }
  def hadoopObserve(ns: String, warehouse: String): Unit = {
    val t = hadoopCatalog(ns, warehouse).loadTable(TableIdentifier.of(ns, "metadata_depth_native"))
    t.refresh()
    require(ownedLocation(t.location()) && fieldDepth(t.schema()) == 1)
    val before = tag(t, "depth_before_drop"); val original = snapshotSchema(t, before)
    require(fieldDepth(original) == 64 && original.findType(DeepPath) == Types.IntegerType.get())
    require(t.currentSnapshot().snapshotId() != before && t.snapshots().asScala.size == 3)
    require(bag(t, before, original) == expected(original, Vector(
      depthRow(original, 1, None), depthRow(original, 1, None), depthRow(original, 2, None), depthRow(original, 4, Some(7)))))
    val rows = bag(t, t.currentSnapshot().snapshotId(), t.schema())
    require(rows == Map("{\"id\":1}" -> 2, "{\"id\":2}" -> 1, "{\"id\":3}" -> 1, "{\"id\":4}" -> 1))
    emit(obj("record" -> "hadoop_metadata_depth_observed", "facts" -> facts(t),
      "historical_snapshot" -> before, "current_bag" -> bagFact(rows)))
    println("HADOOP_METADATA_DEPTH_OBSERVED")
  }
}
