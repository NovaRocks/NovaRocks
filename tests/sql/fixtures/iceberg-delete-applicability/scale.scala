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


object DeleteApplicabilityScale {
  import DeleteApplicabilityFixture._
  val rowsPerFile = 2048
  val keysPerDelete = 256
  val fingerprinted = scala.collection.mutable.Set.empty[String]
  case class DataEntry(file: DataFile, ordinal: Int, partition: Long, cohort: Int, sequence: Long)
  case class DeleteEntry(file: DeleteFile, id: Int, global: Boolean, partition: Long,
      fieldId: Int, sequence: Long)
  class State(val table: Table, val globalSpec: PartitionSpec, val partitionCount: Int) {
    val data = scala.collection.mutable.ArrayBuffer.empty[DataEntry]
    val deletes = scala.collection.mutable.ArrayBuffer.empty[DeleteEntry]
  }
  def event(kind: String, fields: (String, Any)*): Unit = emit(obj((Seq("record" -> kind) ++ fields): _*))
  def fingerprint(t: Table, path: String, kind: String): Unit = if (fingerprinted.add(path)) {
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    val stream = t.io().newInputFile(path).newStream()
    val buffer = new Array[Byte](65536)
    var size = 0L
    try { var n = stream.read(buffer); while (n >= 0) {
      if (n > 0) { digest.update(buffer, 0, n); size += n }; n = stream.read(buffer)
    } } finally stream.close()
    event("scale-artifact", "kind" -> kind, "path" -> path, "size" -> size,
      "sha256" -> digest.digest().map(b => f"${b & 0xff}%02x").mkString)
  }
  def createState(name: String, partitionCount: Int, global: Boolean = false): State = {
    val qualified = s"ice_rest.$namespace.${prefix}_$name"
    val part = if (global) "" else "PARTITIONED BY (partition_key)"
    DeleteApplicabilityFixture.spark.sql(s"CREATE TABLE $qualified " +
      s"(row_id BIGINT, eq_key BIGINT, partition_key BIGINT, payload BIGINT) USING iceberg $part " +
      "TBLPROPERTIES ('format-version'='2', 'write.metadata.metrics.default'='none')")
    val t = Spark3Util.loadIcebergTable(DeleteApplicabilityFixture.spark, qualified)
    val originalSpec = t.spec()
    if (global) { t.updateSpec().addField("partition_key").commit(); t.refresh() }
    new State(t, originalSpec, partitionCount)
  }
  def tuple(spec: PartitionSpec, partition: Long): StructLike = {
    val result = new PartitionData(spec.partitionType())
    if (!spec.isUnpartitioned()) result.set(0, Long.box(partition))
    result
  }
  def row(file: Int, local: Int, partitions: Int): Array[Long] = {
    val key = local.toLong * rowsPerFile + file
    Array(file.toLong * rowsPerFile + local, key, file.toLong % partitions, key * 3L + 1L)
  }
  def appendData(state: State, count: Int, cohort: Int): Unit = {
    val t = state.table
    val nextSequence = metadata(t).lastSequenceNumber() + 1L
    val append = t.newAppend()
    (0 until count).foreach { _ =>
      val ordinal = state.data.size
      val p = ordinal.toLong % state.partitionCount
      val writer = new GenericAppenderFactory(t.schema(), t.spec())
        .set("write.metadata.metrics.default", "none")
        .newDataWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, ".parquet")),
          FileFormat.PARQUET, tuple(t.spec(), p))
      try { (0 until rowsPerFile).foreach { local =>
        writer.write(record(t.schema(), row(ordinal, local, state.partitionCount).toSeq))
      } } finally writer.close()
      val f = writer.toDataFile()
      require(f.recordCount() == rowsPerFile)
      append.appendFile(f)
      state.data += DataEntry(f, ordinal, p, cohort, nextSequence)
      fingerprint(t, f.location(), "data-content")
    }
    append.commit(); t.refresh()
    require(metadata(t).lastSequenceNumber() == nextSequence)
  }
  def writeDelete(state: State, ordinal: Int, global: Boolean, p: Long,
      fieldId: Int, keyBase: Long, sequence: Long): DeleteEntry = {
    val t = state.table
    val spec = if (global) state.globalSpec else t.spec()
    val schema = t.schema().select(if (fieldId == 2) "eq_key" else "payload")
    val writer = new GenericAppenderFactory(t.schema(), spec, Array(fieldId), schema, null)
      .set("write.metadata.metrics.default", "none")
      .newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, ".parquet")),
        FileFormat.PARQUET, tuple(spec, p))
    try { (0 until keysPerDelete).foreach { index =>
      val key = keyBase + ordinal.toLong * keysPerDelete + index
      writer.write(record(schema, Seq[Any](if (fieldId == 2) key else key * 3L + 1L)))
    } } finally writer.close()
    val file = writer.toDeleteFile()
    require(file.recordCount() == keysPerDelete)
    val entry = DeleteEntry(file, state.deletes.size, global, p, fieldId, sequence)
    state.deletes += entry
    fingerprint(t, file.location(), "equality-content")
    entry
  }
  def appendDeletes(state: State, requests: Seq[(Int, Boolean, Long, Int, Long)]): Unit = {
    val t = state.table
    val nextSequence = metadata(t).lastSequenceNumber() + 1L
    val delta = t.newRowDelta()
    requests.foreach { case (ordinal, global, p, fieldId, base) =>
      delta.addDeletes(writeDelete(state, ordinal, global, p, fieldId, base, nextSequence).file)
    }
    delta.commit(); t.refresh()
    require(metadata(t).lastSequenceNumber() == nextSequence)
  }
  def captureSnapshot(state: State, allHistory: Boolean): Unit = {
    val t = state.table
    fingerprint(t, metadata(t).metadataFileLocation(), "metadata")
    val snapshots = if (allHistory) t.snapshots().asScala.toVector else Vector(t.currentSnapshot())
    snapshots.foreach { snapshot =>
      fingerprint(t, snapshot.manifestListLocation(), "manifest-list")
      snapshot.allManifests(t.io()).asScala.foreach(m => fingerprint(t, m.path(), "manifest"))
    }
  }
  class Bag(val size: Int) {
    val bits = new java.util.BitSet(size)
    val sums = Array.fill[Long](4)(0L)
    var count = 0L
    def add(values: Array[Long]): Unit = {
      val id = values(0)
      require(id >= 0 && id < size && !bits.get(id.toInt), "Unexpected or duplicate row_id")
      bits.set(id.toInt); count += 1
      values.indices.foreach(i => sums(i) += values(i))
    }
    def digest(partitions: Int): String = {
      val result = java.security.MessageDigest.getInstance("SHA-256")
      val buffer = java.nio.ByteBuffer.allocate(32).order(java.nio.ByteOrder.BIG_ENDIAN)
      var id = bits.nextSetBit(0)
      while (id >= 0) {
        buffer.clear()
        row(id / rowsPerFile, id % rowsPerFile, partitions).foreach(buffer.putLong)
        result.update(buffer.array()); id = bits.nextSetBit(id + 1)
      }
      result.digest().map(b => f"${b & 0xff}%02x").mkString
    }
    def receipt(partitions: Int): JsonNode = obj("row_count" -> count,
      "sum_row_id" -> sums(0), "sum_eq_key" -> sums(1), "sum_partition_key" -> sums(2),
      "sum_payload" -> sums(3), "sorted_row_sha256" -> digest(partitions))
  }
  def observe(state: State, cfg: JsonNode): Unit = {
    val t = state.table
    val name = cfg.get("case").asText()
    val family = cfg.get("family").asText()
    val m = cfg.get("equality_per_partition").asInt()
    val g = cfg.get("global_equality").asInt()
    val u = cfg.get("suffixes").asInt()
    val partitionBase = (if (family == "global_repeat") 100 else g).toLong * keysPerDelete
    require(state.data.size == cfg.get("data_files").asInt())
    captureSnapshot(state, family == "suffix")
    val actualData = state.data.map(d => d.file.location() -> d).toMap
    val actualDeletes = state.deletes.map(d => d.file.location() -> d).toMap
    val plannedFiles = scala.collection.mutable.Set.empty[String]
    val memberSizes = scala.collection.mutable.Map.empty[Int, Int].withDefaultValue(0)
    val sequenceSizes = scala.collection.mutable.Map.empty[Long, Int].withDefaultValue(0)
    val descriptors = scala.collection.mutable.Map.empty[Int, JsonNode]
    val tasks = t.newScan().useSnapshot(t.currentSnapshot().snapshotId()).includeColumnStats().planFiles()
    try tasks.asScala.foreach { task =>
      val file = task.file(); val known = actualData(file.location())
      require(plannedFiles.add(file.location()), "Duplicate planned data file")
      require(file.dataSequenceNumber().longValue() == known.sequence,
        "Data sequence does not match the independently recorded append commit")
      require(file.recordCount() == rowsPerFile)
      val members = task.deletes().asScala.toVector.map { file =>
        val knownDelete = actualDeletes(file.location())
        require(file.dataSequenceNumber().longValue() == knownDelete.sequence)
        require(file.recordCount() == keysPerDelete)
        descriptors.getOrElseUpdate(knownDelete.id, describe(file))
        knownDelete.id
      }
      val expected = state.deletes.filter(d => d.sequence > known.sequence &&
        (d.global || d.partition == known.partition)).map(_.id).sorted
      require(members.sorted == expected, s"$name: Java closure differs from independent commit/scope schedule")
      memberSizes(members.size) += 1; sequenceSizes(known.sequence) += 1
      event("scale-plan-file", "case" -> name, "data_ordinal" -> known.ordinal,
        "data" -> describe(file), "delete_member_ids" -> members,
        "delete_list_size" -> members.size)
    } finally tasks.close()
    require(plannedFiles.size == state.data.size)
    descriptors.toVector.sortBy(_._1).foreach { case (id, description) =>
      event("scale-delete-member", "case" -> name, "member_id" -> id, "content" -> description)
    }
    val expectedBag = new Bag(state.data.size * rowsPerFile)
    state.data.foreach { data => (0 until rowsPerFile).foreach { local =>
      val values = row(data.ordinal, local, state.partitionCount)
      val key = values(1)
      val removed = if (family == "no_delete") false
        else if (family == "suffix") key >= data.cohort.toLong * (m / u) * keysPerDelete && key < m.toLong * keysPerDelete
        else if (family == "equality_ladder") key < m.toLong * keysPerDelete
        else key < g.toLong * keysPerDelete || (key >= partitionBase && key < partitionBase + m.toLong * keysPerDelete)
      if (!removed) expectedBag.add(values)
    } }
    val actualBag = new Bag(state.data.size * rowsPerFile)
    val reader = IcebergGenerics.read(t).useSnapshot(t.currentSnapshot().snapshotId()).project(t.schema()).build()
    try reader.asScala.foreach { value =>
      val values = Array("row_id", "eq_key", "partition_key", "payload").map(name =>
        value.getField(name).asInstanceOf[java.lang.Long].longValue())
      val id = values(0)
      require(id >= 0L && id < actualBag.size)
      require(values.sameElements(row((id / rowsPerFile).toInt, (id % rowsPerFile).toInt, state.partitionCount)),
        "Actual row values differ from independent four-integer construction")
      actualBag.add(values)
    } finally reader.close()
    val expectedReceipt = expectedBag.receipt(state.partitionCount)
    val actualReceipt = actualBag.receipt(state.partitionCount)
    require(expectedBag.bits == actualBag.bits && expectedReceipt == actualReceipt,
      s"$name: independent row bag mismatch")
    event("scale-case", "case" -> name, "table" -> t.name(), "location" -> t.location(),
      "metadata" -> metadata(t).metadataFileLocation(), "snapshot" -> t.currentSnapshot().snapshotId(),
      "configuration" -> cfg, "java_planFiles" -> "success", "java_row_read" -> "success",
      "plan_file_count" -> plannedFiles.size, "delete_dictionary_size" -> descriptors.size,
      "delete_list_size_histogram" -> memberSizes.toVector.sortBy(_._1).map { case (size, n) => obj("size" -> size, "files" -> n) },
      "data_sequence_histogram" -> sequenceSizes.toVector.sortBy(_._1).map { case (sequence, n) => obj("sequence" -> sequence, "files" -> n) },
      "independent_oracle" -> expectedReceipt, "java_oracle" -> actualReceipt,
      "exact_bag_checked" -> true, "statistics" -> "none")
  }
  def run(manifest: JsonNode): Unit = {
    require(manifest.get("status").asText().startsWith("frozen"))
    require(manifest.get("rows_per_file").asInt() == rowsPerFile && manifest.get("rows_per_equality_file").asInt() == keysPerDelete)
    val cases = manifest.get("cases").elements().asScala.toVector
    val noDeletes = createState("no_delete_ladder", 1)
    cases.filter(_.get("family").asText() == "no_delete").foreach { cfg =>
      appendData(noDeletes, cfg.get("data_files").asInt() - noDeletes.data.size, 0); observe(noDeletes, cfg)
    }
    val ladder = createState("equality_ladder", 1)
    appendData(ladder, 1, 0)
    cases.filter(_.get("family").asText() == "equality_ladder").foreach { cfg =>
      val target = cfg.get("equality_per_partition").asInt()
      appendDeletes(ladder, (ladder.deletes.size until target).map(i => (i, false, 0L, 2, 0L)))
      observe(ladder, cfg)
    }
    cases.filter(_.get("family").asText() == "suffix").foreach { cfg =>
      val state = createState(cfg.get("table").asText(), 1)
      val u = cfg.get("suffixes").asInt(); val n = cfg.get("data_files").asInt()
      val m = cfg.get("equality_per_partition").asInt()
      (0 until u).foreach { cohort =>
        appendData(state, n / u, cohort)
        appendDeletes(state, (cohort * (m / u) until (cohort + 1) * (m / u)).map(i => (i, false, 0L, 2, 0L)))
        event("scale-checkpoint", "table" -> state.table.name(), "cohort" -> cohort,
          "snapshot" -> state.table.currentSnapshot().snapshotId(),
          "data_files" -> state.data.size, "delete_files" -> state.deletes.size)
      }
      observe(state, cfg)
    }
    cases.filter(_.get("family").asText() == "buckets").foreach { cfg =>
      val p = cfg.get("partitions").asInt(); val g = cfg.get("global_equality").asInt()
      val m = cfg.get("equality_per_partition").asInt()
      val state = createState(cfg.get("table").asText(), p, true)
      appendData(state, cfg.get("data_files").asInt(), 0)
      val global = (0 until g).map(i => (i, true, 0L, 2, 0L))
      val partition = (0 until p).flatMap(part => (0 until m).map(i => (i, false, part.toLong, 4, g.toLong * keysPerDelete)))
      appendDeletes(state, global ++ partition); observe(state, cfg)
    }
    val repeated = createState("global_repeat", 10, true)
    appendData(repeated, 100, 0)
    var previousGlobal = 0
    cases.filter(_.get("family").asText() == "global_repeat").foreach { cfg =>
      val target = cfg.get("global_equality").asInt()
      val global = (previousGlobal until target).map(i => (i, true, 0L, 2, 0L))
      val partition = if (previousGlobal == 0) (0 until 10).flatMap(part =>
        (0 until 10).map(i => (i, false, part.toLong, 4, 100L * keysPerDelete))) else Seq.empty
      appendDeletes(repeated, global ++ partition); observe(repeated, cfg); previousGlobal = target
    }
    event("scale-complete", "case_count" -> cases.size, "artifact_count" -> fingerprinted.size)
  }
}
