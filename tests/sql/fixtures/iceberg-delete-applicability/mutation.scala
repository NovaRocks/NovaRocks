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

// The Java writers establish the v2 physical inputs before the v3 upgrade.
// The checker observes the native mutation through a fresh Java table load.
object DeleteMaterializationFixture {
  import DeleteApplicabilityFixture._

  private val name = "materialization_history"

  private def rows(ids: Seq[Long]): Seq[Row] =
    ids.map(id => Seq[Any](id, 1, s"v$id"))

  private def deletes(t: Table, snapshot: Snapshot): Seq[DeleteFile] =
    snapshot.deleteManifests(t.io()).asScala.toVector.flatMap { manifest =>
      val reader = ManifestFiles.readDeleteManifest(manifest, t.io(), t.specs())
      try reader.iterator().asScala.map(_.copy()).toVector finally reader.close()
    }

  private def capture(t: Table, phase: String, expected: Seq[Row], extra: (String, Any)*): Unit = {
    t.refresh()
    val snapshot = t.currentSnapshot()
    export(t.io(), metadata(t).metadataFileLocation(), "metadata")
    t.snapshots().asScala.foreach { s =>
      export(t.io(), s.manifestListLocation(), "manifest-list")
      s.allManifests(t.io()).asScala.foreach(m => export(t.io(), m.path(), "manifest"))
      deletes(t, s).foreach(d => export(t.io(), d.location(), "delete-content"))
    }
    val data = t.newScan().useSnapshot(snapshot.snapshotId()).planFiles()
    val inventory = try data.asScala.map { task =>
      val f = task.file()
      export(t.io(), f.location(), "data-content")
      obj("file" -> describe(f), "first_row_id" -> f.firstRowId())
    }.toVector finally data.close()
    val live = deletes(t, snapshot).map(describe)
    observe(phase, t, obj((Seq("table_uuid" -> metadata(t).uuid(),
      "parent_snapshot" -> snapshot.parentId(), "snapshot_sequence" -> snapshot.sequenceNumber(),
      "current_spec" -> t.spec().specId(), "format_version" -> metadata(t).formatVersion(),
      "live_data" -> inventory, "live_deletes" -> live) ++ extra): _*), Some(expected))
  }

  def seed(): Unit = {
    val t = create(name, 2)
    val a = data(t, rows(1L to 6L))
    val b = data(t, rows(7L to 10L))
    val c = data(t, rows(11L to 14L))
    t.newAppend().appendFile(a).appendFile(b).appendFile(c).commit()
    val exclusive = positionDelete(t, Seq((a.location(), 0L), (a.location(), 0L)))
    val shared = positionDelete(t, Seq((b.location(), 0L), (c.location(), 0L)))
    val shadowed = positionDelete(t, Seq((b.location(), 1L)))
    val eq = equality(t, Seq(5L))
    require(exclusive.recordCount() == 2L && shared.recordCount() == 2L,
      "fixture requires two physical records in each legacy position file")
    t.newRowDelta().addDeletes(exclusive).addDeletes(shared).addDeletes(shadowed).addDeletes(eq).commit()
    t.updateProperties().set("format-version", "3").commit()
    t.refresh()
    val oldDvs = dv(t, Seq((b.location(), 0L), (b.location(), 1L), (c.location(), 0L)))
    require(oldDvs.size == 2 && oldDvs.map(_.location()).distinct.size == 1 &&
      oldDvs.map(_.contentOffset()).distinct.size == 2, "expected two blobs in one old Puffin")
    val vectors = t.newRowDelta()
    oldDvs.foreach(vectors.addDeletes)
    vectors.commit()
    t.updateSpec().removeField("p").addField(org.apache.iceberg.expressions.Expressions.bucket("p", 8)).commit()
    capture(t, "materialization_seed", rows(Seq(2L,3L,4L,6L,9L,10L,12L,13L,14L)),
      "target_a" -> a.location(), "target_b" -> b.location(), "untouched_c" -> c.location(),
      "exclusive" -> describe(exclusive), "shared" -> describe(shared),
      "shadowed" -> describe(shadowed), "equality" -> describe(eq), "old_dvs" -> oldDvs.map(describe))
  }

  def check(): Unit = {
    val t = Spark3Util.loadIcebergTable(DeleteApplicabilityFixture.spark, s"ice_rest.$namespace.${prefix}_$name")
    require(ownedLocation(t.location()), "native result table is outside the original run range")
    capture(t, "materialization_after", rows(Seq(3L,4L,6L,10L,12L,13L,14L)))
  }
}
