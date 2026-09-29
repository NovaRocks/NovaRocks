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

object DeleteApplicabilityPromotion {
  import DeleteApplicabilityFixture._

  def physicalSchema(t: Table, path: String): String = {
    val local = java.nio.file.Files.createTempFile("uea4g-schema-", ".parquet")
    try {
      java.nio.file.Files.write(local, bytes(t.io(), path))
      val input = org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
        new org.apache.hadoop.fs.Path(local.toUri()), new org.apache.hadoop.conf.Configuration())
      val reader = org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(input)
      try reader.getFooter().getFileMetaData().getSchema().toString finally reader.close()
    } finally java.nio.file.Files.deleteIfExists(local)
  }

  def capture(t: Table, dataFile: DataFile, deleteFile: DeleteFile): Unit = {
    export(t.io(), metadata(t).metadataFileLocation(), "metadata")
    export(t.io(), t.currentSnapshot().manifestListLocation(), "manifest-list")
    t.currentSnapshot().allManifests(t.io()).asScala.foreach(m => export(t.io(), m.path(), "manifest"))
    export(t.io(), dataFile.location(), "data-content")
    export(t.io(), deleteFile.location(), "delete-content")
  }

  def control(partitioned: Boolean): Unit = {
    val label = if (partitioned) "partitioned" else "unpartitioned"
    val t = create("promotion_" + label, 2, partitioned)
    val initialRows = if (partitioned) Seq(Seq[Any](7L, 1, "old7"), Seq[Any](10L, 1, "old10"))
      else Seq(Seq[Any](7L, 1, "old7"), Seq[Any](10L, 2, "old10"))
    val f = data(t, initialRows)
    t.newAppend().appendFile(f).commit()
    val deleteSchema = t.schema().select("p")
    val writer = new GenericAppenderFactory(t.schema(), t.spec(), Array(2), deleteSchema, null)
      .newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, ".parquet")),
        FileFormat.PARQUET, partition(t, 1))
    try writer.write(record(deleteSchema, Seq[Any](1))) finally writer.close()
    val d = writer.toDeleteFile()
    t.newRowDelta().addDeletes(d).commit(); t.refresh()
    val expected = if (partitioned) Seq.empty[Row] else Seq(Seq[Any](10L, 2, "old10"))
    val generation = obj("method" -> "official equality IDs=[2], physical p INT; legal updateSchema INT-to-LONG",
      "writer_commit" -> "success", "data_physical_schema" -> physicalSchema(t, f.location()),
      "delete_physical_schema" -> physicalSchema(t, d.location()), "equality_ids" -> Seq(2))
    capture(t, f, d)
    observe("promotion_" + label + "_before", t, generation, Some(expected), enforceExpected = false)
    t.updateSchema().updateColumn("p", org.apache.iceberg.types.Types.LongType.get()).commit(); t.refresh()
    capture(t, f, d)
    observe("promotion_" + label + "_snapshot", t, generation, Some(expected), enforceExpected = false)
    observe("promotion_" + label + "_current_projection", t, generation, Some(expected),
      projectCurrentSchema = true, enforceExpected = false)
  }
  def run(): Unit = { control(false); control(true) }
}
