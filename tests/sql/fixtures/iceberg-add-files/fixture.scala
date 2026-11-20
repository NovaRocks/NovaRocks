// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession

object IcebergAddFilesFixture {
  val restMapping = """[{"field-id":1,"names":["id","old_id"]},{"field-id":2,"names":["note","old_note"]}]"""

  def hadoopPath(uri: String): Path = new Path(uri.replaceFirst("^s3://", "s3a://"))

  def put(spark: SparkSession, uri: String, bytes: Array[Byte]): Unit = {
    val path = hadoopPath(uri)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(path.getParent)
    val output = fs.create(path, false)
    try output.write(bytes) finally output.close()
  }

  def copy(spark: SparkSession, original: String, destination: String): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val source = hadoopPath(original)
    val target = hadoopPath(destination)
    val targetFs = target.getFileSystem(conf)
    targetFs.mkdirs(target.getParent)
    require(FileUtil.copy(source.getFileSystem(conf), source, targetFs, target, false, conf),
      s"Could not copy fixture to $destination")
  }

  def oneFile(spark: SparkSession, table: String): String = {
    val files = spark.table(table + ".files").select("file_path").collect()
    require(files.length == 1, s"Expected one fixture data file for $table, found " + files.length)
    files.head.getString(0)
  }

  def prepareRest(spark: SparkSession, schema: String, token: String, root: String): Unit = {
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS ice_rest.$schema")
    val source = s"ice_rest.$schema.rwm17c_source_$token"
    try {
      spark.sql(s"CREATE TABLE $source (id BIGINT, note STRING) USING iceberg " +
        "TBLPROPERTIES ('format-version'='2', 'write.distribution-mode'='none')")
      spark.sql(s"INSERT INTO $source SELECT /*+ COALESCE(1) */ id,note FROM VALUES " +
        "(CAST(11 AS BIGINT),'alpha'),(CAST(22 AS BIGINT),'beta') source(id,note)")
      val original = oneFile(spark, source)
      Seq("flat/top.data", "single/_one.parquet", "include/top.parquet", "include/nested/more.parquet",
        "skip/top.parquet", "skip/nested/more.parquet", "fail/top.parquet", "fail/nested/more.parquet",
        "mixed/z/nested.parquet", "v1/top.parquet", "incompatible/top.parquet", "missing-column/top.parquet"
      ).foreach(path => copy(spark, original, s"$root/$path"))
      spark.sql(s"TRUNCATE TABLE $source")
      spark.sql(s"INSERT INTO $source SELECT CAST(NULL AS BIGINT), 'null-value'")
      copy(spark, oneFile(spark, source), s"$root/nullable/top.parquet")
    } finally spark.sql(s"DROP TABLE IF EXISTS $source PURGE")

    val invalid = "not Parquet".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    Seq("flat/_SUCCESS", "include/_temporary/hidden.bin", "include/.dot", "include/nested/_temporary/hidden",
      "empty/_SUCCESS", "bad/top.parquet", "mixed/a.bad").foreach(path => put(spark, s"$root/$path", invalid))

    spark.conf.set("spark.sql.parquet.fieldId.write.enabled", "false")
    spark.sql("SELECT CAST(33 AS BIGINT) AS id, 'gamma' AS note")
      .coalesce(1).write.parquet(hadoopPath(s"$root/no-id").toString)
    spark.sql(s"CREATE TABLE ice_rest.$schema.rwm17c_main_$token (id BIGINT, note STRING) USING iceberg " +
      s"TBLPROPERTIES ('format-version'='2','schema.name-mapping.default'='$restMapping')")
    println("RWM17C_REST_SOURCE_READY")
  }

  def observeRest(spark: SparkSession, schema: String, token: String): Unit = {
    val table = Spark3Util.loadIcebergTable(spark, s"ice_rest.$schema.rwm17c_main_$token")
    require(table.properties().get("schema.name-mapping.default") == restMapping)
    val empty = Spark3Util.loadIcebergTable(spark, s"ice_rest.$schema.rwm17c_empty_$token")
    val snapshots = empty.snapshots().iterator()
    require(snapshots.hasNext)
    val append = snapshots.next()
    require(!snapshots.hasNext && append.operation() == "append")
    require(append.summary().get("total-data-files") == "0" &&
      append.summary().get("total-records") == "0")
    println("RWM17C_REST_MAPPING_AND_EMPTY_APPEND_VERIFIED")
  }
  val customMapping = """[{"field-id":1,"names":["new_id","old_id"]},{"field-id":2,"names":["new_note","old_note"]}]"""

  def publishReference(spark: SparkSession, root: String, name: String, encoded: String, sha256: String): Unit = {
    val data = java.util.Base64.getDecoder.decode(encoded)
    val digest = java.security.MessageDigest.getInstance("SHA-256").digest(data).map(byte => f"${byte & 0xff}%02x").mkString
    require(digest == sha256, s"Unexpected $name reference SHA-256 $digest")
    put(spark, s"$root/$name.parquet", data)
  }

  def prepareHadoop(spark: SparkSession, root: String, metrics: String, legacy: String, legacySha: String): Unit = {
    spark.conf.set("spark.sql.parquet.fieldId.write.enabled", "false")
    def write(query: String, suffix: String): Unit =
      spark.sql(query).coalesce(1).write.parquet(hadoopPath(s"$root/$suffix").toString)
    write("SELECT 'alpha' AS note, 99 AS unmapped, CAST(11 AS BIGINT) AS id " +
      "UNION ALL SELECT 'beta', 100, CAST(22 AS BIGINT)", "automatic")
    write("SELECT 'mapped' AS old_note, CAST(33 AS BIGINT) AS old_id", "mapped")
    publishReference(spark, root, "metrics", metrics,
      "b90e7a6aafaeb38254b6a0b450f0b1b9775142ef85137f4af19f7414e3813de7")
    publishReference(spark, root, "legacy", legacy, legacySha)
    println("RWM17C_HADOOP_SOURCE_READY")
  }

  def hadoopCatalog(spark: SparkSession, warehouse: String): org.apache.iceberg.catalog.Catalog = {
    val config = "spark.sql.catalog.rwm17c_hadoop"
    spark.conf.set(config, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(config + ".type", "hadoop")
    spark.conf.set(config + ".warehouse", hadoopPath(warehouse).toString)
    spark.conf.set(config + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    Seq("s3.endpoint", "s3.region", "s3.path-style-access", "s3.access-key-id", "s3.secret-access-key",
      "client.region", "client.credentials-provider", "client.credentials-provider.role-name").foreach { key =>
      spark.conf.getOption("spark.sql.catalog.ice_rest." + key).foreach(v => spark.conf.set(config + "." + key, v))
    }
    Spark3Util.loadIcebergCatalog(spark, "rwm17c_hadoop")
  }

  def observeHadoop(spark: SparkSession, schema: String, warehouse: String, expectedMetricsMapping: String): Unit = {
    import org.apache.iceberg.catalog.TableIdentifier
    import org.apache.iceberg.mapping.NameMappingParser
    import scala.collection.JavaConverters._

    val catalog = hadoopCatalog(spark, warehouse)
    def load(name: String) = catalog.loadTable(TableIdentifier.of(schema, name))
    val automatic = load("automatic_names")
    require(automatic.snapshots().asScala.size == 1 && automatic.currentSnapshot().operation() == "append")
    def mapping(table: org.apache.iceberg.Table) =
      NameMappingParser.fromJson(table.properties().get("schema.name-mapping.default"))
    Seq("id" -> 1, "note" -> 2).foreach { case (name, fieldId) =>
      require(mapping(automatic).find(name).id() == fieldId)
    }
    require(mapping(automatic).find("unmapped") == null)
    require(load("existing_names").properties().get("schema.name-mapping.default") == customMapping)

    val metrics = load("metrics")
    val reference = NameMappingParser.fromJson(expectedMetricsMapping)
    require(mapping(metrics).asMappedFields() == reference.asMappedFields())
    val tasks = metrics.newScan().includeColumnStats().planFiles()
    try {
      val files = tasks.iterator().asScala.map(_.file()).toVector
      require(files.size == 1 && files.head.recordCount() == 4)
      verifyReferenceMetrics(metrics.schema(), files.head)
    } finally tasks.close()
    println("RWM17C_HADOOP_MAPPING_AND_JAVA_METRICS_VERIFIED")
  }

  def verifyReferenceMetrics(schema: org.apache.iceberg.Schema, file: org.apache.iceberg.DataFile): Unit = {
    import java.nio.ByteBuffer
    import org.apache.iceberg.types.{Comparators, Conversions}
    import scala.collection.JavaConverters._

    def counts(values: java.util.Map[Integer, java.lang.Long]): Map[Int, Long] =
      Option(values).toSeq.flatMap(_.asScala).map { case (id, n) => id.intValue() -> n.longValue() }.toMap
    // Iceberg Java 1.12.0 ParquetUtil.fileMetrics, default truncate(16).
    require(counts(file.columnSizes()) == Map(1 -> 122L, 2 -> 144L, 3 -> 295L, 4 -> 212L,
      7 -> 123L, 8 -> 98L, 10 -> 134L, 11 -> 112L))
    require(counts(file.valueCounts()) == Map(1 -> 4L, 2 -> 4L, 3 -> 4L, 4 -> 4L, 7 -> 4L, 8 -> 4L))
    require(counts(file.nullValueCounts()) == Map(2 -> 1L, 3 -> 0L, 4 -> 1L, 8 -> 2L))
    require(counts(file.nanValueCounts()).isEmpty)
    // The oracle recorded a physical INT32 for ID 1. Promote it to the target LONG encoding.
    val lower = Map(1 -> "f9ffffffffffffff", 2 -> "ff3c41", 3 -> "616c7068612dc3a62d4141414141414141",
      4 -> "00106161616161616161616161616161", 7 -> "feffffff", 8 -> "616161")
    val upper = Map(1 -> "ffffff7f00000000", 2 -> "009d27",
      3 -> "7a6f6f2df09f92a1f09f92a1f09f92a1f09f92a1f09f92a1f09f92a1f09f92a1f09f92a1f09f92a1",
      4 -> "ff117a7a7a7a7a7a7a7a7a7a7a7a7a7b", 7 -> "53000000", 8 -> "7a7a7a")
    def check(actual: java.util.Map[Integer, ByteBuffer], expected: Map[Int, String], lower: Boolean): Unit = {
      val retained = Option(actual).toSeq.flatMap(_.asScala).map { case (id, value) => id.intValue() -> value }.toMap
      require(Set(1, 2, 7, 8).subsetOf(retained.keySet), "Missing exact primitive bounds")
      require(retained.keySet.subsetOf(expected.keySet), "Unexpected repeated or unmapped bound")
      retained.toSeq.sortBy(_._1).foreach { case (id, value) =>
        val primitive = schema.findType(id).asPrimitiveType()
        val input = ByteBuffer.wrap(expected(id).grouped(2).map(Integer.parseInt(_, 16).toByte).toArray)
        val compare = Comparators.forType[AnyRef](primitive).compare(
          Conversions.fromByteBuffer[AnyRef](primitive, value.duplicate()),
          Conversions.fromByteBuffer[AnyRef](primitive, input))
        require(if (lower) compare <= 0 else compare >= 0, s"Unsafe bound for Iceberg field $id")
      }
    }
    check(file.lowerBounds(), lower, lower = true)
    check(file.upperBounds(), upper, lower = false)
  }
  def oneParquet(spark: SparkSession, directory: String): String = {
    val path = hadoopPath(directory)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val files = fs.listStatus(path).filter(file => file.isFile && file.getPath.getName.endsWith(".parquet"))
    require(files.length == 1, s"Expected one Parquet file in $directory, found " + files.length)
    files.head.getPath.toString.replaceFirst("^s3a://", "s3://")
  }

  def addFixtureFile(spark: SparkSession, table: org.apache.iceberg.Table, source: String, rows: Long): Unit = {
    val path = hadoopPath(source)
    val size = path.getFileSystem(spark.sparkContext.hadoopConfiguration).getFileStatus(path).getLen
    val file = org.apache.iceberg.DataFiles.builder(table.spec()).withPath(source)
      .withRecordCount(rows).withFileSizeInBytes(size).build()
    table.newAppend().appendFile(file).commit()
  }

  def prepareNested(spark: SparkSession, schema: String, token: String, root: String): Unit = {
    import org.apache.iceberg.expressions.Literal
    import org.apache.iceberg.types.Types
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.types.MetadataBuilder
    import org.apache.parquet.hadoop.ParquetFileReader
    import org.apache.parquet.hadoop.util.HadoopInputFile

    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS ice_rest.$schema")
    def name(label: String): String = s"ice_rest.$schema.rwm17c_${label}_$token"
    def table(label: String): org.apache.iceberg.Table = Spark3Util.loadIcebergTable(spark, name(label))
    val nested = "id BIGINT, details STRUCT<flag:INT,note:STRING,obsolete:STRING>"
    def create(label: String, fields: String, version: Int): Unit = {
      spark.sql(s"CREATE TABLE ${name(label)} ($fields) USING iceberg " +
        s"TBLPROPERTIES ('format-version'='$version','write.distribution-mode'='none')")
    }

    create("old", nested, 2)
    create("registered", nested, 2)
    create("required_add", nested, 2)
    create("required_read", nested, 2)
    spark.sql(s"INSERT INTO ${name("old")} SELECT /*+ COALESCE(1) */ id,details FROM VALUES " +
      "(CAST(1 AS BIGINT),named_struct('flag',17,'note','alpha','obsolete','a'))," +
      "(CAST(2 AS BIGINT),named_struct('flag',22,'note','beta','obsolete','b')) source(id,details)")
    val original = oneFile(spark, name("old"))
    copy(spark, original, s"$root/registered.parquet")
    copy(spark, original, s"$root/required-add.parquet")
    val requiredRead = s"$root/required-read.parquet"
    copy(spark, original, requiredRead)
    addFixtureFile(spark, table("required_read"), requiredRead, 2)
    Seq("old", "registered").foreach { label =>
      table(label).updateSchema().moveFirst("details.note").deleteColumn("details.obsolete")
        .addColumn("details", "fresh", Types.StringType.get()).commit()
    }
    Seq("required_add", "required_read").foreach { label =>
      table(label).updateSchema().allowIncompatibleChanges()
        .addRequiredColumn("details", "required_child", Types.StringType.get()).commit()
    }

    create("mixed_add", "id BIGINT,note STRING", 2)
    create("mixed_read", "id BIGINT,note STRING", 2)
    spark.conf.set("spark.sql.parquet.fieldId.write.enabled", "true")
    val mixed = spark.sql("SELECT CAST(1 AS BIGINT) AS id, 'mixed' AS note")
    val metadata = new MetadataBuilder().putLong("parquet.field.id", 1L).build()
    mixed.select(col("id").as("id", metadata), col("note")).coalesce(1)
      .write.parquet(hadoopPath(s"$root/mixed").toString)
    val mixedFile = oneParquet(spark, s"$root/mixed")
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
      hadoopPath(mixedFile), spark.sparkContext.hadoopConfiguration))
    try {
      val fields = reader.getFooter.getFileMetaData.getSchema
      require(fields.getType("id").getId.intValue() == 1 && fields.getType("note").getId == null)
    } finally reader.close()
    copy(spark, mixedFile, s"$root/mixed-add.parquet")
    addFixtureFile(spark, table("mixed_read"), mixedFile, 1)

    create("v3_default", "id BIGINT, details STRUCT<flag:INT,absent:STRING>," +
      " items ARRAY<STRUCT<value:INT>>, codes MAP<STRING,INT>", 3)
    val v3 = table("v3_default")
    val sourceSchema = v3.schema()
    def id(path: String): Int = sourceSchema.findField(path).fieldId()
    val mapping = s"""[{"field-id":${id("id")},"names":["id"]},
      {"field-id":${id("details")},"names":["details","old_details"],"fields":[
        {"field-id":${id("details.flag")},"names":["flag","legacy_flag"]},
        {"field-id":${id("details.absent")},"names":[]},
        {"names":["unmapped"]}]},
      {"field-id":${id("items")},"names":["items"],"fields":[
        {"field-id":${id("items.element")},"names":["element","array"],"fields":[
          {"field-id":${id("items.element.value")},"names":["value","legacy_value"]}]}]},
      {"field-id":${id("codes")},"names":["codes"],"fields":[
        {"field-id":${id("codes.key")},"names":["key"]},
        {"field-id":${id("codes.value")},"names":["value"]}]}]"""
    v3.updateProperties().set("schema.name-mapping.default", mapping).commit()
    spark.conf.set("spark.sql.parquet.fieldId.write.enabled", "false")
    spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
    val beforeDefault = s"$root/before-default"
    spark.sql("SELECT CAST(1 AS BIGINT) id, named_struct('legacy_flag',7,'unmapped','ignored') old_details," +
      " array(named_struct('legacy_value',17),CAST(NULL AS STRUCT<legacy_value:INT>)) items, map('x',91) codes" +
      " UNION ALL SELECT CAST(2 AS BIGINT), CAST(NULL AS STRUCT<legacy_flag:INT,unmapped:STRING>)," +
      " CAST(NULL AS ARRAY<STRUCT<legacy_value:INT>>), CAST(NULL AS MAP<STRING,INT>)")
      .coalesce(1).write.parquet(hadoopPath(beforeDefault).toString)
    addFixtureFile(spark, v3, oneParquet(spark, beforeDefault), 2)
    v3.updateSchema()
      .addColumn("details", "initialized", Types.StringType.get(), Literal.of("seed"))
      .commit()
    println("RWM17C_NESTED_SOURCE_READY")
  }
}
