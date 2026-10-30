// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file for additional
// information. Licensed under the Apache License, Version 2.0.
// Independent producer: Iceberg 1.11 Java API inside the original Spark 3.5 JVM.
// Spark SQL cannot represent TIME/VARIANT; this path preserves their true schema.
try {
  import org.apache.iceberg.{FileFormat, PartitionSpec, Schema}
  import org.apache.iceberg.catalog.TableIdentifier
  import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord}
  import org.apache.iceberg.spark.Spark3Util
  import org.apache.iceberg.types.Types
  import org.apache.iceberg.variants.{Variant, Variants}
  import java.time.LocalTime
  import java.util.{Arrays, HashMap, UUID}
  val catalog = Spark3Util.loadIcebergCatalog(spark, "ice_rest")
  def writeTable(name: String, schema: Schema, records: Seq[GenericRecord], version: String): Unit = {
    val properties = new HashMap[String, String]()
    properties.put("format-version", version)
    val table = catalog.createTable(TableIdentifier.of("__NAMESPACE__", name), schema,
      PartitionSpec.unpartitioned(), "__SCOPE__/" + name, properties)
    val output = table.io().newOutputFile(table.location() + "/data/" + UUID.randomUUID().toString + ".parquet")
    val encrypted = table.encryption().encrypt(output)
    val writer = new GenericAppenderFactory(schema).newDataWriter(encrypted, FileFormat.PARQUET, null)
    try { records.foreach(writer.write) } finally { writer.close() }
    table.newAppend().appendFile(writer.toDataFile()).commit()
    require(table.location() == "__SCOPE__/" + name, "independent table escaped its original run location")
  }
  val timeSchema = new Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "time_value", Types.TimeType.get()),
    Types.NestedField.optional(3, "time_array", Types.ListType.ofOptional(4, Types.TimeType.get())),
    Types.NestedField.optional(5, "time_map", Types.MapType.ofOptional(6, 7, Types.TimeType.get(), Types.StringType.get())))
  val timeRecords = Seq("00:00:00.000001", "12:34:56.123456", "23:59:59.999999", null).zipWithIndex.map { case (text, index) =>
    val record = GenericRecord.create(timeSchema)
    record.setField("id", Integer.valueOf(index))
    if (text != null) {
      val time = LocalTime.parse(text)
      record.setField("time_value", time)
      record.setField("time_array", Arrays.asList(time, null))
      val map = new HashMap[LocalTime, String]()
      map.put(time, "time-key")
      record.setField("time_map", map)
    }
    record
  }
  writeTable("time_values", timeSchema, timeRecords, "2")
  val rowType = Types.StructType.of(Types.NestedField.optional(12, "v", Types.VariantType.get()))
  val variantSchema = new Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "variant_value", Types.VariantType.get()),
    Types.NestedField.optional(3, "variant_array", Types.ListType.ofOptional(4, Types.VariantType.get())),
    Types.NestedField.optional(5, "variant_map", Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.VariantType.get())),
    Types.NestedField.optional(8, "variant_row", rowType))
  val metadata = Variants.metadata("a", "unicode")
  val objectValue = Variants.object(metadata)
  objectValue.put("a", Variants.of(-7L))
  objectValue.put("unicode", Variants.of("雪"))
  val variants = Seq(Variant.of(metadata, objectValue), Variant.of(Variants.emptyMetadata(), Variants.of(0.125d)), null)
  val variantRecords = variants.zipWithIndex.map { case (value, index) =>
    val record = GenericRecord.create(variantSchema)
    record.setField("id", Integer.valueOf(index))
    if (value != null) {
      record.setField("variant_value", value)
      record.setField("variant_array", Arrays.asList(value, null))
      val map = new HashMap[String, Variant]()
      map.put("variant", value)
      record.setField("variant_map", map)
      val row = GenericRecord.create(rowType)
      row.setField("v", value)
      record.setField("variant_row", row)
    }
    record
  }
  writeTable("variant_values", variantSchema, variantRecords, "3")
  println("TRC1_INDEPENDENT_TIME_VARIANT_WRITER_OK")
  System.exit(0)
} catch {
  case error: Throwable => error.printStackTrace(); System.exit(1)
}
