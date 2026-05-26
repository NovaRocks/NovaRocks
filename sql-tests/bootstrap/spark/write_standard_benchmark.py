#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


SSB_TABLES = {
    "customer": [
        ("c_custkey", "int"),
        ("c_name", "string"),
        ("c_address", "string"),
        ("c_city", "string"),
        ("c_nation", "string"),
        ("c_region", "string"),
        ("c_phone", "string"),
        ("c_mktsegment", "string"),
    ],
    "dates": [
        ("d_datekey", "int"),
        ("d_date", "string"),
        ("d_dayofweek", "string"),
        ("d_month", "string"),
        ("d_year", "int"),
        ("d_yearmonthnum", "int"),
        ("d_yearmonth", "string"),
        ("d_daynuminweek", "int"),
        ("d_daynuminmonth", "int"),
        ("d_daynuminyear", "int"),
        ("d_monthnuminyear", "int"),
        ("d_weeknuminyear", "int"),
        ("d_sellingseason", "string"),
        ("d_lastdayinweekfl", "int"),
        ("d_lastdayinmonthfl", "int"),
        ("d_holidayfl", "int"),
        ("d_weekdayfl", "int"),
    ],
    "lineorder": [
        ("lo_orderkey", "int"),
        ("lo_linenumber", "int"),
        ("lo_custkey", "int"),
        ("lo_partkey", "int"),
        ("lo_suppkey", "int"),
        ("lo_orderdate", "int"),
        ("lo_orderpriority", "string"),
        ("lo_shippriority", "int"),
        ("lo_quantity", "int"),
        ("lo_extendedprice", "int"),
        ("lo_ordtotalprice", "int"),
        ("lo_discount", "int"),
        ("lo_revenue", "int"),
        ("lo_supplycost", "int"),
        ("lo_tax", "int"),
        ("lo_commitdate", "int"),
        ("lo_shipmode", "string"),
    ],
    "part": [
        ("p_partkey", "int"),
        ("p_name", "string"),
        ("p_mfgr", "string"),
        ("p_category", "string"),
        ("p_brand", "string"),
        ("p_color", "string"),
        ("p_type", "string"),
        ("p_size", "int"),
        ("p_container", "string"),
    ],
    "supplier": [
        ("s_suppkey", "int"),
        ("s_name", "string"),
        ("s_address", "string"),
        ("s_city", "string"),
        ("s_nation", "string"),
        ("s_region", "string"),
        ("s_phone", "string"),
    ],
}

SSB_RAW_NAMES = {
    "customer": "customer.tbl",
    "dates": "date.tbl",
    "lineorder": "lineorder.tbl",
    "part": "part.tbl",
    "supplier": "supplier.tbl",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Write standard benchmark raw files to Iceberg"
    )
    parser.add_argument("--suite", required=True)
    parser.add_argument("--scale", required=True)
    parser.add_argument("--raw-base-uri", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--manifest-output", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--s3-access-key", required=True)
    parser.add_argument("--s3-secret-key", required=True)
    parser.add_argument("--generator", required=True)
    parser.add_argument("--generator-version", required=True)
    return parser.parse_args()


def configure_catalog(spark, args):
    catalog_prefix = f"spark.sql.catalog.{args.catalog}"
    spark.conf.set(catalog_prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"{catalog_prefix}.type", "hadoop")
    spark.conf.set(f"{catalog_prefix}.warehouse", args.warehouse)
    spark.conf.set(f"{catalog_prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set(f"{catalog_prefix}.s3.endpoint", args.s3_endpoint)
    spark.conf.set(f"{catalog_prefix}.s3.path-style-access", "true")
    spark.conf.set(f"{catalog_prefix}.s3.access-key-id", args.s3_access_key)
    spark.conf.set(f"{catalog_prefix}.s3.secret-access-key", args.s3_secret_key)
    spark.conf.set(f"{catalog_prefix}.s3.region", "us-east-1")
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
    spark.conf.set("spark.hadoop.fs.s3a.access.key", args.s3_access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    spark.conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def spark_type(type_name):
    if type_name == "int":
        return IntegerType()
    if type_name == "string":
        return StringType()
    raise ValueError(f"unsupported SSB type: {type_name}")


def read_pipe_table(spark, path, columns):
    raw_schema = StructType(
        [StructField(f"c{idx}", StringType(), True) for idx in range(len(columns) + 1)]
    )
    df = (
        spark.read.option("delimiter", "|")
        .option("header", "false")
        .schema(raw_schema)
        .csv(path)
    )

    projected = []
    for idx, (name, type_name) in enumerate(columns):
        value = trim(col(f"c{idx}"))
        if type_name == "int":
            value = value.cast(spark_type(type_name))
        projected.append(value.alias(name))
    return df.select(*projected)


def sql_ident(name):
    return f"`{name.replace('`', '``')}`"


def qualified_name(*parts):
    return ".".join(sql_ident(part) for part in parts)


def main():
    args = parse_args()
    if args.suite != "ssb":
        raise ValueError(f"unsupported suite in first phase: {args.suite}")

    spark = (
        SparkSession.builder.appName("NovaRocksBenchmarkBootstrap")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )

    try:
        configure_catalog(spark, args)
        spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {qualified_name(args.catalog, args.database)}"
        )

        row_counts = []
        raw_base_uri = args.raw_base_uri.rstrip("/")
        for table, columns in SSB_TABLES.items():
            raw_path = f"{raw_base_uri}/{SSB_RAW_NAMES[table]}"
            df = read_pipe_table(spark, raw_path, columns)
            target = qualified_name(args.catalog, args.database, table)

            spark.sql(f"DROP TABLE IF EXISTS {target}")
            (
                df.writeTo(target)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .create()
            )
            row_counts.append({"name": table, "rows": df.count()})

        manifest = {
            "suite": args.suite,
            "scale": args.scale,
            "catalog": args.catalog,
            "database": args.database,
            "generator": args.generator,
            "generator_version": args.generator_version,
            "schema_version": "2026-05-26",
            "warehouse": args.warehouse,
            "tables": row_counts,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest_df = spark.createDataFrame(
            [(json.dumps(manifest, sort_keys=True),)], ["value"]
        )
        manifest_df.coalesce(1).write.mode("overwrite").text(args.manifest_output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
