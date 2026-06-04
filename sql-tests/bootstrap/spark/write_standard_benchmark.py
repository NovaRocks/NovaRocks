#!/usr/bin/env python3
import argparse
import json
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, when
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


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

TPCH_RAW_NAMES = {
    table: f"{table}.tbl"
    for table in [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]
}

TPCDS_RAW_NAMES = {
    table: f"{table}.dat"
    for table in [
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
    ]
}

SUITE_DATABASES = {
    "ssb": "ssb",
    "tpc-h": "tpch",
    "tpc-ds": "tpcds",
}

SUITE_RAW_NAMES = {
    "ssb": SSB_RAW_NAMES,
    "tpc-h": TPCH_RAW_NAMES,
    "tpc-ds": TPCDS_RAW_NAMES,
}

TABLE_LAYOUTS = {
    ("ssb", "lineorder"): {
        "range_partitions": 64,
        "sort_columns": ["lo_discount", "lo_quantity", "lo_orderdate"],
        "target_file_size_bytes": 4 * 1024 * 1024,
    },
    ("tpc-h", "lineitem"): {
        "range_partitions": 64,
        "sort_columns": ["l_shipdate", "l_discount", "l_quantity"],
        "target_file_size_bytes": 16 * 1024 * 1024,
    },
    ("tpc-h", "orders"): {
        "range_partitions": 16,
        "sort_columns": ["o_orderdate", "o_orderkey"],
        "target_file_size_bytes": 16 * 1024 * 1024,
    },
    ("tpc-ds", "store_sales"): {
        "range_partitions": 64,
        "sort_columns": ["ss_sold_date_sk", "ss_item_sk"],
        "target_file_size_bytes": 16 * 1024 * 1024,
    },
    ("tpc-ds", "catalog_sales"): {
        "range_partitions": 64,
        "sort_columns": ["cs_sold_date_sk", "cs_item_sk"],
        "target_file_size_bytes": 16 * 1024 * 1024,
    },
    ("tpc-ds", "web_sales"): {
        "range_partitions": 64,
        "sort_columns": ["ws_sold_date_sk", "ws_item_sk"],
        "target_file_size_bytes": 16 * 1024 * 1024,
    },
    ("tpc-ds", "inventory"): {
        "range_partitions": 16,
        "sort_columns": ["inv_date_sk", "inv_item_sk"],
        "target_file_size_bytes": 16 * 1024 * 1024,
    },
}

PARQUET_WRITE_PROPERTIES = {
    "write.parquet.row-group-size-bytes": str(128 * 1024 * 1024),
    "write.parquet.page-size-bytes": str(16 * 1024 * 1024),
    "write.parquet.page-row-limit": str(1_048_576),
}

PARQUET_HADOOP_PROPERTIES = {
    "parquet.block.size": str(128 * 1024 * 1024),
    "parquet.page.size": str(16 * 1024 * 1024),
    "parquet.page.row.count.limit": str(1_048_576),
}

RAW_TEXT_ENCODINGS = {
    "ssb": "UTF-8",
    "tpc-h": "UTF-8",
    "tpc-ds": "ISO-8859-1",
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
    parser.add_argument("--schema-ddl")
    return parser.parse_args()


def configure_catalog(spark, args):
    configure_s3a(spark, args)
    configure_parquet_writer(spark)

    catalog_prefix = f"spark.sql.catalog.{args.catalog}"
    spark.conf.set(catalog_prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"{catalog_prefix}.type", "hadoop")
    spark.conf.set(f"{catalog_prefix}.warehouse", spark_s3_uri(args.warehouse))
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


def configure_s3a(spark, args):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.endpoint", args.s3_endpoint)
    hadoop_conf.set("fs.s3a.access.key", args.s3_access_key)
    hadoop_conf.set("fs.s3a.secret.key", args.s3_secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def configure_parquet_writer(spark):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in PARQUET_HADOOP_PROPERTIES.items():
        spark.conf.set(key, value)
        hadoop_conf.set(key, value)


def spark_type(type_name):
    normalized = type_name.lower()
    if normalized in ("int", "integer"):
        return IntegerType()
    if normalized == "date":
        return DateType()
    if normalized.startswith("decimal"):
        match = re.match(r"decimal\((\d+),\s*(\d+)\)", normalized)
        if match:
            return DecimalType(int(match.group(1)), int(match.group(2)))
        return DecimalType(15, 2)
    if normalized.startswith("char") or normalized.startswith("varchar"):
        return StringType()
    if normalized == "string":
        return StringType()
    raise ValueError(f"unsupported benchmark type: {type_name}")


def spark_s3_uri(uri):
    if uri.startswith("s3://"):
        return "s3a://" + uri[len("s3://") :]
    return uri


def parse_schema_ddl(path):
    with open(path, "r", encoding="utf-8") as ddl_file:
        ddl = ddl_file.read()

    schemas = {}
    pattern = re.compile(
        r"create\s+table\s+[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s*\((.*?)\)\s*;",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(ddl):
        table = match.group(1).lower()
        if table == "dbgen_version":
            continue
        columns = []
        for raw_line in match.group(2).splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            if line.lower().startswith(("primary key", "foreign key", "constraint")):
                continue
            col_match = re.match(
                r"[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s+([A-Za-z]+(?:\(\d+(?:\s*,\s*\d+)?\))?)",
                line,
                re.IGNORECASE,
            )
            if not col_match:
                continue
            columns.append((col_match.group(1).lower(), col_match.group(2).lower()))
        if columns:
            schemas[table] = columns
    return schemas


def suite_schemas(args):
    if args.suite == "ssb":
        return SSB_TABLES
    if args.suite in ("tpc-h", "tpc-ds"):
        if not args.schema_ddl:
            raise ValueError(f"--schema-ddl is required for {args.suite}")
        return parse_schema_ddl(args.schema_ddl)
    raise ValueError(f"unsupported suite: {args.suite}")


def read_pipe_table(spark, path, columns, encoding):
    raw_schema = StructType(
        [StructField(f"c{idx}", StringType(), True) for idx in range(len(columns) + 1)]
    )
    df = (
        spark.read.option("delimiter", "|")
        .option("header", "false")
        .option("encoding", encoding)
        .schema(raw_schema)
        .csv(path)
    )

    projected = []
    for idx, (name, type_name) in enumerate(columns):
        value = col(f"c{idx}")
        target_type = spark_type(type_name)
        if isinstance(target_type, StringType):
            value = when(value == "", lit(None)).otherwise(value)
        else:
            value = trim(value)
            value = when(value == "", lit(None)).otherwise(value)
            value = value.cast(target_type)
        projected.append(value.alias(name))
    return df.select(*projected)


def apply_table_layout(suite, table, df):
    layout = TABLE_LAYOUTS.get((suite, table))
    if layout is None:
        return df, {}

    sort_columns = layout["sort_columns"]
    repartitioned = df.repartitionByRange(
        layout["range_partitions"], *[col(name) for name in sort_columns]
    )
    sorted_df = repartitioned.sortWithinPartitions(*sort_columns)
    return sorted_df, layout


def sql_ident(name):
    return f"`{name.replace('`', '``')}`"


def qualified_name(*parts):
    return ".".join(sql_ident(part) for part in parts)


def main():
    args = parse_args()
    if args.suite not in SUITE_DATABASES:
        raise ValueError(f"unsupported suite: {args.suite}")
    if args.database != SUITE_DATABASES[args.suite]:
        raise ValueError(
            f"database {args.database} does not match suite {args.suite}; "
            f"expected {SUITE_DATABASES[args.suite]}"
        )

    spark = (
        SparkSession.builder.appName("NovaRocksBenchmarkBootstrap")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        configure_catalog(spark, args)
        spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {qualified_name(args.catalog, args.database)}"
        )

        schemas = suite_schemas(args)
        raw_names = SUITE_RAW_NAMES[args.suite]
        missing_schemas = sorted(set(raw_names) - set(schemas))
        if missing_schemas:
            raise ValueError(
                f"schema DDL for {args.suite} is missing tables: {missing_schemas}"
            )

        row_counts = []
        raw_base_uri = spark_s3_uri(args.raw_base_uri).rstrip("/")
        raw_text_encoding = RAW_TEXT_ENCODINGS[args.suite]
        for table, raw_name in raw_names.items():
            columns = schemas[table]
            raw_path = f"{raw_base_uri}/{raw_name}"
            df = read_pipe_table(spark, raw_path, columns, raw_text_encoding)
            row_count = df.count()
            df, layout = apply_table_layout(args.suite, table, df)
            target = qualified_name(args.catalog, args.database, table)

            spark.sql(f"DROP TABLE IF EXISTS {target}")
            writer = (
                df.writeTo(target).using("iceberg").tableProperty("format-version", "2")
            )
            for key, value in PARQUET_WRITE_PROPERTIES.items():
                writer = writer.tableProperty(key, value)
            if layout:
                writer = (
                    writer.tableProperty("write.distribution-mode", "none")
                    .tableProperty(
                        "write.target-file-size-bytes",
                        str(layout["target_file_size_bytes"]),
                    )
                    .tableProperty(
                        "novarocks.bootstrap.layout",
                        json.dumps(layout, sort_keys=True),
                    )
                )
            writer.create()
            row_counts.append(
                {
                    "name": table,
                    "rows": row_count,
                    "layout": layout or None,
                }
            )

        manifest = {
            "suite": args.suite,
            "scale": args.scale,
            "catalog": args.catalog,
            "database": args.database,
            "generator": args.generator,
            "generator_version": args.generator_version,
            "schema_version": "2026-05-26",
            "raw_text_encoding": raw_text_encoding,
            "warehouse": args.warehouse,
            "parquet_write_properties": PARQUET_WRITE_PROPERTIES,
            "tables": row_counts,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest_df = spark.createDataFrame(
            [(json.dumps(manifest, sort_keys=True),)], ["value"]
        )
        manifest_df.coalesce(1).write.mode("overwrite").text(
            spark_s3_uri(args.manifest_output)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
