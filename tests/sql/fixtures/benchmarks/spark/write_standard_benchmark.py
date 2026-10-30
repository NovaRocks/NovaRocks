#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

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
        "range_partitions": 4,
        "sort_columns": ["lo_discount", "lo_quantity", "lo_orderdate"],
        "target_file_size_bytes": 64 * 1024 * 1024,
    },
    ("tpc-h", "lineitem"): {
        "range_partitions": 4,
        "sort_columns": ["l_shipdate", "l_discount", "l_quantity"],
        "target_file_size_bytes": 256 * 1024 * 1024,
    },
    ("tpc-h", "orders"): {
        "range_partitions": 1,
        "sort_columns": ["o_orderdate", "o_orderkey"],
        "target_file_size_bytes": 256 * 1024 * 1024,
    },
    ("tpc-ds", "store_sales"): {
        "range_partitions": 4,
        "sort_columns": ["ss_sold_date_sk", "ss_item_sk"],
        "target_file_size_bytes": 256 * 1024 * 1024,
    },
    ("tpc-ds", "catalog_sales"): {
        "range_partitions": 4,
        "sort_columns": ["cs_sold_date_sk", "cs_item_sk"],
        "target_file_size_bytes": 256 * 1024 * 1024,
    },
    ("tpc-ds", "web_sales"): {
        "range_partitions": 4,
        "sort_columns": ["ws_sold_date_sk", "ws_item_sk"],
        "target_file_size_bytes": 256 * 1024 * 1024,
    },
    ("tpc-ds", "inventory"): {
        "range_partitions": 1,
        "sort_columns": ["inv_date_sk", "inv_item_sk"],
        "target_file_size_bytes": 256 * 1024 * 1024,
    },
}

PARQUET_WRITE_PROPERTIES = {
    "write.parquet.row-group-size-bytes": str(128 * 1024 * 1024),
    "write.parquet.page-size-bytes": str(16 * 1024 * 1024),
    "write.parquet.page-row-limit": str(1_048_576),
}

ICEBERG_FORMAT_VERSION = "3"

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


def parse_args(argv=None):
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
    parser.add_argument("--object-store-kind", choices=("minio", "aliyun-oss"), default="minio")
    parser.add_argument("--s3-region")
    parser.add_argument("--s3-role-name")
    parser.add_argument("--s3-access-key")
    parser.add_argument("--s3-secret-key")
    parser.add_argument("--generator", required=True)
    parser.add_argument("--generator-version", required=True)
    parser.add_argument("--schema-ddl")
    parser.add_argument("--fixture-contract-json")
    args = parser.parse_args(argv)
    if args.object_store_kind == "aliyun-oss":
        if args.s3_access_key or args.s3_secret_key:
            parser.error("OSS node-local producers cannot contain static credential material")
        if (not args.s3_role_name or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@-]*", args.s3_role_name)
                or not args.s3_region or not re.fullmatch(r"[a-z0-9-]+", args.s3_region)
                or args.s3_endpoint != f"https://s3.oss-{args.s3_region}-internal.aliyuncs.com"):
            parser.error("OSS producers require an explicit role, region and exact regional internal S3 endpoint")
    else:
        args.s3_access_key = args.s3_access_key or os.environ.get("AWS_ACCESS_KEY_ID")
        args.s3_secret_key = args.s3_secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY")
        args.s3_region = args.s3_region or "us-east-1"
        if not args.s3_access_key or not args.s3_secret_key:
            parser.error("S3 credentials require the explicit arguments or the producer environment")
    return args


def load_fixture_contract(args):
    if not args.fixture_contract_json:
        return None
    contract_path = Path(args.fixture_contract_json)
    try:
        resolved = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unable to read fixture contract: {exc}") from exc
    required = {
        "schema_version",
        "dataset_key",
        "contract",
        "fixture_contract_id",
        "dataset_root",
        "ready_uri",
        "staging_parent",
        "producer_fingerprint",
    }
    missing = sorted(required - set(resolved))
    if missing:
        raise ValueError(f"fixture contract is missing fields: {missing}")
    dataset_key = resolved["dataset_key"]
    contract = resolved["contract"]
    if (
        resolved["schema_version"] != 1
        or dataset_key.get("suite") != args.suite
        or dataset_key.get("scale") != args.scale
        or contract.get("suite") != args.suite
        or contract.get("normalized_scale") != args.scale
        or contract.get("generator", {}).get("name") != args.generator
        or contract.get("generator", {}).get("version") != args.generator_version
        or resolved["producer_fingerprint"].get("object_store_kind", "minio") != args.object_store_kind
    ):
        raise ValueError("fixture contract does not match the requested benchmark load")
    return resolved


def s3_settings(args):
    cloud = args.object_store_kind == "aliyun-oss"
    catalog = {"s3.endpoint": args.s3_endpoint,
               "s3.path-style-access": "false" if cloud else "true",
               "s3.region": args.s3_region, "client.region": args.s3_region}
    hadoop = {"fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
              "fs.s3a.endpoint": args.s3_endpoint,
              "fs.s3a.path.style.access": "false" if cloud else "true",
              "fs.s3a.connection.ssl.enabled": "true" if args.s3_endpoint.startswith("https://") else "false"}
    if cloud:
        catalog.update({"client.credentials-provider": "org.novarocks.fixture.auth.AliyunEcsCredentialsProvider",
                        "client.credentials-provider.role-name": args.s3_role_name})
        hadoop["fs.s3a.aws.credentials.provider"] = "org.novarocks.fixture.auth.AliyunEcsV1CredentialsProvider"
    else:
        catalog.update({"s3.access-key-id": args.s3_access_key, "s3.secret-access-key": args.s3_secret_key})
        hadoop.update({"fs.s3a.access.key": args.s3_access_key, "fs.s3a.secret.key": args.s3_secret_key,
                       "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"})
    return catalog, hadoop


def configure_catalog(spark, args):
    configure_s3a(spark, args)
    configure_parquet_writer(spark)
    catalog_prefix = f"spark.sql.catalog.{args.catalog}"
    spark.conf.set(catalog_prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"{catalog_prefix}.type", "hadoop")
    spark.conf.set(f"{catalog_prefix}.warehouse", spark_s3_uri(args.warehouse))
    spark.conf.set(f"{catalog_prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    catalog, hadoop = s3_settings(args)
    if args.object_store_kind == "aliyun-oss":
        for key in ["s3.access-key-id", "s3.secret-access-key", "s3.session-token"]:
            spark.conf.unset(f"{catalog_prefix}.{key}")
    for key, value in catalog.items():
        spark.conf.set(f"{catalog_prefix}.{key}", value)
    for key, value in hadoop.items():
        spark.conf.set(f"spark.hadoop.{key}", value)


def configure_s3a(spark, args):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    if args.object_store_kind == "aliyun-oss":
        for key in ["fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.session.token"]:
            hadoop_conf.unset(key)
            spark.conf.unset(f"spark.hadoop.{key}")
    _, hadoop = s3_settings(args)
    for key, value in hadoop.items():
        hadoop_conf.set(key, value)


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


def parse_fixture_layouts(fixture_contract):
    if fixture_contract is None:
        return TABLE_LAYOUTS
    layouts = fixture_contract["contract"].get("table_layouts")
    if not isinstance(layouts, list):
        raise ValueError("fixture contract is missing table_layouts")
    parsed = {}
    for layout in layouts:
        required = {"suite", "table", "range_partitions", "sort_columns", "target_file_size_bytes"}
        if not isinstance(layout, dict) or required - set(layout):
            raise ValueError("fixture contract has an invalid table layout")
        key = (layout["suite"], layout["table"])
        if key in parsed:
            raise ValueError(f"fixture contract duplicates table layout: {key}")
        parsed[key] = {field: layout[field] for field in required if field not in {"suite", "table"}}
    return parsed


def apply_table_layout(suite, table, df, layouts):
    layout = layouts.get((suite, table))
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


def sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def compute_table_stats(spark, catalog, database, table):
    procedure = qualified_name(catalog, "system", "compute_table_stats")
    table_name = f"{database}.{table}"
    rows = spark.sql(f"CALL {procedure}(table => {sql_string(table_name)})").collect()
    if not rows:
        raise RuntimeError(f"compute_table_stats returned no rows for {table_name}")

    row = rows[0].asDict(recursive=True)
    statistics_file = row.get("statistics_file")
    if not statistics_file:
        raise RuntimeError(
            f"compute_table_stats did not return statistics_file for {table_name}: {row}"
        )
    return statistics_file


def table_publication_metadata(spark, catalog, database, table):
    table_ref = qualified_name(catalog, database, table)
    metadata_rows = spark.sql(
        f"SELECT file, latest_snapshot_id FROM {table_ref}.metadata_log_entries "
        "ORDER BY timestamp DESC LIMIT 1"
    ).collect()
    if not metadata_rows or not metadata_rows[0]["file"]:
        raise RuntimeError(f"missing metadata log entry for {database}.{table}")
    snapshot_rows = spark.sql(
        f"SELECT snapshot_id FROM {table_ref}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not snapshot_rows:
        raise RuntimeError(f"missing current snapshot for {database}.{table}")
    return metadata_rows[0]["file"], str(snapshot_rows[0]["snapshot_id"])


def main():
    args = parse_args()
    if args.suite not in SUITE_DATABASES:
        raise ValueError(f"unsupported suite: {args.suite}")
    if args.database != SUITE_DATABASES[args.suite]:
        raise ValueError(
            f"database {args.database} does not match suite {args.suite}; "
            f"expected {SUITE_DATABASES[args.suite]}"
        )

    fixture_contract = load_fixture_contract(args)
    fixture_layouts = parse_fixture_layouts(fixture_contract)

    if args.object_store_kind == "aliyun-oss":
        if os.environ.get("NOVA_OSS_ROLE_NAME") != args.s3_role_name:
            raise ValueError("Spark driver role differs from the explicit fixture role")
        os.environ["NOVA_OSS_ROLE_NAME"] = args.s3_role_name
        for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
            os.environ.pop(key, None)
    builder = SparkSession.builder.appName("NovaRocksBenchmarkBootstrap")
    if args.object_store_kind == "aliyun-oss":
        builder = builder.config("spark.executorEnv.NOVA_OSS_ROLE_NAME", args.s3_role_name)
    spark = (
        builder
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
            df, layout = apply_table_layout(args.suite, table, df, fixture_layouts)
            target = qualified_name(args.catalog, args.database, table)

            spark.sql(f"DROP TABLE IF EXISTS {target}")
            writer = (
                df.writeTo(target)
                .using("iceberg")
                .tableProperty("format-version", ICEBERG_FORMAT_VERSION)
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
            statistics_file = compute_table_stats(spark, args.catalog, args.database, table)
            metadata_uri, snapshot_id = table_publication_metadata(
                spark, args.catalog, args.database, table
            )
            row_counts.append(
                {
                    "name": table,
                    "rows": row_count,
                    "format_version": ICEBERG_FORMAT_VERSION,
                    "statistics_file": statistics_file,
                    "metadata_uri": metadata_uri,
                    "snapshot_id": snapshot_id,
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
            "iceberg_format_version": ICEBERG_FORMAT_VERSION,
            "puffin_ndv": "spark_compute_table_stats",
            "raw_text_encoding": raw_text_encoding,
            "warehouse": args.warehouse,
            "parquet_write_properties": PARQUET_WRITE_PROPERTIES,
            "tables": row_counts,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        if fixture_contract is not None:
            manifest["fixture_contract"] = fixture_contract["contract"]
            manifest["producer_fingerprint"] = fixture_contract["producer_fingerprint"]
            manifest["dataset_key"] = fixture_contract["dataset_key"]
            manifest["fixture_contract_id"] = fixture_contract["fixture_contract_id"]
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
