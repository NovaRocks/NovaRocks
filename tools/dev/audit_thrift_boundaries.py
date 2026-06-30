#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import argparse
import json
import re
import sys

SCAN_ROOTS = (
    "src/exec",
    "src/connector",
    "src/engine",
    "src/formats",
    "src/runtime",
)

THRIFT_PATTERN = re.compile(
    r"\b("
    r"use\s+crate\s+as|"
    r"crate::thrift|"
    r"crate::types::arrow_thrift|"
    r"TTypeDesc|TPrimitiveType|TExpr|TExprNode|TDescriptorTable|"
    r"TIcebergTable|TIcebergTableSink|TIcebergDataFile|"
    r"TSinkCommitInfo|TIcebergPartitionDescriptor|"
    r"TOlapTableSink|TNetworkAddress|TCompressionType|"
    r"thrift_desc_to_arrow_type|thrift_desc_to_primitive|"
    r"thrift_type_desc_from_primitive|arrow_type_to_primitive"
    r")\b"
)

USE_ITEM_START_PATTERN = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?use\b")


@dataclass(frozen=True)
class Hit:
    path: str
    line: int
    text: str


@dataclass(frozen=True)
class BaselineEntry:
    category: str
    owner: str
    max_hits: int
    reason: str


BASELINE: dict[str, BaselineEntry] = {
    "src/connector/iceberg/sink.rs": BaselineEntry("domain-leak", "B1", 0, "Iceberg sink no longer accepts FE thrift sink/table/expr descriptors"),
    "src/connector/iceberg/schema.rs": BaselineEntry("domain-leak", "B1", 0, "Iceberg schema binding uses internal descriptors"),
    "src/connector/iceberg/position_delete_descriptor.rs": BaselineEntry("domain-leak", "B1", 0, "Position delete descriptor uses internal descriptor inputs"),
    "src/connector/iceberg/position_delete.rs": BaselineEntry("domain-leak", "B1", 0, "Position delete path uses internal delete-file specs"),
    "src/connector/iceberg/equality_delete.rs": BaselineEntry("domain-leak", "B1", 0, "Equality delete path uses internal delete-file specs"),
    "src/connector/iceberg/changes.rs": BaselineEntry("domain-leak", "B1", 0, "Iceberg change path uses internal delete-file specs"),
    "src/connector/iceberg/report_wire.rs": BaselineEntry("legal-boundary", "B1-wire", 4, "Iceberg writer report wire adapter serializes internal reports to TSinkCommitInfo"),
    "src/exec/row_position.rs": BaselineEntry("domain-leak", "B1", 0, "Row-position binding uses internal row position type"),
    "src/connector/iceberg/commit/collector.rs": BaselineEntry("domain-leak", "B2", 0, "Commit collector consumes internal writer reports and written file reports"),
    "src/connector/iceberg/data_writer.rs": BaselineEntry("domain-leak", "B2", 0, "Data writer emits internal IcebergWriterReport"),
    "src/connector/iceberg/write_descriptor.rs": BaselineEntry("domain-leak", "B2", 0, "Write descriptor exposes internal partition descriptors only"),
    "src/connector/iceberg/commit/row_delta_dv_from_files.rs": BaselineEntry("domain-leak", "B2", 0, "DV row-delta commit path still shares thrift-facing report objects"),
    "src/connector/iceberg/commit/types.rs": BaselineEntry("domain-leak", "B2", 0, "Iceberg commit type module still exposes thrift report compatibility"),
    "src/connector/iceberg/state.rs": BaselineEntry("domain-leak", "B5", 0, "Iceberg state caches table locations from normalized descriptor snapshots"),
    "src/engine/iceberg_writer.rs": BaselineEntry("domain-leak", "B7", 0, "Standalone Iceberg writer produces internal position-delete descriptors only"),
    "src/engine/delete_flow.rs": BaselineEntry("domain-leak", "B2", 0, "Delete flow no longer uses thrift-facing Iceberg write descriptors"),
    "src/engine/write_operation_lifecycle.rs": BaselineEntry("domain-leak", "B2", 0, "Standalone write lifecycle still shares thrift sink-commit payload handling"),
    "src/engine/write_transaction.rs": BaselineEntry("domain-leak", "B2", 0, "Standalone write transaction still shares thrift sink-commit payload handling"),
    "src/engine/insert_flow.rs": BaselineEntry("domain-leak", "B7", 0, "Standalone insert flow consumes thrift-free standalone query options"),
    "src/engine/query_options_wire.rs": BaselineEntry("legal-boundary", "B7-wire", 1, "Standalone engine query options thrift adapter converts FE-compatible query options"),
    "src/runtime/sink_commit.rs": BaselineEntry("legal-boundary", "B2-wire", 4, "Runtime sink commit is the wire boundary for sink commit reports"),
    "src/runtime/sink_commit_wire.rs": BaselineEntry("legal-boundary", "B2-wire", 9, "Runtime sink commit wire adapter is the explicit wire boundary for Iceberg writer reports and partition descriptors"),
    "src/runtime/write_coordinator.rs": BaselineEntry("legal-boundary", "B2-wire", 3, "Runtime write coordinator transports sink commit control-plane payloads"),
    "src/runtime/write_operation_lifecycle.rs": BaselineEntry("legal-boundary", "B2-wire", 1, "Runtime write lifecycle transports sink commit control-plane payloads"),
    "src/exec/runtime_filter/min_max.rs": BaselineEntry("domain-leak", "B3", 0, "Runtime min/max filter uses RuntimeFilterType internally"),
    "src/exec/runtime_filter/bitset.rs": BaselineEntry("domain-leak", "B3", 0, "Runtime bitset filter uses RuntimeFilterType internally"),
    "src/exec/runtime_filter/mod.rs": BaselineEntry("domain-leak", "B3", 0, "Runtime filter module exports internal type conversion only"),
    "src/exec/runtime_filter/bloom.rs": BaselineEntry("domain-leak", "B3", 0, "Runtime bloom filter uses RuntimeFilterType internally"),
    "src/exec/runtime_filter/membership.rs": BaselineEntry("domain-leak", "B3", 0, "Runtime membership filters use RuntimeFilterType internally"),
    "src/exec/runtime_filter/merger.rs": BaselineEntry("domain-leak", "B3", 0, "Runtime filter merger uses RuntimeFilterType internally"),
    "src/runtime/runtime_filter_worker.rs": BaselineEntry("domain-leak", "B3", 0, "Runtime filter worker no longer touches thrift primitive tags"),
    "src/exec/operators/hashjoin/hash_join_build_sink.rs": BaselineEntry("domain-leak", "B3", 0, "Hash join build sink derives RuntimeFilterType from Arrow types"),
    "src/exec/runtime_filter/codec.rs": BaselineEntry("legal-boundary", "B3-wire", 51, "Runtime filter codec is an explicit thrift/proto wire boundary"),
    "src/exec/runtime_filter/proto_type.rs": BaselineEntry("legal-boundary", "B3-wire", 24, "Runtime filter proto type mapping is an explicit wire boundary"),
    "src/connector/starrocks/sink/factory.rs": BaselineEntry("domain-leak", "B4", 0, "StarRocks sink factory consumes thrift-free sink descriptors"),
    "src/connector/starrocks/sink/operator.rs": BaselineEntry("domain-leak", "B4", 0, "StarRocks sink operator evaluates internal predicate plans"),
    "src/connector/starrocks/sink/routing.rs": BaselineEntry("domain-leak", "B4", 0, "StarRocks sink routing consumes internal partition and location descriptors"),
    "src/connector/starrocks/sink/partition_key.rs": BaselineEntry("domain-leak", "B4", 0, "StarRocks partition key path consumes internal partition key values"),
    "src/connector/starrocks/sink/auto_increment.rs": BaselineEntry("domain-leak", "B4", 0, "StarRocks auto-increment sink path uses internal FE address descriptors"),
    "src/connector/starrocks/sink/frontend_wire.rs": BaselineEntry("legal-boundary", "B4-wire", 14, "StarRocks sink FE RPC wire adapter converts internal requests to thrift RPC payloads"),
    "src/connector/starrocks/sink/report_wire.rs": BaselineEntry("legal-boundary", "control-plane-wire", 1, "StarRocks sink tablet status report wire adapter exposes FE-compatible report payloads"),
    "src/connector/hdfs.rs": BaselineEntry("domain-leak", "B5", 0, "HDFS connector consumes normalized scan ranges and descriptor-derived config"),
    "src/connector/iceberg/scan_planner.rs": BaselineEntry("domain-leak", "B5", 0, "Iceberg scan planner exposes domain handles and splits only"),
    "src/runtime/lookup.rs": BaselineEntry("domain-leak", "B5", 0, "Runtime lookup consumes DescriptorSnapshot instead of thrift descriptors"),
    "src/exec/chunk/schema.rs": BaselineEntry("domain-leak", "B7", 0, "Execution chunk schema is thrift-free"),
    "src/exec/chunk/schema_thrift.rs": BaselineEntry("legal-boundary", "B7-wire", 0, "Execution chunk schema thrift adapter builds chunk schemas from FE type descriptors"),
    "src/exec/operators/fetch_processor.rs": BaselineEntry("domain-leak", "B5", 0, "Fetch processor discovers lookup output slots from DescriptorSnapshot"),
    "src/connector/scan_planning.rs": BaselineEntry("domain-leak", "B5", 0, "Shared scan planning trait is domain-only"),
    "src/connector/starrocks/table/scan_planner.rs": BaselineEntry("domain-leak", "B5", 0, "StarRocks table scan planner exposes domain handles and splits only"),
    "src/exec/node/scan.rs": BaselineEntry("domain-leak", "B5", 0, "Execution scan node consumes normalized incremental scan ranges"),
    "src/exec/operators/scan/runner.rs": BaselineEntry("legal-boundary", "control-plane-wire", 1, "Scan runner records FE-compatible runtime profile metric units"),
    "src/runtime/query_context.rs": BaselineEntry("legal-boundary", "B5-wire", 12, "Query context may retain FE-compatible descriptor and incremental scan payloads while exposing normalized runtime snapshots"),
    "src/runtime/descriptor_snapshot_thrift.rs": BaselineEntry("legal-boundary", "B5-wire", 29, "Runtime descriptor snapshot thrift adapter converts FE descriptors into internal descriptor snapshots"),
    "src/connector/starrocks/lake/schema.rs": BaselineEntry("legal-boundary", "B6-wire", 2, "StarRocks lake tablet creation is a storage/protocol IO boundary"),
    "src/connector/starrocks/lake/schema_adapter.rs": BaselineEntry("legal-boundary", "B6-wire", 85, "StarRocks lake schema adapter converts thrift schema requests into tablet schema protobufs"),
    "src/connector/starrocks/table/ddl.rs": BaselineEntry("legal-boundary", "B6-wire", 7, "StarRocks table DDL keeps protocol metadata only at orchestration boundaries"),
    "src/connector/starrocks/table/schema_adapter.rs": BaselineEntry("legal-boundary", "B6-wire", 52, "StarRocks table schema adapter converts standalone table definitions into StarRocks thrift request schema"),
    "src/connector/schema/fe_tables.rs": BaselineEntry("legal-boundary", "B6-wire", 66, "Schema FE tables expose StarRocks-compatible protocol metadata"),
    "src/formats/starrocks/writer/segment_meta.rs": BaselineEntry("legal-boundary", "B6-wire", 0, "StarRocks segment metadata production code uses internal storage-format wire type"),
    "src/formats/parquet/mod.rs": BaselineEntry("legal-boundary", "B6-wire", 1, "Parquet reader keeps FE-compatible runtime profile metric thrift unit only"),
    "src/connector/starrocks/table_schema_service.rs": BaselineEntry("legal-boundary", "B6-wire", 11, "StarRocks table schema service is a protocol adapter boundary"),
    "src/connector/starrocks/lake/schema_change.rs": BaselineEntry("legal-boundary", "B6-wire", 2, "StarRocks lake schema change is a protocol adapter boundary"),
    "src/connector/starrocks/fe_v2_meta.rs": BaselineEntry("legal-boundary", "B6-wire", 8, "StarRocks FE v2 metadata bridge is a protocol adapter boundary"),
    "src/formats/parquet/variant_read.rs": BaselineEntry("domain-leak", "B6", 0, "Parquet variant reader uses ParquetSlotKind instead of thrift primitive type"),
    "src/connector/schema/frontend.rs": BaselineEntry("legal-boundary", "B6-wire", 6, "Schema frontend adapter exposes StarRocks-compatible metadata"),
    "src/connector/schema/op.rs": BaselineEntry("legal-boundary", "B6-wire", 3, "Schema operator adapter exposes StarRocks-compatible metadata"),
    "src/connector/schema/loads.rs": BaselineEntry("legal-boundary", "B6-wire", 3, "Schema loads adapter exposes StarRocks-compatible metadata"),
    "src/connector/schema/load_tracking_logs.rs": BaselineEntry("legal-boundary", "B6-wire", 3, "Schema load tracking adapter exposes StarRocks-compatible metadata"),
    "src/connector/starrocks/scan/op.rs": BaselineEntry("legal-boundary", "B6-wire", 3, "StarRocks scan operator is a StarRocks protocol adapter boundary"),
    "src/connector/starrocks/lake/context.rs": BaselineEntry("legal-boundary", "B6-wire", 1, "StarRocks lake context carries protocol adapter metadata"),
    "src/connector/starrocks/lake/txn_log.rs": BaselineEntry("legal-boundary", "B6-wire", 0, "StarRocks lake transaction log carries protocol adapter metadata"),
    "src/formats/parquet/reader.rs": BaselineEntry("legal-boundary", "B6-wire", 0, "Parquet reader contains a format adapter boundary"),
    "src/formats/orc.rs": BaselineEntry("legal-boundary", "B6-wire", 1, "ORC reader contains a format adapter boundary"),
    "src/connector/starrocks/table/mv_ddl.rs": BaselineEntry("legal-boundary", "B6-wire", 0, "StarRocks MV DDL is a protocol adapter boundary"),
    "src/connector/starrocks/scan/reader.rs": BaselineEntry("legal-boundary", "B6-wire", 1, "StarRocks scan reader is a StarRocks protocol adapter boundary"),
    "src/connector/schema/context.rs": BaselineEntry("legal-boundary", "B6-wire", 1, "Schema context exposes StarRocks-compatible metadata"),
    "src/runtime/coordinator.rs": BaselineEntry("legal-boundary", "control-plane-wire", 36, "Runtime coordinator transports FE-compatible control-plane payloads"),
    "src/runtime/exec_params.rs": BaselineEntry("legal-boundary", "control-plane-wire", 6, "Execution parameters transport FE-compatible control-plane payloads"),
    "src/runtime/scheduler.rs": BaselineEntry("legal-boundary", "control-plane-wire", 10, "Runtime scheduler transports FE-compatible control-plane payloads"),
    "src/runtime/dispatcher.rs": BaselineEntry("legal-boundary", "control-plane-wire", 2, "Runtime dispatcher transports FE-compatible control-plane payloads"),
    "src/runtime/profile.rs": BaselineEntry("legal-boundary", "control-plane-wire", 1, "Runtime profile stores FE-compatible profile metadata"),
    "src/runtime/profile_correlate.rs": BaselineEntry("legal-boundary", "control-plane-wire", 1, "Runtime profile correlation reads FE-compatible profile trees"),
    "src/runtime/result_buffer.rs": BaselineEntry("legal-boundary", "control-plane-wire", 3, "Result buffer exports FE-compatible result metadata"),
    "src/runtime/runtime_state.rs": BaselineEntry("legal-boundary", "control-plane-wire", 7, "Runtime state carries FE-compatible payload references"),
    "src/runtime/exchange.rs": BaselineEntry("legal-boundary", "control-plane-wire", 0, "Exchange runtime carries cross-node wire metadata"),
    "src/runtime/exchange_scan.rs": BaselineEntry("legal-boundary", "control-plane-wire", 5, "Exchange scan carries cross-node wire metadata"),
    "src/exec/operators/data_stream_sink.rs": BaselineEntry("legal-boundary", "control-plane-wire", 6, "Data stream sink emits exchange wire payloads"),
    "src/exec/operators/result_buffer_sink.rs": BaselineEntry("legal-boundary", "control-plane-wire", 4, "Result buffer sink emits FE-compatible result metadata"),
    "src/exec/pipeline/fragment_context.rs": BaselineEntry("legal-boundary", "control-plane-wire", 4, "Fragment context carries FE-compatible control-plane payloads"),
    "src/exec/operators/split_data_stream_sink.rs": BaselineEntry("legal-boundary", "control-plane-wire", 2, "Split data stream sink emits exchange wire payloads"),
    "src/exec/operators/multi_cast_data_stream_sink.rs": BaselineEntry("legal-boundary", "control-plane-wire", 2, "Multicast data stream sink emits exchange wire payloads"),
    "src/exec/pipeline/executor.rs": BaselineEntry("legal-boundary", "control-plane-wire", 2, "Pipeline executor carries FE-compatible control-plane payloads"),
    "src/exec/pipeline/driver.rs": BaselineEntry("legal-boundary", "control-plane-wire", 1, "Pipeline driver carries FE-compatible control-plane payloads"),
    "src/exec/operators/exchange_source.rs": BaselineEntry("legal-boundary", "control-plane-wire", 1, "Exchange source reads cross-node wire metadata"),
    "src/exec/node/fetch.rs": BaselineEntry("legal-boundary", "control-plane-wire", 1, "Fetch node exposes FE-compatible result metadata"),
    "src/engine/mod.rs": BaselineEntry("domain-leak", "B7", 0, "Standalone engine consumes thrift-free standalone query options"),
    "src/exec/spill/mod.rs": BaselineEntry("domain-leak", "B7", 0, "Spill path consumes thrift-free spill configuration"),
    "src/exec/spill/query_options_wire.rs": BaselineEntry("legal-boundary", "B7-wire", 1, "Spill query options thrift adapter converts FE-compatible spill settings"),
    "src/exec/node/join.rs": BaselineEntry("domain-leak", "B7", 0, "Execution join node uses internal runtime-filter merge endpoints"),
    "src/exec/node/join_thrift.rs": BaselineEntry("legal-boundary", "B7-wire", 0, "Execution join thrift adapter converts runtime-filter merge endpoints"),
    "src/exec/node/aggregate.rs": BaselineEntry("domain-leak", "B7", 0, "Execution aggregate node no longer carries thrift-derived metadata"),
    "src/exec/expr/cast.rs": BaselineEntry("domain-leak", "B7", 0, "Execution cast path still uses thrift primitive conversion helpers"),
    "src/exec/operators/aggregate/mod.rs": BaselineEntry("domain-leak", "B7", 0, "Aggregate operator no longer carries thrift-derived metadata"),
    "src/engine/statement.rs": BaselineEntry("domain-leak", "B7", 0, "Standalone statement path consumes thrift-free standalone query options"),
    "src/sql/codegen/iceberg_write_sink_wire.rs": BaselineEntry("legal-boundary", "B7-wire", 0, "Standalone Iceberg write sink wire adapter builds FE-compatible sink thrift payloads"),
}


@dataclass(frozen=True)
class AuditResult:
    hits: dict[str, list[Hit]]
    errors: list[str]
    warnings: list[str]


def starts_raw_string(line: str, i: int) -> tuple[int, int] | None:
    start = i
    if line.startswith("br", i):
        i += 2
    elif line.startswith("r", i):
        i += 1
    else:
        return None

    hashes_start = i
    while i < len(line) and line[i] == "#":
        i += 1
    if i < len(line) and line[i] == '"':
        return i - hashes_start, i + 1 - start
    return None


def char_literal_end(line: str, i: int) -> int | None:
    if i >= len(line) or line[i] != "'":
        return None
    j = i + 1
    escaped = False
    while j < len(line):
        ch = line[j]
        if escaped:
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == "'":
            return j + 1
        j += 1
    return None


def is_lifetime_or_label_start(line: str, i: int) -> bool:
    if i + 1 >= len(line) or line[i] != "'":
        return False
    if not (line[i + 1].isalpha() or line[i + 1] == "_"):
        return False

    j = i + 2
    while j < len(line) and (line[j].isalnum() or line[j] == "_"):
        j += 1

    return j >= len(line) or line[j] != "'"


def mask_rust_non_code(line: str, state: dict[str, object]) -> tuple[str, dict[str, object]]:
    out = []
    i = 0
    while i < len(line):
        block_depth = int(state["block_depth"])
        raw_hashes = state["raw_hashes"]
        string_delim = state["string_delim"]
        escaped = bool(state["escaped"])

        if block_depth:
            if line.startswith("/*", i):
                state["block_depth"] = block_depth + 1
                out.append("  ")
                i += 2
                continue
            if line.startswith("*/", i):
                state["block_depth"] = block_depth - 1
                out.append("  ")
                i += 2
                continue
            out.append(" ")
            i += 1
            continue

        if raw_hashes is not None:
            end = '"' + ("#" * int(raw_hashes))
            if line.startswith(end, i):
                state["raw_hashes"] = None
                out.append(" " * len(end))
                i += len(end)
            else:
                out.append(" ")
                i += 1
            continue

        if string_delim is not None:
            ch = line[i]
            out.append(" ")
            if escaped:
                state["escaped"] = False
            elif ch == "\\":
                state["escaped"] = True
            elif ch == string_delim:
                state["string_delim"] = None
            i += 1
            continue

        if line.startswith("//", i):
            out.append(" " * (len(line) - i))
            break

        if line.startswith("/*", i):
            state["block_depth"] = 1
            out.append("  ")
            i += 2
            continue

        raw_start = starts_raw_string(line, i)
        if raw_start is not None:
            hashes, consumed = raw_start
            state["raw_hashes"] = hashes
            out.append(" " * consumed)
            i += consumed
            continue

        if line.startswith('b"', i):
            state["string_delim"] = '"'
            state["escaped"] = False
            out.append("  ")
            i += 2
            continue

        if line[i] == '"':
            state["string_delim"] = '"'
            state["escaped"] = False
            out.append(" ")
            i += 1
            continue

        if line.startswith("b'", i):
            end = char_literal_end(line, i + 1)
            if end is not None:
                out.append(" " * (end - i))
                i = end
                continue

        if line[i] == "'" and not is_lifetime_or_label_start(line, i):
            end = char_literal_end(line, i)
            if end is not None:
                out.append(" " * (end - i))
                i = end
                continue

        out.append(line[i])
        i += 1

    return "".join(out), state


def brace_delta(line: str) -> int:
    return line.count("{") - line.count("}")


def skip_test_item_state(line: str) -> tuple[bool, int | None]:
    if "{" in line:
        depth = brace_delta(line)
        return False, depth if depth > 0 else None
    if ";" in line:
        return False, None
    return True, None


def iter_hits(path: Path, include_tests: bool) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    pending_test_item = False
    test_item_brace_depth = None
    scanner_state: dict[str, object] = {
        "block_depth": 0,
        "raw_hashes": None,
        "string_delim": None,
        "escaped": False,
    }

    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        code_line, scanner_state = mask_rust_non_code(line, scanner_state)
        stripped_code = code_line.strip()

        if not include_tests and test_item_brace_depth is not None:
            test_item_brace_depth += brace_delta(code_line)
            if test_item_brace_depth <= 0:
                test_item_brace_depth = None
            continue

        if not include_tests and pending_test_item:
            if stripped_code:
                pending_test_item, test_item_brace_depth = skip_test_item_state(code_line)
            continue

        if not stripped_code:
            continue

        if not include_tests and "#![cfg(test)]" in code_line:
            break

        test_attr_start = code_line.find("#[cfg(test)]")
        if not include_tests and test_attr_start != -1:
            after_attr = code_line[test_attr_start + len("#[cfg(test)]") :]
            pending_test_item, test_item_brace_depth = skip_test_item_state(after_attr)
            continue

        if THRIFT_PATTERN.search(code_line):
            hits.append((lineno, line.rstrip()))

    return hits


def iter_rust_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for scan_root in SCAN_ROOTS:
        base = root / scan_root
        if not base.exists():
            continue
        files.extend(sorted(base.rglob("*.rs")))
    return files


def collect_hits(root: Path, include_tests: bool) -> dict[str, list[Hit]]:
    result: dict[str, list[Hit]] = {}
    for path in iter_rust_files(root):
        rel = path.relative_to(root).as_posix()
        file_hits = [Hit(rel, lineno, text) for lineno, text in iter_hits(path, include_tests)]
        if file_hits:
            result[rel] = file_hits
    return result


def evaluate(hits: dict[str, list[Hit]], include_tests: bool) -> AuditResult:
    errors: list[str] = []
    warnings: list[str] = []

    for rel, file_hits in sorted(hits.items()):
        entry = BASELINE.get(rel)
        if entry is None:
            errors.append(f"unknown thrift boundary hit: {rel} ({len(file_hits)} hits)")
            continue
        if entry.category == "test-fixture" and not include_tests:
            continue
        if len(file_hits) > entry.max_hits:
            errors.append(
                f"{rel}: {len(file_hits)} hits exceeds {entry.category}/{entry.owner} baseline {entry.max_hits}"
            )
        elif len(file_hits) < entry.max_hits:
            warnings.append(
                f"{rel}: {len(file_hits)} hits below {entry.category}/{entry.owner} baseline {entry.max_hits}; baseline can be lowered"
            )

    for rel, entry in sorted(BASELINE.items()):
        if entry.category == "domain-leak" and entry.max_hits > 0 and rel not in hits:
            warnings.append(f"{rel}: 0 hits; remove or lower {entry.category}/{entry.owner} baseline")

    return AuditResult(hits=hits, errors=errors, warnings=warnings)


def summarize(result: AuditResult) -> dict[str, object]:
    by_category: dict[str, int] = {}
    by_owner: dict[str, int] = {}
    files: list[dict[str, object]] = []

    for rel, hits in sorted(result.hits.items()):
        entry = BASELINE.get(rel)
        category = entry.category if entry else "unknown"
        owner = entry.owner if entry else "unknown"
        by_category[category] = by_category.get(category, 0) + len(hits)
        by_owner[owner] = by_owner.get(owner, 0) + len(hits)
        files.append(
            {
                "path": rel,
                "category": category,
                "owner": owner,
                "hits": len(hits),
                "baseline": entry.max_hits if entry else None,
            }
        )

    return {
        "total_hits": sum(len(hits) for hits in result.hits.values()),
        "file_count": len(result.hits),
        "by_category": dict(sorted(by_category.items())),
        "by_owner": dict(sorted(by_owner.items())),
        "files": files,
        "errors": result.errors,
        "warnings": result.warnings,
    }


def print_summary(summary: dict[str, object]) -> None:
    by_category = summary["by_category"]
    by_owner = summary["by_owner"]
    files = summary["files"]
    warnings = summary["warnings"]
    errors = summary["errors"]

    print(f"total_hits: {summary['total_hits']}")
    print(f"file_count: {summary['file_count']}")
    print("by_category:")
    for category, count in by_category.items():
        print(f"  {category}: {count}")
    print("by_owner:")
    for owner, count in by_owner.items():
        print(f"  {owner}: {count}")
    print("files:")
    for item in files:
        print(
            f"  {item['path']}: {item['hits']} hits "
            f"[{item['category']}/{item['owner']}, baseline={item['baseline']}]"
        )

    if warnings:
        print("warnings:")
        for warning in warnings:
            print(f"  {warning}")
    if errors:
        print("errors:")
        for error in errors:
            print(f"  {error}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit thrift usage outside NovaRocks protocol/lowering boundaries.")
    parser.add_argument("--root", default=".", help="Repository root. Defaults to current directory.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when unknown or increased thrift boundary hits exist.",
    )
    parser.add_argument("--summary", action="store_true", help="Print category, owner, and file-level summary.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON summary.")
    parser.add_argument("--include-tests", action="store_true", help="Scan cfg(test) items instead of production-only code.")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    if not root.is_dir():
        print(f"error: repository root does not exist or is not a directory: {root}", file=sys.stderr)
        return 1
    if args.strict and not any((root / scan_root).is_dir() for scan_root in SCAN_ROOTS):
        print(
            "error: none of the configured thrift audit scan roots exist under "
            f"{root}: {', '.join(SCAN_ROOTS)}",
            file=sys.stderr,
        )
        return 1

    hits = collect_hits(root, include_tests=args.include_tests)
    result = evaluate(hits, include_tests=args.include_tests)
    summary = summarize(result)

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    elif args.summary or not args.strict:
        print_summary(summary)
    elif result.errors:
        for error in result.errors:
            print(error)
        for warning in result.warnings:
            print(f"warning: {warning}")

    return 1 if args.strict and result.errors else 0


if __name__ == "__main__":
    sys.exit(main())
