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

//! Architecture guards for the plan-IR layering arc (PIR-8).
//!
//! These tests mechanically enforce the PIR import and stage boundaries. Test
//! modules may still build optimizer trees as inputs; production code may not
//! leak optimizer physical types into planner/codegen main paths.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const NIDL_D3B_BASELINE_PATH: &str = "tests/proto_schema_baseline/novarocks_schema.json";
const NIDL_D3B_WRITE_BASELINE_ENV: &str = "NOVA_WRITE_PROTO_SCHEMA_BASELINE";
const NIDL_D3B_WRITE_BASELINE_COMMAND: &str = "NOVA_WRITE_PROTO_SCHEMA_BASELINE=1 cargo test --test architecture_guard nidl_d3b_current_schema_matches_baseline -- --nocapture";

fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn src_dir() -> PathBuf {
    Path::new(manifest_dir()).join("src")
}

fn rs_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(rs_files(&path));
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn rel(path: &Path) -> String {
    path.strip_prefix(manifest_dir())
        .unwrap_or(path)
        .display()
        .to_string()
}

fn brace_delta(line: &str) -> isize {
    line.chars().fold(0, |delta, ch| match ch {
        '{' => delta + 1,
        '}' => delta - 1,
        _ => delta,
    })
}

fn paren_delta(line: &str) -> isize {
    line.chars().fold(0, |delta, ch| match ch {
        '(' => delta + 1,
        ')' => delta - 1,
        _ => delta,
    })
}

fn is_comment_or_blank(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.is_empty()
        || trimmed.starts_with("//")
        || trimmed.starts_with("/*")
        || trimmed.starts_with('*')
}

fn non_comment_trimmed_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut in_block_comment = false;

    for line in text.lines() {
        let trimmed = line.trim();
        if in_block_comment {
            if trimmed.contains("*/") {
                in_block_comment = false;
            }
            continue;
        }

        if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with('*') {
            continue;
        }
        if trimmed.starts_with("/*") {
            if !trimmed.contains("*/") {
                in_block_comment = true;
            }
            continue;
        }

        lines.push(trimmed);
    }

    lines
}

fn has_non_comment_line(text: &str, needle: &str) -> bool {
    non_comment_trimmed_lines(text)
        .into_iter()
        .any(|line| line == needle)
}

fn has_cfg_test_mod_tests(text: &str) -> bool {
    non_comment_trimmed_lines(text)
        .windows(2)
        .any(|lines| lines == ["#[cfg(test)]", "mod tests;"])
}

fn is_cfg_test_attr(trimmed: &str) -> bool {
    if trimmed.starts_with("#[cfg(test") {
        return true;
    }
    compact_line(trimmed).starts_with("#[cfg(all(test,")
}

fn module_declarations(text: &str) -> BTreeSet<String> {
    non_comment_trimmed_lines(text)
        .into_iter()
        .filter_map(|line| {
            let declaration = line.strip_suffix(';')?;
            let module = declaration
                .strip_prefix("mod ")
                .or_else(|| declaration.strip_prefix("pub mod "))
                .or_else(|| declaration.strip_prefix("pub(crate) mod "))
                .or_else(|| declaration.strip_prefix("pub(super) mod "))?;
            if module
                .chars()
                .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
            {
                Some(module.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn has_module_declaration(text: &str, module: &str) -> bool {
    module_declarations(text).contains(module)
}

fn non_test_line_hits<F>(path: &Path, mut predicate: F) -> Vec<(usize, String)>
where
    F: FnMut(&str) -> bool,
{
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut hits = Vec::new();
    let mut pending_cfg_test = false;
    let mut test_depth = 0isize;

    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();

        if test_depth > 0 {
            test_depth += brace_delta(line);
            if test_depth < 0 {
                test_depth = 0;
            }
            continue;
        }

        if is_cfg_test_attr(trimmed) {
            pending_cfg_test = true;
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
                pending_cfg_test = false;
            }
            continue;
        }

        if pending_cfg_test {
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
            }
            pending_cfg_test = false;
            continue;
        }

        if !is_comment_or_blank(line) && predicate(line) {
            hits.push((idx + 1, line.trim().to_string()));
        }
    }

    hits
}

fn source_line_hits<F>(path: &Path, mut predicate: F) -> Vec<(usize, String)>
where
    F: FnMut(&str) -> bool,
{
    let text = fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            if !is_comment_or_blank(line) && predicate(line) {
                Some((idx + 1, line.trim().to_string()))
            } else {
                None
            }
        })
        .collect()
}

fn rust_production_text_without_cfg_test(text: &str) -> String {
    let mut production = String::with_capacity(text.len());
    let mut pending_cfg_test = false;
    let mut skipping_cfg_item = false;
    let mut skip_depth = 0isize;

    for line in text.lines() {
        let trimmed = line.trim_start();

        if skip_depth > 0 {
            skip_depth += brace_delta(line);
            if skip_depth <= 0 {
                skip_depth = 0;
                skipping_cfg_item = false;
            }
            continue;
        }

        if skipping_cfg_item {
            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
            } else if trimmed.ends_with(';') {
                skipping_cfg_item = false;
            }
            continue;
        }

        if pending_cfg_test {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                continue;
            }

            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
            } else if !trimmed.ends_with(';') {
                skipping_cfg_item = true;
            }
            pending_cfg_test = false;
            continue;
        }

        if is_cfg_test_attr(trimmed) {
            pending_cfg_test = true;
            continue;
        }

        production.push_str(line);
        production.push('\n');
    }

    production
}

fn is_cfg_test_or_compat_attr(trimmed: &str) -> bool {
    if is_cfg_test_attr(trimmed) {
        return true;
    }
    let compact = compact_line(trimmed);
    compact == "#[cfg(feature=\"compat\")]"
}

fn rust_production_text_without_cfg_test_or_compat(text: &str) -> String {
    let mut production = String::with_capacity(text.len());
    let mut pending_skip_attr = false;
    let mut skipping_cfg_item = false;
    let mut skip_depth = 0isize;
    let mut skip_paren_depth = 0isize;

    for line in text.lines() {
        let trimmed = line.trim_start();

        if skip_depth > 0 {
            skip_depth += brace_delta(line);
            if skip_depth <= 0 {
                skip_depth = 0;
                skipping_cfg_item = false;
                skip_paren_depth = 0;
            }
            production.push('\n');
            continue;
        }

        if skipping_cfg_item {
            skip_paren_depth += paren_delta(line);
            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
                skip_paren_depth = 0;
            } else if skip_paren_depth <= 0 && (trimmed.ends_with(';') || trimmed.ends_with(',')) {
                skipping_cfg_item = false;
                skip_paren_depth = 0;
            }
            production.push('\n');
            continue;
        }

        if pending_skip_attr {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                production.push('\n');
                continue;
            }

            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
                skip_paren_depth = 0;
            } else if !trimmed.ends_with(';') && !trimmed.ends_with(',') {
                skipping_cfg_item = true;
                skip_paren_depth = paren_delta(line).max(0);
            }
            pending_skip_attr = false;
            production.push('\n');
            continue;
        }

        if is_cfg_test_or_compat_attr(trimmed) {
            pending_skip_attr = true;
            production.push('\n');
            continue;
        }

        production.push_str(line);
        production.push('\n');
    }

    production
}

fn nidl_e9_rust_production_text_without_cfg_test(text: &str) -> String {
    let mut production = String::with_capacity(text.len());
    let mut pending_skip_attr = false;
    let mut skipping_cfg_item = false;
    let mut skip_depth = 0isize;
    let mut skip_paren_depth = 0isize;

    for line in text.lines() {
        let trimmed = line.trim_start();

        if skip_depth > 0 {
            skip_depth += brace_delta(line);
            if skip_depth <= 0 {
                skip_depth = 0;
                skipping_cfg_item = false;
                skip_paren_depth = 0;
            }
            production.push('\n');
            continue;
        }

        if skipping_cfg_item {
            skip_paren_depth += paren_delta(line);
            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
                skip_paren_depth = 0;
            } else if skip_paren_depth <= 0 && (trimmed.ends_with(';') || trimmed.ends_with(',')) {
                skipping_cfg_item = false;
                skip_paren_depth = 0;
            }
            production.push('\n');
            continue;
        }

        if pending_skip_attr {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                production.push('\n');
                continue;
            }

            let delta = brace_delta(line);
            if line.contains('{') {
                skip_depth = delta.max(0);
                skipping_cfg_item = skip_depth > 0;
                skip_paren_depth = 0;
            } else if !trimmed.ends_with(';') && !trimmed.ends_with(',') {
                skipping_cfg_item = true;
                skip_paren_depth = paren_delta(line).max(0);
            }
            pending_skip_attr = false;
            production.push('\n');
            continue;
        }

        if is_cfg_test_attr(trimmed) {
            pending_skip_attr = true;
            production.push('\n');
            continue;
        }

        production.push_str(line);
        production.push('\n');
    }

    production
}

fn nidl_e2_rust_text_without_cfg_test_or_compat(text: &str) -> String {
    rust_production_text_without_cfg_test_or_compat(text)
}

fn push_forbidden_terms(
    violations: &mut Vec<String>,
    source: &str,
    text: &str,
    terms: &[&str],
    reason: &str,
) {
    for term in terms {
        if let Some((line, text)) = text
            .lines()
            .enumerate()
            .find(|(_, line)| line.contains(term))
        {
            violations.push(format!(
                "{source}:{}: {reason}: `{term}` in `{}`",
                line + 1,
                text.trim()
            ));
        }
    }
}

#[test]
fn d3l_rust_production_text_without_cfg_test_removes_cfg_test_items() {
    let input = r#"
pub(crate) fn production() {
    let keep = "TDataSink";
}

#[cfg(test)]
mod tests {
    fn fixture() {
        let forbidden = "test-only TPlan)";
    }
}

pub(crate) fn production_after_tests() {
    let keep = "TPlan)";
}

#[cfg(test)]
fn test_helper() {
    let forbidden = "test-only TDataSink";
}

#[cfg(test)]
const TEST_ONLY: &str = "test-only find_scan_plan_nodes(";

pub(crate) fn production_tail() {
    let keep = "fragment_sink_is_terminal_write_sink";
}
"#;

    let production = rust_production_text_without_cfg_test(input);

    assert!(production.contains("pub(crate) fn production()"));
    assert!(production.contains("pub(crate) fn production_after_tests()"));
    assert!(production.contains("pub(crate) fn production_tail()"));
    assert!(!production.contains("test-only TPlan)"));
    assert!(!production.contains("test-only TDataSink"));
    assert!(!production.contains("test-only find_scan_plan_nodes("));
}

fn non_test_optimizer_refs(path: &Path) -> Vec<(usize, String)> {
    non_test_line_hits(path, |line| line.contains("crate::sql::optimizer::"))
}

fn test_dir() -> PathBuf {
    Path::new(manifest_dir()).join("tests")
}

fn source_and_test_rs_files() -> Vec<PathBuf> {
    let mut files = rs_files(&src_dir());
    files.extend(rs_files(&test_dir()));
    files
        .into_iter()
        .filter(|path| rel(path) != "tests/architecture_guard.rs")
        .collect()
}

#[test]
fn nidl_d3g_native_runtime_query_options_do_not_use_thrift_model() {
    let forbidden = [
        "src/runtime/runtime_state.rs",
        "src/cache/mod.rs",
        "src/exec/spill/query_options_wire.rs",
        "src/runtime/coordinator.rs",
        "src/runtime/native_fragment_wire.rs",
        "src/sql/codegen/proto_encode/instance.rs",
    ];
    let repo = Path::new(manifest_dir());
    for path in forbidden {
        let text = fs::read_to_string(repo.join(path)).expect(path);
        assert!(
            !text.contains("TQueryOptions") && !text.contains("internal_service::TQueryOptions"),
            "{path} must use runtime::query_options::QueryOptions, not thrift TQueryOptions"
        );
    }
}

#[test]
fn nidl_d3h_native_runtime_filter_params_do_not_use_thrift_model() {
    let repo = Path::new(manifest_dir());
    let guarded_files = [
        "src/runtime/runtime_state.rs",
        "src/runtime/query_context.rs",
        "src/runtime/native_fragment_wire.rs",
        "src/runtime/coordinator.rs",
        "src/sql/codegen/proto_encode/instance.rs",
        "src/lower/common/fragment_runtime.rs",
        "src/exec/operators/hashjoin/hash_join_build_sink.rs",
        "src/runtime/runtime_filter_worker.rs",
    ];
    let forbidden = [
        "TRuntimeFilterParams",
        "TRuntimeFilterProberParams",
        "runtime_filter::TRuntimeFilterParams",
        "runtime_filter::TRuntimeFilterProberParams",
    ];
    let mut violations = Vec::new();

    for rel_path in guarded_files {
        let path = repo.join(rel_path);
        for (line, text) in source_line_hits(&path, |line| {
            forbidden.iter().any(|symbol| line.contains(symbol))
        }) {
            violations.push(format!("{rel_path}:{line}: {text}"));
        }
    }

    assert!(
        violations.is_empty(),
        "native runtime filter params must use runtime::runtime_filter_params::RuntimeFilterParams, not thrift runtime filter models:\n{}",
        violations.join("\n")
    );
}

fn rs_files_under(relative_roots: &[&str]) -> Vec<PathBuf> {
    let repo = Path::new(manifest_dir());
    let mut files = Vec::new();
    for root in relative_roots {
        files.extend(rs_files(&repo.join(root)));
    }
    files
}

fn is_ident_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

#[derive(Clone, Copy)]
enum RustWirePolicy {
    StrictNoWire,
    StarRocksProtoOnly,
    StrictNoStarRocksWire,
    AllowNativeProto,
    PlannerPartitionBridge,
}

#[derive(Clone, Copy, Default)]
struct RustWireContext {
    in_crate_use_group: bool,
    in_proto_use_group: bool,
    in_thrift_use_group: bool,
}

fn compact_line(line: &str) -> String {
    line.chars().filter(|ch| !ch.is_whitespace()).collect()
}

fn first_ident(text: &str) -> Option<String> {
    let start = text.find(|ch| is_ident_char(ch))?;
    let tail = &text[start..];
    let end = tail.find(|ch| !is_ident_char(ch)).unwrap_or(tail.len());
    Some(tail[..end].to_string())
}

fn group_entry_modules(text: &str) -> Vec<String> {
    text.split(',')
        .filter_map(first_ident)
        .filter(|entry| !matches!(entry.as_str(), "use" | "crate" | "self" | "super"))
        .collect()
}

fn modules_after_needle(compact: &str, needle: &str) -> Vec<String> {
    let mut modules = Vec::new();
    let mut rest = compact;
    while let Some(pos) = rest.find(needle) {
        let after = &rest[pos + needle.len()..];
        if let Some(group) = after.strip_prefix('{') {
            let end = group.find('}').unwrap_or(group.len());
            modules.extend(group_entry_modules(&group[..end]));
        } else if let Some(module) = first_ident(after) {
            modules.push(module);
        }
        rest = &after[after.len().min(1)..];
    }
    modules
}

fn line_has_ident(line: &str, ident: &str) -> bool {
    line.match_indices(ident).any(|(idx, _)| {
        let before = line[..idx].chars().next_back();
        let after = line[idx + ident.len()..].chars().next();
        before.is_none_or(|ch| !is_ident_char(ch)) && after.is_none_or(|ch| !is_ident_char(ch))
    })
}

fn proto_reference_modules(line: &str, context: RustWireContext) -> Vec<String> {
    let compact = compact_line(line);
    let in_crate_group = context.in_crate_use_group || compact.contains("crate::{");
    let mut modules = modules_after_needle(&compact, "crate::proto::");
    modules.extend(modules_after_needle(&compact, "grpc_client::proto::"));
    modules.extend(modules_after_needle(
        &compact,
        "service::grpc_client::proto::",
    ));
    if in_crate_group {
        modules.extend(modules_after_needle(&compact, "proto::"));
        if line_has_ident(line, "proto") && !compact.contains("proto::") {
            modules.push("proto".to_string());
        }
    }
    if context.in_proto_use_group {
        modules.extend(group_entry_modules(&compact));
    }
    modules.sort();
    modules.dedup();
    modules
}

fn thrift_reference_modules(line: &str, context: RustWireContext) -> Vec<String> {
    let compact = compact_line(line);
    let in_crate_group = context.in_crate_use_group || compact.contains("crate::{");
    let mut modules = modules_after_needle(&compact, "crate::thrift::");
    if in_crate_group {
        modules.extend(modules_after_needle(&compact, "thrift::"));
        if line_has_ident(line, "thrift") && !compact.contains("thrift::") {
            modules.push("thrift".to_string());
        }
    }
    if context.in_thrift_use_group {
        modules.extend(group_entry_modules(&compact));
    }
    if compact.contains("crate::types::arrow_thrift")
        || (in_crate_group && compact.contains("types::arrow_thrift"))
    {
        modules.push("arrow_thrift".to_string());
    }
    modules.sort();
    modules.dedup();
    modules
}

fn contains_starrocks_proto_ref(line: &str) -> bool {
    proto_reference_modules(line, RustWireContext::default())
        .iter()
        .any(|module| module == "starrocks")
}

fn contains_staros_proto_ref(line: &str) -> bool {
    proto_reference_modules(line, RustWireContext::default())
        .iter()
        .any(|module| module == "staros")
}

fn contains_thrift_ref(line: &str) -> bool {
    !thrift_reference_modules(line, RustWireContext::default()).is_empty()
}

fn rust_wire_policy_violates_line(
    line: &str,
    context: RustWireContext,
    policy: RustWirePolicy,
) -> bool {
    let proto_modules = proto_reference_modules(line, context);
    let thrift_modules = thrift_reference_modules(line, context);
    let starrocks_proto = proto_modules.iter().any(|module| module == "starrocks");
    let staros_proto = proto_modules.iter().any(|module| module == "staros");
    let thrift = !thrift_modules.is_empty();

    match policy {
        RustWirePolicy::StrictNoWire => !proto_modules.is_empty() || thrift,
        RustWirePolicy::StarRocksProtoOnly => starrocks_proto || staros_proto,
        RustWirePolicy::StrictNoStarRocksWire | RustWirePolicy::AllowNativeProto => {
            starrocks_proto || staros_proto || thrift
        }
        RustWirePolicy::PlannerPartitionBridge => {
            starrocks_proto
                || staros_proto
                || (thrift && thrift_modules.iter().any(|module| module != "partitions"))
        }
    }
}

fn update_wire_group_depth(depth: &mut isize, line: &str) {
    if *depth > 0 {
        *depth += brace_delta(line);
        if *depth < 0 {
            *depth = 0;
        }
    }
}

fn start_wire_group_depth(depth: &mut isize, line: &str, needle: &str) {
    if *depth == 0 && compact_line(line).contains(needle) {
        *depth = brace_delta(line).max(0);
    }
}

fn rust_wire_reference_hits(path: &Path, policy: RustWirePolicy) -> Vec<(usize, String)> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut hits = Vec::new();
    let mut pending_cfg_test = false;
    let mut test_depth = 0isize;
    let mut crate_use_group_depth = 0isize;
    let mut proto_use_group_depth = 0isize;
    let mut thrift_use_group_depth = 0isize;

    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();

        if test_depth > 0 {
            test_depth += brace_delta(line);
            if test_depth < 0 {
                test_depth = 0;
            }
            continue;
        }

        if is_cfg_test_attr(trimmed) {
            pending_cfg_test = true;
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
                pending_cfg_test = false;
            }
            continue;
        }

        if pending_cfg_test {
            let delta = brace_delta(line);
            if delta > 0 {
                test_depth = delta;
            }
            pending_cfg_test = false;
            continue;
        }

        let context = RustWireContext {
            in_crate_use_group: crate_use_group_depth > 0,
            in_proto_use_group: proto_use_group_depth > 0,
            in_thrift_use_group: thrift_use_group_depth > 0,
        };
        if !is_comment_or_blank(line) && rust_wire_policy_violates_line(line, context, policy) {
            hits.push((idx + 1, line.trim().to_string()));
        }

        update_wire_group_depth(&mut crate_use_group_depth, line);
        update_wire_group_depth(&mut proto_use_group_depth, line);
        update_wire_group_depth(&mut thrift_use_group_depth, line);
        start_wire_group_depth(&mut crate_use_group_depth, line, "usecrate::{");
        start_wire_group_depth(&mut proto_use_group_depth, line, "usecrate::proto::{");
        start_wire_group_depth(&mut thrift_use_group_depth, line, "usecrate::thrift::{");
    }

    hits
}

fn proto_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(proto_files(&path));
            } else if path.extension().is_some_and(|ext| ext == "proto") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn proto_imports(path: &Path) -> Vec<(usize, String)> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut imports = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("import ") {
            let rest = rest
                .strip_prefix("public ")
                .or_else(|| rest.strip_prefix("weak "))
                .unwrap_or(rest);
            if let Some(rest) = rest.strip_prefix('"')
                && let Some((import, _)) = rest.split_once('"')
            {
                imports.push((idx + 1, import.to_string()));
            }
        }
    }
    imports
}

fn disallowed_novarocks_proto_imports(files: &[PathBuf]) -> Vec<String> {
    let allowed = [
        "common.proto",
        "expr.proto",
        "filter.proto",
        "plan.proto",
        "service.proto",
    ];
    let mut hits = Vec::new();
    for file in files {
        for (line, import) in proto_imports(file) {
            if !allowed.contains(&import.as_str()) {
                hits.push(format!("{}:{}: import \"{}\"", rel(file), line, import));
            }
        }
    }
    hits
}

fn named_let_array_lines<'a>(text: &'a str, name: &str) -> Option<Vec<(usize, &'a str)>> {
    let lines = text.lines().collect::<Vec<_>>();
    let start = lines
        .iter()
        .position(|line| line.contains(&format!("let {name} = [")))?;
    let mut block = Vec::new();
    for (idx, line) in lines.iter().enumerate().skip(start) {
        block.push((idx + 1, *line));
        if line.contains("];") {
            return Some(block);
        }
    }
    Some(block)
}

fn compile_protos_call_lines<'a>(
    text: &'a str,
    protos_name: &str,
) -> Option<Vec<(usize, &'a str)>> {
    let lines = text.lines().collect::<Vec<_>>();
    let start = lines
        .iter()
        .position(|line| compact_line(line).contains(&format!("compile_protos(&{protos_name}")))?;
    let mut call = Vec::new();
    for (idx, line) in lines.iter().enumerate().skip(start).take(12) {
        call.push((idx + 1, *line));
        if line.contains(';') || line.contains(".unwrap()") || line.contains(".context(") {
            break;
        }
    }
    Some(call)
}

fn contains_compat_proto_root(line: &str) -> bool {
    line.contains("COMPAT_PROTO_DIR")
        || line.contains("COMPAT_STAROS_DIR")
        || line.contains("compat/proto")
        || line.contains("compat/staros")
}

fn block_contains(lines: &[(usize, &str)], needle: &str) -> bool {
    lines.iter().any(|(_, line)| line.contains(needle))
}

fn native_proto_codegen_boundary_violations(build_rs: &Path) -> Vec<String> {
    let text = fs::read_to_string(build_rs).unwrap_or_default();
    let mut hits = Vec::new();
    let build_rel = rel(build_rs);

    if let Some(native_block) = named_let_array_lines(&text, "novarocks_protos") {
        for (line, text) in &native_block {
            if contains_compat_proto_root(text) {
                hits.push(format!(
                    "{build_rel}:{line}: novarocks_protos must not include compat proto dirs: {}",
                    text.trim()
                ));
            }
        }
    } else {
        hits.push(format!("{build_rel}:1: novarocks_protos block must exist"));
    }

    if let Some(native_call) = compile_protos_call_lines(&text, "novarocks_protos") {
        let mut call_has_compat_root = false;
        for (line, text) in &native_call {
            if contains_compat_proto_root(text) {
                call_has_compat_root = true;
                hits.push(format!(
                    "{build_rel}:{line}: native compile_protos include roots must stay NOVAROCKS_IDL_DIR only: {}",
                    text.trim()
                ));
            }
        }
        let compact_call = native_call
            .iter()
            .map(|(_, line)| compact_line(line))
            .collect::<String>();
        if !call_has_compat_root
            && !compact_call.contains("compile_protos(&novarocks_protos,&[NOVAROCKS_IDL_DIR])")
        {
            let line = native_call.first().map(|(line, _)| *line).unwrap_or(1);
            hits.push(format!(
                "{build_rel}:{line}: native compile_protos include roots must be &[NOVAROCKS_IDL_DIR]"
            ));
        }
    } else {
        hits.push(format!(
            "{build_rel}:1: native compile_protos call for novarocks_protos must exist"
        ));
    }

    if let Some(starrocks_block) = named_let_array_lines(&text, "starrocks_protos") {
        if !block_contains(&starrocks_block, "COMPAT_PROTO_DIR") {
            let line = starrocks_block.first().map(|(line, _)| *line).unwrap_or(1);
            hits.push(format!(
                "{build_rel}:{line}: starrocks_protos must explicitly use COMPAT_PROTO_DIR"
            ));
        }
    } else {
        hits.push(format!("{build_rel}:1: starrocks_protos block must exist"));
    }

    if let Some(staros_block) = named_let_array_lines(&text, "staros_protos") {
        if !block_contains(&staros_block, "COMPAT_STAROS_DIR") {
            let line = staros_block.first().map(|(line, _)| *line).unwrap_or(1);
            hits.push(format!(
                "{build_rel}:{line}: staros_protos must explicitly use COMPAT_STAROS_DIR"
            ));
        }
    } else {
        hits.push(format!("{build_rel}:1: staros_protos block must exist"));
    }
    hits
}

#[test]
fn nidl_d2c_detector_flags_proto_build_and_rust_wire_violations() {
    let tmp_dir = std::env::temp_dir().join(format!(
        "nidl_d2c_guard_probe_{}_{}",
        std::process::id(),
        "wire_refs"
    ));
    fs::create_dir_all(&tmp_dir).unwrap();

    let proto = tmp_dir.join("service.proto");
    fs::write(
        &proto,
        concat!(
            "syntax = \"proto3\";\n",
            "import \"common.proto\";\n",
            "import \"../compat/proto/internal_service.proto\";\n",
            "import \"staros/starlet.proto\";\n",
            "import public \"../compat/proto/public.proto\";\n",
            "import weak \"staros/weak.proto\";\n",
        ),
    )
    .unwrap();
    let proto_hits = disallowed_novarocks_proto_imports(&[proto.clone()]);
    assert_eq!(proto_hits.len(), 4, "{proto_hits:?}");

    let build_rs = tmp_dir.join("build.rs");
    fs::write(
        &build_rs,
        concat!(
            "let novarocks_protos = [idl_path(NOVAROCKS_IDL_DIR, \"service.proto\"), idl_path(COMPAT_PROTO_DIR, \"internal_service.proto\")];\n",
            "tonic_build::configure().compile_protos(&novarocks_protos, &[NOVAROCKS_IDL_DIR, COMPAT_PROTO_DIR]).unwrap();\n",
            "let starrocks_protos = [idl_path(COMPAT_PROTO_DIR, \"internal_service.proto\")];\n",
            "tonic_build::configure().compile_protos(&starrocks_protos, &[COMPAT_PROTO_DIR]).unwrap();\n",
            "let staros_protos = [idl_path(COMPAT_STAROS_DIR, \"starlet.proto\")];\n",
            "tonic_build::configure().compile_protos(&staros_protos, &[COMPAT_STAROS_DIR]).unwrap();\n",
        ),
    )
    .unwrap();
    let build_hits = native_proto_codegen_boundary_violations(&build_rs);
    assert_eq!(build_hits.len(), 2, "{build_hits:?}");
    assert!(
        build_hits.iter().all(|hit| hit.contains("build.rs:")),
        "{build_hits:?}"
    );

    let rust = tmp_dir.join("planner.rs");
    fs::write(
        &rust,
        concat!(
            "use crate::proto::starrocks::PPlanFragment;\n",
            "use crate::proto::staros::StarStatus;\n",
            "use crate::thrift::types;\n",
            "use crate::thrift::partitions;\n",
            "use crate::{runtime, thrift::types};\n",
            "use crate::thrift::partitions; use crate::thrift::exprs;\n",
            "use crate::service::grpc_client::proto::starrocks::PPlanFragment;\n",
        ),
    )
    .unwrap();
    let strict_hits = rust_wire_reference_hits(&rust, RustWirePolicy::StrictNoStarRocksWire);
    assert_eq!(strict_hits.len(), 7, "{strict_hits:?}");
    let planner_hits = rust_wire_reference_hits(&rust, RustWirePolicy::PlannerPartitionBridge);
    assert_eq!(planner_hits.len(), 6, "{planner_hits:?}");
    assert!(contains_starrocks_proto_ref(
        "use crate::proto::{starrocks};"
    ));
    assert!(contains_staros_proto_ref("use crate::proto::{staros};"));
    assert!(contains_thrift_ref("use crate::{runtime, thrift::types};"));

    let common = tmp_dir.join("common.rs");
    fs::write(
        &common,
        concat!(
            "use crate::{runtime, proto::plan};\n",
            "use crate::proto::{common, plan};\n",
            "use crate::service::grpc_client::proto::starrocks::PPlanFragment;\n",
            "use crate::{\n",
            "    runtime,\n",
            "    thrift::types,\n",
            "};\n",
        ),
    )
    .unwrap();
    let common_hits = rust_wire_reference_hits(&common, RustWirePolicy::StrictNoWire);
    assert_eq!(common_hits.len(), 4, "{common_hits:?}");
    let proto_only_hits = rust_wire_reference_hits(&common, RustWirePolicy::StarRocksProtoOnly);
    assert_eq!(proto_only_hits.len(), 1, "{proto_only_hits:?}");

    fs::remove_dir_all(&tmp_dir).ok();
}

#[test]
fn nidl_d2c_novarocks_proto_imports_stay_native_only() {
    let files = proto_files(&Path::new(manifest_dir()).join("idl/novarocks"));
    let violations = disallowed_novarocks_proto_imports(&files);
    assert!(
        violations.is_empty(),
        "idl/novarocks proto files must import only native proto files:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d2c_native_proto_codegen_root_excludes_compat_idl() {
    let build_rs = Path::new(manifest_dir()).join("src/build.rs");
    let violations = native_proto_codegen_boundary_violations(&build_rs);
    assert!(
        violations.is_empty(),
        "native proto codegen must stay rooted at idl/novarocks, with StarRocks protos generated explicitly:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d2c_rust_wire_imports_stay_inside_owned_boundaries() {
    let mut violations = Vec::new();

    for file in rs_files_under(&["src/sql/analyzer", "src/sql/optimizer"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::StrictNoStarRocksWire) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/sql/planner"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::PlannerPartitionBridge)
        {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/sql/codegen/proto_encode"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::StarRocksProtoOnly) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/lower/novarocks"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::AllowNativeProto) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    for file in rs_files_under(&["src/lower/common"]) {
        for (line, text) in rust_wire_reference_hits(&file, RustWirePolicy::StrictNoWire) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "D2C Rust wire imports crossed native/planner/lowering ownership boundaries:\n{}",
        violations.join("\n")
    );
}

#[test]
fn detector_flags_non_test_and_skips_cfg_test_blocks() {
    let tmp = std::env::temp_dir().join(format!(
        "pir8_guard_probe_{}_{}.rs",
        std::process::id(),
        "optimizer_refs"
    ));
    fs::write(
        &tmp,
        "\
use crate::sql::optimizer::operator::AggMode;
#[cfg(test)]
mod tests {
    use crate::sql::optimizer::operator::TopNPhase;
    fn fixture() { let _ = crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(); }
}
fn prod() { let _ = crate::sql::optimizer::property::DistributionSpec::Any; }
",
    )
    .unwrap();
    let hits = non_test_optimizer_refs(&tmp);
    fs::remove_file(&tmp).ok();

    assert_eq!(
        hits,
        vec![
            (
                1,
                "use crate::sql::optimizer::operator::AggMode;".to_string()
            ),
            (
                7,
                "fn prod() { let _ = crate::sql::optimizer::property::DistributionSpec::Any; }"
                    .to_string()
            ),
        ]
    );
}

#[test]
fn nidl_d3a_detector_ignores_commented_module_declarations() {
    let commented = "\
// mod proto_contract;
/*
mod proto_contract;
*/
/*
#[cfg(test)]
mod tests;
*/
";
    assert!(!has_non_comment_line(commented, "mod proto_contract;"));
    assert!(!has_cfg_test_mod_tests(commented));
    assert!(module_declarations(commented).is_empty());

    let active = "\
#[cfg(test)]
// comment between attribute and module
mod tests;
mod proto_contract;
pub(crate) mod chunk;
";
    assert!(has_cfg_test_mod_tests(active));
    assert!(has_non_comment_line(active, "mod proto_contract;"));
    assert_eq!(
        module_declarations(active),
        BTreeSet::from([
            "chunk".to_string(),
            "proto_contract".to_string(),
            "tests".to_string()
        ])
    );
}

#[test]
fn planner_distributed_and_codegen_do_not_import_optimizer() {
    let mut checked = vec![
        src_dir().join("sql/planner/plan.rs"),
        src_dir().join("sql/planner/distributed_fragment.rs"),
        src_dir().join("sql/planner/distributed_node.rs"),
        src_dir().join("sql/planner/distributed_plan_build.rs"),
    ];
    checked.extend(rs_files(&src_dir().join("sql/codegen")));

    let mut violations = Vec::new();
    for file in &checked {
        for (line, text) in non_test_optimizer_refs(file) {
            violations.push(format!("{}:{}: {}", rel(file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "planner distributed/codegen production paths must not reference optimizer types; \
         optimizer_bridge/** is the conversion boundary. Violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn optimizer_bridge_is_the_only_allowlisted_converter() {
    let bridge = src_dir().join("sql/planner/optimizer_bridge/physical.rs");
    assert!(bridge.exists(), "Bridge 2a must exist at {}", rel(&bridge));
    let text = fs::read_to_string(&bridge).unwrap();
    assert!(
        text.contains("crate::sql::optimizer"),
        "Bridge 2a should be the explicit optimizer-to-planner conversion boundary"
    );
}

#[test]
fn engine_has_no_direct_exec_resurrection() {
    let forbidden = [
        "collapse_distribution_enforcers_for_single_fragment",
        "DirectExecutionReason",
        "execute_query_direct_for_explicit_exception",
        "single_fragment_plan",
    ];
    let mut violations = Vec::new();

    for file in rs_files(&src_dir().join("engine")) {
        for symbol in forbidden {
            for (line, text) in non_test_line_hits(&file, |line| line.contains(symbol)) {
                violations.push(format!(
                    "{}:{}: forbidden direct-exec symbol `{}` in `{}`",
                    rel(&file),
                    line,
                    symbol,
                    text
                ));
            }
        }

        let rel_path = rel(&file);
        let optimizer_physical_allowlist = [
            "src/engine/query_stats.rs",
            "src/engine/dml_change_stream.rs",
            "src/engine/iceberg_change_stream_write.rs",
            "src/engine/mod.rs",
            "src/engine/mutation_flow.rs",
            "src/engine/mv/iceberg_refresh.rs",
        ];
        if !optimizer_physical_allowlist.contains(&rel_path.as_str()) {
            for (line, text) in non_test_line_hits(&file, |line| {
                line.contains("crate::sql::optimizer::physical_tree")
                    || line.contains("OptimizerPhysicalNode")
            }) {
                violations.push(format!(
                    "{}:{}: engine must not consume optimizer physical tree: {}",
                    rel(&file),
                    line,
                    text
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "engine direct-exec / optimizer-physical guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn stage_validation_guard_stays_deleted() {
    let mut violations = Vec::new();
    for file in rs_files(&src_dir().join("sql/planner")) {
        for (line, text) in non_test_line_hits(&file, |line| {
            line.contains("validate_logical_plan_stage")
                || line.contains("validate_physical_plan_stage")
        }) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "stage validation helpers must stay deleted; use type-level stage separation:\n{}",
        violations.join("\n")
    );
}

#[test]
fn build_distributed_plan_signature_is_planner_typed() {
    let path = src_dir().join("sql/planner/distributed_plan_build.rs");
    let text = fs::read_to_string(&path).unwrap();
    let sig = text
        .lines()
        .find(|line| line.contains("fn build_distributed_plan("))
        .expect("build_distributed_plan must exist");

    assert!(
        sig.contains("&PhysicalPlanNode") && !sig.contains("optimizer"),
        "build_distributed_plan must accept planner &PhysicalPlanNode, not optimizer types: {sig}"
    );
}

#[test]
fn distributed_plan_node_has_no_optimizer_payloads() {
    let file = src_dir().join("sql/planner/distributed_node.rs");
    let violations = non_test_optimizer_refs(&file)
        .into_iter()
        .map(|(line, text)| format!("{}:{}: {}", rel(&file), line, text))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "DistributedPlanNode must not contain optimizer paths:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d1_pure_mode_gates_starrocks_compat_behavior() {
    let repo = Path::new(manifest_dir());
    let service_mod = fs::read_to_string(repo.join("src/service/mod.rs")).unwrap();
    for module in [
        "backend_service",
        "heartbeat_service",
        "internal_service",
        "stream_load",
        "stream_load_http",
    ] {
        let expected = format!("#[cfg(feature = \"compat\")]\npub mod {module};");
        assert!(
            service_mod.contains(&expected),
            "service module `{module}` must be compat-gated"
        );
    }

    let grpc = fs::read_to_string(repo.join("src/service/grpc_server.rs")).unwrap();
    assert!(
        grpc.contains("#[cfg(feature = \"compat\")]\nfn build_novarocks_http_app"),
        "stream-load HTTP routes must only exist in compat grpc app"
    );
    assert!(
        grpc.contains(
            "#[cfg(feature = \"compat\")]\n#[derive(Default)]\npub struct StarletGrpcService"
        ),
        "Starlet gRPC service must be compat-gated"
    );
    assert!(
        grpc.contains("SubmitFragmentRequest requires native plan and instance_params"),
        "NovaRocksGrpc SubmitFragment must require native plan and instance_params"
    );
    assert!(
        !grpc.contains("exec_plan_fragment_params_thrift"),
        "NovaRocksGrpc SubmitFragment must not retain thrift fallback payloads"
    );
}

#[test]
fn nidl_d2d_lowering_root_exposes_named_ownership_modules() {
    let repo = Path::new(manifest_dir());
    assert!(
        !repo.join(concat!("src/lower", "_native")).exists(),
        concat!(
            "src/lower",
            "_native must be deleted; native lowering lives under src/lower/novarocks"
        )
    );
    for dir in [
        "src/lower/common",
        "src/lower/compat",
        "src/lower/novarocks",
    ] {
        assert!(repo.join(dir).is_dir(), "{dir} must exist");
    }

    let lower_mod = fs::read_to_string(repo.join("src/lower/mod.rs")).unwrap();
    for expected in [
        "pub(crate) mod common;",
        "pub(crate) mod compat;",
        "pub(crate) mod novarocks;",
    ] {
        assert!(
            lower_mod.contains(expected),
            "src/lower/mod.rs must contain `{expected}`"
        );
    }
    for forbidden in [
        "pub(crate) mod expr;",
        "pub(crate) mod fragment;",
        "pub(crate) mod layout;",
        "pub(crate) mod node;",
        "pub(crate) mod sink;",
        "pub(crate) mod type_lowering;",
        "mod thrift",
        "pub(crate) mod thrift",
    ] {
        assert!(
            !lower_mod.contains(forbidden),
            "src/lower/mod.rs must not keep legacy direct module `{forbidden}`"
        );
    }
}

#[test]
fn nidl_d2d_legacy_lowering_paths_do_not_remain() {
    let forbidden = [
        concat!("crate::", "lower", "_native"),
        concat!("lower", "::thrift"),
        concat!("crate::lower", "::fragment"),
        concat!("crate::lower", "::expr"),
        concat!("crate::lower", "::layout"),
        concat!("crate::lower", "::node"),
        concat!("crate::lower", "::sink"),
        concat!("crate::lower", "::type_lowering"),
    ];

    let mut violations = Vec::new();
    for file in source_and_test_rs_files() {
        for needle in forbidden {
            for (line, text) in source_line_hits(&file, |line| line.contains(needle)) {
                violations.push(format!("{}:{}: {}", rel(&file), line, text));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "D2D lowering paths must use crate::lower::compat, crate::lower::novarocks, or crate::lower::common:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_compat_boundary_names_use_compat_spelling() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let old_lower_dir = concat!("src/lower/", "compact");
    if repo.join(old_lower_dir).exists() {
        violations.push(format!(
            "{old_lower_dir}: compat lowering directory must use compat spelling"
        ));
    }
    let lower_mod = fs::read_to_string(repo.join("src/lower/mod.rs")).unwrap();
    if lower_mod.contains(concat!("pub(crate) mod ", "compact")) {
        violations
            .push("src/lower/mod.rs: compat lowering module must use compat spelling".to_string());
    }

    let forbidden_terms = [
        concat!("lower::", "compact"),
        concat!("src/lower/", "compact"),
        concat!("compact", "_output_partition"),
        concat!("compact", "_exec_params_from_parts"),
        concat!("compact", "_destination_from_runtime"),
        concat!("to_", "compact", "_exec_params"),
        concat!("compact", "_scan_ranges"),
        concat!("compact", "_scan_range_for_test"),
        concat!("compact", "_scan_ranges_for_placement"),
        concat!("Compact", "CteConsumer"),
        concat!("compact", "_cte"),
        concat!("compact", "_consumers"),
        concat!("compact", "_query_options"),
        concat!("compact", "_ranges"),
        concat!("compact", "_boundary"),
        concat!("compact", "_projection"),
        concat!("compact", "_only"),
        concat!("compact", " projection"),
        concat!("compact", " marker"),
    ];

    for file in source_and_test_rs_files() {
        if rel(&file) == "tests/architecture_guard.rs" {
            continue;
        }
        let text = fs::read_to_string(&file).unwrap();
        for term in forbidden_terms {
            for (line_no, line) in text.lines().enumerate() {
                if line.contains(term) {
                    violations.push(format!(
                        "{}:{}: compat boundary typo `{term}`",
                        rel(&file),
                        line_no + 1
                    ));
                }
            }
        }
    }

    for doc in ["AGENTS.md"] {
        let path = repo.join(doc);
        let text = fs::read_to_string(&path).unwrap();
        for term in forbidden_terms {
            for (line_no, line) in text.lines().enumerate() {
                if line.contains(term) {
                    violations.push(format!(
                        "{doc}:{}: compat boundary typo `{term}`",
                        line_no + 1
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "compat boundary names must use compat spelling, not compact:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d2d_common_lowering_has_no_wire_dependencies() {
    let common_dir = src_dir().join("lower/common");
    let forbidden = [
        "native_fragment_wire",
        "crate::thrift",
        "crate::proto",
        "thrift::",
        "proto::",
    ];

    let mut violations = Vec::new();
    for file in rs_files(&common_dir) {
        for needle in forbidden {
            for (line, text) in source_line_hits(&file, |line| line.contains(needle)) {
                violations.push(format!("{}:{}: {}", rel(&file), line, text));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "src/lower/common must stay protocol-neutral and must not depend on thrift/proto/native wire adapters:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3a_crate_internal_tests_live_under_src_tests() {
    let repo = Path::new(manifest_dir());
    let proto_contract_dir = repo.join("src/tests/proto_contract");
    let testutil_dir = repo.join("src/tests/testutil");
    let mut violations = Vec::new();
    if repo.join("src/proto_contract").exists() {
        violations.push(
            "src/proto_contract must not be a top-level src module; move it to src/tests/proto_contract"
                .to_string(),
        );
    }
    if repo.join("src/testutil").exists() {
        violations.push(
            "src/testutil must not be a top-level src module; move it to src/tests/testutil"
                .to_string(),
        );
    }
    if !repo.join("src/tests/mod.rs").is_file() {
        violations
            .push("src/tests/mod.rs must own crate-internal white-box test suites".to_string());
    }
    if !repo.join("src/tests/proto_contract/mod.rs").is_file() {
        violations.push(
            "src/tests/proto_contract/mod.rs must own native proto contract tests".to_string(),
        );
    }
    if !testutil_dir.join("mod.rs").is_file() {
        violations.push("src/tests/testutil/mod.rs must own test utility modules".to_string());
    }
    if !testutil_dir.join("chunk.rs").is_file() {
        violations
            .push("chunk test utilities must live at src/tests/testutil/chunk.rs".to_string());
    }

    for file in [
        "common.rs",
        "expr.rs",
        "filter.rs",
        "instance_params.rs",
        "plan.rs",
        "report.rs",
        "service.rs",
    ] {
        let path = proto_contract_dir.join(file);
        if !path.is_file() {
            violations.push(format!(
                "native proto contract test file must live at {}",
                rel(&path)
            ));
        }
    }

    let lib = fs::read_to_string(repo.join("src/lib.rs")).unwrap();
    if !has_cfg_test_mod_tests(&lib) {
        violations.push(
            "src/lib.rs must mount crate-internal white-box tests through #[cfg(test)] mod tests"
                .to_string(),
        );
    }
    if has_module_declaration(&lib, "proto_contract") {
        violations.push("src/lib.rs must not keep the legacy proto_contract module".to_string());
    }
    if has_module_declaration(&lib, "testutil") {
        violations.push("src/lib.rs must not keep the legacy testutil module".to_string());
    }

    if let Ok(root_mod) = fs::read_to_string(repo.join("src/tests/mod.rs")) {
        if !has_module_declaration(&root_mod, "proto_contract") {
            violations.push("src/tests/mod.rs must mount the proto contract suite".to_string());
        }
        if !has_module_declaration(&root_mod, "testutil") {
            violations.push("src/tests/mod.rs must mount test utility modules".to_string());
        }
    }

    if let Ok(testutil_mod) = fs::read_to_string(testutil_dir.join("mod.rs")) {
        if !has_module_declaration(&testutil_mod, "chunk") {
            violations
                .push("src/tests/testutil/mod.rs must mount chunk test utilities".to_string());
        }
    }

    if let Ok(proto_mod) = fs::read_to_string(proto_contract_dir.join("mod.rs")) {
        let declared_modules = module_declarations(&proto_mod);
        let mut file_modules = BTreeSet::new();
        if let Ok(entries) = fs::read_dir(&proto_contract_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "rs")
                    && path.file_name().and_then(|name| name.to_str()) != Some("mod.rs")
                {
                    if let Some(module) = path.file_stem().and_then(|stem| stem.to_str()) {
                        file_modules.insert(module.to_string());
                    }
                }
            }
        }

        for module in &file_modules {
            if !declared_modules.contains(module) {
                violations.push(format!(
                    "src/tests/proto_contract/mod.rs must declare `mod {module};`"
                ));
            }
        }
        for module in &declared_modules {
            if !file_modules.contains(module) {
                violations.push(format!(
                    "src/tests/proto_contract/mod.rs declares `{module}`, but src/tests/proto_contract/{module}.rs is missing"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "proto contract test layout guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3a_test_contract_modules_do_not_leak_into_production_code() {
    let mut violations = Vec::new();
    for file in rs_files(&src_dir()) {
        let rel_path = rel(&file);
        if rel_path == "src/lib.rs" || rel_path.starts_with("src/tests/") {
            continue;
        }

        for (line, text) in non_test_line_hits(&file, |line| {
            line.contains("crate::tests") || line.contains("proto_contract")
        }) {
            violations.push(format!("{}:{}: {}", rel_path, line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "test-only contract modules must not be referenced by production code:\n{}",
        violations.join("\n")
    );
}

#[test]
fn distributed_build_does_not_call_optimizer_cost_model() {
    let file = src_dir().join("sql/planner/distributed_plan_build.rs");
    let mut violations = Vec::new();
    for needle in ["compute_cost_estimate", "broadcast_decision("] {
        for (line, text) in non_test_line_hits(&file, |line| line.contains(needle)) {
            violations.push(format!("{}:{}: {}", rel(&file), line, text));
        }
    }

    assert!(
        violations.is_empty(),
        "distributed build must not call optimizer cost model:\n{}",
        violations.join("\n")
    );
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoSchema {
    version: u32,
    files: BTreeMap<String, ProtoFileSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoFileSchema {
    package: String,
    messages: BTreeMap<String, ProtoMessageSchema>,
    enums: BTreeMap<String, ProtoEnumSchema>,
    services: BTreeMap<String, ProtoServiceSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoMessageSchema {
    fields: BTreeMap<u32, ProtoFieldSchema>,
    reserved_numbers: BTreeSet<u32>,
    reserved_names: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoFieldSchema {
    number: u32,
    name: String,
    type_name: String,
    label: String,
    oneof: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoEnumSchema {
    values: Vec<ProtoEnumValueSchema>,
    reserved_numbers: BTreeSet<u32>,
    reserved_names: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoEnumValueSchema {
    number: i32,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoServiceSchema {
    rpcs: BTreeMap<String, ProtoRpcSchema>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ProtoRpcSchema {
    request: String,
    response: String,
    client_streaming: bool,
    server_streaming: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProtoParseContext {
    Message(String),
    Enum(String),
    Service(String),
    Oneof(String),
}

fn proto_context_label(context: &ProtoParseContext) -> String {
    match context {
        ProtoParseContext::Message(name) => format!("message {name}"),
        ProtoParseContext::Enum(name) => format!("enum {name}"),
        ProtoParseContext::Service(name) => format!("service {name}"),
        ProtoParseContext::Oneof(name) => format!("oneof {name}"),
    }
}

fn proto_parse_error(path: &str, statement: &str, detail: impl Into<String>) -> String {
    format!(
        "{}: failed to parse `{}`: {}",
        path,
        statement.trim(),
        detail.into()
    )
}

fn remove_proto_comments(path: &str, input: &str) -> Result<String, String> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut in_block_comment = false;

    while idx < chars.len() {
        let ch = chars[idx];
        let next = chars.get(idx + 1).copied();

        if in_block_comment {
            if ch == '*' && next == Some('/') {
                out.push(' ');
                out.push(' ');
                idx += 2;
                in_block_comment = false;
            } else {
                out.push(if ch == '\n' { '\n' } else { ' ' });
                idx += 1;
            }
            continue;
        }

        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            idx += 1;
            continue;
        }

        if ch == '"' {
            in_string = true;
            out.push(ch);
            idx += 1;
        } else if ch == '/' && next == Some('/') {
            while idx < chars.len() && chars[idx] != '\n' {
                out.push(' ');
                idx += 1;
            }
        } else if ch == '/' && next == Some('*') {
            out.push(' ');
            out.push(' ');
            idx += 2;
            in_block_comment = true;
        } else {
            out.push(ch);
            idx += 1;
        }
    }

    if in_block_comment {
        Err(format!(
            "{path}: failed to parse comment: unterminated block comment"
        ))
    } else {
        Ok(out)
    }
}

fn normalize_proto_statement(statement: &str) -> String {
    let mut out = String::new();
    let mut in_string = false;
    let mut escaped = false;
    let mut pending_space = false;

    for ch in statement.chars() {
        if in_string {
            if pending_space && !out.is_empty() {
                out.push(' ');
                pending_space = false;
            }
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            if pending_space && !out.is_empty() {
                out.push(' ');
            }
            pending_space = false;
            in_string = true;
            out.push(ch);
        } else if ch.is_whitespace() {
            pending_space = true;
        } else {
            if pending_space && !out.is_empty() {
                out.push(' ');
            }
            pending_space = false;
            out.push(ch);
        }
    }

    out.trim().to_string()
}

fn proto_logical_statements(input: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut escaped = false;

    for ch in input.chars() {
        current.push(ch);

        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
        } else if matches!(ch, ';' | '{' | '}') {
            let statement = normalize_proto_statement(&current);
            if !statement.is_empty() {
                statements.push(statement);
            }
            current.clear();
        }
    }

    let trailing = normalize_proto_statement(&current);
    if !trailing.is_empty() {
        statements.push(trailing);
    }

    statements
}

fn proto_statement_body<'a>(statement: &'a str, suffix: &str) -> Option<&'a str> {
    statement.trim().strip_suffix(suffix).map(str::trim)
}

fn proto_keyword_tail<'a>(statement: &'a str, keyword: &str) -> Option<&'a str> {
    let tail = statement.trim().strip_prefix(keyword)?;
    if tail
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        None
    } else {
        Some(tail.trim_start())
    }
}

fn is_proto_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_proto_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn is_proto_ident(value: &str) -> bool {
    let mut chars = value.chars();
    chars.next().is_some_and(is_proto_ident_start) && chars.all(is_proto_ident_continue)
}

fn parse_proto_named_block(path: &str, statement: &str, keyword: &str) -> Result<String, String> {
    let body = proto_statement_body(statement, "{")
        .ok_or_else(|| proto_parse_error(path, statement, "expected block opener"))?;
    let name = proto_keyword_tail(body, keyword)
        .ok_or_else(|| proto_parse_error(path, statement, format!("expected `{keyword}`")))?;
    if !is_proto_ident(name) {
        Err(proto_parse_error(
            path,
            statement,
            format!("invalid {keyword} name `{name}`"),
        ))
    } else {
        Ok(name.to_string())
    }
}

fn parse_proto_package(path: &str, statement: &str) -> Result<String, String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected package terminator"))?;
    let package = proto_keyword_tail(body, "package")
        .ok_or_else(|| proto_parse_error(path, statement, "expected package name"))?;
    if package.is_empty()
        || !package
            .chars()
            .all(|ch| ch == '.' || ch == '_' || ch.is_ascii_alphanumeric())
    {
        Err(proto_parse_error(
            path,
            statement,
            format!("invalid package name `{package}`"),
        ))
    } else {
        Ok(package.to_string())
    }
}

fn parse_proto_syntax(path: &str, statement: &str) -> Result<(), String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected syntax terminator"))?;
    let body = proto_keyword_tail(body, "syntax")
        .ok_or_else(|| proto_parse_error(path, statement, "expected syntax declaration"))?;
    let (left, right) = proto_split_once_top_level(body, '=')
        .ok_or_else(|| proto_parse_error(path, statement, "expected syntax assignment"))?;
    if !left.trim().is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unexpected syntax assignment prefix `{}`", left.trim()),
        ));
    }

    let value = right.trim();
    if !(value.starts_with('"') && value.ends_with('"') && value.len() >= 2) {
        return Err(proto_parse_error(
            path,
            statement,
            format!("invalid syntax literal `{value}`"),
        ));
    }
    let syntax = &value[1..value.len() - 1];
    if syntax != "proto3" {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unsupported syntax `{syntax}`; expected `proto3`"),
        ));
    }

    Ok(())
}

fn current_proto_path(stack: &[String], name: &str) -> String {
    if stack.is_empty() {
        name.to_string()
    } else {
        format!("{}.{}", stack.join("."), name)
    }
}

fn truncate_proto_field_options(statement: &str) -> &str {
    let mut angle_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in statement.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '<' => angle_depth += 1,
            '>' => angle_depth = angle_depth.saturating_sub(1),
            '[' if angle_depth == 0 => return &statement[..idx],
            _ => {}
        }
    }

    statement
}

fn proto_split_once_top_level<'a>(input: &'a str, delimiter: char) -> Option<(&'a str, &'a str)> {
    let mut angle_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '<' => angle_depth += 1,
            '>' => angle_depth = angle_depth.saturating_sub(1),
            ch if ch == delimiter && angle_depth == 0 => {
                return Some((&input[..idx], &input[idx + ch.len_utf8()..]));
            }
            _ => {}
        }
    }

    None
}

fn parse_proto_u32(path: &str, statement: &str, value: &str) -> Result<u32, String> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|err| proto_parse_error(path, statement, format!("invalid field number: {err}")))
}

fn parse_proto_i32(path: &str, statement: &str, value: &str) -> Result<i32, String> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|err| proto_parse_error(path, statement, format!("invalid enum value: {err}")))
}

fn proto_take_label(input: &str) -> (&'static str, &str) {
    for label in ["optional", "repeated"] {
        if let Some(tail) = proto_keyword_tail(input, label) {
            return (label, tail);
        }
    }
    ("singular", input.trim())
}

fn proto_split_type_and_name<'a>(
    path: &str,
    statement: &str,
    input: &'a str,
) -> Result<(&'a str, &'a str), String> {
    let input = input.trim();
    let Some(name_start) = input
        .char_indices()
        .rev()
        .find_map(|(idx, ch)| ch.is_whitespace().then_some(idx + ch.len_utf8()))
    else {
        return Err(proto_parse_error(
            path,
            statement,
            "expected `<type> <name>` before `=`",
        ));
    };

    let type_name = input[..name_start].trim();
    let name = input[name_start..].trim();
    if type_name.is_empty() || !is_proto_ident(name) {
        Err(proto_parse_error(
            path,
            statement,
            "expected valid field type and name",
        ))
    } else {
        Ok((type_name, name))
    }
}

fn parse_proto_field(
    path: &str,
    statement: &str,
    oneof: Option<&str>,
) -> Result<ProtoFieldSchema, String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected field terminator"))?;
    let body = truncate_proto_field_options(body).trim();
    let (left, right) = proto_split_once_top_level(body, '=')
        .ok_or_else(|| proto_parse_error(path, statement, "expected field number"))?;
    let (number_text, tail) = proto_take_first_token(right)
        .ok_or_else(|| proto_parse_error(path, statement, "missing field number"))?;
    if !tail.is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unexpected field number suffix `{tail}`"),
        ));
    }
    let number = parse_proto_u32(path, statement, number_text)?;
    let (label, left) = proto_take_label(left);
    let (type_name, name) = proto_split_type_and_name(path, statement, left)?;

    Ok(ProtoFieldSchema {
        number,
        name: name.to_string(),
        type_name: type_name.to_string(),
        label: label.to_string(),
        oneof: oneof.map(str::to_string),
    })
}

fn proto_split_comma_list(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
        } else if ch == ',' {
            parts.push(input[start..idx].trim());
            start = idx + ch.len_utf8();
        }
    }
    parts.push(input[start..].trim());
    parts
}

fn parse_proto_string_literal(path: &str, statement: &str, value: &str) -> Result<String, String> {
    let value = value.trim();
    if !(value.starts_with('"') && value.ends_with('"') && value.len() >= 2) {
        return Err(proto_parse_error(
            path,
            statement,
            format!("invalid reserved name literal `{value}`"),
        ));
    }

    let inner = &value[1..value.len() - 1];
    let mut out = String::new();
    let mut escaped = false;
    for ch in inner.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    if escaped {
        Err(proto_parse_error(
            path,
            statement,
            "unterminated escape in string literal",
        ))
    } else {
        Ok(out)
    }
}

fn parse_proto_reserved(
    path: &str,
    statement: &str,
) -> Result<(BTreeSet<u32>, BTreeSet<String>), String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected reserved terminator"))?;
    let body = proto_keyword_tail(body, "reserved")
        .ok_or_else(|| proto_parse_error(path, statement, "expected reserved clause"))?;
    let mut numbers = BTreeSet::new();
    let mut names = BTreeSet::new();

    for part in proto_split_comma_list(body) {
        if part.is_empty() {
            return Err(proto_parse_error(path, statement, "empty reserved item"));
        }
        if part.starts_with('"') {
            names.insert(parse_proto_string_literal(path, statement, part)?);
        } else if let Some((start, end)) = part.split_once(" to ") {
            let start = parse_proto_u32(path, statement, start)?;
            let end = parse_proto_u32(path, statement, end)?;
            if start > end {
                return Err(proto_parse_error(
                    path,
                    statement,
                    "reserved range start is greater than end",
                ));
            }
            numbers.extend(start..=end);
        } else {
            numbers.insert(parse_proto_u32(path, statement, part)?);
        }
    }

    Ok((numbers, names))
}

fn proto_take_first_token(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }

    for (idx, ch) in input.char_indices() {
        if ch.is_whitespace() {
            return Some((&input[..idx], input[idx..].trim_start()));
        }
    }

    Some((input, ""))
}

fn parse_proto_enum_value(path: &str, statement: &str) -> Result<ProtoEnumValueSchema, String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected enum value terminator"))?;
    let body = truncate_proto_field_options(body).trim();
    let (name, right) = body
        .split_once('=')
        .ok_or_else(|| proto_parse_error(path, statement, "expected enum value number"))?;
    let name = name.trim();
    if !is_proto_ident(name) {
        return Err(proto_parse_error(
            path,
            statement,
            format!("invalid enum value name `{name}`"),
        ));
    }
    let (number_text, tail) = proto_take_first_token(right)
        .ok_or_else(|| proto_parse_error(path, statement, "missing enum value number"))?;
    if !tail.is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unexpected enum value number suffix `{tail}`"),
        ));
    }
    Ok(ProtoEnumValueSchema {
        number: parse_proto_i32(path, statement, number_text)?,
        name: name.to_string(),
    })
}

fn proto_take_ident(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    let mut chars = input.char_indices();
    let (_, first) = chars.next()?;
    if !is_proto_ident_start(first) {
        return None;
    }

    let mut end = first.len_utf8();
    for (idx, ch) in chars {
        if is_proto_ident_continue(ch) {
            end = idx + ch.len_utf8();
        } else {
            break;
        }
    }
    Some((&input[..end], &input[end..]))
}

fn proto_take_parenthesized(
    path: &str,
    statement: &str,
    input: &str,
) -> Result<(String, String), String> {
    let input = input.trim_start();
    if !input.starts_with('(') {
        return Err(proto_parse_error(path, statement, "expected `(`"));
    }

    let mut depth = 0isize;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let inside = input[1..idx].trim().to_string();
                    let tail = input[idx + ch.len_utf8()..].trim_start().to_string();
                    return Ok((inside, tail));
                }
            }
            _ => {}
        }
    }

    Err(proto_parse_error(
        path,
        statement,
        "unterminated parenthesized type",
    ))
}

fn parse_proto_stream_type(input: &str) -> (bool, String) {
    if let Some(tail) = proto_keyword_tail(input, "stream") {
        (true, tail.trim().to_string())
    } else {
        (false, input.trim().to_string())
    }
}

fn parse_proto_rpc(path: &str, statement: &str) -> Result<(String, ProtoRpcSchema), String> {
    let body = proto_statement_body(statement, ";")
        .ok_or_else(|| proto_parse_error(path, statement, "expected rpc terminator"))?;
    let body = proto_keyword_tail(body, "rpc")
        .ok_or_else(|| proto_parse_error(path, statement, "expected rpc declaration"))?;
    let (name, tail) = proto_take_ident(body)
        .ok_or_else(|| proto_parse_error(path, statement, "expected rpc name"))?;
    let (request, tail) = proto_take_parenthesized(path, statement, tail)?;
    let tail = proto_keyword_tail(&tail, "returns")
        .ok_or_else(|| proto_parse_error(path, statement, "expected returns clause"))?;
    let (response, tail) = proto_take_parenthesized(path, statement, tail)?;
    if !tail.trim().is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            format!("unexpected rpc suffix `{tail}`"),
        ));
    }

    let (client_streaming, request) = parse_proto_stream_type(&request);
    let (server_streaming, response) = parse_proto_stream_type(&response);
    if request.is_empty() || response.is_empty() {
        return Err(proto_parse_error(
            path,
            statement,
            "rpc request and response types must be non-empty",
        ));
    }

    Ok((
        name.to_string(),
        ProtoRpcSchema {
            request,
            response,
            client_streaming,
            server_streaming,
        },
    ))
}

fn parse_proto_schema(path: &str, input: &str) -> Result<ProtoFileSchema, String> {
    let input = remove_proto_comments(path, input)?;
    let statements = proto_logical_statements(&input);
    let mut schema = ProtoFileSchema {
        package: String::new(),
        messages: BTreeMap::new(),
        enums: BTreeMap::new(),
        services: BTreeMap::new(),
    };
    let mut contexts = Vec::new();
    let mut message_stack = Vec::new();
    let mut enum_stack = Vec::new();
    let mut service_stack = Vec::new();
    let mut oneof_stack = Vec::new();
    let mut last_statement = None;

    for statement in statements {
        last_statement = Some(statement.clone());
        if statement == "}" {
            match contexts.pop() {
                Some(ProtoParseContext::Message(name)) => {
                    let popped = message_stack.pop();
                    if popped.as_deref() != Some(name.as_str()) {
                        return Err(proto_parse_error(
                            path,
                            &statement,
                            "message context stack became inconsistent",
                        ));
                    }
                }
                Some(ProtoParseContext::Enum(name)) => {
                    let popped = enum_stack.pop();
                    if popped.as_deref() != Some(name.as_str()) {
                        return Err(proto_parse_error(
                            path,
                            &statement,
                            "enum context stack became inconsistent",
                        ));
                    }
                }
                Some(ProtoParseContext::Service(name)) => {
                    let popped = service_stack.pop();
                    if popped.as_deref() != Some(name.as_str()) {
                        return Err(proto_parse_error(
                            path,
                            &statement,
                            "service context stack became inconsistent",
                        ));
                    }
                }
                Some(ProtoParseContext::Oneof(name)) => {
                    let popped = oneof_stack.pop();
                    if popped.as_deref() != Some(name.as_str()) {
                        return Err(proto_parse_error(
                            path,
                            &statement,
                            "oneof context stack became inconsistent",
                        ));
                    }
                }
                None => {
                    return Err(proto_parse_error(
                        path,
                        &statement,
                        "unexpected closing brace",
                    ));
                }
            }
            continue;
        }

        if proto_keyword_tail(&statement, "message").is_some() && statement.ends_with('{') {
            let name = parse_proto_named_block(path, &statement, "message")?;
            let key = current_proto_path(&message_stack, &name);
            if schema
                .messages
                .insert(
                    key.clone(),
                    ProtoMessageSchema {
                        fields: BTreeMap::new(),
                        reserved_numbers: BTreeSet::new(),
                        reserved_names: BTreeSet::new(),
                    },
                )
                .is_some()
            {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    format!("duplicate message `{key}`"),
                ));
            }
            message_stack.push(name.clone());
            contexts.push(ProtoParseContext::Message(name));
            continue;
        }

        if proto_keyword_tail(&statement, "enum").is_some() && statement.ends_with('{') {
            let name = parse_proto_named_block(path, &statement, "enum")?;
            let key = current_proto_path(&message_stack, &name);
            if schema
                .enums
                .insert(
                    key.clone(),
                    ProtoEnumSchema {
                        values: Vec::new(),
                        reserved_numbers: BTreeSet::new(),
                        reserved_names: BTreeSet::new(),
                    },
                )
                .is_some()
            {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    format!("duplicate enum `{key}`"),
                ));
            }
            enum_stack.push(key.clone());
            contexts.push(ProtoParseContext::Enum(key));
            continue;
        }

        if proto_keyword_tail(&statement, "service").is_some() && statement.ends_with('{') {
            let name = parse_proto_named_block(path, &statement, "service")?;
            if !message_stack.is_empty() || !enum_stack.is_empty() || !service_stack.is_empty() {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    "service declarations must be top-level",
                ));
            }
            if schema
                .services
                .insert(
                    name.clone(),
                    ProtoServiceSchema {
                        rpcs: BTreeMap::new(),
                    },
                )
                .is_some()
            {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    format!("duplicate service `{name}`"),
                ));
            }
            service_stack.push(name.clone());
            contexts.push(ProtoParseContext::Service(name));
            continue;
        }

        if proto_keyword_tail(&statement, "oneof").is_some() && statement.ends_with('{') {
            let name = parse_proto_named_block(path, &statement, "oneof")?;
            if message_stack.is_empty() || !enum_stack.is_empty() || !service_stack.is_empty() {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    "oneof declarations must be inside a message",
                ));
            }
            oneof_stack.push(name.clone());
            contexts.push(ProtoParseContext::Oneof(name));
            continue;
        }

        if statement.starts_with("syntax ") {
            parse_proto_syntax(path, &statement)?;
            continue;
        }

        if statement.starts_with("import ") {
            if statement.ends_with(';') {
                continue;
            }
            return Err(proto_parse_error(
                path,
                &statement,
                "expected statement terminator",
            ));
        }

        if proto_keyword_tail(&statement, "package").is_some() {
            schema.package = parse_proto_package(path, &statement)?;
            continue;
        }

        if proto_keyword_tail(&statement, "reserved").is_some() {
            let (numbers, names) = parse_proto_reserved(path, &statement)?;
            if let Some(enum_key) = enum_stack.last() {
                let enum_schema = schema.enums.get_mut(enum_key).ok_or_else(|| {
                    proto_parse_error(path, &statement, "enum context is missing")
                })?;
                enum_schema.reserved_numbers.extend(numbers);
                enum_schema.reserved_names.extend(names);
            } else {
                let message_key = current_proto_path(&message_stack, "");
                let message_key = message_key.trim_end_matches('.');
                let message = schema.messages.get_mut(message_key).ok_or_else(|| {
                    proto_parse_error(path, &statement, "reserved clause must be inside a message")
                })?;
                message.reserved_numbers.extend(numbers);
                message.reserved_names.extend(names);
            }
            continue;
        }

        if let Some(service_key) = service_stack.last() {
            if proto_keyword_tail(&statement, "rpc").is_some() {
                let (name, rpc) = parse_proto_rpc(path, &statement)?;
                let service = schema.services.get_mut(service_key).ok_or_else(|| {
                    proto_parse_error(path, &statement, "service context is missing")
                })?;
                if service.rpcs.insert(name.clone(), rpc).is_some() {
                    return Err(proto_parse_error(
                        path,
                        &statement,
                        format!("duplicate rpc `{name}`"),
                    ));
                }
                continue;
            }
            return Err(proto_parse_error(
                path,
                &statement,
                "unsupported service statement",
            ));
        }

        if let Some(enum_key) = enum_stack.last() {
            let value = parse_proto_enum_value(path, &statement)?;
            let enum_schema = schema
                .enums
                .get_mut(enum_key)
                .ok_or_else(|| proto_parse_error(path, &statement, "enum context is missing"))?;
            enum_schema.values.push(value);
            continue;
        }

        if !message_stack.is_empty() {
            let message_key = current_proto_path(&message_stack, "");
            let message_key = message_key.trim_end_matches('.');
            let field =
                parse_proto_field(path, &statement, oneof_stack.last().map(String::as_str))?;
            let message = schema
                .messages
                .get_mut(message_key)
                .ok_or_else(|| proto_parse_error(path, &statement, "message context is missing"))?;
            if message.fields.insert(field.number, field).is_some() {
                return Err(proto_parse_error(
                    path,
                    &statement,
                    "duplicate field number",
                ));
            }
            continue;
        }

        return Err(proto_parse_error(
            path,
            &statement,
            "unsupported top-level statement",
        ));
    }

    if !contexts.is_empty() {
        let context = contexts
            .iter()
            .map(proto_context_label)
            .collect::<Vec<_>>()
            .join(" > ");
        let statement = last_statement.unwrap_or_else(|| "<end of file>".to_string());
        return Err(proto_parse_error(
            path,
            &statement,
            format!("unclosed block context: {context}"),
        ));
    }

    Ok(schema)
}

fn parse_current_novarocks_proto_schema() -> Result<ProtoSchema, String> {
    let mut files = BTreeMap::new();
    for file in proto_files(&Path::new(manifest_dir()).join("idl/novarocks")) {
        let relative = rel(&file);
        let input = fs::read_to_string(&file)
            .map_err(|err| format!("{}: failed to read proto file: {err}", relative))?;
        files.insert(relative.clone(), parse_proto_schema(&relative, &input)?);
    }

    Ok(ProtoSchema { version: 1, files })
}

fn nidl_d3b_baseline_path() -> PathBuf {
    Path::new(manifest_dir()).join(NIDL_D3B_BASELINE_PATH)
}

fn read_proto_schema_baseline(path: &Path) -> Result<ProtoSchema, String> {
    let input = fs::read_to_string(path)
        .map_err(|err| format!("{}: failed to read proto schema baseline: {err}", rel(path)))?;
    serde_json::from_str(&input).map_err(|err| {
        format!(
            "{}: failed to parse proto schema baseline JSON: {err}",
            rel(path)
        )
    })
}

fn write_proto_schema_baseline(path: &Path, schema: &ProtoSchema) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "{}: failed to create proto schema baseline directory: {err}",
                rel(parent)
            )
        })?;
    }

    let mut json = serde_json::to_string_pretty(schema)
        .map_err(|err| format!("failed to serialize proto schema baseline JSON: {err}"))?;
    json.push('\n');
    fs::write(path, json).map_err(|err| {
        format!(
            "{}: failed to write proto schema baseline: {err}",
            rel(path)
        )
    })
}

fn next_proto_schema_baseline_for_write(
    current: &ProtoSchema,
    baseline_path: &Path,
) -> Result<ProtoSchema, String> {
    if baseline_path.exists() {
        let existing = read_proto_schema_baseline(baseline_path)?;
        merge_proto_schema_baseline(current, &existing)
    } else {
        Err(format!(
            "{}: proto schema baseline is missing; {NIDL_D3B_WRITE_BASELINE_ENV}=1 can only update an existing baseline after D3B is established",
            rel(baseline_path)
        ))
    }
}

fn compare_proto_schema_to_baseline(current: &ProtoSchema, baseline: &ProtoSchema) -> Vec<String> {
    let mut violations = Vec::new();

    if current.version != 1 {
        violations.push(format!(
            "current proto schema version must be 1, got {}",
            current.version
        ));
    }
    if baseline.version != 1 {
        violations.push(format!(
            "baseline proto schema version must be 1, got {}",
            baseline.version
        ));
    }

    for path in baseline.files.keys() {
        if !current.files.contains_key(path) {
            violations.push(format!("{path} file removed from current proto schema"));
        }
    }

    for (path, current_file) in &current.files {
        if !baseline.files.contains_key(path) {
            violations.push(format!(
                "{path} baseline stale: new file is missing from baseline; run the proto schema baseline write command"
            ));
            for service_name in current_file.services.keys() {
                violations.push(format!(
                    "{path} service {service_name} new service is not allowed; D3B only allows extending existing NovaRocksGrpc"
                ));
            }
        }
    }

    for (path, baseline_file) in &baseline.files {
        let Some(current_file) = current.files.get(path) else {
            continue;
        };

        if current_file.package != baseline_file.package {
            violations.push(format!(
                "{path} package changed from {} to {}",
                baseline_file.package, current_file.package
            ));
        }

        compare_proto_messages_to_baseline(path, current_file, baseline_file, &mut violations);
        compare_proto_enums_to_baseline(path, current_file, baseline_file, &mut violations);
        compare_proto_services_to_baseline(path, current_file, baseline_file, &mut violations);
    }

    violations.sort();
    violations.dedup();
    violations
}

fn merge_proto_schema_baseline(
    current: &ProtoSchema,
    existing: &ProtoSchema,
) -> Result<ProtoSchema, String> {
    let unsafe_violations = compare_proto_schema_to_baseline(current, existing)
        .into_iter()
        .filter(|violation| !is_proto_schema_baseline_stale_violation(violation))
        .collect::<Vec<_>>();
    if !unsafe_violations.is_empty() {
        return Err(format!(
            "cannot merge proto schema baseline because current schema contains incompatible changes:\n{}",
            format_proto_schema_violations(&unsafe_violations)
        ));
    }

    let mut merged = current.clone();
    for (path, existing_file) in &existing.files {
        let Some(current_file) = current.files.get(path) else {
            continue;
        };
        let Some(merged_file) = merged.files.get_mut(path) else {
            continue;
        };
        merge_proto_file_schema_baseline(path, current_file, existing_file, merged_file)?;
    }

    Ok(merged)
}

fn is_proto_schema_baseline_stale_violation(violation: &str) -> bool {
    violation.contains("baseline stale: new ")
}

fn merge_proto_file_schema_baseline(
    path: &str,
    current_file: &ProtoFileSchema,
    existing_file: &ProtoFileSchema,
    merged_file: &mut ProtoFileSchema,
) -> Result<(), String> {
    for (message_name, existing_message) in &existing_file.messages {
        let Some(current_message) = current_file.messages.get(message_name) else {
            continue;
        };
        let Some(merged_message) = merged_file.messages.get_mut(message_name) else {
            continue;
        };
        merge_proto_message_schema_baseline(
            path,
            message_name,
            current_message,
            existing_message,
            merged_message,
        )?;
    }

    Ok(())
}

fn merge_proto_message_schema_baseline(
    path: &str,
    message_name: &str,
    current_message: &ProtoMessageSchema,
    existing_message: &ProtoMessageSchema,
    merged_message: &mut ProtoMessageSchema,
) -> Result<(), String> {
    for (number, existing_field) in &existing_message.fields {
        if current_message.fields.contains_key(number) {
            continue;
        }
        if current_message.reserved_numbers.contains(number)
            && current_message
                .reserved_names
                .contains(&existing_field.name)
        {
            merged_message
                .fields
                .insert(*number, existing_field.clone());
        } else {
            return Err(format!(
                "{path} {message_name} removed field #{number} {} without reserved number {number} and reserved name {}; refusing to write proto schema baseline",
                existing_field.name, existing_field.name
            ));
        }
    }

    Ok(())
}

fn format_proto_schema_violations(violations: &[String]) -> String {
    violations
        .iter()
        .map(|violation| format!("  - {violation}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn compare_proto_messages_to_baseline(
    path: &str,
    current_file: &ProtoFileSchema,
    baseline_file: &ProtoFileSchema,
    violations: &mut Vec<String>,
) {
    for message_name in baseline_file.messages.keys() {
        if !current_file.messages.contains_key(message_name) {
            violations.push(format!("{path} message {message_name} removed"));
        }
    }

    for message_name in current_file.messages.keys() {
        if !baseline_file.messages.contains_key(message_name) {
            violations.push(format!(
                "{path} message {message_name} baseline stale: new message is missing from baseline; run the proto schema baseline write command"
            ));
        }
    }

    for (message_name, baseline_message) in &baseline_file.messages {
        let Some(current_message) = current_file.messages.get(message_name) else {
            continue;
        };
        compare_proto_message_to_baseline(
            path,
            message_name,
            current_message,
            baseline_message,
            violations,
        );
    }
}

fn compare_proto_message_to_baseline(
    path: &str,
    message_name: &str,
    current_message: &ProtoMessageSchema,
    baseline_message: &ProtoMessageSchema,
    violations: &mut Vec<String>,
) {
    for number in &baseline_message.reserved_numbers {
        if !current_message.reserved_numbers.contains(number) {
            violations.push(format!(
                "{path} {message_name} reserved number {number} removed from current schema"
            ));
        }
    }
    for name in &baseline_message.reserved_names {
        if !current_message.reserved_names.contains(name) {
            violations.push(format!(
                "{path} {message_name} reserved name {name} removed from current schema"
            ));
        }
    }

    for current_field in current_message.fields.values() {
        if baseline_message
            .reserved_numbers
            .contains(&current_field.number)
        {
            violations.push(format!(
                "{path} {message_name} field #{} {} uses baseline reserved number {}",
                current_field.number, current_field.name, current_field.number
            ));
        }
        if baseline_message
            .reserved_names
            .contains(&current_field.name)
        {
            violations.push(format!(
                "{path} {message_name} field #{} {} uses baseline reserved name {}",
                current_field.number, current_field.name, current_field.name
            ));
        }
    }

    let baseline_fields_by_name: BTreeMap<&str, u32> = baseline_message
        .fields
        .values()
        .map(|field| (field.name.as_str(), field.number))
        .collect();

    for (number, baseline_field) in &baseline_message.fields {
        let Some(current_field) = current_message.fields.get(number) else {
            if !current_message.reserved_numbers.contains(number) {
                violations.push(format!(
                    "{path} {message_name} removed field #{number} {} without reserved number {number}",
                    baseline_field.name
                ));
            }
            if !current_message
                .reserved_names
                .contains(&baseline_field.name)
            {
                violations.push(format!(
                    "{path} {message_name} removed field #{number} {} without reserved name {}",
                    baseline_field.name, baseline_field.name
                ));
            }
            continue;
        };

        let baseline_signature = proto_field_signature(baseline_field);
        let current_signature = proto_field_signature(current_field);
        if baseline_field.name != current_field.name
            && baseline_field.type_name != current_field.type_name
        {
            violations.push(format!(
                "{path} {message_name} field #{number} field number reuse: changed from {baseline_signature} to {current_signature}"
            ));
        } else if baseline_field.name != current_field.name {
            violations.push(format!(
                "{path} {message_name} field #{number} field rename: changed from {baseline_signature} to {current_signature}"
            ));
        } else if baseline_field.type_name != current_field.type_name {
            violations.push(format!(
                "{path} {message_name} field #{number} field type change: changed from {baseline_signature} to {current_signature}"
            ));
        }

        if baseline_field.label != current_field.label {
            violations.push(format!(
                "{path} {message_name} field #{number} field label change: changed from {baseline_signature} to {current_signature}"
            ));
        }
        if baseline_field.oneof != current_field.oneof {
            violations.push(format!(
                "{path} {message_name} field #{number} field oneof change: changed from {baseline_signature} to {current_signature}"
            ));
        }
    }

    for current_field in current_message.fields.values() {
        if baseline_message.fields.contains_key(&current_field.number) {
            continue;
        }
        if baseline_message
            .reserved_numbers
            .contains(&current_field.number)
            || baseline_message
                .reserved_names
                .contains(&current_field.name)
        {
            continue;
        }
        if let Some(baseline_number) = baseline_fields_by_name.get(current_field.name.as_str()) {
            violations.push(format!(
                "{path} {message_name} field {} field renumbered from #{baseline_number} to #{}",
                current_field.name, current_field.number
            ));
        } else {
            violations.push(format!(
                "{path} {message_name} field #{} {} baseline stale: new field is missing from baseline; run the proto schema baseline write command",
                current_field.number, current_field.name
            ));
        }
    }
}

fn compare_proto_enums_to_baseline(
    path: &str,
    current_file: &ProtoFileSchema,
    baseline_file: &ProtoFileSchema,
    violations: &mut Vec<String>,
) {
    for enum_name in baseline_file.enums.keys() {
        if !current_file.enums.contains_key(enum_name) {
            violations.push(format!("{path} enum {enum_name} removed"));
        }
    }

    for (enum_name, current_enum) in &current_file.enums {
        validate_proto_enum_zero_value(path, enum_name, current_enum, violations);
        if !baseline_file.enums.contains_key(enum_name) {
            violations.push(format!(
                "{path} enum {enum_name} baseline stale: new enum is missing from baseline; run the proto schema baseline write command"
            ));
        }
    }

    for (enum_name, baseline_enum) in &baseline_file.enums {
        let Some(current_enum) = current_file.enums.get(enum_name) else {
            continue;
        };
        compare_proto_enum_to_baseline(path, enum_name, current_enum, baseline_enum, violations);
    }
}

fn compare_proto_enum_to_baseline(
    path: &str,
    enum_name: &str,
    current_enum: &ProtoEnumSchema,
    baseline_enum: &ProtoEnumSchema,
    violations: &mut Vec<String>,
) {
    for number in &baseline_enum.reserved_numbers {
        if !current_enum.reserved_numbers.contains(number) {
            violations.push(format!(
                "{path} enum {enum_name} reserved number {number} removed from current schema"
            ));
        }
    }
    for name in &baseline_enum.reserved_names {
        if !current_enum.reserved_names.contains(name) {
            violations.push(format!(
                "{path} enum {enum_name} reserved name {name} removed from current schema"
            ));
        }
    }

    let baseline_values_by_number: BTreeMap<i32, &ProtoEnumValueSchema> = baseline_enum
        .values
        .iter()
        .map(|value| (value.number, value))
        .collect();
    let baseline_values_by_name: BTreeMap<&str, i32> = baseline_enum
        .values
        .iter()
        .map(|value| (value.name.as_str(), value.number))
        .collect();
    let current_values_by_number: BTreeMap<i32, &ProtoEnumValueSchema> = current_enum
        .values
        .iter()
        .map(|value| (value.number, value))
        .collect();
    let current_values_by_name: BTreeMap<&str, i32> = current_enum
        .values
        .iter()
        .map(|value| (value.name.as_str(), value.number))
        .collect();

    for current_value in &current_enum.values {
        if u32::try_from(current_value.number)
            .ok()
            .is_some_and(|number| baseline_enum.reserved_numbers.contains(&number))
        {
            violations.push(format!(
                "{path} enum {enum_name} value {}={} uses baseline reserved number {}",
                current_value.name, current_value.number, current_value.number
            ));
        }
        if baseline_enum.reserved_names.contains(&current_value.name) {
            violations.push(format!(
                "{path} enum {enum_name} value {}={} uses baseline reserved name {}",
                current_value.name, current_value.number, current_value.name
            ));
        }
    }

    for baseline_value in &baseline_enum.values {
        if let Some(current_value) = current_values_by_number.get(&baseline_value.number) {
            if current_value.name != baseline_value.name {
                violations.push(format!(
                    "{path} enum {enum_name} value #{} renamed from {} to {}",
                    baseline_value.number, baseline_value.name, current_value.name
                ));
            }
        } else if let Some(current_number) =
            current_values_by_name.get(baseline_value.name.as_str())
        {
            violations.push(format!(
                "{path} enum {enum_name} value {} renumbered from #{} to #{}",
                baseline_value.name, baseline_value.number, current_number
            ));
        } else {
            violations.push(format!(
                "{path} enum {enum_name} value {}={} removed",
                baseline_value.name, baseline_value.number
            ));
        }
    }

    for current_value in &current_enum.values {
        if baseline_values_by_number.contains_key(&current_value.number)
            || baseline_values_by_name.contains_key(current_value.name.as_str())
            || u32::try_from(current_value.number)
                .ok()
                .is_some_and(|number| baseline_enum.reserved_numbers.contains(&number))
            || baseline_enum.reserved_names.contains(&current_value.name)
        {
            continue;
        }
        violations.push(format!(
            "{path} enum {enum_name} value {}={} baseline stale: new enum value is missing from baseline; run the proto schema baseline write command",
            current_value.name, current_value.number
        ));
    }
}

fn validate_proto_enum_zero_value(
    path: &str,
    enum_name: &str,
    current_enum: &ProtoEnumSchema,
    violations: &mut Vec<String>,
) {
    if !current_enum
        .values
        .first()
        .is_some_and(|value| value.number == 0 && value.name.ends_with("_UNSPECIFIED"))
    {
        violations.push(format!(
            "{path} enum {enum_name} enum zero value: first value must be *_UNSPECIFIED = 0"
        ));
    }
}

fn compare_proto_services_to_baseline(
    path: &str,
    current_file: &ProtoFileSchema,
    baseline_file: &ProtoFileSchema,
    violations: &mut Vec<String>,
) {
    for service_name in baseline_file.services.keys() {
        if !current_file.services.contains_key(service_name) {
            violations.push(format!("{path} service {service_name} removed"));
        }
    }

    for service_name in current_file.services.keys() {
        if !baseline_file.services.contains_key(service_name) {
            violations.push(format!(
                "{path} service {service_name} new service is not allowed; D3B only allows extending existing NovaRocksGrpc"
            ));
        }
    }

    for (service_name, baseline_service) in &baseline_file.services {
        let Some(current_service) = current_file.services.get(service_name) else {
            continue;
        };
        compare_proto_service_to_baseline(
            path,
            service_name,
            current_service,
            baseline_service,
            violations,
        );
    }
}

fn compare_proto_service_to_baseline(
    path: &str,
    service_name: &str,
    current_service: &ProtoServiceSchema,
    baseline_service: &ProtoServiceSchema,
    violations: &mut Vec<String>,
) {
    for rpc_name in baseline_service.rpcs.keys() {
        if !current_service.rpcs.contains_key(rpc_name) {
            violations.push(format!(
                "{path} service {service_name} rpc {rpc_name} removed"
            ));
        }
    }

    for (rpc_name, current_rpc) in &current_service.rpcs {
        let Some(baseline_rpc) = baseline_service.rpcs.get(rpc_name) else {
            violations.push(format!(
                "{path} service {service_name} rpc {rpc_name} baseline stale: new rpc is missing from baseline; run the proto schema baseline write command"
            ));
            continue;
        };

        if current_rpc != baseline_rpc {
            violations.push(format!(
                "{path} service {service_name} rpc {rpc_name} signature changed: rpc signature changed from {} to {}",
                proto_rpc_signature(baseline_rpc),
                proto_rpc_signature(current_rpc)
            ));
        }
    }
}

fn proto_field_signature(field: &ProtoFieldSchema) -> String {
    let mut signature = format!("{}:{}/{}", field.name, field.type_name, field.label);
    if let Some(oneof) = &field.oneof {
        signature.push_str("/oneof=");
        signature.push_str(oneof);
    }
    signature
}

fn proto_rpc_signature(rpc: &ProtoRpcSchema) -> String {
    let request = if rpc.client_streaming {
        format!("stream {}", rpc.request)
    } else {
        rpc.request.clone()
    };
    let response = if rpc.server_streaming {
        format!("stream {}", rpc.response)
    } else {
        rpc.response.clone()
    };
    format!("{request} -> {response}")
}

fn test_proto_field(number: u32, name: &str, type_name: &str) -> ProtoFieldSchema {
    ProtoFieldSchema {
        number,
        name: name.to_string(),
        type_name: type_name.to_string(),
        label: "singular".to_string(),
        oneof: None,
    }
}

fn test_proto_field_with_label(
    number: u32,
    name: &str,
    type_name: &str,
    label: &str,
) -> ProtoFieldSchema {
    let mut field = test_proto_field(number, name, type_name);
    field.label = label.to_string();
    field
}

fn test_proto_field_with_oneof(
    number: u32,
    name: &str,
    type_name: &str,
    oneof: &str,
) -> ProtoFieldSchema {
    let mut field = test_proto_field(number, name, type_name);
    field.oneof = Some(oneof.to_string());
    field
}

fn test_proto_message_with_reserved(
    fields: Vec<ProtoFieldSchema>,
    reserved_numbers: &[u32],
    reserved_names: &[&str],
) -> ProtoMessageSchema {
    ProtoMessageSchema {
        fields: fields
            .into_iter()
            .map(|field| (field.number, field))
            .collect(),
        reserved_numbers: reserved_numbers.iter().copied().collect(),
        reserved_names: reserved_names
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    }
}

fn test_proto_message(fields: Vec<ProtoFieldSchema>) -> ProtoMessageSchema {
    test_proto_message_with_reserved(fields, &[], &[])
}

fn test_proto_enum(values: Vec<(i32, &str)>) -> ProtoEnumSchema {
    test_proto_enum_with_reserved(values, &[], &[])
}

fn test_proto_enum_with_reserved(
    values: Vec<(i32, &str)>,
    reserved_numbers: &[u32],
    reserved_names: &[&str],
) -> ProtoEnumSchema {
    ProtoEnumSchema {
        values: values
            .into_iter()
            .map(|(number, name)| ProtoEnumValueSchema {
                number,
                name: name.to_string(),
            })
            .collect(),
        reserved_numbers: reserved_numbers.iter().copied().collect(),
        reserved_names: reserved_names
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    }
}

fn test_proto_rpc(request: &str, response: &str) -> ProtoRpcSchema {
    ProtoRpcSchema {
        request: request.to_string(),
        response: response.to_string(),
        client_streaming: false,
        server_streaming: false,
    }
}

fn test_proto_service(rpcs: Vec<(&str, ProtoRpcSchema)>) -> ProtoServiceSchema {
    ProtoServiceSchema {
        rpcs: rpcs
            .into_iter()
            .map(|(name, rpc)| (name.to_string(), rpc))
            .collect(),
    }
}

fn test_proto_schema(
    messages: Vec<(&str, ProtoMessageSchema)>,
    enums: Vec<(&str, ProtoEnumSchema)>,
    services: Vec<(&str, ProtoServiceSchema)>,
) -> ProtoSchema {
    test_proto_schema_with_files(vec![(
        "idl/novarocks/test.proto",
        test_proto_file("novarocks.test", messages, enums, services),
    )])
}

fn test_proto_file(
    package: &str,
    messages: Vec<(&str, ProtoMessageSchema)>,
    enums: Vec<(&str, ProtoEnumSchema)>,
    services: Vec<(&str, ProtoServiceSchema)>,
) -> ProtoFileSchema {
    ProtoFileSchema {
        package: package.to_string(),
        messages: messages
            .into_iter()
            .map(|(name, message)| (name.to_string(), message))
            .collect(),
        enums: enums
            .into_iter()
            .map(|(name, enum_schema)| (name.to_string(), enum_schema))
            .collect(),
        services: services
            .into_iter()
            .map(|(name, service)| (name.to_string(), service))
            .collect(),
    }
}

fn test_proto_schema_with_files(files: Vec<(&str, ProtoFileSchema)>) -> ProtoSchema {
    ProtoSchema {
        version: 1,
        files: files
            .into_iter()
            .map(|(path, file)| (path.to_string(), file))
            .collect(),
    }
}

fn assert_proto_schema_comparator_rejects(
    current: ProtoSchema,
    baseline: ProtoSchema,
    expected_violation: &str,
) {
    let violations = compare_proto_schema_to_baseline(&current, &baseline);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains(expected_violation)),
        "expected proto schema comparator violation containing `{expected_violation}`, got: {violations:?}"
    );
}

fn assert_proto_schema_comparator_accepts(current: ProtoSchema, baseline: ProtoSchema) {
    let violations = compare_proto_schema_to_baseline(&current, &baseline);
    assert!(
        violations.is_empty(),
        "expected proto schema comparator to accept compatible schema, got: {violations:?}"
    );
}

fn assert_proto_schema_comparator_rejects_all(
    current: ProtoSchema,
    baseline: ProtoSchema,
    expected_violations: &[&str],
) {
    let violations = compare_proto_schema_to_baseline(&current, &baseline);
    for expected_violation in expected_violations {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected_violation)),
            "expected proto schema comparator violation containing `{expected_violation}`, got: {violations:?}"
        );
    }
}

fn assert_proto_schema_baseline_merge_rejects(
    current: ProtoSchema,
    existing: ProtoSchema,
    expected_error: &str,
) {
    let err = merge_proto_schema_baseline(&current, &existing)
        .expect_err("expected proto schema baseline merge to reject unsafe change");
    assert!(
        err.contains(expected_error),
        "expected proto schema baseline merge error containing `{expected_error}`, got: {err}"
    );
}

#[test]
fn nidl_d3f_native_scan_range_proto_is_file_only() {
    let repo = Path::new(manifest_dir());
    let service_proto =
        fs::read_to_string(repo.join("idl/novarocks/service.proto")).expect("read service.proto");

    for forbidden in ["HdfsScanRange", "InternalScanRange", "TScanRangeParams"] {
        assert!(
            !service_proto.contains(forbidden),
            "idl/novarocks/service.proto must not expose thrift-shaped native scan range symbol `{forbidden}`"
        );
    }
    assert!(
        service_proto.contains("message ScanRangeParams")
            && service_proto.contains("FileScanRange file = 1"),
        "idl/novarocks/service.proto must expose native ScanRangeParams -> FileScanRange"
    );
}

#[test]
fn nidl_d3f_native_runtime_layers_do_not_import_thrift_scan_ranges() {
    let repo = Path::new(manifest_dir());
    let guarded_files = [
        "src/runtime/scheduler.rs",
        "src/sql/codegen/proto_encode/instance.rs",
    ];
    let forbidden = ["TScanRangeParams", "THdfsScanRange", "TInternalScanRange"];
    let mut violations = Vec::new();

    for rel_path in guarded_files {
        let path = repo.join(rel_path);
        for (line, text) in source_line_hits(&path, |line| {
            forbidden.iter().any(|symbol| line.contains(symbol))
        }) {
            violations.push(format!("{rel_path}:{line}: {text}"));
        }
    }

    assert!(
        violations.is_empty(),
        "native scheduling/proto encoding must use runtime::scan_range, not thrift scan range types:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3f_scan_node_dispatch_keeps_native_file_emitter_on_iceberg_only() {
    let repo = Path::new(manifest_dir());
    let source =
        fs::read_to_string(repo.join("src/sql/codegen/nodes.rs")).expect("read codegen nodes");
    let starrocks_start = source
        .find("ScanSource::StarRocks { .. } => {")
        .expect("find StarRocks scan node branch");
    let iceberg_start = source[starrocks_start..]
        .find("ScanSource::IcebergDataFiles")
        .map(|offset| starrocks_start + offset)
        .expect("find Iceberg scan node branch");
    let starrocks_branch = &source[starrocks_start..iceberg_start];
    assert!(
        starrocks_branch.contains("to_thrift_scan(")
            && !starrocks_branch.contains("to_native_file_scan("),
        "StarRocks build_scan_node branch must stay on the compat thrift scan emitter"
    );

    let after_iceberg = &source[iceberg_start..];
    let iceberg_end = after_iceberg
        .find("ScanSource::IcebergDeltaTable")
        .expect("find end of Iceberg data-file scan node branch");
    let iceberg_branch = &after_iceberg[..iceberg_end];
    assert!(
        iceberg_branch.contains("to_native_file_scan(")
            && !iceberg_branch.contains("to_thrift_scan("),
        "Iceberg data-file build_scan_node branch must use the native file scan emitter"
    );
}

#[test]
fn nidl_d3b_proto_schema_parser_handles_current_syntax() {
    let input = r#"
        syntax = "proto3";
        package novarocks.plan;

        message Outer {
          reserved 4, 6 to 8;
          reserved "old_name", "old_flag";
          message Inner {
            string value = 1;
          }
          optional string name = 1;
          repeated int64 ids = 2;
          map<int32, novarocks.plan.ScanRangeList> ranges = 3;
          oneof kind {
            bool enabled = 5;
          }
          enum InnerState {
            INNER_STATE_UNSPECIFIED = 0;
            reserved 2, 4 to 5;
            reserved "old_state";
            INNER_STATE_READY = 1;
          }
        }

        service NovaRocksGrpc {
          rpc TransmitRuntimeFilter(novarocks.filter.TransmitRuntimeFilterRequest)
              returns (novarocks.filter.TransmitRuntimeFilterResponse);
          rpc Exchange(stream ExchangeRequest) returns (stream ExchangeResponse);
        }
    "#;

    let schema =
        parse_proto_schema("idl/novarocks/sample.proto", input).expect("sample proto should parse");
    assert_eq!(schema.package, "novarocks.plan");
    assert_eq!(schema.messages["Outer"].fields[&1].label, "optional");
    assert_eq!(schema.messages["Outer"].fields[&2].label, "repeated");
    assert_eq!(
        schema.messages["Outer"].fields[&3].type_name,
        "map<int32, novarocks.plan.ScanRangeList>"
    );
    assert_eq!(schema.messages["Outer.Inner"].fields[&1].name, "value");
    assert_eq!(
        schema.messages["Outer"].fields[&5].oneof.as_deref(),
        Some("kind")
    );
    assert!(schema.messages["Outer"].reserved_numbers.contains(&4));
    assert!(schema.messages["Outer"].reserved_numbers.contains(&7));
    assert!(schema.messages["Outer"].reserved_names.contains("old_name"));
    assert_eq!(
        schema.enums["Outer.InnerState"].values[0].name,
        "INNER_STATE_UNSPECIFIED"
    );
    assert!(
        schema.enums["Outer.InnerState"]
            .reserved_numbers
            .contains(&4)
    );
    assert!(
        schema.enums["Outer.InnerState"]
            .reserved_names
            .contains("old_state")
    );
    assert_eq!(
        schema.services["NovaRocksGrpc"].rpcs["TransmitRuntimeFilter"].request,
        "novarocks.filter.TransmitRuntimeFilterRequest"
    );
    assert_eq!(
        schema.services["NovaRocksGrpc"].rpcs["TransmitRuntimeFilter"].response,
        "novarocks.filter.TransmitRuntimeFilterResponse"
    );
    assert!(schema.services["NovaRocksGrpc"].rpcs["Exchange"].client_streaming);
    assert!(schema.services["NovaRocksGrpc"].rpcs["Exchange"].server_streaming);
}

#[test]
fn nidl_d3b_proto_schema_parser_rejects_proto2_syntax() {
    let err = parse_proto_schema(
        "idl/novarocks/proto2.proto",
        r#"
        syntax = "proto2";
        package novarocks.bad;
        message Bad {
          optional string value = 1;
        }
        "#,
    )
    .expect_err("proto2 syntax should fail");

    assert!(err.contains("syntax = \"proto2\";"), "{err}");
    assert!(err.contains("expected `proto3`"), "{err}");
}

#[test]
fn nidl_d3b_proto_schema_parser_parses_all_native_proto_files() {
    let schema =
        parse_current_novarocks_proto_schema().expect("current native proto schema should parse");
    assert!(schema.files.contains_key("idl/novarocks/service.proto"));
    assert!(
        schema.files["idl/novarocks/service.proto"]
            .services
            .contains_key("NovaRocksGrpc")
    );
    assert!(
        schema.files["idl/novarocks/service.proto"]
            .messages
            .contains_key("SubmitFragmentRequest")
    );
    let fetch_status =
        &schema.files["idl/novarocks/service.proto"].enums["FetchResultResponse.Status"];
    assert_eq!(
        fetch_status
            .values
            .iter()
            .map(|value| (value.number, value.name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (0, "RESULT_STATUS_UNSPECIFIED"),
            (1, "READY"),
            (2, "NOT_READY"),
            (3, "EOF"),
            (4, "ERROR"),
        ]
    );
    assert!(fetch_status.reserved_numbers.is_empty());
    assert!(fetch_status.reserved_names.is_empty());
}

#[test]
fn nidl_d3b_current_schema_matches_baseline() {
    let current =
        parse_current_novarocks_proto_schema().expect("current native proto schema should parse");
    let baseline_path = nidl_d3b_baseline_path();

    match env::var(NIDL_D3B_WRITE_BASELINE_ENV) {
        Ok(value) if value == "1" => {
            let next_baseline = next_proto_schema_baseline_for_write(&current, &baseline_path)
                .unwrap_or_else(|err| panic!("{err}"));

            write_proto_schema_baseline(&baseline_path, &next_baseline)
                .unwrap_or_else(|err| panic!("{err}"));
            let written =
                read_proto_schema_baseline(&baseline_path).unwrap_or_else(|err| panic!("{err}"));
            let violations = compare_proto_schema_to_baseline(&current, &written);
            assert!(
                violations.is_empty(),
                "written proto schema baseline still violates current schema:\n{}",
                format_proto_schema_violations(&violations)
            );
        }
        Ok(value) => panic!(
            "{NIDL_D3B_WRITE_BASELINE_ENV} must be exactly `1` to write the proto schema baseline, got `{value}`"
        ),
        Err(env::VarError::NotUnicode(_)) => panic!(
            "{NIDL_D3B_WRITE_BASELINE_ENV} must be valid UTF-8 and exactly `1` to write the proto schema baseline"
        ),
        Err(env::VarError::NotPresent) => {
            let baseline = read_proto_schema_baseline(&baseline_path)
                .unwrap_or_else(|err| panic!("{err}\n\n{}", nidl_d3b_baseline_update_hint()));
            let violations = compare_proto_schema_to_baseline(&current, &baseline);
            assert!(
                violations.is_empty(),
                "current native proto schema does not match baseline:\n{}\n\n{}",
                format_proto_schema_violations(&violations),
                nidl_d3b_baseline_update_hint()
            );
        }
    }
}

fn source_region<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    let start_idx = source
        .find(start)
        .unwrap_or_else(|| panic!("missing start marker `{start}`"));
    let after_start = &source[start_idx..];
    let end_idx = after_start
        .find(end)
        .unwrap_or_else(|| panic!("missing end marker `{end}` after `{start}`"));
    &after_start[..end_idx]
}

#[test]
fn nidl_e5_native_exchange_response_has_common_status() {
    let schema =
        parse_current_novarocks_proto_schema().expect("current native proto schema should parse");
    let service_proto = &schema.files["idl/novarocks/service.proto"];
    let response = &service_proto.messages["ExchangeResponse"];
    let status = response
        .fields
        .get(&2)
        .expect("ExchangeResponse field 2 must be native status");

    assert_eq!(status.name, "status");
    assert_eq!(status.type_name, "novarocks.common.Status");
    assert_eq!(status.label, "singular");
}

#[test]
fn nidl_e5_native_exchange_rpc_paths_do_not_reference_starrocks_proto() {
    let repo = Path::new(manifest_dir());
    let grpc_server =
        fs::read_to_string(repo.join("src/service/grpc_server.rs")).expect("read grpc_server.rs");
    let exchange_region = source_region(
        &grpc_server,
        "async fn exchange(",
        "async fn transmit_runtime_filter(",
    );
    assert!(
        !exchange_region.contains("proto::starrocks"),
        "native grpc_server exchange path must not reference proto::starrocks:\n{exchange_region}"
    );

    let grpc_client =
        fs::read_to_string(repo.join("src/service/grpc_client.rs")).expect("read grpc_client.rs");
    let send_chunks_region = source_region(
        &grpc_client,
        "pub fn send_chunks(",
        "pub fn transmit_runtime_filter(",
    );
    assert!(
        !send_chunks_region.contains("proto::starrocks"),
        "native grpc_client send_chunks path must not reference proto::starrocks:\n{send_chunks_region}"
    );

    let internal_rpc =
        fs::read_to_string(repo.join("src/service/internal_rpc.rs")).expect("read internal_rpc.rs");
    let native_handler = source_region(
        &internal_rpc,
        "pub(crate) fn handle_transmit_chunk(",
        "#[cfg(feature = \"compat\")]\npub(crate) fn handle_transmit_chunk_compat(",
    );
    assert!(
        native_handler.contains("proto::novarocks::ExchangeRequest"),
        "native transmit_chunk handler must accept ExchangeRequest:\n{native_handler}"
    );
    assert!(
        native_handler.contains("proto::novarocks::ExchangeResponse"),
        "native transmit_chunk handler must return ExchangeResponse:\n{native_handler}"
    );
    assert!(
        !native_handler.contains("proto::starrocks"),
        "native transmit_chunk handler must not reference proto::starrocks:\n{native_handler}"
    );
}

#[test]
fn nidl_d3e_native_runtime_routing_has_no_thrift_shaped_endpoint_model() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    for proto in ["idl/novarocks/service.proto", "idl/novarocks/plan.proto"] {
        let text = fs::read_to_string(repo.join(proto)).unwrap();
        for forbidden in [
            "brpc_addr",
            "fragment_instance_address",
            "grpc_endpoint",
            "report_addr",
        ] {
            if text.contains(forbidden) {
                violations.push(format!(
                    "{proto}: native proto must not contain `{forbidden}`"
                ));
            }
        }
    }

    let checked_sources = [
        "src/runtime/scheduler.rs",
        "src/sql/codegen/proto_encode/instance.rs",
    ];
    for source in checked_sources {
        let path = repo.join(source);
        let text = fs::read_to_string(&path).unwrap();
        for forbidden in [
            "TPlanFragmentDestination",
            "TRuntimeFilterProberParams",
            "brpc_server",
            "fragment_instance_address",
            "grpc_endpoint",
            "brpc_addr",
        ] {
            if text.contains(forbidden) {
                violations.push(format!(
                    "{source}: native runtime routing must not contain `{forbidden}`"
                ));
            }
        }
    }

    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    assert!(
        coordinator.contains("fn exec_destination_from_runtime"),
        "coordinator must keep destination conversion in a named execution-parameter boundary helper"
    );
    assert!(
        coordinator.contains("fn native_stream_destination"),
        "coordinator must encode native stream destinations without thrift roundtrip"
    );

    assert!(
        violations.is_empty(),
        "D3E native runtime endpoint guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3i_native_fragment_exec_params_are_not_thrift_shaped() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let native_wire = fs::read_to_string(repo.join("src/runtime/native_fragment_wire.rs")).unwrap();
    for forbidden in ["TPlanFragmentExecParams", "TPlanFragmentDestination"] {
        if native_wire.contains(forbidden) {
            violations.push(format!(
                "src/runtime/native_fragment_wire.rs: native fragment wire must not expose `{forbidden}`"
            ));
        }
    }

    let codegen_nodes = fs::read_to_string(repo.join("src/sql/codegen/nodes.rs")).unwrap();
    if codegen_nodes.contains("pub(crate) exec_params: internal_service::TPlanFragmentExecParams") {
        violations.push(
            "src/sql/codegen/nodes.rs: ScanRangeBuildResult must not expose thrift exec_params as its native scan-range result".to_string(),
        );
    }
    if codegen_nodes.contains("fragment_exec_params: internal_service::TPlanFragmentExecParams") {
        violations.push(
            "src/sql/codegen/nodes.rs: ScanRangeBuildResult.fragment_exec_params must be the native FragmentExecParams model".to_string(),
        );
    }

    let fragment_exec_params =
        fs::read_to_string(repo.join("src/runtime/fragment_exec_params.rs")).unwrap();
    let struct_start = fragment_exec_params
        .find("pub(crate) struct FragmentExecParams")
        .expect("FragmentExecParams struct must exist");
    let impl_start = fragment_exec_params[struct_start..]
        .find("impl FragmentExecParams")
        .expect("FragmentExecParams impl must exist");
    let struct_body = &fragment_exec_params[struct_start..struct_start + impl_start];
    for forbidden in ["TUniqueId", "types::TUniqueId"] {
        if struct_body.contains(forbidden) {
            violations.push(format!(
                "src/runtime/fragment_exec_params.rs: FragmentExecParams fields must use native UniqueId, not `{forbidden}`"
            ));
        }
    }
    if !struct_body.contains("query_id: UniqueId")
        || !struct_body.contains("fragment_instance_id: UniqueId")
    {
        violations.push(
            "src/runtime/fragment_exec_params.rs: FragmentExecParams must keep query ids in crate::common::types::UniqueId".to_string(),
        );
    }

    let new_signature_start = fragment_exec_params
        .find("pub(crate) fn new(")
        .expect("FragmentExecParams::new must exist");
    let new_signature_end = fragment_exec_params[new_signature_start..]
        .find(") -> Result<Self, String>")
        .expect("FragmentExecParams::new signature must return Result");
    let new_signature =
        &fragment_exec_params[new_signature_start..new_signature_start + new_signature_end];
    if new_signature.contains("TUniqueId") || new_signature.contains("types::TUniqueId") {
        violations.push(
            "src/runtime/fragment_exec_params.rs: FragmentExecParams::new must accept native UniqueId inputs".to_string(),
        );
    }

    assert!(
        violations.is_empty(),
        "D3I native fragment exec params guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3j_native_delta_scan_sidecar_is_not_patched_from_thrift_plan() {
    let repo = Path::new(manifest_dir());
    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    let mut violations = Vec::new();

    for forbidden in [
        "patch_native_iceberg_delta_scan_payloads",
        "TIcebergDeltaScanNode",
        "TIcebergDeltaScanPlan",
        "encode_native_delta_scan_plan",
    ] {
        if coordinator.contains(forbidden) {
            violations.push(format!(
                "src/runtime/coordinator.rs: native Iceberg delta sidecar must not use `{forbidden}`"
            ));
        }
    }

    let proto_plan = fs::read_to_string(repo.join("src/sql/codegen/proto_encode/plan.rs")).unwrap();
    if proto_plan.contains("delta_plan: None") {
        violations.push(
            "src/sql/codegen/proto_encode/plan.rs: IcebergDeltaTable native encoder must not leave delta_plan as None".to_string(),
        );
    }

    assert!(
        violations.is_empty(),
        "D3J native Iceberg delta sidecar guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3k_native_dynamic_sink_partition_does_not_roundtrip_thrift_partition() {
    let repo = Path::new(manifest_dir());
    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    let mut violations = Vec::new();

    for forbidden in [
        "native_data_partition_from_thrift",
        "native_data_partition_from_thrift_with_exprs",
    ] {
        if coordinator.contains(forbidden) {
            violations.push(format!(
                "src/runtime/coordinator.rs: native dynamic sink patch must not use `{forbidden}`"
            ));
        }
    }

    if coordinator.contains("Vec<(FragmentId, i32, partitions::TDataPartition, Vec<i32>)>") {
        violations.push(
            "src/runtime/coordinator.rs: CTE native consumer index must not store thrift TDataPartition"
                .to_string(),
        );
    }

    let scheduler = fs::read_to_string(repo.join("src/runtime/scheduler.rs")).unwrap();
    for forbidden in [
        "use crate::thrift::partitions::TPartitionType;",
        "Vec<(FragmentId, TPartitionType, FragmentStreamKind)>",
        "e.compat_output_partition.type_",
    ] {
        if scheduler.contains(forbidden) {
            violations.push(format!(
                "src/runtime/scheduler.rs: scheduling topology must use native edge.output_partition, not compat thrift partition via `{forbidden}`"
            ));
        }
    }

    let codegen_mod = fs::read_to_string(repo.join("src/sql/codegen/mod.rs")).unwrap();
    if !codegen_mod.contains("pub output_partition: crate::sql::planner::DataPartition") {
        violations.push(
            "src/sql/codegen/mod.rs: FragmentEdge must carry native output_partition".to_string(),
        );
    }
    if codegen_mod.contains("pub compat_output_partition: partitions::TDataPartition") {
        violations.push(
            "src/sql/codegen/mod.rs: FragmentEdge must no longer carry compat TDataPartition in planner IR"
                .to_string(),
        );
    }

    assert!(
        violations.is_empty(),
        "D3K native dynamic sink partition guard failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_d3l_native_mainline_thrift_usage_is_explicitly_allowlisted() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let scheduler = fs::read_to_string(repo.join("src/runtime/scheduler.rs")).unwrap();
    let scheduler = rust_production_text_without_cfg_test(&scheduler);
    push_forbidden_terms(
        &mut violations,
        "src/runtime/scheduler.rs",
        &scheduler,
        &[
            "fragment_sink_is_terminal_write_sink",
            "find_scan_plan_nodes(",
            "TDataSink",
            "TPlan)",
        ],
        "native scheduler must use FragmentBuildResult metadata, not compat thrift structs",
    );

    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    let coordinator = rust_production_text_without_cfg_test(&coordinator);
    push_forbidden_terms(
        &mut violations,
        "src/runtime/coordinator.rs",
        &coordinator,
        &[
            "patch_native_iceberg_delta_scan_payloads",
            "native_data_partition_from_thrift",
            "native_data_partition_from_thrift_with_exprs",
            "TIcebergDeltaScanNode",
            "TIcebergDeltaScanPlan",
            "Vec<(FragmentId, i32, partitions::TDataPartition, Vec<i32>)>",
        ],
        "native coordinator must not patch native sidecars from thrift-shaped payloads",
    );

    for source in [
        "src/lower/novarocks/fragment.rs",
        "src/lower/novarocks/layout.rs",
        "src/lower/novarocks/node.rs",
        "src/lower/novarocks/scan.rs",
        "src/lower/novarocks/sink.rs",
    ] {
        let text = fs::read_to_string(repo.join(source)).unwrap();
        let text = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            source,
            &text,
            &[
                "crate::thrift",
                "thrift::",
                "TPlanFragment",
                "TPlanNode",
                "TDataSink",
            ],
            "native lowering must not take thrift as input contract",
        );
    }

    for path in rs_files(&repo.join("src/sql/codegen/proto_encode")) {
        let source = rel(&path);
        let text = fs::read_to_string(&path).unwrap();
        let text = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            &source,
            &text,
            &[
                "crate::thrift::partitions::TDataPartition::new",
                "crate::thrift::data_sinks::TDataSink",
                "crate::thrift::plan_nodes::TPlan",
            ],
            "native proto encoder must not construct compat thrift artifacts",
        );
    }

    let compat_allowlist = [
        (
            "src/runtime/fragment_exec_params.rs",
            &[
                "compat_exec_params_from_parts",
                "compat_destination_from_runtime",
            ][..],
        ),
        (
            "src/runtime/scan_range.rs",
            &[
                "thrift_scan_range_params_from_native",
                "thrift_scan_range_map_from_native",
            ][..],
        ),
        (
            "src/runtime/query_options.rs",
            &["from_thrift", "to_thrift"][..],
        ),
        (
            "src/runtime/runtime_filter_params.rs",
            &["from_thrift", "to_thrift"][..],
        ),
    ];
    for (source, markers) in compat_allowlist {
        let text = fs::read_to_string(repo.join(source)).unwrap();
        let production_text = rust_production_text_without_cfg_test(&text);
        for marker in markers {
            if !production_text.contains(marker) {
                violations.push(format!(
                    "{source}: compat allowlist must contain `{marker}`"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "D3L native mainline thrift usage guard failed:\n{}",
        violations.join("\n")
    );
}

fn nidl_d3b_baseline_update_hint() -> String {
    format!(
        "To intentionally update the proto schema ledger, run:\n{}",
        NIDL_D3B_WRITE_BASELINE_COMMAND
    )
}

#[test]
fn nidl_d3b_proto_schema_write_mode_rejects_missing_baseline_without_bootstrap() {
    let missing_path = std::env::temp_dir().join(format!(
        "novarocks-missing-proto-schema-baseline-{}.json",
        std::process::id()
    ));
    fs::remove_file(&missing_path).ok();
    let current = test_proto_schema(vec![], vec![], vec![]);

    let err = next_proto_schema_baseline_for_write(&current, &missing_path)
        .expect_err("write mode should reject a missing baseline");

    assert!(err.contains("proto schema baseline is missing"), "{err}");
    assert!(err.contains(NIDL_D3B_WRITE_BASELINE_ENV), "{err}");
    assert!(
        !missing_path.exists(),
        "missing-baseline decision test must not write a real baseline"
    );
}

#[test]
fn nidl_d3b_proto_schema_parser_reports_unclosed_context_statement() {
    let err = parse_proto_schema(
        "idl/novarocks/broken.proto",
        r#"
        syntax = "proto3";
        package novarocks.broken;
        message Broken {
          string value = 1;
        "#,
    )
    .expect_err("unclosed message should fail");

    assert!(err.contains("idl/novarocks/broken.proto"), "{err}");
    assert!(err.contains("string value = 1;"), "{err}");
    assert!(err.contains("message Broken"), "{err}");
}

#[test]
fn nidl_d3b_proto_schema_parser_rejects_unsupported_tails_and_bad_identifiers() {
    for (name, input, expected_statement) in [
        (
            "field-tail",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message Bad {
              string x = 1 unexpected;
            }
            "#,
            "string x = 1 unexpected;",
        ),
        (
            "enum-tail",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            enum Bad {
              FOO = 1 alias;
            }
            "#,
            "FOO = 1 alias;",
        ),
        (
            "message-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message 1Bad {
            }
            "#,
            "message 1Bad {",
        ),
        (
            "field-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message Bad {
              string 1x = 1;
            }
            "#,
            "string 1x = 1;",
        ),
        (
            "field-bad-continue",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message Bad {
              string x-y = 1;
            }
            "#,
            "string x-y = 1;",
        ),
        (
            "enum-value-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            enum Bad {
              1FOO = 1;
            }
            "#,
            "1FOO = 1;",
        ),
        (
            "service-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            service 1Bad {
            }
            "#,
            "service 1Bad {",
        ),
        (
            "oneof-digit-start",
            r#"
            syntax = "proto3";
            package novarocks.bad;
            message Bad {
              oneof 1kind {
              }
            }
            "#,
            "oneof 1kind {",
        ),
    ] {
        let err = match parse_proto_schema("idl/novarocks/bad.proto", input) {
            Ok(_) => panic!("{name} should fail"),
            Err(err) => err,
        };
        assert!(err.contains("idl/novarocks/bad.proto"), "{name}: {err}");
        assert!(err.contains(expected_statement), "{name}: {err}");
    }
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_version_drift() {
    let mut baseline = test_proto_schema(vec![], vec![], vec![]);
    let mut current = baseline.clone();
    baseline.version = 0;
    current.version = 2;

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "current proto schema version must be 1",
            "baseline proto schema version must be 1",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_package_drift() {
    let baseline = test_proto_schema_with_files(vec![(
        "idl/novarocks/service.proto",
        test_proto_file("novarocks.baseline", vec![], vec![], vec![]),
    )]);
    let current = test_proto_schema_with_files(vec![(
        "idl/novarocks/service.proto",
        test_proto_file("novarocks.current", vec![], vec![], vec![]),
    )]);

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "package changed from novarocks.baseline to novarocks.current",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_baseline_file_missing_in_current() {
    let baseline = test_proto_schema_with_files(vec![(
        "idl/novarocks/service.proto",
        test_proto_file("novarocks.test", vec![], vec![], vec![]),
    )]);
    let current = test_proto_schema_with_files(vec![]);

    assert_proto_schema_comparator_rejects(current, baseline, "file removed");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_file_as_baseline_stale() {
    let baseline = test_proto_schema_with_files(vec![]);
    let current = test_proto_schema_with_files(vec![(
        "idl/novarocks/new.proto",
        test_proto_file("novarocks.test", vec![], vec![], vec![]),
    )]);

    assert_proto_schema_comparator_rejects(current, baseline, "new file is missing from baseline");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_new_file_with_service_as_unsafe() {
    let baseline = test_proto_schema_with_files(vec![]);
    let current = test_proto_schema_with_files(vec![(
        "idl/novarocks/admin.proto",
        test_proto_file(
            "novarocks.admin",
            vec![],
            vec![],
            vec![(
                "AdminGrpc",
                test_proto_service(vec![(
                    "Reload",
                    test_proto_rpc("ReloadRequest", "ReloadResponse"),
                )]),
            )],
        ),
    )]);

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "new file is missing from baseline",
            "service AdminGrpc new service is not allowed",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_baseline_message_missing_in_current() {
    let baseline = test_proto_schema(
        vec![("SubmitFragmentRequest", test_proto_message(vec![]))],
        vec![],
        vec![],
    );
    let current = test_proto_schema(vec![], vec![], vec![]);

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "message SubmitFragmentRequest removed",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_message_as_baseline_stale() {
    let baseline = test_proto_schema(vec![], vec![], vec![]);
    let current = test_proto_schema(
        vec![("SubmitFragmentRequest", test_proto_message(vec![]))],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "new message is missing from baseline",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_field_as_baseline_stale() {
    let baseline = test_proto_schema(
        vec![("SubmitFragmentRequest", test_proto_message(vec![]))],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(3, "fragment_plan", "bytes")]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "new field is missing from baseline");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_label_drift() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field_with_label(
                2, "plan", "PlanNode", "repeated",
            )]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field label change");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_oneof_drift() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field_with_oneof(
                2, "plan", "PlanNode", "payload",
            )]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field oneof change");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_missing_message_reserved_retention() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[7], &["old_plan"]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![("SubmitFragmentRequest", test_proto_message(vec![]))],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "reserved number 7 removed from current schema",
            "reserved name old_plan removed from current schema",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_reusing_baseline_reserved_number_or_name() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[7], &["old_plan"]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(
                vec![
                    test_proto_field(7, "fragment_plan", "bytes"),
                    test_proto_field(8, "old_plan", "bytes"),
                ],
                &[7],
                &["old_plan"],
            ),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "uses baseline reserved number 7",
            "uses baseline reserved name old_plan",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_type_change() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanFragment")]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field type change");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_rename() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "query_plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field rename");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_field_number_reuse() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "scan_node", "ScanNode")]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "field number reuse");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_deleted_field_without_reserved_number() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[], &["plan"]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "reserved number");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_deleted_field_without_reserved_name() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[2], &[]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "reserved name");
}

#[test]
fn nidl_d3b_proto_schema_comparator_accepts_deleted_field_with_reserved_number_and_name() {
    let baseline = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[2], &["plan"]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_comparator_accepts(current, baseline);
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_preserves_reserved_deleted_field_history() {
    let existing = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(
                vec![test_proto_field(3, "fragment_plan", "bytes")],
                &[2],
                &["plan"],
            ),
        )],
        vec![],
        vec![],
    );

    let merged =
        merge_proto_schema_baseline(&current, &existing).expect("baseline merge should succeed");
    let merged_message =
        &merged.files["idl/novarocks/test.proto"].messages["SubmitFragmentRequest"];

    assert_eq!(merged_message.fields[&2].name, "plan");
    assert_eq!(merged_message.fields[&3].name, "fragment_plan");
    assert_proto_schema_comparator_accepts(current, merged);
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_deleted_field_without_reserved_name() {
    let existing = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message(vec![test_proto_field(2, "plan", "PlanNode")]),
        )],
        vec![],
        vec![],
    );
    let current = test_proto_schema(
        vec![(
            "SubmitFragmentRequest",
            test_proto_message_with_reserved(vec![], &[2], &[]),
        )],
        vec![],
        vec![],
    );

    assert_proto_schema_baseline_merge_rejects(current, existing, "without reserved name plan");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_zero_value_drift() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNKNOWN"), (1, "OK")]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "enum zero value");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_rpc_signature_change() {
    let baseline = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequestV2", "FetchResultResponse"),
            )]),
        )],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "rpc signature");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_new_service() {
    let baseline = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![
            (
                "NovaRocksGrpc",
                test_proto_service(vec![(
                    "FetchResult",
                    test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
                )]),
            ),
            (
                "AdminGrpc",
                test_proto_service(vec![(
                    "Reload",
                    test_proto_rpc("ReloadRequest", "ReloadResponse"),
                )]),
            ),
        ],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "new service");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_deletion() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(vec![], vec![], vec![]);

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "enum FetchResultResponse.Status removed",
    );
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_enum_value_deletion() {
    let existing = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED")]),
        )],
        vec![],
    );

    assert_proto_schema_baseline_merge_rejects(current, existing, "value STATUS_OK=1 removed");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_renumber() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (2, "STATUS_OK")]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "renumbered from #1 to #2");
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_rename() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_DONE")]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "renamed from STATUS_OK to STATUS_DONE",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_enum_value_as_baseline_stale() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED")]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED"), (1, "STATUS_OK")]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects(
        current,
        baseline,
        "new enum value is missing from baseline",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_enum_reserved_retention_or_reuse() {
    let baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum_with_reserved(vec![(0, "STATUS_UNSPECIFIED")], &[2], &["STATUS_OLD"]),
        )],
        vec![],
    );
    let current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![
                (0, "STATUS_UNSPECIFIED"),
                (2, "STATUS_REUSED_NUMBER"),
                (3, "STATUS_OLD"),
            ]),
        )],
        vec![],
    );

    assert_proto_schema_comparator_rejects_all(
        current,
        baseline,
        &[
            "reserved number 2 removed from current schema",
            "reserved name STATUS_OLD removed from current schema",
            "uses baseline reserved number 2",
            "uses baseline reserved name STATUS_OLD",
        ],
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_rpc_deletion() {
    let baseline = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![("NovaRocksGrpc", test_proto_service(vec![]))],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "rpc FetchResult removed");
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_service_deletion() {
    let existing = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(vec![], vec![], vec![]);

    assert_proto_schema_baseline_merge_rejects(current, existing, "service NovaRocksGrpc removed");
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_rpc_deletion() {
    let existing = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![("NovaRocksGrpc", test_proto_service(vec![]))],
    );

    assert_proto_schema_baseline_merge_rejects(current, existing, "rpc FetchResult removed");
}

#[test]
fn nidl_d3b_proto_schema_baseline_merge_rejects_new_file_with_service() {
    let existing = test_proto_schema_with_files(vec![]);
    let current = test_proto_schema_with_files(vec![(
        "idl/novarocks/admin.proto",
        test_proto_file(
            "novarocks.admin",
            vec![],
            vec![],
            vec![(
                "AdminGrpc",
                test_proto_service(vec![(
                    "Reload",
                    test_proto_rpc("ReloadRequest", "ReloadResponse"),
                )]),
            )],
        ),
    )]);

    assert_proto_schema_baseline_merge_rejects(
        current,
        existing,
        "service AdminGrpc new service is not allowed",
    );
}

#[test]
fn nidl_d3b_proto_schema_comparator_rejects_current_new_rpc_as_baseline_stale() {
    let baseline = test_proto_schema(
        vec![],
        vec![],
        vec![("NovaRocksGrpc", test_proto_service(vec![]))],
    );
    let current = test_proto_schema(
        vec![],
        vec![],
        vec![(
            "NovaRocksGrpc",
            test_proto_service(vec![(
                "FetchResult",
                test_proto_rpc("FetchResultRequest", "FetchResultResponse"),
            )]),
        )],
    );

    assert_proto_schema_comparator_rejects(current, baseline, "new rpc is missing from baseline");
}

#[test]
fn nidl_d3b_proto_schema_comparator_returns_stable_sorted_deduped_violations() {
    let mut baseline = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![(0, "STATUS_UNSPECIFIED")]),
        )],
        vec![],
    );
    let mut current = test_proto_schema(
        vec![],
        vec![(
            "FetchResultResponse.Status",
            test_proto_enum(vec![
                (0, "STATUS_UNSPECIFIED"),
                (1, "STATUS_DUP"),
                (1, "STATUS_DUP"),
            ]),
        )],
        vec![],
    );
    baseline.version = 0;
    current.version = 2;

    let violations = compare_proto_schema_to_baseline(&current, &baseline);
    let mut sorted_deduped = violations.clone();
    sorted_deduped.sort();
    sorted_deduped.dedup();

    assert_eq!(violations, sorted_deduped);
    assert_eq!(
        violations
            .iter()
            .filter(|violation| violation.contains("STATUS_DUP=1 baseline stale"))
            .count(),
        1,
        "expected duplicate enum-value violations to be deduped, got: {violations:?}"
    );
    assert!(
        violations[0].starts_with("baseline proto schema version must be 1"),
        "expected sorted output, got: {violations:?}"
    );
}

// ---------------------------------------------------------------------------
// NIDL-E0: non-compat StarRocks-IDL ledger guard
// ---------------------------------------------------------------------------
//
// Goal (see specs/2026-07-07-nidl-e0-noncompat-idl-ledger-guard-design):
// the non-compat compile graph must eventually contain zero references to
// StarRocks IDL (`crate::thrift`, `crate::proto::starrocks`,
// `crate::proto::staros`). This guard is a conservative lexical scan: it strips
// test-only items and directly compat-only items, but keeps ambiguous cfg
// expressions scanned. It accounts for the current production-code references
// via a shrink-only ledger; milestones E1..E9 remove ledger entries as clusters
// are cleaned, and E10 empties the ledger and adds the build.rs / lib.rs gate
// assertions.

#[test]
fn nidl_e7_result_path_uses_native_result_batch_and_primitive_types() {
    let repo = Path::new(manifest_dir());
    let guarded = [
        "src/common/types.rs",
        "src/common/util.rs",
        "src/runtime/result_buffer.rs",
        "src/service/result_batch_wire.rs",
        "src/exec/operators/result_buffer_sink.rs",
    ];
    let mut violations = Vec::new();

    for source in guarded {
        let text = fs::read_to_string(repo.join(source)).unwrap();
        let production = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            source,
            &production,
            &[
                "crate::thrift",
                "TResultBatch",
                "TPrimitiveType",
                "TResultSinkType",
                "TResultSinkFormatType",
                "exprs::TExpr",
                "data_sinks::",
                "types::T",
                "crate::types::arrow_thrift",
            ],
            "E7 result execution path must use native result batch, primitive tags, and sink config",
        );
    }

    let native_fragment_wire =
        fs::read_to_string(repo.join("src/runtime/native_fragment_wire.rs")).unwrap();
    let native_fragment_wire = rust_production_text_without_cfg_test(&native_fragment_wire);
    push_forbidden_terms(
        &mut violations,
        "src/runtime/native_fragment_wire.rs",
        &native_fragment_wire,
        &[
            "pub(crate) type ResultSinkType =",
            "TResultSinkType",
            "TResultSinkFormatType",
        ],
        "E7 native fragment wire must not expose thrift result-sink aliases",
    );

    let arrow_thrift = fs::read_to_string(repo.join("src/types/arrow_thrift.rs")).unwrap();
    let arrow_thrift = rust_production_text_without_cfg_test(&arrow_thrift);
    push_forbidden_terms(
        &mut violations,
        "src/types/arrow_thrift.rs",
        &arrow_thrift,
        &[
            "fn logical_type_to_primitive",
            "fn field_logical_primitive",
            "fn arrow_field_to_primitive",
            "fn arrow_type_to_primitive",
            "fn thrift_node_to_primitive",
            "fn thrift_desc_to_primitive",
        ],
        "Arrow/native primitive helpers must live outside thrift type descriptors",
    );

    let common_thrift = fs::read_to_string(repo.join("src/common/thrift.rs")).unwrap();
    let common_thrift = rust_production_text_without_cfg_test(&common_thrift);
    push_forbidden_terms(
        &mut violations,
        "src/common/thrift.rs",
        &common_thrift,
        &[
            "crate::thrift::data",
            "TResultBatch",
            "thrift_serialize_result_batch",
        ],
        "generic thrift helpers must not know the result-batch runtime model",
    );

    assert!(
        violations.is_empty(),
        "NIDL-E7 native result-batch/primitive guard failed:\n{}",
        violations.join("\n")
    );
}

const NIDL_E0_LEDGER_PATH: &str = "tests/nidl_noncompat_idl_ledger.txt";

/// Files/prefixes already gated to `#[cfg(feature = "compat")]` (or expected to
/// be, and verified elsewhere). The lexical scan skips these so the ledger only
/// tracks the non-compat mainline. Milestones E2/E9 append entries here as
/// modules are gated (e.g. "src/lower/compat").
const NIDL_E0_COMPAT_SCOPE: &[&str] = &[
    "src/connector/iceberg/file_pruning_wire.rs",
    "src/connector/starrocks",
    "src/connector/schema/fe_tables.rs",
    "src/connector/schema/frontend.rs",
    "src/connector/schema/load_tracking_logs.rs",
    "src/connector/schema/loads.rs",
    "src/formats/starrocks",
    "src/exec/chunk/schema_thrift.rs",
    "src/exec/node/fetch.rs",
    "src/exec/operators/fetch_processor.rs",
    "src/lower/compat",
    "src/runtime/descriptor_snapshot_thrift.rs",
    "src/runtime/exec_params.rs",
    "src/runtime/exec_params_compat.rs",
    "src/runtime/sink_commit_wire.rs",
    "src/runtime/write_coordinator_compat.rs",
    "src/service/backend_service.rs",
    "src/service/heartbeat_service.rs",
    "src/service/internal_service.rs",
    "src/service/internal_rpc_client.rs",
    "src/service/stream_load.rs",
    "src/service/stream_load_http.rs",
    "src/service/engine_ffi.rs",
    "src/service/compat.rs",
    "src/service/disk_report.rs",
    "src/service/exec_state_reporter.rs",
    "src/service/exec_status_report.rs",
    "src/service/fe_report_compat.rs",
    "src/service/frontend_rpc.rs",
    "src/service/stream_load_registry.rs",
    "src/sql/codegen/iceberg_write_sink_wire.rs",
    "src/sql/codegen/descriptors.rs",
    "src/sql/codegen/expr_compiler.rs",
    "src/sql/codegen/iceberg_change_stream_router_wire.rs",
    "src/sql/codegen/ir/lowering.rs",
    "src/sql/codegen/nodes.rs",
    "src/sql/codegen/type_infer.rs",
    "src/types/arrow_thrift.rs",
];

fn nidl_e0_starrocks_idl_terms() -> &'static [&'static str] {
    &[
        "crate::thrift",
        "crate::proto::starrocks",
        "crate::proto::staros",
    ]
}

fn nidl_e0_is_in_compat_scope(rel_path: &str) -> bool {
    NIDL_E0_COMPAT_SCOPE
        .iter()
        .any(|prefix| rel_path == *prefix || rel_path.starts_with(&format!("{prefix}/")))
}

/// Collect `.rs` files under `dir` whose production code (test modules and
/// direct compat-only items stripped) references any StarRocks IDL term. Returned
/// sorted; no compat-scope filtering (that is applied by
/// `nidl_e0_current_offenders`).
fn nidl_e0_offenders_in(dir: &Path) -> Vec<PathBuf> {
    let mut offenders = Vec::new();
    for path in rs_files(dir) {
        let text = fs::read_to_string(&path).unwrap_or_default();
        let production = rust_production_text_without_cfg_test_or_compat(&text);
        let has_hit = production.lines().any(|line| {
            !is_comment_or_blank(line)
                && nidl_e0_starrocks_idl_terms()
                    .iter()
                    .any(|term| line.contains(*term))
        });
        if has_hit {
            offenders.push(path);
        }
    }
    offenders.sort();
    offenders
}

/// Repo-relative paths of non-compat production files still referencing
/// StarRocks IDL (compat-scope excluded).
fn nidl_e0_current_offenders() -> Vec<String> {
    let mut out = Vec::new();
    for path in nidl_e0_offenders_in(&src_dir()) {
        let rel_path = rel(&path);
        if nidl_e0_is_in_compat_scope(&rel_path) {
            continue;
        }
        out.push(rel_path);
    }
    out.sort();
    out.dedup();
    out
}

fn nidl_e0_read_ledger() -> Vec<String> {
    let path = Path::new(manifest_dir()).join(NIDL_E0_LEDGER_PATH);
    let text = fs::read_to_string(&path).unwrap_or_default();
    let mut entries: Vec<String> = text
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    entries.sort();
    entries.dedup();
    entries
}

#[test]
fn nidl_e0_detector_flags_starrocks_idl_and_ignores_native_and_tests() {
    let dir = std::env::temp_dir().join("nidl_e0_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("offender_thrift.rs"),
        "use crate::thrift::types::TUniqueId;\n",
    )
    .unwrap();
    fs::write(
        dir.join("offender_proto.rs"),
        "let _ = crate::proto::starrocks::StatusPb::default();\n",
    )
    .unwrap();
    fs::write(
        dir.join("native.rs"),
        "use crate::proto::plan::PlanFragment;\n",
    )
    .unwrap();
    fs::write(
        dir.join("test_only.rs"),
        "#[cfg(test)]\nmod tests {\n    use crate::thrift::types::TUniqueId;\n}\n",
    )
    .unwrap();

    let offenders = nidl_e0_offenders_in(&dir);
    let names: Vec<String> = offenders
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(
        names.iter().any(|n| n == "offender_thrift.rs"),
        "must flag crate::thrift; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "offender_proto.rs"),
        "must flag crate::proto::starrocks; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "native.rs"),
        "must ignore native crate::proto::plan; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "test_only.rs"),
        "must ignore #[cfg(test)] module references; got {names:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e6_ledger_detector_ignores_compat_cfg_items() {
    let dir = std::env::temp_dir().join("nidl_e6_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("compat_fn.rs"),
        "#[cfg(feature = \"compat\")]\nfn compat_only() {\n    let _ = crate::thrift::types::TUniqueId::new(1, 2);\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_multiline_fn.rs"),
        "#[cfg(feature = \"compat\")]\nfn compat_multiline(\n    _id: i32,\n) -> crate::thrift::types::TUniqueId {\n    crate::thrift::types::TUniqueId::new(1, 2)\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_mod.rs"),
        "#[cfg(feature = \"compat\")]\nmod compat_only {\n    use crate::proto::starrocks;\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("offender.rs"),
        "fn default_build_offender() {\n    let _ = crate::thrift::types::TUniqueId::new(3, 4);\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_not.rs"),
        "#[cfg(not(feature = \"compat\"))]\nfn non_compat_offender() {\n    let _ = crate::thrift::types::TUniqueId::new(7, 8);\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_any.rs"),
        "#[cfg(any(feature = \"compat\", unix))]\nfn maybe_default_offender() {\n    let _ = crate::thrift::types::TUniqueId::new(9, 10);\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("comma_then_offender.rs"),
        "enum Demo {\n    #[cfg(feature = \"compat\")]\n    CompatVariant(crate::thrift::types::TUniqueId),\n}\nfn offender_after_comma() {\n    let _ = crate::thrift::types::TUniqueId::new(5, 6);\n}\n",
    )
    .unwrap();

    let offenders = nidl_e0_offenders_in(&dir);
    let names: Vec<String> = offenders
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    assert!(
        names.iter().any(|n| n == "offender.rs"),
        "must keep default-build offenders; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "compat_not.rs"),
        "must keep not(feature = \"compat\") offenders; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "compat_any.rs"),
        "must keep ambiguous any(feature = \"compat\", ...) offenders; got {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "comma_then_offender.rs"),
        "must keep default-build offenders after compat comma items; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "compat_fn.rs"),
        "must ignore compat-only functions; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "compat_multiline_fn.rs"),
        "must ignore multiline compat-only functions; got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "compat_mod.rs"),
        "must ignore compat-only modules; got {names:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e0_noncompat_starrocks_idl_stays_within_ledger() {
    let offenders = nidl_e0_current_offenders();
    let ledger = nidl_e0_read_ledger();

    assert!(
        ledger.is_empty(),
        "NIDL-E10 final ledger must stay empty; remove these stale entries:\n{}",
        ledger.join("\n")
    );
    assert!(
        offenders.is_empty(),
        "NIDL-E10 non-compat production code must not reference StarRocks IDL:\n{}",
        offenders.join("\n")
    );
}

fn nidl_e9_module_has_compat_cfg(module_file: &Path, module_name: &str) -> bool {
    let Ok(text) = fs::read_to_string(module_file) else {
        return false;
    };
    let mut previous_non_blank = "";
    let mut in_block_comment = false;
    let target = format!("mod {module_name};");
    for line in text.lines() {
        let trimmed = line.trim();
        if nidl_e9_is_comment_or_blank_line(trimmed, &mut in_block_comment) {
            continue;
        }
        if trimmed == target
            || trimmed == format!("pub(crate) mod {module_name};")
            || trimmed == format!("pub mod {module_name};")
        {
            return previous_non_blank == "#[cfg(feature = \"compat\")]";
        }
        previous_non_blank = trimmed;
    }
    false
}

fn nidl_e9_file_is_cfg_compat_module(path: &Path) -> bool {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(parent) = path.parent() else {
        return false;
    };

    if file_name == "mod.rs" {
        let Some(module_name) = parent.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        let Some(parent_parent) = parent.parent() else {
            return false;
        };
        return nidl_e9_module_has_compat_cfg(&parent_parent.join("mod.rs"), module_name);
    }

    let Some(module_name) = path.file_stem().and_then(|name| name.to_str()) else {
        return false;
    };
    nidl_e9_module_has_compat_cfg(&parent.join("mod.rs"), module_name)
}

fn nidl_e9_is_comment_or_blank_line(trimmed: &str, in_block_comment: &mut bool) -> bool {
    if *in_block_comment {
        if trimmed.contains("*/") {
            *in_block_comment = false;
        }
        return true;
    }

    if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with('*') {
        return true;
    }

    if trimmed.starts_with("/*") {
        if !trimmed.contains("*/") {
            *in_block_comment = true;
        }
        return true;
    }

    false
}

fn nidl_e9_noncompat_lower_compat_import_hits_in(root: &Path) -> Vec<String> {
    let mut hits = Vec::new();
    for path in rs_files(root) {
        let rel_path = rel(&path);
        if nidl_e9_is_lower_compat_scope(&rel_path) {
            continue;
        }
        if nidl_e9_file_is_cfg_compat_module(&path) {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap_or_default();
        let production = nidl_e9_rust_production_text_without_cfg_test(&text);
        let mut in_block_comment = false;
        for (idx, line) in production.lines().enumerate() {
            let trimmed = line.trim();
            if nidl_e9_is_comment_or_blank_line(trimmed, &mut in_block_comment) {
                continue;
            }
            if line.contains("crate::lower::compat") || line.contains("lower::compat") {
                hits.push(format!("{rel_path}:{}:{line}", idx + 1));
            }
        }
    }
    hits.sort();
    hits
}

fn nidl_e9_is_lower_compat_scope(rel_path: &str) -> bool {
    rel_path == "src/lower/compat" || rel_path.starts_with("src/lower/compat/")
}

fn nidl_e9_noncompat_lower_compat_import_hits() -> Vec<String> {
    nidl_e9_noncompat_lower_compat_import_hits_in(&src_dir())
}

fn nidl_e9_is_lower_compat_type_lowering_hit(hit: &str) -> bool {
    hit.contains("crate::lower::compat::type_lowering")
        || hit.contains("lower::compat::type_lowering")
}

fn nidl_e9_read(rel_path: &str) -> String {
    fs::read_to_string(Path::new(manifest_dir()).join(rel_path))
        .unwrap_or_else(|err| panic!("read {rel_path}: {err}"))
}

fn nidl_e9_text_region_between<'a>(text: &'a str, start: &str, end: &str) -> &'a str {
    let start_idx = text
        .find(start)
        .unwrap_or_else(|| panic!("missing start marker `{start}`"));
    let after_start = &text[start_idx..];
    let end_idx = after_start
        .find(end)
        .unwrap_or_else(|| panic!("missing end marker `{end}` after `{start}`"));
    &after_start[..end_idx]
}

fn nidl_e10_without_explicit_compat_regions(text: &str) -> String {
    let mut out = String::new();
    let mut in_region = false;
    for line in text.lines() {
        if line.contains("// NIDL-E10 compat-only generated IDL/codegen start")
            || line.contains("// NIDL-E10 compat-only Rust proto codegen start")
        {
            in_region = true;
            continue;
        }
        if line.contains("// NIDL-E10 compat-only generated IDL/codegen end")
            || line.contains("// NIDL-E10 compat-only Rust proto codegen end")
        {
            in_region = false;
            continue;
        }
        if !in_region {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn nidl_e10_previous_nonblank_line<'a>(lines: &'a [&'a str], idx: usize) -> Option<&'a str> {
    lines[..idx]
        .iter()
        .rev()
        .map(|line| line.trim())
        .find(|line| !line.is_empty())
}

#[test]
fn nidl_e10_build_rs_gates_compat_generated_idl() {
    let build_rs = nidl_e9_read("src/build.rs");
    let default_region = nidl_e10_without_explicit_compat_regions(&build_rs);
    for forbidden in [
        "validate_thrift_rs_namespaces();",
        "resolve_thirdparty_root(&manifest_dir)",
        "let thrift_rs_cmd = find_tool(\"thrift\", &tp_bin);",
        "patch_plan_nodes_rs(&thrift_rs_out);",
        "thrift_root_mod.rs",
        "let starrocks_protos = [",
        "compile_protos(&starrocks_protos",
        "let staros_protos = [",
        "compile_protos(&staros_protos",
    ] {
        assert!(
            !default_region.contains(forbidden),
            "default build.rs path must not contain compat generated IDL/codegen `{forbidden}`"
        );
    }
}

#[test]
fn nidl_e10_proto_root_only_exposes_starrocks_and_staros_for_compat() {
    let build_rs = nidl_e9_read("src/build.rs");
    let emit_region = nidl_e9_text_region_between(&build_rs, "fn emit_proto_root_mod", "fn main()");
    assert!(
        emit_region.contains("fn emit_proto_root_mod(out_dir: &Path, compat: bool)"),
        "emit_proto_root_mod must accept compat so generated proto root can hide compat modules in default builds"
    );
    let default_region = nidl_e10_without_explicit_compat_regions(emit_region);
    for forbidden in ["pub mod starrocks", "pub mod staros"] {
        assert!(
            !default_region.contains(forbidden),
            "proto_root_mod default wrapper must not expose `{forbidden}`"
        );
    }
}

#[test]
fn nidl_e10_lib_only_includes_thrift_root_for_compat() {
    let lib_rs = nidl_e9_read("src/lib.rs");
    let lines = lib_rs.lines().collect::<Vec<_>>();
    let include_idx = lines
        .iter()
        .position(|line| line.contains("thrift_root_mod.rs"))
        .expect("src/lib.rs must contain thrift_root_mod include for compat builds");
    assert_eq!(
        nidl_e10_previous_nonblank_line(&lines, include_idx),
        Some("#[cfg(feature = \"compat\")]"),
        "src/lib.rs must cfg-gate thrift_root_mod.rs include to compat builds"
    );
}

#[test]
fn nidl_e9_native_fragment_wire_has_no_starrocks_thrift_aliases() {
    let text = nidl_e9_read("src/runtime/native_fragment_wire.rs");
    let production = rust_production_text_without_cfg_test_or_compat(&text);
    for forbidden in [
        "type DataStreamSink = data_sinks::TDataStreamSink",
        "type MultiCastDataStreamSink = data_sinks::TMultiCastDataStreamSink",
        "type DataPartition = partitions::TDataPartition",
        "crate::thrift::{data_sinks, partitions, types}",
    ] {
        assert!(
            !production.contains(forbidden),
            "native_fragment_wire must not keep StarRocks thrift alias `{forbidden}`"
        );
    }
}

#[test]
fn nidl_e9_write_coordinator_uses_native_report_types() {
    let coordinator = nidl_e9_read("src/runtime/write_coordinator.rs");
    let coordinator_region = nidl_e9_text_region_between(
        &coordinator,
        "pub(crate) use crate::runtime::write_report",
        "impl WriteCoordinator",
    );
    let write_report = nidl_e9_read("src/runtime/write_report.rs");
    let write_report_region = nidl_e9_text_region_between(
        &write_report,
        "pub(crate) struct WriterKey",
        "pub(crate) fn unique_id_from_native",
    );
    let region = format!("{coordinator_region}\n{write_report_region}");
    for forbidden in [
        "types::TUniqueId",
        "status::TStatus",
        "types::TSinkCommitInfo",
        "types::TTabletCommitInfo",
        "types::TTabletFailInfo",
    ] {
        assert!(
            !region.contains(forbidden),
            "write coordinator public report structs must not contain `{forbidden}`:\n{region}"
        );
    }
}

#[test]
fn nidl_e9_noncompat_startup_does_not_init_frontend_rpc() {
    let text = nidl_e9_read("src/main.rs");
    let production = rust_production_text_without_cfg_test_or_compat(&text);
    assert!(
        !production.contains("frontend_rpc::init_frontend_rpc_manager"),
        "non-compat startup must not initialize Frontend RPC manager"
    );
}

#[test]
fn nidl_e9_lower_compat_import_detector_ignores_cfg_compat_files() {
    let dir = std::env::temp_dir().join("nidl_e9_lower_compat_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("native_hit.rs"),
        "use crate::lower::compat::type_lowering::scalar_type_desc;\n",
    )
    .unwrap();
    fs::write(
        dir.join("native_generic_hit.rs"),
        "use crate::lower::compat::expr::parse_min_max_conjuncts;\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_only.rs"),
        "#[cfg(feature = \"compat\")]\nfn compat_only() { let _ = crate::lower::compat::type_lowering::scalar_type_desc; }\n",
    )
    .unwrap();
    fs::write(
        dir.join("mod.rs"),
        "#[cfg(feature = \"compat\")]\npub(crate) mod compat_module;\n",
    )
    .unwrap();
    fs::write(
        dir.join("compat_module.rs"),
        "use crate::lower::compat::fragment::execute_fragment;\n",
    )
    .unwrap();
    fs::write(
        dir.join("comment_note.rs"),
        "// This note mentions crate::lower::compat::type_lowering but is not production code.\n",
    )
    .unwrap();
    fs::write(
        dir.join("block_comment_note.rs"),
        "/* This note mentions crate::lower::compat::type_lowering but is not production code. */\n",
    )
    .unwrap();
    fs::write(
        dir.join("multiline_block_comment_note.rs"),
        "/*\n * This note mentions lower::compat::type_lowering but is not production code.\n */\n",
    )
    .unwrap();

    let hits = nidl_e9_noncompat_lower_compat_import_hits_in(&dir);
    assert!(
        hits.iter().any(|hit| hit.contains("native_hit.rs")),
        "must report default-build lower::compat::type_lowering imports: {hits:?}"
    );
    assert!(
        hits.iter().any(|hit| hit.contains("native_generic_hit.rs")),
        "must report default-build lower::compat imports: {hits:?}"
    );
    assert!(
        hits.iter().any(|hit| hit.contains("compat_only.rs")),
        "must report item-level cfg(feature=\"compat\") lower::compat imports in non-compat files: {hits:?}"
    );
    assert!(
        !hits.iter().any(|hit| hit.contains("compat_module.rs")),
        "must ignore lower::compat imports from modules declared cfg(feature=\"compat\"): {hits:?}"
    );
    assert!(
        !hits.iter().any(|hit| hit.contains("comment_note.rs")),
        "must ignore commented lower::compat::type_lowering mentions: {hits:?}"
    );
    assert!(
        !hits.iter().any(|hit| hit.contains("block_comment_note.rs")),
        "must ignore block-commented lower::compat::type_lowering mentions: {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("multiline_block_comment_note.rs")),
        "must ignore multiline block-commented lower::compat::type_lowering mentions: {hits:?}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e9_lower_compat_scope_only_skips_lower_compat_implementation() {
    assert!(nidl_e9_is_lower_compat_scope("src/lower/compat"));
    assert!(nidl_e9_is_lower_compat_scope(
        "src/lower/compat/node/hdfs_scan.rs"
    ));
    assert!(
        !nidl_e9_is_lower_compat_scope("src/service/compat.rs"),
        "E9 must not inherit E0 service compat scope"
    );
    assert!(
        nidl_e9_file_is_cfg_compat_module(
            &Path::new(manifest_dir()).join("src/service/internal_service.rs")
        ),
        "E9 may ignore service/internal_service.rs only because its module declaration is cfg(feature=\"compat\")"
    );
    assert!(
        !nidl_e9_is_lower_compat_scope("src/connector/starrocks/lake/schema_change.rs"),
        "E9 must report connector lower::compat imports"
    );
    assert!(
        !nidl_e9_is_lower_compat_scope("src/exec/chunk/schema_thrift.rs"),
        "E9 must report exec/chunk lower::compat imports"
    );
}

#[test]
fn nidl_e9_native_codegen_does_not_import_lower_compat_type_lowering() {
    let hits: Vec<String> = nidl_e9_noncompat_lower_compat_import_hits()
        .into_iter()
        .filter(|hit| {
            hit.contains("src/sql/codegen/")
                || hit.contains("src/runtime/")
                || hit.contains("src/formats/parquet/")
        })
        .filter(|hit| nidl_e9_is_lower_compat_type_lowering_hit(hit))
        .collect();
    assert!(
        hits.is_empty(),
        "native codegen/runtime must not import lower::compat type lowering helpers:\n{}",
        hits.join("\n")
    );
}

#[test]
fn nidl_e9_noncompat_paths_do_not_import_lower_compat() {
    let hits = nidl_e9_noncompat_lower_compat_import_hits();
    assert!(
        hits.is_empty(),
        "non-compat paths must not import lower::compat:\n{}",
        hits.join("\n")
    );
}

#[test]
fn nidl_e9_lower_compat_module_is_cfg_gated() {
    let module_file = Path::new(manifest_dir()).join("src/lower/mod.rs");
    assert!(
        nidl_e9_module_has_compat_cfg(&module_file, "compat"),
        "src/lower/mod.rs must gate lower::compat with #[cfg(feature = \"compat\")]"
    );
}

#[test]
fn nidl_e9_guard_helpers_find_text_regions() {
    let text = "alpha\nstart\nbody\nend\nomega\n";
    assert_eq!(
        nidl_e9_text_region_between(text, "start", "end"),
        "start\nbody\n"
    );
}

#[test]
#[should_panic(expected = "missing start marker `start`")]
fn nidl_e9_guard_helpers_panic_when_text_region_start_is_missing() {
    let text = "alpha\nbody\nend\nomega\n";
    let _ = nidl_e9_text_region_between(text, "start", "end");
}

#[test]
#[should_panic(expected = "missing end marker `end` after `start`")]
fn nidl_e9_guard_helpers_panic_when_text_region_end_is_missing() {
    let text = "alpha\nstart\nbody\nomega\n";
    let _ = nidl_e9_text_region_between(text, "start", "end");
}

#[test]
fn nidl_e9_guard_helpers_ignore_block_comments_between_cfg_and_module() {
    let dir = std::env::temp_dir().join("nidl_e9_module_cfg_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let file = dir.join("mod_fixture.rs");
    fs::write(
        &file,
        "#[cfg(feature = \"compat\")]\n/* compatibility module note */\npub(crate) mod compat_only;\n",
    )
    .unwrap();

    assert!(
        nidl_e9_module_has_compat_cfg(&file, "compat_only"),
        "must ignore block comments between cfg(feature=\"compat\") and module declarations"
    );
    let _ = fs::remove_dir_all(&dir);
}

fn nidl_e1_native_mv_starrocks_table_import_hits() -> Vec<String> {
    nidl_e1_native_mv_starrocks_table_import_hits_in(&[
        Path::new(manifest_dir()).join("src/exec"),
        Path::new(manifest_dir()).join("src/engine"),
        Path::new(manifest_dir()).join("src/sql"),
    ])
}

fn nidl_e1_native_mv_starrocks_table_import_hits_in(roots: &[PathBuf]) -> Vec<String> {
    let forbidden = [
        "crate::connector::starrocks::table::state_codec",
        "crate::connector::starrocks::table::aggregate_sql_calls",
        "crate::connector::starrocks::table::mv_agg_state",
        "crate::connector::starrocks::table::mv_shape",
        "crate::connector::starrocks::table::model::IcebergTableRef",
    ];
    let grouped_root = "crate::connector::starrocks::table::{";
    let grouped_terms = [
        "state_codec",
        "aggregate_sql_calls",
        "mv_agg_state",
        "mv_shape",
        "model::IcebergTableRef",
    ];

    let mut hits = Vec::new();
    for root in roots {
        for path in rs_files(root) {
            for (line, text) in non_test_line_hits(&path, |source| {
                forbidden.iter().any(|needle| source.contains(needle))
            }) {
                hits.push(format!("{}:{line}: {text}", rel(&path)));
            }
            let text = fs::read_to_string(&path).unwrap_or_default();
            let production = rust_production_text_without_cfg_test(&text);
            let compact: String = non_comment_trimmed_lines(&production).join("");
            let mut search_start = 0usize;
            while let Some(offset) = compact[search_start..].find(grouped_root) {
                let start = search_start + offset;
                let span = &compact[start..];
                let end = span.find(';').unwrap_or(span.len());
                let import_span = &span[..end];
                if let Some(term) = grouped_terms
                    .iter()
                    .find(|term| import_span.contains(**term))
                {
                    hits.push(format!(
                        "{}:1: grouped import references connector::starrocks::table::{term}",
                        rel(&path)
                    ));
                }
                search_start = start + grouped_root.len();
            }
        }
    }
    hits.sort();
    hits
}

#[test]
fn nidl_e1_detector_flags_grouped_imports_and_ignores_tests() {
    let dir = std::env::temp_dir().join("nidl_e1_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("grouped.rs"),
        "use crate::connector::starrocks::table::{\n    mv_shape::AggregateMvShape,\n    state_codec::decode_count_state,\n};\n",
    )
    .unwrap();
    fs::write(
        dir.join("test_only.rs"),
        "#[cfg(test)]\nmod tests {\n    use crate::connector::starrocks::table::state_codec;\n}\n",
    )
    .unwrap();

    let hits = nidl_e1_native_mv_starrocks_table_import_hits_in(&[dir.clone()]);
    assert!(
        hits.iter().any(|hit| hit.contains("grouped.rs")),
        "must flag grouped StarRocks table helper imports; got {hits:?}"
    );
    assert!(
        !hits.iter().any(|hit| hit.contains("test_only.rs")),
        "must ignore #[cfg(test)] imports; got {hits:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e1_native_mv_codecs_do_not_import_starrocks_table_modules() {
    let hits = nidl_e1_native_mv_starrocks_table_import_hits();
    assert!(
        hits.is_empty(),
        "native MV/aggregate code must import native agg_state/table_ref modules, not connector::starrocks::table helpers:\n{}",
        hits.join("\n")
    );
}

#[test]
fn nidl_e3_planner_ir_uses_native_partition_and_runtime_filter_types() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    for source in [
        "src/sql/planner/distributed_fragment.rs",
        "src/sql/planner/distributed_node.rs",
        "src/sql/planner/distributed_plan_build.rs",
        "src/sql/planner/runtime_filter.rs",
        "src/sql/planner/write_plan.rs",
        "src/sql/planner/plan.rs",
    ] {
        let text = fs::read_to_string(repo.join(source)).unwrap();
        let text = rust_production_text_without_cfg_test(&text);
        push_forbidden_terms(
            &mut violations,
            source,
            &text,
            &[
                "crate::thrift",
                "thrift::",
                "TPartitionType",
                "TDataPartition",
                "TRuntimeFilterDescription",
            ],
            "planner-owned DistributedPlan IR must use native partition and runtime-filter types",
        );
    }

    let codegen_mod = fs::read_to_string(repo.join("src/sql/codegen/mod.rs")).unwrap();
    let codegen_mod = rust_production_text_without_cfg_test(&codegen_mod);
    push_forbidden_terms(
        &mut violations,
        "src/sql/codegen/mod.rs",
        &codegen_mod,
        &[
            "pub compat_output_partition:",
            "crate::thrift::runtime_filter::TRuntimeFilterDescription",
        ],
        "codegen public IR must not expose thrift partition/RF descriptor fields",
    );

    let proto_plan = fs::read_to_string(repo.join("src/sql/codegen/proto_encode/plan.rs")).unwrap();
    let proto_plan = rust_production_text_without_cfg_test(&proto_plan);
    push_forbidden_terms(
        &mut violations,
        "src/sql/codegen/proto_encode/plan.rs",
        &proto_plan,
        &["crate::thrift::partitions", "TPartitionType"],
        "native proto encoder must encode ExchangeReceiver from native DataPartition",
    );

    assert!(
        violations.is_empty(),
        "NIDL-E3 planner IR native-type guard failed:\n{}",
        violations.join("\n")
    );
}

#[derive(Clone, Copy)]
enum NidlE4CodeScanState {
    Code,
    BlockComment { depth: usize },
    String { escaped: bool },
    RawString { hashes: usize },
}

fn nidl_e4_has_code_line<F>(text: &str, mut predicate: F) -> bool
where
    F: FnMut(&str) -> bool,
{
    nidl_e4_code_line_entries(text)
        .into_iter()
        .map(|(_, line)| line)
        .any(|line| !line.is_empty() && predicate(&line))
}

fn nidl_e4_is_ident_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn nidl_e4_raw_string_start(chars: &[char], index: usize) -> Option<(usize, usize)> {
    if index > 0 && nidl_e4_is_ident_char(chars[index - 1]) {
        return None;
    }

    let mut cursor = match chars.get(index).copied()? {
        'r' => index + 1,
        'b' if chars.get(index + 1) == Some(&'r') => index + 2,
        _ => return None,
    };

    let mut hashes = 0usize;
    while chars.get(cursor) == Some(&'#') {
        hashes += 1;
        cursor += 1;
    }

    if chars.get(cursor) == Some(&'"') {
        Some((cursor - index + 1, hashes))
    } else {
        None
    }
}

fn nidl_e4_raw_string_end(chars: &[char], index: usize, hashes: usize) -> Option<usize> {
    if chars.get(index) != Some(&'"') {
        return None;
    }

    for offset in 0..hashes {
        if chars.get(index + 1 + offset) != Some(&'#') {
            return None;
        }
    }

    Some(1 + hashes)
}

fn nidl_e4_char_literal_len(chars: &[char], index: usize) -> Option<usize> {
    if chars.get(index) != Some(&'\'') {
        return None;
    }

    let mut cursor = index + 1;
    let first = chars.get(cursor).copied()?;
    if first == '\'' {
        return None;
    }

    if first == '\\' {
        cursor += 1;
        let escaped = chars.get(cursor).copied()?;
        if escaped == 'u' && chars.get(cursor + 1) == Some(&'{') {
            cursor += 2;
            while chars.get(cursor).is_some() && chars[cursor] != '}' {
                cursor += 1;
            }
            if chars.get(cursor) != Some(&'}') {
                return None;
            }
            cursor += 1;
        } else {
            cursor += 1;
        }
    } else {
        cursor += 1;
    }

    if chars.get(cursor) == Some(&'\'') {
        Some(cursor - index + 1)
    } else {
        None
    }
}

fn nidl_e4_code_line_entries(text: &str) -> Vec<(usize, String)> {
    let mut lines = Vec::new();
    let mut state = NidlE4CodeScanState::Code;

    for (idx, line) in text.lines().enumerate() {
        let chars: Vec<char> = line.chars().collect();
        let mut code = String::with_capacity(line.len());
        let mut cursor = 0usize;

        while cursor < chars.len() {
            match state {
                NidlE4CodeScanState::Code => {
                    if chars.get(cursor) == Some(&'/') && chars.get(cursor + 1) == Some(&'/') {
                        break;
                    }

                    if chars.get(cursor) == Some(&'/') && chars.get(cursor + 1) == Some(&'*') {
                        state = NidlE4CodeScanState::BlockComment { depth: 1 };
                        cursor += 2;
                        continue;
                    }

                    if let Some((len, hashes)) = nidl_e4_raw_string_start(&chars, cursor) {
                        state = NidlE4CodeScanState::RawString { hashes };
                        cursor += len;
                        continue;
                    }

                    if chars.get(cursor) == Some(&'"') {
                        state = NidlE4CodeScanState::String { escaped: false };
                        cursor += 1;
                        continue;
                    }

                    if let Some(len) = nidl_e4_char_literal_len(&chars, cursor) {
                        cursor += len;
                        continue;
                    }

                    code.push(chars[cursor]);
                    cursor += 1;
                }
                NidlE4CodeScanState::BlockComment { mut depth } => {
                    if chars.get(cursor) == Some(&'/') && chars.get(cursor + 1) == Some(&'*') {
                        depth += 1;
                        state = NidlE4CodeScanState::BlockComment { depth };
                        cursor += 2;
                    } else if chars.get(cursor) == Some(&'*') && chars.get(cursor + 1) == Some(&'/')
                    {
                        depth -= 1;
                        cursor += 2;
                        if depth == 0 {
                            state = NidlE4CodeScanState::Code;
                        } else {
                            state = NidlE4CodeScanState::BlockComment { depth };
                        }
                    } else {
                        cursor += 1;
                    }
                }
                NidlE4CodeScanState::String { mut escaped } => {
                    if escaped {
                        escaped = false;
                        state = NidlE4CodeScanState::String { escaped };
                    } else if chars[cursor] == '\\' {
                        state = NidlE4CodeScanState::String { escaped: true };
                    } else if chars[cursor] == '"' {
                        state = NidlE4CodeScanState::Code;
                    } else {
                        state = NidlE4CodeScanState::String { escaped };
                    }
                    cursor += 1;
                }
                NidlE4CodeScanState::RawString { hashes } => {
                    if let Some(len) = nidl_e4_raw_string_end(&chars, cursor, hashes) {
                        state = NidlE4CodeScanState::Code;
                        cursor += len;
                    } else {
                        cursor += 1;
                    }
                }
            }
        }

        let code = code.trim().to_string();
        if !code.is_empty() {
            lines.push((idx + 1, code));
        }
    }

    lines
}

fn nidl_e4_has_exact_code_line(text: &str, expected: &str) -> bool {
    nidl_e4_has_code_line(text, |line| line == expected)
}

fn nidl_e4_struct_code_span(text: &str, header: &str) -> Option<Vec<(usize, String)>> {
    let lines = nidl_e4_code_line_entries(text);
    let start = lines.iter().position(|(_, line)| line == header)?;
    let mut depth = 0isize;
    let mut seen_open = false;

    for idx in start..lines.len() {
        let line = &lines[idx].1;
        if line.contains('{') {
            seen_open = true;
        }
        depth += brace_delta(line);
        if seen_open && depth <= 0 {
            return Some(lines[start..=idx].to_vec());
        }
    }

    None
}

fn nidl_e4_struct_has_code_line<F>(text: &str, header: &str, mut predicate: F) -> bool
where
    F: FnMut(&str) -> bool,
{
    nidl_e4_struct_code_span(text, header)
        .map(|span| span.into_iter().any(|(_, line)| predicate(&line)))
        .unwrap_or(false)
}

fn nidl_e4_function_signature_contains(text: &str, fn_name: &str, needle: &str) -> bool {
    let lines = nidl_e4_code_line_entries(text);
    let fn_pattern = format!("fn {fn_name}(");
    let Some(start) = lines
        .iter()
        .position(|(_, line)| line.contains(&fn_pattern))
    else {
        return false;
    };

    let mut signature = String::new();
    for (_, line) in lines.iter().skip(start) {
        if !signature.is_empty() {
            signature.push(' ');
        }
        signature.push_str(line);
        if line.contains('{') {
            break;
        }
    }

    signature.contains(needle)
}

fn nidl_e4_push_forbidden_code_terms(
    violations: &mut Vec<String>,
    source: &str,
    text: &str,
    terms: &[&str],
    reason: &str,
) {
    let lines = nidl_e4_code_line_entries(text);
    for term in terms {
        if let Some((line, text)) = lines.iter().find(|(_, line)| line.contains(term)) {
            violations.push(format!("{source}:{line}: {reason}: `{term}` in `{text}`"));
        }
    }
}

#[test]
fn nidl_e4_scheduler_and_coordinator_use_native_scheduling_metadata() {
    let repo = Path::new(manifest_dir());
    let mut violations = Vec::new();

    let codegen_mod = fs::read_to_string(repo.join("src/sql/codegen/mod.rs")).unwrap();
    let codegen_mod_prod = rust_production_text_without_cfg_test(&codegen_mod);
    if !nidl_e4_has_exact_code_line(
        &codegen_mod_prod,
        "pub(crate) struct FragmentSchedulingMetadata {",
    ) {
        violations.push(
            "src/sql/codegen/mod.rs: E4 must expose a native FragmentSchedulingMetadata result"
                .to_string(),
        );
    }
    if !nidl_e4_struct_has_code_line(
        &codegen_mod_prod,
        "pub(crate) struct MultiFragmentBuildResult {",
        |line| {
            line.trim_end_matches(',') == "pub fragment_schedules: Vec<FragmentSchedulingMetadata>"
        },
    ) {
        violations.push(
            "src/sql/codegen/mod.rs: MultiFragmentBuildResult must carry native fragment_schedules"
                .to_string(),
        );
    }

    let scheduler = fs::read_to_string(repo.join("src/runtime/scheduler.rs")).unwrap();
    let scheduler_prod = rust_production_text_without_cfg_test(&scheduler);
    nidl_e4_push_forbidden_code_terms(
        &mut violations,
        "src/runtime/scheduler.rs",
        &scheduler_prod,
        &[
            "FragmentBuildResult",
            "plan_nodes::TPlan",
            "TPlanNodeType",
            ".plan.nodes",
            ".exec_params",
            ".output_sink",
        ],
        "scheduler must consume native FragmentSchedulingMetadata, not thrift fragment build payloads",
    );
    for fn_name in ["assign", "assign_with_live"] {
        if !nidl_e4_function_signature_contains(
            &scheduler_prod,
            fn_name,
            "fragments: &[FragmentSchedulingMetadata]",
        ) {
            violations.push(format!(
                "src/runtime/scheduler.rs: {fn_name} signature must accept FragmentSchedulingMetadata"
            ));
        }
    }

    let exec_params = fs::read_to_string(repo.join("src/runtime/exec_params.rs")).unwrap();
    let exec_params_prod = rust_production_text_without_cfg_test(&exec_params);
    nidl_e4_push_forbidden_code_terms(
        &mut violations,
        "src/runtime/exec_params.rs",
        &exec_params_prod,
        &["FragmentBuildResult", "fr.desc_tbl"],
        "exec-params helper must accept explicit compat descriptor payload, not FragmentBuildResult",
    );

    let coordinator = fs::read_to_string(repo.join("src/runtime/coordinator.rs")).unwrap();
    let coordinator_prod = rust_production_text_without_cfg_test(&coordinator);
    nidl_e4_push_forbidden_code_terms(
        &mut violations,
        "src/runtime/coordinator.rs",
        &coordinator_prod,
        &[
            "scheduler.assign_with_live(&fragment_results",
            "topological_sort_bottom_up(&fragment_results",
            "TPlanFragment::new",
            "crate::thrift::planner::TPlanFragment",
            "planner::TPlanFragment",
            "TPlanFragment as",
            "use crate::thrift::planner::TPlanFragment",
            "use crate::thrift::planner::{TPlanFragment",
        ],
        "coordinator must schedule from native metadata and must not directly construct thrift TPlanFragment",
    );
    if !nidl_e4_has_code_line(&coordinator_prod, |line| {
        line.contains("fragment_schedules")
    }) {
        violations.push(
            "src/runtime/coordinator.rs: coordinator must destructure and use fragment_schedules"
                .to_string(),
        );
    }

    assert!(
        violations.is_empty(),
        "NIDL-E4 native scheduling metadata guard failed:\n{}",
        violations.join("\n")
    );
}
// ---------------------------------------------------------------------------
// NIDL-E2: StarRocks connector/format compat gate
// ---------------------------------------------------------------------------

fn nidl_e2_is_allowed_compat_scope(rel_path: &str) -> bool {
    rel_path == "src/connector/starrocks"
        || rel_path.starts_with("src/connector/starrocks/")
        || rel_path == "src/formats/starrocks"
        || rel_path.starts_with("src/formats/starrocks/")
        || rel_path == "src/lower/compat"
        || rel_path.starts_with("src/lower/compat/")
        || nidl_e0_is_in_compat_scope(rel_path)
}

fn nidl_e2_forbidden_terms() -> &'static [&'static str] {
    &[
        "crate::connector::starrocks",
        "crate::formats::starrocks",
        "crate::novarocks_connector_starrocks",
    ]
}

fn nidl_e2_rel_path_under_scan_root(root: &Path, path: &Path) -> String {
    root.parent()
        .and_then(|base| path.strip_prefix(base).ok())
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| rel(path))
}

fn nidl_e2_is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn nidl_e2_has_token(text: &str, token: &str) -> bool {
    let mut search_start = 0usize;
    while let Some(offset) = text[search_start..].find(token) {
        let start = search_start + offset;
        let end = start + token.len();
        let before_is_ident = start
            .checked_sub(1)
            .and_then(|idx| text.as_bytes().get(idx))
            .is_some_and(|byte| nidl_e2_is_ident_byte(*byte));
        let after_is_ident = text
            .as_bytes()
            .get(end)
            .is_some_and(|byte| nidl_e2_is_ident_byte(*byte));
        if !before_is_ident && !after_is_ident {
            return true;
        }
        search_start = end;
    }
    false
}

fn nidl_e2_import_span_has_grouped_module(import_span: &str, parent: &str, module: &str) -> bool {
    let grouped_parent = format!("{parent}::{{");
    import_span.find(&grouped_parent).is_some_and(|start| {
        nidl_e2_has_token(&import_span[start + grouped_parent.len()..], module)
    })
}

fn nidl_e2_grouped_import_hits(path: &Path) -> Vec<String> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let production = nidl_e2_rust_text_without_cfg_test_or_compat(&text);
    let compact: String = non_comment_trimmed_lines(&production).join("");
    let mut hits = Vec::new();

    for grouped_root in ["crate::connector::{", "crate::formats::{", "crate::{"] {
        let mut search_start = 0usize;
        while let Some(offset) = compact[search_start..].find(grouped_root) {
            let start = search_start + offset;
            let span = &compact[start..];
            let end = span.find(';').unwrap_or(span.len());
            let import_span = &span[..end];

            if grouped_root == "crate::connector::{" && nidl_e2_has_token(import_span, "starrocks")
            {
                hits.push("grouped import references connector::starrocks".to_string());
            }
            if grouped_root == "crate::formats::{" && nidl_e2_has_token(import_span, "starrocks") {
                hits.push("grouped import references formats::starrocks".to_string());
            }
            if grouped_root == "crate::{" {
                if nidl_e2_has_token(import_span, "connector::starrocks")
                    || nidl_e2_import_span_has_grouped_module(import_span, "connector", "starrocks")
                {
                    hits.push(
                        "grouped import references StarRocks connector/format module: connector::starrocks"
                            .to_string(),
                    );
                }
                if nidl_e2_has_token(import_span, "formats::starrocks")
                    || nidl_e2_import_span_has_grouped_module(import_span, "formats", "starrocks")
                {
                    hits.push(
                        "grouped import references StarRocks connector/format module: formats::starrocks"
                            .to_string(),
                    );
                }
                if nidl_e2_has_token(import_span, "novarocks_connector_starrocks") {
                    hits.push(
                        "grouped import references StarRocks connector/format module: novarocks_connector_starrocks"
                            .to_string(),
                    );
                }
            }
            search_start = start + grouped_root.len();
        }
    }

    hits.sort();
    hits.dedup();
    hits
}

fn nidl_e2_format_hits_by_file(hits: &[String], max_per_file: usize) -> String {
    let mut by_file = BTreeMap::<String, Vec<String>>::new();
    for hit in hits {
        let file = hit.split_once(':').map(|(file, _)| file).unwrap_or(hit);
        by_file
            .entry(file.to_string())
            .or_default()
            .push(hit.to_string());
    }

    let mut out = Vec::new();
    for (_file, file_hits) in by_file {
        for hit in file_hits.iter().take(max_per_file) {
            out.push(hit.clone());
        }
        if file_hits.len() > max_per_file {
            out.push(format!(
                "{}: ... {} more hit(s)",
                file_hits[0].split_once(':').map(|(file, _)| file).unwrap(),
                file_hits.len() - max_per_file
            ));
        }
    }
    out.join("\n")
}

fn nidl_e2_noncompat_starrocks_gateway_hits_in(root: &Path) -> Vec<String> {
    let mut hits = Vec::new();
    for path in rs_files(root) {
        let rel_path = nidl_e2_rel_path_under_scan_root(root, &path);
        if nidl_e2_is_allowed_compat_scope(&rel_path) {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap_or_default();
        let production = nidl_e2_rust_text_without_cfg_test_or_compat(&text);
        for (idx, line) in production.lines().enumerate() {
            if !is_comment_or_blank(line)
                && nidl_e2_forbidden_terms()
                    .iter()
                    .any(|term| line.contains(*term))
            {
                hits.push(format!("{rel_path}:{}: {}", idx + 1, line.trim()));
            }
        }
        for text in nidl_e2_grouped_import_hits(&path) {
            hits.push(format!("{rel_path}:1: {text}"));
        }
    }
    hits.sort();
    hits
}

fn has_cfg_feature_compat_before_item(text: &str, item: &str) -> bool {
    let lines: Vec<&str> = text.lines().collect();
    for (idx, line) in lines.iter().enumerate() {
        if line.trim() == item {
            let mut cursor = idx;
            while cursor > 0 {
                cursor -= 1;
                let previous = lines[cursor].trim();
                if previous.is_empty() || previous.starts_with("//") {
                    continue;
                }
                return previous == "#[cfg(feature = \"compat\")]";
            }
        }
    }
    false
}

#[test]
fn nidl_e2_detector_flags_noncompat_gateway_imports_and_ignores_compat_scopes() {
    let dir = std::env::temp_dir().join("nidl_e2_detector");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("src/engine")).unwrap();
    fs::create_dir_all(dir.join("src/connector/starrocks")).unwrap();
    fs::create_dir_all(dir.join("src/lower/compat")).unwrap();
    fs::write(
        dir.join("src/engine/offender.rs"),
        "use crate::connector::starrocks::scan::StarRocksScanRange;\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/grouped_parent.rs"),
        "use crate::connector::{starrocks::scan::StarRocksScanRange};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/grouped_crate.rs"),
        "use crate::{connector::starrocks, formats::starrocks};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/nested_grouped_crate.rs"),
        "use crate::{connector::{starrocks::scan::StarRocksScanRange}, formats::{starrocks::metadata::load_tablet_snapshot}};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/similar_name.rs"),
        "use crate::connector::{iceberg::starrocks_profile};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/test_only.rs"),
        "#[cfg(test)]\nmod tests {\n    use crate::{connector::starrocks, formats::starrocks};\n}\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/compat_direct.rs"),
        "#[cfg(feature = \"compat\")]\nuse crate::connector::starrocks::scan::StarRocksScanRange;\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/engine/compat_grouped.rs"),
        "#[cfg(feature = \"compat\")]\nuse crate::{connector::{starrocks::scan::StarRocksScanRange}, formats::{starrocks::metadata::load_tablet_snapshot}};\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/connector/starrocks/allowed.rs"),
        "use crate::connector::starrocks::scan::StarRocksScanRange;\n",
    )
    .unwrap();
    fs::write(
        dir.join("src/lower/compat/allowed.rs"),
        "use crate::formats::starrocks::metadata::load_tablet_snapshot;\n",
    )
    .unwrap();

    let hits = nidl_e2_noncompat_starrocks_gateway_hits_in(&dir.join("src"));
    assert!(
        hits.iter()
            .any(|hit| hit.contains("src/engine/offender.rs")),
        "must flag non-compat StarRocks connector imports; got {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|hit| hit.contains("src/engine/grouped_parent.rs")),
        "must flag grouped connector parent imports; got {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|hit| hit.contains("src/engine/grouped_crate.rs")),
        "must flag grouped crate imports; got {hits:?}"
    );
    assert!(
        hits.iter()
            .any(|hit| hit.contains("src/engine/nested_grouped_crate.rs")),
        "must flag nested grouped crate imports; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/engine/similar_name.rs")),
        "must not flag similar names that are not the starrocks module; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/engine/test_only.rs")),
        "must ignore #[cfg(test)] grouped imports; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/engine/compat_direct.rs")),
        "must ignore #[cfg(feature = \"compat\")] direct imports; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/engine/compat_grouped.rs")),
        "must ignore #[cfg(feature = \"compat\")] grouped imports; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/connector/starrocks/allowed.rs")),
        "must ignore the gated connector module itself; got {hits:?}"
    );
    assert!(
        !hits
            .iter()
            .any(|hit| hit.contains("src/lower/compat/allowed.rs")),
        "must ignore lower compat scope; got {hits:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn nidl_e2_cfg_feature_helper_checks_nearest_non_comment_attribute() {
    assert!(
        has_cfg_feature_compat_before_item(
            "#[cfg(feature = \"compat\")]\n// module comment\npub mod starrocks;\n",
            "pub mod starrocks;"
        ),
        "must accept compat cfg immediately before module item"
    );
    assert!(
        !has_cfg_feature_compat_before_item(
            "#[cfg(feature = \"other\")]\npub mod starrocks;\n",
            "pub mod starrocks;"
        ),
        "must reject non-compat cfg before module item"
    );
    assert!(
        !has_cfg_feature_compat_before_item(
            "#[cfg(feature = \"compat\")]\npub mod iceberg;\npub mod starrocks;\n",
            "pub mod starrocks;"
        ),
        "must not treat cfg on a previous item as gating starrocks"
    );
}

#[test]
fn nidl_e2_starrocks_connector_and_format_modules_are_compat_gated() {
    let connector_mod = fs::read_to_string(Path::new(manifest_dir()).join("src/connector/mod.rs"))
        .expect("connector mod");
    let formats_mod = fs::read_to_string(Path::new(manifest_dir()).join("src/formats/mod.rs"))
        .expect("formats mod");
    let mut violations = Vec::new();
    if !has_cfg_feature_compat_before_item(&connector_mod, "pub mod starrocks;") {
        violations.push(
            "src/connector/mod.rs must gate pub mod starrocks with #[cfg(feature = \"compat\")]",
        );
    }
    if !has_cfg_feature_compat_before_item(&formats_mod, "pub mod starrocks;") {
        violations.push(
            "src/formats/mod.rs must gate pub mod starrocks with #[cfg(feature = \"compat\")]",
        );
    }
    assert!(
        violations.is_empty(),
        "StarRocks connector/format modules must be compat-gated:\n{}",
        violations.join("\n")
    );
}

#[test]
fn nidl_e2_noncompat_code_does_not_import_starrocks_connector_or_format_modules() {
    let hits = nidl_e2_noncompat_starrocks_gateway_hits_in(&src_dir());
    assert!(
        hits.is_empty(),
        "non-compat production code must not import StarRocks connector/format modules outside compat scopes:\n{}",
        nidl_e2_format_hits_by_file(&hits, 5)
    );
}

#[test]
fn nidl_e6_runtime_adapters_are_compat_only() {
    let repo = Path::new(manifest_dir());
    let guarded = [
        (
            "src/runtime/query_options.rs",
            &[
                "crate::thrift",
                "TQueryOptions",
                "TSpillMode",
                "TSpillOptions",
            ][..],
        ),
        (
            "src/runtime/runtime_filter_params.rs",
            &[
                "crate::thrift",
                "runtime_filter::TRuntimeFilterParams",
                "runtime_filter::TRuntimeFilterProberParams",
            ][..],
        ),
        (
            "src/runtime/scan_range.rs",
            &[
                "crate::thrift",
                "descriptors::",
                "exprs::",
                "internal_service::",
                "plan_nodes::",
                "types::",
            ][..],
        ),
    ];

    let mut violations = Vec::new();
    for (source, terms) in guarded {
        let text = fs::read_to_string(repo.join(source)).expect(source);
        let default_build_text = rust_production_text_without_cfg_test_or_compat(&text);
        for term in terms {
            if let Some((idx, line)) = default_build_text
                .lines()
                .enumerate()
                .find(|(_, line)| !is_comment_or_blank(line) && line.contains(term))
            {
                violations.push(format!(
                    "{source}:{}: `{term}` in `{}`",
                    idx + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "E6 query/rf thrift adapters must be compat-only in the default build:\n{}",
        violations.join("\n")
    );
}
