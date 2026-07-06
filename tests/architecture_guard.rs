//! Architecture guards for the plan-IR layering arc (PIR-8).
//!
//! These tests mechanically enforce the PIR import and stage boundaries. Test
//! modules may still build optimizer trees as inputs; production code may not
//! leak optimizer physical types into planner/codegen main paths.

use std::fs;
use std::path::{Path, PathBuf};

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

fn is_comment_or_blank(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.is_empty()
        || trimmed.starts_with("//")
        || trimmed.starts_with("/*")
        || trimmed.starts_with('*')
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

        if trimmed.starts_with("#[cfg(test") {
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

fn non_test_optimizer_refs(path: &Path) -> Vec<(usize, String)> {
    non_test_line_hits(path, |line| line.contains("crate::sql::optimizer::"))
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
        grpc.contains("thrift SubmitFragment requires the compat feature"),
        "pure SubmitFragment must reject thrift fallback explicitly"
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
