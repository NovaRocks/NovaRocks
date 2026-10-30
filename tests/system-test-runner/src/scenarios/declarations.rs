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

//! Shared declarations consumed by scenario owners in the same domain. This
//! module declares no scenarios; discovery continues to use `scenarios::all`.

use crate::scenario::{
    ScenarioDeclaration, ScenarioEnvironmentInput, ScenarioHeavyReason, ScenarioImpact,
};

pub(super) fn for_module(
    module: &str,
    name: &str,
    explicit: bool,
    compatibility: bool,
) -> ScenarioDeclaration {
    let domains: &[&str] = match module {
        "trino_protocol" => &[
            "novarocks/trino-adapter/**",
            "novarocks/result-render/**",
            "novarocks/result-contract/**",
            "novarocks/sql/src/compiler/root_render_type.rs",
            "novarocks/query-application/src/session*.rs",
            "novarocks/query-application/src/session_control/**",
            "novarocks/query-application/src/sql/session/**",
            "novarocks/query-application/src/query_control.rs",
            "novarocks/query-application/src/protocol_delivery.rs",
            "novarocks/query-application/src/client_query_id.rs",
            "novarocks/frontend-application/**",
            "novarocks-server/**",
            "tests/system-test-runner/src/actors/trino_protocol.rs",
        ],
        "backend_membership" => &[
            "novarocks/frontend-application/**",
            "novarocks/coordination-contract/**",
            "novarocks/worker/**",
            "novarocks/native-adapter/**",
            "novarocks/execution-contract/**",
            "novarocks-server/**",
        ],
        "native_trust" | "native_compatibility" => &[
            "novarocks/native-trust/**",
            "novarocks/native-adapter/**",
            "novarocks/worker/**",
            "novarocks/frontend-application/**",
            "novarocks/coordination-contract/**",
            "novarocks/execution-contract/**",
            "novarocks-server/**",
        ],
        "catalog_state"
        | "state_family"
        | "catalog_publication"
        | "listing"
        | "listing_real_rest" => &[
            "novarocks/catalog-application/**",
            "novarocks/frontend-application/**",
            "novarocks/coordination-contract/**",
            "novarocks/query-application/**",
            "novarocks/sql/**",
            "novarocks/connector/**",
            "novarocks/fs/**",
        ],
        "connector"
        | "paimon"
        | "iceberg_delete_applicability"
        | "iceberg_delete_warm"
        | "uea4_catalog_planning"
        | "uea4_range_reads"
        | "uea4_scan_readiness" => &[
            "novarocks/connector/**",
            "novarocks/fs/**",
            "novarocks/frontend-application/**",
            "novarocks/coordination-contract/**",
            "novarocks/worker/**",
            "novarocks/execution/**",
            "novarocks/sql/**",
            "novarocks/local-program/**",
            "novarocks/native-adapter/**",
        ],
        "distributed_writer" | "table_maintenance" => &[
            "novarocks/connector/iceberg/**",
            "novarocks/fs/**",
            "novarocks/frontend-application/**",
            "novarocks/coordination-contract/**",
            "novarocks/query-application/**",
            "novarocks/sql/**",
            "novarocks/execution/**",
            "novarocks/worker/**",
            "novarocks/native-adapter/**",
        ],
        "memory_attribution" => &[
            "novarocks/memory/**",
            "novarocks/execution/**",
            "novarocks/frontend-application/**",
            "novarocks/coordination-contract/**",
            "novarocks/worker/**",
            "novarocks-server/**",
        ],
        module if module.starts_with("mv_") => &[
            "novarocks/frontend-application/**",
            "novarocks/coordination-contract/**",
            "novarocks/query-application/**",
            "novarocks/catalog-application/**",
            "novarocks/connector/iceberg/**",
            "novarocks/sql/**",
            "novarocks/execution/**",
            "novarocks/fs/**",
        ],
        _ => &[
            "novarocks/frontend-application/**",
            "novarocks/coordination-contract/**",
            "novarocks/query-application/**",
            "novarocks/mysql-adapter/**",
            "novarocks/worker/**",
            "novarocks/execution/**",
            "novarocks/execution-contract/**",
            "novarocks/native-adapter/**",
            "novarocks/plan-codec/**",
            "novarocks/local-program/**",
            "novarocks/sql/**",
            "novarocks/result-contract/**",
        ],
    };
    let mut paths = domains
        .iter()
        .map(|path| (*path).to_owned())
        .collect::<Vec<_>>();
    // Public materialization and storage geometry are execution dependencies.
    // Keep their impact with the same scenario domain, including failure facts.
    if domains.contains(&"novarocks/execution/**") {
        paths.push("novarocks/value-arrays/**".into());
    }
    paths.extend([
        format!("tests/system-test-runner/src/scenarios/{module}.rs"),
        "tests/cluster-harness/**".into(),
        "tests/test-support/**".into(),
        "novarocks/failpoint/**".into(),
        "tests/system-test-runner/src/*.rs".into(),
        "tests/system-test-runner/src/scenarios/declarations.rs".into(),
        "novarocks/types/**".into(),
    ]);
    if matches!(
        module,
        "execution_failure"
            | "execution_failure_prepared"
            | "execution_failure_representation"
            | "execution_failure_streaming"
    ) {
        paths.extend(
            [
                "novarocks/failure-contract/**",
                "novarocks/user-error/**",
                "novarocks/memory-failure/**",
                "novarocks/memory/**",
                "novarocks/function-contract/**",
                "novarocks/functions/**",
                "novarocks/type-contract/**",
                "novarocks/connector-contract/**",
                "novarocks/proto-codec/**",
                "novarocks/task-codec/**",
                "novarocks/local-compiler/**",
                "novarocks/result-render/**",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }
    if module == "mv_recovery" && name == "mv/current-dependency-recheck" {
        paths.extend(
            [
                "novarocks/spi/src/connector/**",
                "novarocks/connector-contract/**",
                "novarocks/connector/iceberg/**",
                "novarocks/failure-contract/**",
                "tests/system-test-runner/src/scenarios/mv_control_plan.rs",
                "tests/system-test-runner/src/scenarios/mv_uea7.rs",
                "tests/system-test-runner/src/scenarios/mv_uea7_documents.rs",
                "tests/system-test-runner/src/scenarios/mv_uea7_documents/wire.rs",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }
    if module == "distributed_writer" {
        paths.extend(
            [
                "tests/system-test-runner/src/scenarios/distributed_writer_row_level.rs",
                "tests/system-test-runner/src/scenarios/distributed_writer_dop_observation.rs",
                "tests/system-test-runner/src/scenarios/distributed_writer_same_file.rs",
                "novarocks/execution-contract/**",
                "novarocks/result-contract/**",
                "novarocks/physical-plan/**",
                "novarocks/plan-codec/**",
                "novarocks/task-codec/**",
                "novarocks/local-program/**",
                "novarocks/local-compiler/**",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }
    if module == "query_lifecycle" {
        paths.push("tests/system-test-runner/src/scenarios/query_lifecycle/**".into());
    }
    if module.starts_with("mv_")
        || matches!(
            module,
            "mv_uea7"
                | "mv_uea7_documents"
                | "mv_uea7_handover"
                | "mv_uea7_incarnation"
                | "mv_uea7_storage"
                | "catalog_publication"
                | "table_maintenance"
                | "listing_real_rest"
        )
        || name.contains("vended-")
    {
        paths.extend([
            "docker/iceberg-rest/**".into(),
            "tests/fixtures/iceberg-rest-publication/**".into(),
        ]);
    }
    let mut declaration = ScenarioDeclaration {
        impact: ScenarioImpact { paths },
        env_inputs: Vec::new(),
        initial_state: Some("isolated"),
        basis: Some(
            "Each invocation launches fresh FE/BE processes and a unique runtime directory; generated local stores, in-process services and any private Docker fixture belong to this invocation.",
        ),
        heavy: None,
    };
    let env: &[(&str, &str)] = match module {
        "paimon" => &[
            ("NOVAROCKS_PAIMON_FIXTURE_MANIFEST", "file-content"),
            ("NOVA_ENV_REST_ENV_FILE", "file-content"),
            ("AWS_S3_ACCESS_KEY_ID", "value"),
            ("AWS_S3_SECRET_ACCESS_KEY", "value"),
        ],
        "uea4_scan_readiness" => &[
            ("NOVAROCKS_ICEBERG_REST_URI", "value"),
            ("NOVAROCKS_ICEBERG_REST_WAREHOUSE", "value"),
            ("AWS_S3_ENDPOINT", "value"),
            ("NOVAROCKS_PAIMON_FIXTURE_MANIFEST", "file-content"),
            ("NOVA_ENV_REST_ENV_FILE", "file-content"),
            ("AWS_S3_ACCESS_KEY_ID", "value"),
            ("AWS_S3_SECRET_ACCESS_KEY", "value"),
        ],
        "uea4_range_reads" => &[
            ("NOVAROCKS_UEA4A2_FIXTURE_MANIFEST", "file-content"),
            ("NOVAROCKS_ICEBERG_SHORT_FIXTURE_MANIFEST", "file-content"),
            ("NOVAROCKS_UEA4A2_S3_ACCESS_KEY_ID", "value"),
            ("NOVAROCKS_UEA4A2_S3_SECRET_ACCESS_KEY", "value"),
            ("AWS_S3_ACCESS_KEY_ID", "value"),
            ("AWS_S3_SECRET_ACCESS_KEY", "value"),
        ],
        "uea4_catalog_planning" => &[
            ("NOVAROCKS_UEA4A4_ICEBERG_FIXTURE_MANIFEST", "file-content"),
            ("NOVAROCKS_UEA4A4_PAIMON_FIXTURE_MANIFEST", "file-content"),
            ("NOVAROCKS_UEA4A4_S3_ACCESS_KEY_ID", "value"),
            ("NOVAROCKS_UEA4A4_S3_SECRET_ACCESS_KEY", "value"),
            ("AWS_S3_ACCESS_KEY_ID", "value"),
            ("AWS_S3_SECRET_ACCESS_KEY", "value"),
        ],
        "iceberg_delete_applicability" => &[
            ("NOVAROCKS_UEA4G_NATIVE_MANIFEST", "file-content"),
            ("AWS_S3_ACCESS_KEY_ID", "value"),
            ("AWS_S3_SECRET_ACCESS_KEY", "value"),
        ],
        "iceberg_delete_warm" => &[
            ("NOVAROCKS_ICEBERG_DELETE_CORPUS_RECEIPT", "file-content"),
            ("NOVAROCKS_ICEBERG_REST_URI", "value"),
            ("NOVAROCKS_ICEBERG_REST_WAREHOUSE", "value"),
            ("AWS_S3_ENDPOINT", "value"),
            ("AWS_S3_ACCESS_KEY_ID", "value"),
            ("AWS_S3_SECRET_ACCESS_KEY", "value"),
        ],
        "frontend_lifecycle" => &[
            ("NOVAROCKS_ICEBERG_REST_URI", "value"),
            ("NOVAROCKS_ICEBERG_REST_WAREHOUSE", "value"),
            ("AWS_S3_ENDPOINT", "value"),
        ],
        _ => &[],
    };
    declaration.env_inputs = env
        .iter()
        .map(|&(name, kind)| ScenarioEnvironmentInput { name, kind })
        .collect();
    if module.starts_with("mv_")
        || matches!(
            module,
            "catalog_publication" | "table_maintenance" | "listing_real_rest"
        )
        || name.contains("vended-")
    {
        declaration.env_inputs.extend(
            [
                "DOCKER_CONTEXT",
                "DOCKER_HOST",
                "DOCKER_TLS_VERIFY",
                "DOCKER_CERT_PATH",
                "DOCKER_CONFIG",
                "NOVA_FIXTURE_STORE",
                "XDG_CONFIG_HOME",
                "XDG_CACHE_HOME",
                "XDG_STATE_HOME",
            ]
            .into_iter()
            .map(|name| ScenarioEnvironmentInput {
                name,
                kind: "value",
            }),
        );
    }
    if matches!(
        module,
        "paimon"
            | "uea4_scan_readiness"
            | "uea4_range_reads"
            | "uea4_catalog_planning"
            | "iceberg_delete_applicability"
            | "frontend_lifecycle"
    ) {
        declaration.initial_state = None;
        declaration.basis = Some(
            "Uses externally prepared or shared provider state; an invocation-wide reset or immutability contract has not been established.",
        );
    }
    if module == "iceberg_delete_warm" {
        declaration.initial_state = Some("reset");
        declaration.basis = Some(
            "Reads independently verified corpus snapshots; writes only an invocation-derived namespace and removes its table and namespace before completion.",
        );
    }
    if explicit {
        let (reason, detail) = if compatibility {
            (
                "variant-binary",
                "Requires compatible or alternate-island server artifacts across the complete distributed lifecycle.",
            )
        } else if matches!(
            module,
            "iceberg_delete_applicability"
                | "iceberg_delete_warm"
                | "uea4_catalog_planning"
                | "uea4_range_reads"
                | "uea4_scan_readiness"
                | "paimon"
        ) {
            (
                "dataset",
                "Requires an explicitly prepared provider dataset or independently generated oracle receipt.",
            )
        } else if matches!(
            module,
            "mv_uea7"
                | "mv_uea7_documents"
                | "mv_uea7_handover"
                | "mv_uea7_incarnation"
                | "mv_uea7_storage"
                | "catalog_publication"
                | "table_maintenance"
                | "listing_real_rest"
                | "frontend_lifecycle"
        ) || name.contains("vended-")
            || module == "native_trust"
        {
            (
                "fixture",
                "Requires an isolated Docker provider fixture or an explicitly supplied external catalog fixture.",
            )
        } else {
            (
                "cost",
                "Explicit distributed lifecycle, protocol-fault or concurrency scenario outside the default functional baseline.",
            )
        };
        declaration.heavy = Some(ScenarioHeavyReason { reason, detail });
    }
    declaration
}

#[cfg(test)]
mod tests {
    #[test]
    fn every_native_scenario_declares_shared_process_and_session_owners() {
        let scenarios = super::super::all();
        assert!(!scenarios.is_empty());
        for scenario in scenarios {
            let declaration = scenario.declaration();
            for shared in [
                "tests/cluster-harness/**",
                "tests/test-support/**",
                "novarocks/failpoint/**",
            ] {
                assert!(
                    declaration.impact.paths.iter().any(|path| path == shared),
                    "{} omits its shared launch owner {shared}",
                    scenario.name(),
                );
            }
        }
    }
}
