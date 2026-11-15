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
        "query_output" => &[
            "novarocks/trino-adapter/**",
            "novarocks/query-application/**",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/result-contract/**",
            "novarocks/result-render/**",
            "novarocks/sql/**",
            "novarocks/execution/**",
            "novarocks/native-adapter/**",
            "novarocks/worker/**",
            "novarocks-server/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "tests/system-test-runner/src/actors/trino_protocol.rs",
        ],
        "sql_client" | "sql_client_ctas" | "query_tool" => &[
            "novarocks/trino-adapter/**",
            "novarocks/query-application/**",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/connector/iceberg/**",
            "novarocks/sql/**",
            "novarocks/execution/**",
            "novarocks/worker/**",
            "novarocks/native-adapter/**",
            "novarocks/result-contract/**",
            "novarocks/result-render/**",
            "novarocks-server/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
        ],
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
            "novarocks/attempt-protocol/**",
            "novarocks-server/**",
            "tests/system-test-runner/src/actors/trino_protocol.rs",
        ],
        "backend_membership" => &[
            "novarocks/trino-adapter/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/coordination-contract/**",
            "novarocks/worker/**",
            "novarocks/native-adapter/**",
            "novarocks/execution-contract/**",
            "novarocks-server/**",
        ],
        "native_trust" | "native_compatibility" => &[
            "novarocks/trino-adapter/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "novarocks/native-trust/**",
            "novarocks/native-adapter/**",
            "novarocks/worker/**",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/coordination-contract/**",
            "novarocks/execution-contract/**",
            "novarocks-server/**",
        ],
        "catalog_state"
        | "state_family"
        | "catalog_publication"
        | "listing"
        | "listing_real_rest" => &[
            "novarocks/trino-adapter/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "novarocks/catalog-application/**",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
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
            "novarocks/trino-adapter/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "novarocks/connector/**",
            "novarocks/fs/**",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/coordination-contract/**",
            "novarocks/worker/**",
            "novarocks/execution/**",
            "novarocks/sql/**",
            "novarocks/local-program/**",
            "novarocks/native-adapter/**",
        ],
        "distributed_writer" | "table_maintenance" | "managed_output_storage" => &[
            "novarocks/trino-adapter/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "novarocks/connector/iceberg/**",
            "novarocks/fs/**",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/coordination-contract/**",
            "novarocks/query-application/**",
            "novarocks/sql/**",
            "novarocks/execution/**",
            "novarocks/worker/**",
            "novarocks/native-adapter/**",
        ],
        "exchange_flow" => &[
            "novarocks/trino-adapter/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "novarocks/execution/**",
            "novarocks/execution-contract/**",
            "novarocks/native-adapter/**",
            "novarocks/worker/**",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/query-application/**",
            "novarocks/sql/**",
            "novarocks/physical-plan/**",
            "novarocks/plan-codec/**",
            "novarocks/task-codec/**",
            "novarocks/proto-codec/**",
            "novarocks/local-program/**",
            "novarocks/local-compiler/**",
            "novarocks/connector/iceberg/**",
            "novarocks/fs/**",
            "novarocks-server/**",
        ],
        "memory_attribution" => &[
            "novarocks/trino-adapter/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "novarocks/memory/**",
            "novarocks/execution/**",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/coordination-contract/**",
            "novarocks/worker/**",
            "novarocks-server/**",
        ],
        module if module.starts_with("mv_") => &[
            "novarocks/trino-adapter/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/coordination-contract/**",
            "novarocks/query-application/**",
            "novarocks/catalog-application/**",
            "novarocks/mv-application/**",
            "novarocks/connector/iceberg/**",
            "novarocks/sql/**",
            "novarocks/execution/**",
            "novarocks/fs/**",
        ],
        "frontend_lifecycle"
        | "runtime_filter"
        | "native_creation"
        | "native_ingress"
        | "execution_failure_prepared" => &[
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/coordination-contract/**",
            "novarocks/query-application/**",
            "novarocks/trino-adapter/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "novarocks/worker/**",
            "novarocks/execution/**",
            "novarocks/execution-contract/**",
            "novarocks/native-adapter/**",
            "novarocks/plan-codec/**",
            "novarocks/local-program/**",
            "novarocks/sql/**",
            "novarocks/result-contract/**",
        ],
        "result_delivery_root_reply"
        | "result_delivery_held_late_ack"
        | "result_delivery_progress"
        | "result_delivery_concurrency"
        | "result_delivery_deadline"
        | "result_delivery_closing"
        | "query_concurrency" => &[
            "novarocks/trino-adapter/**",
            "novarocks/result-render/**",
            "novarocks/result-contract/**",
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/query-application/**",
            "novarocks/coordination-contract/**",
            "novarocks/worker/**",
            "novarocks/execution/**",
            "novarocks/execution-contract/**",
            "novarocks/native-adapter/**",
            "novarocks/plan-codec/**",
            "novarocks/task-codec/**",
            "novarocks/proto-codec/**",
            "novarocks/sql/**",
            "tests/system-test-runner/src/actors/sql.rs",
            "tests/system-test-runner/src/actors/trino_protocol.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
        ],
        _ => &[
            "novarocks/frontend-application/**",
            "novarocks/attempt-protocol/**",
            "novarocks/coordination-contract/**",
            "novarocks/query-application/**",
            "novarocks/trino-adapter/**",
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
        "tests/trino-client/**".into(),
        "tests/cluster-harness/**".into(),
        "tests/test-support/**".into(),
        "novarocks/failpoint/**".into(),
        "tests/system-test-runner/src/*.rs".into(),
        "tests/system-test-runner/src/scenarios/declarations.rs".into(),
        "novarocks/types/**".into(),
    ]);
    if module == "result_delivery" {
        paths.extend(
            [
                "novarocks/trino-adapter/**",
                "novarocks/result-render/**",
                "tests/system-test-runner/src/actors/sql.rs",
                "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
                "tests/system-test-runner/src/scenarios/independent_root_target.rs",
                "tests/system-test-runner/src/scenarios/result_delivery_inputs.rs",
                "tests/system-test-runner/src/scenarios/result_delivery_trino_protocol.rs",
                "tests/system-test-runner/src/scenarios/result_delivery_trino_retention.rs",
                "tests/system-test-runner/src/scenarios/result_delivery_trino_rows.rs",
                "tests/system-test-runner/src/actors/trino_protocol.rs",
                "docs/testing/trc-3/root-read-refusal-v3.json",
                "tests/system-test-runner/src/scenarios/result_delivery_root_protocol.rs",
                "tests/system-test-runner/src/scenarios/result_delivery_root_owned_probe.rs",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }
    if module == "result_delivery_root_reply" {
        paths.push("docs/testing/trc-3/root-reply-*-v2.json".into());
    }
    if module == "result_delivery_held_late_ack" {
        paths.extend(
            [
                "tests/system-test-runner/src/scenarios/result_delivery_root_protocol.rs",
                "tests/system-test-runner/src/scenarios/result_delivery_root_owned_probe.rs",
                "tests/system-test-runner/src/scenarios/result_delivery_root_held_response.rs",
                "tests/system-test-runner/src/scenarios/result_delivery_inputs.rs",
                "tests/system-test-runner/src/scenarios/independent_root_target.rs",
                "tests/system-test-runner/src/scenarios/result_delivery_support.rs",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }
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
    if matches!(
        module,
        "execution_failure" | "execution_failure_representation" | "execution_failure_streaming"
    ) {
        paths.extend(
            [
                "novarocks/trino-adapter/**",
                "tests/trino-client/**",
                "tests/system-test-runner/src/actors/sql.rs",
                "tests/system-test-runner/src/actors/trino_protocol.rs",
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
    if module == "managed_output_storage" {
        paths.extend(
            [
                "tests/system-test-runner/src/scenarios/connector.rs",
                "tests/system-test-runner/src/scenarios/iceberg_delete_applicability.rs",
                "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
                "tests/system-test-runner/src/scenarios/writer_statement_actor.rs",
                "docker/iceberg-rest/**",
                "docker/fixture-inputs/**",
                "tests/fixtures/iceberg-rest-publication/**",
            ]
            .map(str::to_owned),
        );
    }
    if module == "distributed_writer" {
        paths.extend(
            [
                "tests/system-test-runner/src/scenarios/distributed_writer_row_level.rs",
                "tests/system-test-runner/src/scenarios/distributed_writer_dop_observation.rs",
                "tests/system-test-runner/src/scenarios/distributed_writer_same_file.rs",
                "tests/system-test-runner/src/scenarios/distributed_writer_small_slots.rs",
                "tests/system-test-runner/src/scenarios/exchange_flow.rs",
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
    if module == "query_concurrency" {
        paths.extend(
            [
                "novarocks/local-program/**",
                "tests/system-test-runner/src/scenarios/query_concurrency_controls.rs",
                "tests/system-test-runner/src/scenarios/query_concurrency_trino.rs",
                "tests/system-test-runner/src/actors/trino_protocol.rs",
                "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            ]
            .map(str::to_owned),
        );
    }
    if module == "query_lifecycle" {
        paths.push("tests/system-test-runner/src/scenarios/query_lifecycle/**".into());
        if matches!(
            name,
            "query-lifecycle/query-timeout"
                | "query-lifecycle/client-abandonment"
                | "query-lifecycle/no-effect-read-after-backend-exit"
        ) || name.starts_with("query-lifecycle/nid-2-")
        {
            paths.extend(
                [
                    "novarocks/trino-adapter/**",
                    "tests/system-test-runner/src/actors/trino_protocol.rs",
                    "tests/system-test-runner/src/scenarios/result_delivery_deadline.rs",
                    "tests/system-test-runner/src/scenarios/trino_protocol.rs",
                ]
                .map(str::to_owned),
            );
        }
        if matches!(
            name,
            "query-lifecycle/distributed-baseline"
                | "query-lifecycle/live-backend-partition"
                | "query-lifecycle/limit-zero-unknown-sender"
        ) {
            paths.extend(
                [
                    "novarocks/trino-adapter/**",
                    "tests/system-test-runner/src/actors/sql.rs",
                    "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
                ]
                .map(str::to_owned),
            );
        }
    }
    // These private source dependencies follow the existing scenario calls
    // and path modules. They do not grant admission or classify stability.
    let helper_sources: &[&str] = match module {
        "result_delivery_concurrency" => &[
            "tests/system-test-runner/src/scenarios/result_delivery_support.rs",
            "tests/system-test-runner/src/scenarios/result_delivery_concurrency_tests.rs",
        ],
        "result_delivery_deadline" => &[
            "tests/system-test-runner/src/scenarios/result_delivery.rs",
            "tests/system-test-runner/src/scenarios/result_delivery_support.rs",
        ],
        "result_delivery_closing" => &[
            "tests/system-test-runner/src/scenarios/result_delivery_progress.rs",
            "tests/system-test-runner/src/scenarios/result_delivery_support.rs",
            "tests/system-test-runner/src/scenarios/trino_protocol.rs",
        ],
        "result_delivery_progress" => &[
            "tests/system-test-runner/src/scenarios/result_delivery.rs",
            "tests/system-test-runner/src/scenarios/result_delivery_support.rs",
        ],
        "backend_membership" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/distributed_writer.rs",
            "tests/system-test-runner/src/scenarios/distributed_writer_dop_observation.rs",
            "tests/system-test-runner/src/scenarios/distributed_writer_row_level.rs",
            "tests/system-test-runner/src/scenarios/distributed_writer_same_file.rs",
            "tests/system-test-runner/src/scenarios/distributed_writer_small_slots.rs",
            "tests/system-test-runner/src/scenarios/exchange_flow.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
            "tests/system-test-runner/src/scenarios/writer_statement_actor.rs",
        ],
        "connector" => &[
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
        ],
        "distributed_writer" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/distributed_writer_dop_observation.rs",
            "tests/system-test-runner/src/scenarios/distributed_writer_row_level.rs",
            "tests/system-test-runner/src/scenarios/distributed_writer_same_file.rs",
            "tests/system-test-runner/src/scenarios/distributed_writer_small_slots.rs",
            "tests/system-test-runner/src/scenarios/exchange_flow.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
            "tests/system-test-runner/src/scenarios/writer_statement_actor.rs",
        ],
        "exchange_flow" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
        ],
        "execution_failure_prepared" => &[
            "tests/system-test-runner/src/scenarios/compatibility_sql_intent.rs",
            "tests/system-test-runner/src/scenarios/native_compatibility.rs",
            "tests/system-test-runner/src/scenarios/native_creation.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
        ],
        "iceberg_delete_applicability" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
        ],
        "iceberg_delete_warm" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/iceberg_delete_applicability.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/paimon.rs",
            "tests/system-test-runner/src/scenarios/provider_mysql_actors.rs",
            "tests/system-test-runner/src/scenarios/scan_statement_actor.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
        ],
        "listing" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
        ],
        "listing_real_rest" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/listing.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
        ],
        "memory_attribution" => &["tests/system-test-runner/src/scenarios/native_sql_controls.rs"],
        "mv_recovery" => &[
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/scan_statement_actor.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7_documents.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7_storage.rs",
        ],
        "mv_uea7_documents" => &[
            "tests/system-test-runner/src/scenarios/mv_recovery.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7_storage.rs",
        ],
        "mv_uea7_handover" => &["tests/system-test-runner/src/scenarios/mv_uea7.rs"],
        "mv_uea7_incarnation" => &[
            "tests/system-test-runner/src/scenarios/mv_recovery.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7_documents.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7_storage.rs",
        ],
        "mv_uea7_storage" => &[
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/scan_statement_actor.rs",
            "tests/system-test-runner/src/scenarios/mv_recovery.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7.rs",
            "tests/system-test-runner/src/scenarios/mv_uea7_documents.rs",
        ],
        "native_compatibility" => &[
            "tests/system-test-runner/src/scenarios/compatibility_sql_intent.rs",
            "tests/system-test-runner/src/scenarios/native_creation.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
        ],
        "native_creation" => &[
            "tests/system-test-runner/src/scenarios/compatibility_sql_intent.rs",
            "tests/system-test-runner/src/scenarios/native_compatibility.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
        ],
        "native_ingress" => &[
            "tests/system-test-runner/src/scenarios/compatibility_sql_intent.rs",
            "tests/system-test-runner/src/scenarios/native_compatibility.rs",
            "tests/system-test-runner/src/scenarios/native_creation.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
        ],
        "native_trust" => &[
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/native_trust_controls.rs",
        ],
        "paimon" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/scan_statement_actor.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
        ],
        "runtime_filter" => &["tests/system-test-runner/src/scenarios/native_sql_controls.rs"],
        "table_maintenance" => &[
            "tests/system-test-runner/src/scenarios/mv_uea7.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
        ],
        "uea4_catalog_planning" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/paimon.rs",
            "tests/system-test-runner/src/scenarios/provider_mysql_actors.rs",
            "tests/system-test-runner/src/scenarios/scan_statement_actor.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
        ],
        "uea4_range_reads" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/paimon.rs",
            "tests/system-test-runner/src/scenarios/provider_mysql_actors.rs",
            "tests/system-test-runner/src/scenarios/scan_statement_actor.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
        ],
        "uea4_scan_readiness" => &[
            "tests/system-test-runner/src/scenarios/connector.rs",
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/paimon.rs",
            "tests/system-test-runner/src/scenarios/provider_mysql_actors.rs",
            "tests/system-test-runner/src/scenarios/scan_statement_actor.rs",
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs",
            "tests/system-test-runner/src/scenarios/uea4_catalog_planning.rs",
        ],
        _ => &[],
    };
    for &source in helper_sources {
        if !paths.iter().any(|path| path == source) {
            paths.push(source.into());
        }
    }
    if module.starts_with("mv_") {
        for source in [
            "tests/system-test-runner/src/scenarios/mv_control_plan.rs",
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs",
            "tests/system-test-runner/src/scenarios/scan_statement_actor.rs",
        ] {
            if !paths.iter().any(|path| path == source) {
                paths.push(source.into());
            }
        }
    }
    if paths
        .iter()
        .any(|path| path == "tests/system-test-runner/src/scenarios/storage_control_plan.rs")
        && !paths
            .iter()
            .any(|path| path == "tests/system-test-runner/src/scenarios/scan_statement_actor.rs")
    {
        paths.push("tests/system-test-runner/src/scenarios/scan_statement_actor.rs".into());
    }
    if module == "native_creation" {
        paths.push("tests/system-test-runner/src/actors/trino_protocol.rs".into());
    }
    if module == "query_lifecycle" && name == "query-lifecycle/limit-zero-unknown-sender" {
        // Only ordinary setup uses the migrated shared catalog helper. The
        // original target protocol and its P05 properties remain unported.
        paths.extend([
            "novarocks/trino-adapter/**".into(),
            "tests/system-test-runner/src/actors/sql.rs".into(),
            "tests/system-test-runner/src/scenarios/connector.rs".into(),
            "tests/system-test-runner/src/scenarios/connector_control_plan.rs".into(),
            "tests/system-test-runner/src/scenarios/native_sql_controls.rs".into(),
            "tests/system-test-runner/src/scenarios/paimon.rs".into(),
            "tests/system-test-runner/src/scenarios/scan_statement_actor.rs".into(),
            "tests/system-test-runner/src/scenarios/storage_control_plan.rs".into(),
        ]);
    }
    if module.starts_with("mv_")
        || matches!(
            module,
            "mv_uea7"
                | "mv_uea7_documents"
                | "mv_uea7_handover"
                | "mv_uea7_incarnation"
                | "mv_uea7_storage"
                | "sql_client_ctas"
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
    if module == "trino_protocol" && name == "trino-protocol/command-completion-shape" {
        paths.extend(
            [
                "novarocks/query-application/**",
                "novarocks/sql/**",
                "novarocks/connector/iceberg/**",
                "novarocks/execution/**",
                "novarocks/worker/**",
                "novarocks/native-adapter/**",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }
    if module == "sql_client_ctas" {
        paths.extend([
            "tests/system-test-runner/src/actors/trino_protocol.rs".into(),
            "tests/system-test-runner/src/scenarios/trino_protocol.rs".into(),
        ]);
    }
    let mut declaration = ScenarioDeclaration {
        reference_inputs: Vec::new(),
        impact: ScenarioImpact { paths },
        env_inputs: Vec::new(),
        initial_state: Some("isolated"),
        basis: Some(
            "Each invocation launches fresh FE/BE processes and a unique runtime directory; generated local stores, in-process services and any private Docker fixture belong to this invocation.",
        ),
        heavy: None,
    };
    if module == "exchange_flow" {
        declaration.basis = Some(
            "Fresh native FE+3BE and a private managed-runtime Hadoop Iceberg warehouse; original invocation budget, authenticated finite statement/role scopes, exact stream fault receipts, actual SlotPool/call guards and joined SQL/process owners. No shared fixture mutation or prepared benchmark input.",
        );
    }
    if name == "connector/distributed-writer-row-level-small-slots" {
        declaration.basis = Some(
            "Original row-level data, types, DOP and SQL deadlines on isolated FE+3BE; explicit floor1/max2/floating0/window1 and the authenticated actual append hold/end-scope protocol. Original writer/source-summary identities, resource convergence and real exits remain required.",
        );
    }
    if module == "query_tool" {
        declaration.impact.paths.extend([
            "tools/plan-quality/**".into(),
            "tools/ci/cargo-artifacts.py".into(),
            "tests/system-test-runner/fixtures/trino_query_probe.py".into(),
        ]);
    }
    let env: &[(&str, &str)] = match module {
        "query_tool" => &[("NOVA_CI_ARTIFACT_RECEIPT", "file-content")],
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
        "uea4_catalog_planning" => &[
            ("NOVAROCKS_UEA4A4_ICEBERG_FIXTURE_MANIFEST", "file-content"),
            ("NOVAROCKS_UEA4A4_PAIMON_FIXTURE_MANIFEST", "file-content"),
            ("NOVAROCKS_UEA4A4_S3_ACCESS_KEY_ID", "value"),
            ("NOVAROCKS_UEA4A4_S3_SECRET_ACCESS_KEY", "value"),
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
            "catalog_publication"
                | "sql_client_ctas"
                | "table_maintenance"
                | "listing_real_rest"
                | "uea4_range_reads"
                | "iceberg_delete_applicability"
                | "iceberg_delete_warm"
                | "managed_output_storage"
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
        "paimon" | "uea4_scan_readiness" | "uea4_catalog_planning" | "frontend_lifecycle"
    ) {
        declaration.initial_state = None;
        declaration.basis = Some(
            "Uses externally prepared or shared provider state; an invocation-wide reset or immutability contract has not been established.",
        );
    }
    if matches!(
        module,
        "uea4_range_reads" | "iceberg_delete_applicability" | "iceberg_delete_warm"
    ) {
        declaration.initial_state = Some("isolated");
        declaration.basis = Some(
            "Creates and verifies deterministic fixture data inside the scenario's private REST, object-store and Spark lifecycle before using it on 1FE+3BE.",
        );
        declaration
            .impact
            .paths
            .push("docker/iceberg-rest/**".into());
        if matches!(
            module,
            "iceberg_delete_applicability" | "iceberg_delete_warm"
        ) {
            declaration
                .impact
                .paths
                .push("tests/sql/fixtures/iceberg-delete-applicability/**".into());
        }
    }
    if explicit {
        let (reason, detail) = if compatibility {
            (
                "variant-binary",
                "Requires compatible or alternate-island server artifacts across the complete distributed lifecycle.",
            )
        } else if matches!(
            module,
            "uea4_catalog_planning" | "uea4_scan_readiness" | "paimon"
        ) {
            (
                "dataset",
                "Requires an explicitly prepared provider dataset or independently generated oracle receipt.",
            )
        } else if matches!(
            module,
            "uea4_range_reads" | "iceberg_delete_applicability" | "iceberg_delete_warm"
        ) {
            (
                "fixture",
                "Requires private Docker REST, object store and Spark to generate and verify deterministic data within the invocation.",
            )
        } else if matches!(
            module,
            "mv_uea7"
                | "mv_uea7_documents"
                | "mv_uea7_handover"
                | "mv_uea7_incarnation"
                | "mv_uea7_storage"
                | "sql_client_ctas"
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
    fn p10_exchange_and_small_slot_writer_share_their_actual_input_owners() {
        let scenarios = super::super::all();
        for id in [
            "exchange/bounded-backpressure",
            "exchange/cross-query-probe-build",
            "exchange/unit-replay",
            "exchange/grant-subscription-rebuild",
            "exchange/long-wait-without-idle-failure",
            "exchange/control-liveness",
            "connector/distributed-writer-row-level-small-slots",
        ] {
            let matches: Vec<_> = scenarios
                .iter()
                .filter(|scenario| scenario.name() == id)
                .collect();
            assert_eq!(
                matches.len(),
                1,
                "P10 exact ID must be registered once: {id}"
            );
            let declaration = matches[0].declaration();
            assert_eq!(declaration.initial_state, Some("isolated"));
            for owner in [
                "novarocks/execution/**",
                "novarocks/physical-plan/**",
                "novarocks/task-codec/**",
                "tests/cluster-harness/**",
                "tests/system-test-runner/src/scenarios/exchange_flow.rs",
            ] {
                assert!(
                    declaration.impact.paths.iter().any(|path| path == owner),
                    "{id} omits {owner}"
                );
            }
        }
    }

    #[test]
    fn every_native_scenario_declares_shared_process_and_session_owners() {
        let scenarios = super::super::all();
        assert!(!scenarios.is_empty());
        for scenario in scenarios {
            let declaration = scenario.declaration();
            for shared in [
                "tests/trino-client/**",
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
