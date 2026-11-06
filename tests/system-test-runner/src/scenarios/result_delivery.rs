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

use super::result_delivery_support::await_idle;
use crate::actors::sql::{SqlSession, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason, domain};
use novarocks_proto_models::{novarocks as proto, result as result_proto};
use prost::Message;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    let mut scenarios = super::result_delivery_trino_rows::scenarios();
    scenarios.push(Box::new(RootReadRefusal));
    scenarios.extend(super::result_delivery_trino_retention::scenarios());
    scenarios
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefusalManifest {
    schema_version: u32,
    topology: String,
    scope: String,
    path: String,
    cases: Vec<RefusalCase>,
    health: HealthQuery,
    identity_refusal: IdentityRefusalFreeze,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRefusalFreeze {
    class: String,
    domain: u16,
    reason: u16,
    evidence: String,
    safe_detail: String,
    ack_receipt: bool,
    resource_facts: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, serde::Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum RefusalResponseContract {
    TypedIdentityRefusal,
    MalformedNoBody,
}

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct RefusalCase {
    name: String,
    profile_id: u32,
    kind: String,
    wanted_sequence: u64,
    grpc_status: u16,
    response_contract: RefusalResponseContract,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthQuery {
    sql: String,
    expected_value: i64,
}

fn foreign_root_task() -> proto::TaskIdentity {
    proto::TaskIdentity {
        query_execution_id: Some(proto::QueryExecutionId {
            query_id: Some(novarocks_proto_models::common::UniqueId { hi: 1, lo: 2 }),
            attempt_id: 1,
        }),
        stage_id: 1,
        task_id: 1,
        // Deliberately foreign, but structurally valid. A valid V1 read
        // must reach exact-process refusal; each malformed field below
        // must instead be rejected before that same route lookup.
        backend_process_id: Some(proto::BackendProcessId {
            value: novarocks_types::BackendProcessId::new_v7()
                .to_bytes()
                .to_vec(),
        }),
    }
}

fn refusal_request(
    case: &RefusalCase,
    root_task: &proto::TaskIdentity,
) -> Result<proto::FetchRootResultRequest> {
    use result_proto::root_output_kind::Kind;
    let kind = match case.kind.as_str() {
        "client_rows_true" => Some(Kind::ClientRows(true)),
        "client_rows_false" => Some(Kind::ClientRows(false)),
        "absent" => None,
        "unknown_domain_999" => Some(Kind::InternalFacts(999)),
        _ => anyhow::bail!("unknown frozen root refusal case"),
    };
    Ok(proto::FetchRootResultRequest {
        root_task: Some(root_task.clone()),
        profile_id: case.profile_id,
        output_kind: kind.map(|kind| result_proto::RootOutputKind { kind: Some(kind) }),
        wanted_sequence: Some(case.wanted_sequence),
        consumed_sequence: 0,
        max_wait_millis: 100,
    })
}

// This independent response oracle does not call the Root response decoder.
// Transport status alone cannot prove the owning channel's typed refusal.
fn assert_root_refusal_response(
    case: &RefusalCase,
    request: &proto::FetchRootResultRequest,
    response: &super::native_trust::GrpcProbe,
) -> Result<()> {
    ensure!(
        response.http_status == 200 && response.grpc_status == Some(case.grpc_status),
        "root refusal transport status differs from its frozen case"
    );
    let body = response
        .response_body
        .as_deref()
        .context("root refusal requires the original bounded response body observation")?;
    ensure!(
        body.len() <= 4096,
        "root refusal response exceeds the original probe cap"
    );
    match case.response_contract {
        RefusalResponseContract::MalformedNoBody => {
            ensure!(
                case.grpc_status == 3 && body.is_empty(),
                "malformed request must return grpc3 without a business response"
            );
        }
        RefusalResponseContract::TypedIdentityRefusal => {
            ensure!(
                case.grpc_status == 0,
                "a legal request uses its typed owner refusal"
            );
            ensure!(
                body.len() >= 5 && body[0] == 0,
                "root refusal requires one complete uncompressed gRPC frame"
            );
            let length = u32::from_be_bytes(body[1..5].try_into()?) as usize;
            ensure!(
                body[5..].len() == length,
                "root refusal has a truncated or additional frame"
            );
            let decoded = proto::FetchRootResultResponse::decode(&body[5..])?;
            ensure!(
                decoded.root_task == request.root_task
                    && decoded.profile_id == request.profile_id
                    && decoded.output_kind == request.output_kind,
                "typed refusal changed the original exact root/profile/purpose binding"
            );
            let Some(proto::fetch_root_result_response::Result::Refused(refused)) = decoded.result
            else {
                anyhow::bail!(
                    "legal foreign-root request must return Refused, without a reply/ACK or deferral"
                );
            };
            let fact = novarocks_task_codec::status::decode_failure_fact(
                refused
                    .fact
                    .as_ref()
                    .context("typed root refusal omitted its domain fact")?,
                novarocks_proto_codec::FieldPath::root("independent_root_refusal").field("fact"),
            )?;
            let expected = FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(43).unwrap()),
            );
            ensure!(
                fact == expected,
                "owning channel identity refusal lost its exact originating fact or invented resources: {fact:?}"
            );
            ensure!(
                refused.safe_detail == "root read was refused by its owning channel",
                "owning channel refusal changed its fixed safe description"
            );
        }
    }
    Ok(())
}

struct RootReadRefusal;

impl Scenario for RootReadRefusal {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "result-delivery/root-read-profile-kind-refusal"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "root refusal requires native 1FE+3BE"
        );
        let manifest: RefusalManifest = serde_json::from_str(include_str!(
            "../../../../docs/testing/trc-3/root-read-refusal-v3.json"
        ))?;
        ensure!(
            manifest.schema_version == 3
                && manifest.health.expected_value == 5050
                && manifest.topology == "1FE+3BE"
                && !manifest.scope.is_empty()
                && manifest.cases.len() == 7
                && manifest
                    .cases
                    .iter()
                    .filter(|case| case.response_contract
                        == RefusalResponseContract::TypedIdentityRefusal)
                    .count()
                    == 1
                && manifest.identity_refusal.class == "Data"
                && manifest.identity_refusal.domain == domain::WORKER.get()
                && manifest.identity_refusal.reason == 43
                && manifest.identity_refusal.evidence == "Originating"
                && manifest.identity_refusal.safe_detail
                    == "root read was refused by its owning channel"
                && !manifest.identity_refusal.ack_receipt
                && !manifest.identity_refusal.resource_facts
                && manifest.path == "/novarocks.NovaRocksGrpc/FetchRootResult",
            "unsupported root refusal manifest"
        );
        let epoch = Instant::now();
        await_idle(context, "root-refusal", "before", epoch)?;
        let root_task = foreign_root_task();
        let mut observations = Vec::new();
        for case in &manifest.cases {
            let request = refusal_request(case, &root_task)?;
            let payload = request.encode_to_vec();
            let mut frame = vec![0];
            frame.extend_from_slice(&u32::try_from(payload.len())?.to_be_bytes());
            frame.extend_from_slice(&payload);
            let response =
                super::native_trust::bounded_authenticated_probe(context, &manifest.path, &frame)?;
            let checked = assert_root_refusal_response(case, &request, &response);
            observations.push(serde_json::json!({"expected":case,"response":response,"request_bytes":frame.len()}));
            std::fs::write(
                context
                    .scenario_root()
                    .join("root-read-refusal-observations.json"),
                serde_json::to_vec_pretty(&observations)?,
            )?;
            ensure!(
                checked.is_ok(),
                "root read refusal differs from frozen case {}: {:?}",
                case.name,
                checked.err()
            );
        }
        let before = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<Vec<_>>>()?;
        let timeout = context
            .remaining("root refusal follow-up query")?
            .min(Duration::from_secs(10));
        let deadline = context.deadline().min(Instant::now() + timeout);
        let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        let health = session.execute(&manifest.health.sql)?;
        ensure!(
            health.is_result_set()
                && health.columns.len() == 1
                && health.columns[0].name == "total"
                && health.columns[0].type_name == "bigint"
                && health.columns[0].type_signature.raw_type == "bigint"
                && health.columns[0].type_signature.arguments.is_empty()
                && health.rows == vec![vec![TrinoValue::Integer(manifest.health.expected_value)]],
            "native follow-up lost its declared BIGINT total=5050 after root refusals"
        );
        std::fs::write(
            context.scenario_root().join("root-refusal-follow-up.json"),
            serde_json::to_vec_pretty(&serde_json::json!({"query_id":health.query_id,
                "columns":health.columns,"rows":1,"observed_value":5050}))?,
        )?;
        let after = (0..3)
            .map(|index| context.handle().backend_task_execution_tasks_created(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            before
                .iter()
                .zip(&after)
                .all(|(a, b)| a.is_finite() && b.is_finite() && b >= a)
                && before.iter().zip(&after).any(|(a, b)| b > a),
            "follow-up never created a native task"
        );
        await_idle(context, "root-refusal", "after", epoch)?;
        context.record_phase_observation(
            "root-read-refusal",
            1,
            1,
            1,
            "authenticated-native-root",
            7,
            "passed",
            BTreeMap::from([
                ("refusal_probes", 7),
                ("health_rows", health.rows.len() as u64),
            ]),
        )?;
        context.action("verified seven frozen authenticated root request refusals, exact native follow-up rows and public owner convergence");
        Ok(())
    }
}

const ROOT_RESOURCES: [&str; 14] = [
    "channels",
    "terminal_task_records",
    "producers_running",
    "producers_exited",
    "ends_published",
    "ends_acknowledged",
    "sealed",
    "data_positions",
    "payload_bytes",
    "segments",
    "deliveries",
    "retained_reservations",
    "metadata_holders",
    "metadata_bytes",
];

pub(super) fn root_census(rows: &[serde_json::Value]) -> Result<Option<BTreeMap<String, u64>>> {
    use super::result_delivery_support::metric;
    let available = metric(
        rows,
        "novarocks_backend_root_ownership_snapshot_available",
        &[],
    )?;
    ensure!(available <= 1, "invalid root census availability");
    if available == 0 {
        ensure!(
            !rows
                .iter()
                .any(|row| row["tags"]["metric"] == "novarocks_backend_root_ownership"),
            "unavailable root census published stale ownership"
        );
        return Ok(None);
    }
    ROOT_RESOURCES
        .iter()
        .map(|name| {
            Ok((
                (*name).to_owned(),
                metric(
                    rows,
                    "novarocks_backend_root_ownership",
                    &[("resource", name)],
                )?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()
        .map(Some)
}

#[cfg(test)]
mod census_tests {
    use super::*;
    #[test]
    fn census_missing_partial_duplicate_stale_and_invalid_samples_are_refused() {
        use serde_json::json;
        let available = json!({"tags":{"metric":"novarocks_backend_root_ownership_snapshot_available"},"value":1});
        assert!(root_census(&[]).is_err());
        assert!(root_census(std::slice::from_ref(&available)).is_err());
        let mut rows = vec![available.clone()];
        rows.extend(ROOT_RESOURCES.iter().map(|name| json!({"tags":{"metric":"novarocks_backend_root_ownership","resource":name},"value":0})));
        assert!(root_census(&rows).unwrap().is_some());
        rows.push(rows[1].clone());
        assert!(root_census(&rows).is_err());
        rows.pop();
        rows[0]["value"] = json!(0);
        assert!(root_census(&rows).is_err());
        assert_eq!(root_census(&rows[..1]).unwrap(), None);
        rows[0] = available;
        rows[1]["value"] = json!(-1);
        assert!(root_census(&rows).is_err());
    }
}
#[cfg(test)]
mod root_refusal_v3_tests {
    use super::*;

    fn freeze() -> RefusalManifest {
        serde_json::from_str(include_str!(
            "../../../../docs/testing/trc-3/root-read-refusal-v3.json"
        ))
        .unwrap()
    }
    fn captured(message: proto::FetchRootResultResponse) -> super::super::native_trust::GrpcProbe {
        let payload = message.encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        frame.extend_from_slice(&payload);
        super::super::native_trust::GrpcProbe {
            http_status: 200,
            grpc_status: Some(0),
            grpc_message: None,
            response_body: Some(frame),
        }
    }
    fn refused(request: &proto::FetchRootResultRequest) -> proto::FetchRootResultResponse {
        proto::FetchRootResultResponse {
            root_task: request.root_task.clone(),
            profile_id: request.profile_id,
            output_kind: request.output_kind,
            result: Some(proto::fetch_root_result_response::Result::Refused(
                proto::RootReadRefusal {
                    fact: Some(novarocks_task_codec::status::encode_failure_fact(
                        FailureFact::new(
                            FailureClass::Data,
                            FailureReason::new(
                                domain::WORKER,
                                std::num::NonZeroU16::new(43).unwrap(),
                            ),
                        ),
                    )),
                    safe_detail: "root read was refused by its owning channel".into(),
                },
            )),
        }
    }
    #[test]
    fn legal_foreign_root_requires_complete_original_typed_identity_refusal() {
        let freeze = freeze();
        assert_eq!(freeze.schema_version, 3);
        let case = &freeze.cases[0];
        assert_eq!(
            case.response_contract,
            RefusalResponseContract::TypedIdentityRefusal
        );
        let request = refusal_request(case, &foreign_root_task()).unwrap();
        let original = refused(&request);
        assert!(assert_root_refusal_response(case, &request, &captured(original.clone())).is_ok());
        for mode in 0..6 {
            let mut changed = original.clone();
            match mode {
                0 => changed.profile_id = 2,
                1 => changed.root_task.as_mut().unwrap().task_id += 1,
                2 => changed.result = None,
                3 => {
                    let Some(proto::fetch_root_result_response::Result::Refused(value)) =
                        changed.result.as_mut()
                    else {
                        unreachable!()
                    };
                    value.fact = None;
                }
                4 => {
                    let Some(proto::fetch_root_result_response::Result::Refused(value)) =
                        changed.result.as_mut()
                    else {
                        unreachable!()
                    };
                    value.fact.as_mut().unwrap().reason.as_mut().unwrap().code = 44;
                }
                _ => {
                    let Some(proto::fetch_root_result_response::Result::Refused(value)) =
                        changed.result.as_mut()
                    else {
                        unreachable!()
                    };
                    value.safe_detail = "foreign Display is not the safe owner summary".into();
                }
            }
            assert!(
                assert_root_refusal_response(case, &request, &captured(changed)).is_err(),
                "accepted mutation {mode}"
            );
        }
        let mut unknown = captured(original.clone());
        unknown.response_body = None;
        assert!(assert_root_refusal_response(case, &request, &unknown).is_err());
        let mut empty = captured(original.clone());
        empty.response_body = Some(Vec::new());
        assert!(assert_root_refusal_response(case, &request, &empty).is_err());
        let mut suffix = captured(original);
        suffix
            .response_body
            .as_mut()
            .unwrap()
            .extend_from_slice(&[0, 0, 0, 0, 0]);
        assert!(assert_root_refusal_response(case, &request, &suffix).is_err());
    }
    #[test]
    fn all_six_malformed_requests_keep_grpc3_and_no_business_body() {
        let freeze = freeze();
        for case in &freeze.cases[1..] {
            assert_eq!(
                case.response_contract,
                RefusalResponseContract::MalformedNoBody
            );
            assert_eq!(case.grpc_status, 3);
            let request = refusal_request(case, &foreign_root_task()).unwrap();
            let mut response = super::super::native_trust::GrpcProbe {
                http_status: 200,
                grpc_status: Some(3),
                grpc_message: None,
                response_body: Some(Vec::new()),
            };
            assert!(assert_root_refusal_response(case, &request, &response).is_ok());
            response.response_body = captured(refused(&request)).response_body;
            assert!(assert_root_refusal_response(case, &request, &response).is_err());
        }
    }
}
