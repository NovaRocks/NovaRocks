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

//! Exact installed-root identity and bounded authenticated protocol probes.

#[path = "result_delivery_root_held_response.rs"]
pub(crate) mod held_response;

#[path = "result_delivery_root_owned_probe.rs"]
mod owned_probe;
pub(super) use owned_probe::probe_owned;

use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use h2::client;
use http::{HeaderMap, Request, header};
use novarocks_cluster_harness::NativeTrustFixtureMode;
use novarocks_execution_contract::root_result::{
    RootReadOutcome, RootReadResponse, RootResultReply,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{common, novarocks as proto};
use novarocks_task_codec::identity::decode_task_identity;
use novarocks_task_codec::root_result::{decode_read, decode_response};
use novarocks_types::{BackendProcessId, NativeEndpoint};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::time::Instant;

const CREATED_MARKER: &str = "NOVAROCKS_TASK_CREATE_APPLIED";
const REQUEST_CAP: usize = 4096;
const RESPONSE_CAP: usize = 1048576 + 4096;
const ROOT_PATH: &str = "/novarocks.NovaRocksGrpc/FetchRootResult";
const CANDIDATE_CAP: usize = 8;

/// Resolve the sole new task, rather than infer a stage or task from SQL shape.
/// The caller separately proves that this task owns the sole occupied root.
#[cfg(test)]
pub(super) fn parse_unique_created_task(
    before: &[String],
    after: &[String],
    occupied_be: usize,
) -> Result<proto::TaskIdentity> {
    let mut created = appended_created_tasks(before, after, occupied_be)?;
    ensure!(
        created.len() == 1,
        "expected exactly one new task-create marker"
    );
    let (backend, identity) = created.pop().context("missing task-create marker")?;
    ensure!(
        backend == occupied_be,
        "new task-create marker is outside the occupied backend"
    );
    Ok(identity)
}

/// Candidate identities are observed tasks, not synthesized root addresses.
/// The caller must resolve exactly one using the actual context root owner.
pub(super) fn parse_created_task_candidates(
    before: &[String],
    after: &[String],
    occupied_be: usize,
) -> Result<Vec<proto::TaskIdentity>> {
    let created = appended_created_tasks(before, after, occupied_be)?;
    let mut candidates = Vec::with_capacity(CANDIDATE_CAP);
    for (backend, identity) in created {
        if backend == occupied_be {
            ensure!(
                candidates.len() < CANDIDATE_CAP,
                "root task candidate bound exceeded"
            );
            candidates.push(identity);
        }
    }
    ensure!(
        !candidates.is_empty(),
        "no task candidates on the occupied backend"
    );
    Ok(candidates)
}

fn appended_created_tasks(
    before: &[String],
    after: &[String],
    occupied_be: usize,
) -> Result<Vec<(usize, proto::TaskIdentity)>> {
    ensure!(
        before.len() == after.len() && occupied_be < before.len(),
        "root identity log inventory differs from the occupied backend"
    );
    let mut created = Vec::new();
    let mut identities = BTreeSet::new();
    let mut execution = None;
    for (backend, (old, new)) in before.iter().zip(after).enumerate() {
        let appended = new
            .strip_prefix(old)
            .context("backend log was replaced instead of appended")?;
        ensure!(
            old.is_empty() || old.ends_with('\n'),
            "backend log baseline ends inside a line"
        );
        ensure!(
            appended.is_empty() || appended.ends_with('\n'),
            "backend log observation ends inside a line"
        );
        for line in appended.lines() {
            if !line.contains(CREATED_MARKER) {
                continue;
            }
            let identity = parse_created_line(line)?;
            let typed = decode_task_identity(&identity, FieldPath::root("created_task_candidate"))?;
            ensure!(
                identities.insert(typed),
                "duplicate task-create identity in appended logs"
            );
            if let Some(execution) = execution {
                ensure!(
                    execution == typed.query_execution_id(),
                    "appended task-create markers span multiple executions"
                );
            } else {
                execution = Some(typed.query_execution_id());
            }
            created.push((backend, identity));
        }
    }
    Ok(created)
}

fn parse_created_line(line: &str) -> Result<proto::TaskIdentity> {
    let fields: Vec<_> = line.split(' ').collect();
    ensure!(
        fields.len() == 5 && fields[0] == CREATED_MARKER,
        "task-create marker has an unexpected field layout"
    );
    let execution = fields[1]
        .strip_prefix("execution_id=")
        .context("task-create marker is missing execution identity")?;
    let components: Vec<_> = execution.split(':').collect();
    ensure!(components.len() == 3, "invalid execution identity fields");
    // Query high is a signed process namespace, not a positive counter.
    let hi: i64 = canonical_number(components[0])?;
    let lo: i64 = canonical_number(components[1])?;
    let attempt: u64 = canonical_number(components[2])?;
    let stage: u32 = canonical_number(
        fields[2]
            .strip_prefix("stage=")
            .context("missing stage identity")?,
    )?;
    let task: u32 = canonical_number(
        fields[3]
            .strip_prefix("task=")
            .context("missing task identity")?,
    )?;
    // The typed decoder owns validity, including the complete signed QueryId
    // pair and nonzero attempt/stage/task counters.
    let backend_text = fields[4]
        .strip_prefix("backend=")
        .context("missing backend process identity")?;
    let backend: BackendProcessId = backend_text
        .parse()
        .context("invalid backend process UUIDv7")?;
    ensure!(
        backend.to_string() == backend_text,
        "backend process identity is not a canonical UUIDv7"
    );
    let identity = proto::TaskIdentity {
        query_execution_id: Some(proto::QueryExecutionId {
            query_id: Some(common::UniqueId { hi, lo }),
            attempt_id: attempt,
        }),
        stage_id: stage,
        task_id: task,
        backend_process_id: Some(proto::BackendProcessId {
            value: backend.to_bytes().to_vec(),
        }),
    };
    decode_task_identity(&identity, FieldPath::root("created_root"))?;
    Ok(identity)
}

fn canonical_number<T>(text: &str) -> Result<T>
where
    T: std::str::FromStr + std::fmt::Display,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value: T = text.parse().context("invalid task-create numeric field")?;
    ensure!(
        value.to_string() == text,
        "non-canonical task-create numeric field"
    );
    Ok(value)
}

/// One sequential read against the actual BE listener, with no busy retry.
/// The maximum watermark must come from independently proven delivery/End.
pub(super) fn probe(
    context: &mut ScenarioContext,
    backend: usize,
    request: &proto::FetchRootResultRequest,
    maximum_delivered_consumed: u64,
    deadline: Instant,
) -> Result<(RootResultReply, serde_json::Value)> {
    let (reply, observation) = probe_candidate(
        context,
        backend,
        request,
        maximum_delivered_consumed,
        deadline,
    )?;
    Ok((
        reply.context("selected task is not the actual context root")?,
        observation,
    ))
}

/// Unknown-root is the only allowed negative discovery result. A preparing,
/// mismatched or exhausted owner is a failed observation, never a retry hint.
pub(super) fn probe_candidate(
    context: &mut ScenarioContext,
    backend: usize,
    request: &proto::FetchRootResultRequest,
    maximum_delivered_consumed: u64,
    deadline: Instant,
) -> Result<(Option<RootResultReply>, serde_json::Value)> {
    ensure!(
        maximum_delivered_consumed != u64::MAX,
        "root probe requires a finite proven consumption watermark"
    );
    let expected = decode_read(request, FieldPath::root("root_probe_request"))?;
    ensure!(
        request.consumed_sequence <= maximum_delivered_consumed,
        "root probe ACK exceeds its proven delivery watermark"
    );
    let message = request.encode_to_vec();
    let frame_len = message
        .len()
        .checked_add(5)
        .context("root request length overflow")?;
    ensure!(
        frame_len <= REQUEST_CAP,
        "root request frame exceeds its bound"
    );
    let mut frame = Vec::with_capacity(frame_len);
    frame.push(0);
    frame.extend_from_slice(&u32::try_from(message.len())?.to_be_bytes());
    frame.extend_from_slice(&message);

    let handle = context.handle();
    let mode = handle.native_trust_mode();
    let advertised = handle.native_be_endpoint(backend)?;
    ensure!(
        mode == NativeTrustFixtureMode::Plaintext
            && advertised.host().parse::<std::net::IpAddr>().is_ok(),
        "installed root probe requires plaintext and an IP endpoint"
    );
    let port = handle
        .runtime()
        .be
        .get(backend)
        .context("invalid root backend index")?
        .grpc;
    let endpoint =
        NativeEndpoint::from_host_port(advertised.host(), port).map_err(anyhow::Error::msg)?;
    let connector = handle.native_probe_connector(endpoint, mode)?;
    let mut authorization_request = tonic::Request::new(());
    handle
        .native_probe_trust()?
        .apply_client_authorization(authorization_request.metadata_mut())
        .map_err(anyhow::Error::msg)?;
    let authorization = authorization_request
        .metadata()
        .get("authorization")
        .context("root probe authorization was not issued")?
        .to_str()
        .context("root probe authorization is not ASCII")?
        .to_owned();
    let remaining = context.remaining("installed root protocol probe")?;
    let deadline = deadline.min(Instant::now() + remaining);
    ensure!(
        Instant::now() < deadline,
        "root protocol probe deadline expired"
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (frame, status) = runtime.block_on(async {
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            let stream = connector
                .connect()
                .await
                .map_err(anyhow::Error::msg)
                .context("connect actual root BE listener")?;
            let (mut sender, connection) = client::handshake(stream)
                .await
                .context("handshake root probe HTTP/2")?;
            let driver = ConnectionDriver(Some(tokio::spawn(connection)));
            let request = Request::builder()
                .method("POST")
                .uri(ROOT_PATH)
                .header(header::CONTENT_TYPE, "application/grpc")
                .header("te", "trailers")
                .header(header::AUTHORIZATION, authorization)
                .body(())?;
            let (response, mut send) = sender.send_request(request, false)?;
            send.send_data(Bytes::from(frame), true)?;
            drop(send);
            let response = response.await.context("receive root probe response")?;
            ensure!(
                response.status() == http::StatusCode::OK,
                "root probe HTTP status is not 200"
            );
            grpc_content_type(response.headers())?;
            let header_status = candidate_status(response.headers())?;
            let mut body = response.into_body();
            let mut bytes = Vec::with_capacity(RESPONSE_CAP);
            let mut saw_data = false;
            while let Some(chunk) = body.data().await {
                let chunk = chunk.context("read root probe DATA")?;
                saw_data = true;
                let length = bytes
                    .len()
                    .checked_add(chunk.len())
                    .context("root response length overflow")?;
                ensure!(
                    length <= RESPONSE_CAP,
                    "root response DATA exceeds its bound"
                );
                body.flow_control().release_capacity(chunk.len())?;
                bytes.extend_from_slice(&chunk);
            }
            let trailers = body.trailers().await.context("read root probe trailers")?;
            let trailer_status = trailers
                .as_ref()
                .map(candidate_status)
                .transpose()?
                .flatten();
            let status = response_status(header_status, trailer_status, &bytes, saw_data)?;
            drop(body);
            drop(sender);
            driver.finish().await;
            Ok::<_, anyhow::Error>((bytes, status))
        })
        .await
        .context("absolute root protocol probe deadline exceeded")?
    })?;
    ensure!(
        Instant::now() < deadline,
        "root protocol probe completed after its deadline"
    );
    ensure!(
        status == ProbeStatus::Success,
        "root response transport did not succeed"
    );
    let message = single_message(&frame)?;
    let wire =
        proto::FetchRootResultResponse::decode(message).context("decode root response protobuf")?;
    let response = decode_response(
        wire,
        &expected,
        maximum_delivered_consumed,
        FieldPath::root("root_probe_reply"),
    )?;
    let Some(reply) = discovery_reply(response)? else {
        let observation = serde_json::json!({
            "backend_index": backend, "actual_grpc_port": port,
            "response_frame_bytes": frame.len(), "body_bytes": 0,
            "grpc_status": 0, "outcome": "unknown_root",
            "failure_domain": "WORKER", "failure_reason": 165,
            "identity": identity_observation(expected.root_task()),
            "request_identity": identity_observation(expected.root_task())
        });
        ensure!(
            Instant::now() < deadline,
            "root candidate observation exceeded its deadline"
        );
        return Ok((None, observation));
    };
    let (outcome, sequence, body_bytes, body_sha256) = match &reply.outcome {
        RootReadOutcome::Data(data) => (
            "data",
            Some(data.sequence().get()),
            data.body().len(),
            Some(format!("{:x}", Sha256::digest(data.body()))),
        ),
        RootReadOutcome::End(end) => ("end", Some(end.sequence.get()), 0, None),
        RootReadOutcome::AckOnly => ("ack_only", None, 0, None),
        RootReadOutcome::NotReady => ("not_ready", None, 0, None),
        RootReadOutcome::Retired => ("retired", None, 0, None),
        RootReadOutcome::AwaitTerminalControl => ("await_terminal_control", None, 0, None),
    };
    let end = match &reply.outcome {
        RootReadOutcome::Data(data) => data.end_after_data(),
        RootReadOutcome::End(end) => Some(*end),
        _ => None,
    };
    let observation = serde_json::json!({
        "backend_index": backend, "actual_grpc_port": port,
        "response_frame_bytes": frame.len(), "body_bytes": body_bytes,
        "body_sha256": body_sha256, "outcome": outcome, "sequence": sequence,
        "accepted_consumed_sequence": reply.accepted_consumed,
        "end": end.map(|end| serde_json::json!({"sequence": end.sequence.get(), "output_rows": end.output_rows})),
        "grpc_status": 0, "identity": identity_observation(reply.root_task),
        "request_identity": identity_observation(expected.root_task())
    });
    ensure!(
        Instant::now() < deadline,
        "root protocol probe decoding exceeded its deadline"
    );
    Ok((Some(reply), observation))
}

fn identity_observation(identity: novarocks_execution_contract::TaskIdentity) -> serde_json::Value {
    let execution = identity.query_execution_id();
    serde_json::json!({"query_hi": execution.query_id().high(), "query_lo": execution.query_id().low(),
        "attempt": execution.attempt_id().get(), "stage": identity.stage_id().get(),
        "task": identity.task_id().get(), "backend": identity.backend_process_id().to_string()})
}

/// Abort on error/timeout as well as success; dropping a JoinHandle detaches.
struct ConnectionDriver(Option<tokio::task::JoinHandle<Result<(), h2::Error>>>);
impl ConnectionDriver {
    async fn finish(mut self) {
        if let Some(driver) = self.0.take() {
            driver.abort();
            let _ = driver.await;
        }
    }
}
impl Drop for ConnectionDriver {
    fn drop(&mut self) {
        if let Some(driver) = &self.0 {
            driver.abort();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeStatus {
    Success,
}

fn grpc_content_type(headers: &HeaderMap) -> Result<()> {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let value = values.next().context("root response has no content-type")?;
    ensure!(
        values.next().is_none(),
        "root response has duplicate content-type"
    );
    ensure!(
        value.as_bytes() == b"application/grpc",
        "root response content-type differs from application/grpc"
    );
    Ok(())
}

fn candidate_status(headers: &HeaderMap) -> Result<Option<ProbeStatus>> {
    let mut statuses = headers.get_all("grpc-status").iter();
    let Some(status) = statuses.next() else {
        return Ok(None);
    };
    ensure!(
        statuses.next().is_none(),
        "root reply has duplicate gRPC status"
    );
    ensure!(
        status.as_bytes() == b"0",
        "root response gRPC status is not success"
    );
    Ok(Some(ProbeStatus::Success))
}

fn response_status(
    header: Option<ProbeStatus>,
    trailer: Option<ProbeStatus>,
    body: &[u8],
    saw_data: bool,
) -> Result<ProbeStatus> {
    ensure!(
        header.is_none(),
        "root response must carry its status in final trailers"
    );
    ensure!(
        trailer == Some(ProbeStatus::Success),
        "root response has no final success status"
    );
    ensure!(
        saw_data && !body.is_empty(),
        "successful root reply has no DATA message"
    );
    Ok(ProbeStatus::Success)
}

// This wire probe uses the stable producer reason as an independent oracle.
// It does not classify query failures or supply replacement authority.
fn discovery_reply(response: RootReadResponse) -> Result<Option<RootResultReply>> {
    use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason, domain};
    match response {
        RootReadResponse::Reply(reply) => Ok(Some(reply)),
        RootReadResponse::Refused(refusal) => {
            let unknown = FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(165).unwrap()),
            );
            ensure!(
                refusal.fact() == unknown,
                "root candidate was refused with a different producer fact: {:?}",
                refusal.fact()
            );
            Ok(None)
        }
        RootReadResponse::Deferred(deferred) => anyhow::bail!(
            "root candidate did not provide a receipt: {:?}",
            deferred.reason()
        ),
    }
}

fn single_message(frame: &[u8]) -> Result<&[u8]> {
    ensure!(
        frame.len() <= RESPONSE_CAP && frame.len() >= 5,
        "invalid bounded root response frame size"
    );
    ensure!(frame[0] == 0, "compressed root response is unsupported");
    let length = u32::from_be_bytes(frame[1..5].try_into()?) as usize;
    ensure!(
        length == frame.len() - 5,
        "root response is not exactly one gRPC message"
    );
    Ok(&frame[5..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker() -> String {
        format!(
            "{CREATED_MARKER} execution_id=-7:23:2 stage=9 task=17 backend=019a0203-0405-7000-8000-000000000001\n"
        )
    }

    #[test]
    fn exact_new_marker_preserves_signed_namespace_and_all_task_fields() {
        let before = vec!["old log\n".into(), "".into(), "".into()];
        let after = vec!["old log\nother line\n".into(), marker(), "".into()];
        let identity = parse_unique_created_task(&before, &after, 1).unwrap();
        let execution = identity.query_execution_id.unwrap();
        assert_eq!(
            execution.query_id.unwrap(),
            common::UniqueId { hi: -7, lo: 23 }
        );
        assert_eq!(
            (execution.attempt_id, identity.stage_id, identity.task_id),
            (2, 9, 17)
        );
        // Do not impose a narrower QueryId contract than the real decoder.
        for execution_text in ["-7:0:2", "0:-23:2"] {
            let after = vec![marker().replace("-7:23:2", execution_text)];
            assert!(parse_unique_created_task(&["".into()], &after, 0).is_ok());
        }
    }

    #[test]
    fn task_candidates_preserve_the_first_appended_stdout_marker_and_order() {
        let ready = "NOVAROCKS_READY role=be grpc_port=23000\n";
        let earlier = marker().replace("-7:23:2", "-7:22:2");
        let baseline = format!("{ready}{earlier}");
        let first = marker();
        let second = marker().replace("task=17", "task=18");
        let observed = format!("{baseline}{first}{second}");
        let candidates = parse_created_task_candidates(&[baseline], &[observed], 0).unwrap();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], parse_created_line(first.trim_end()).unwrap());
        assert_eq!(
            candidates[1],
            parse_created_line(second.trim_end()).unwrap()
        );
    }

    #[test]
    fn task_candidates_reject_shrunk_or_replaced_stdout_prefixes() {
        let baseline = "NOVAROCKS_READY role=be\n";
        let same_size_replacement = baseline.replace("READY", "OTHER");
        assert_eq!(same_size_replacement.len(), baseline.len());
        for observed in [
            baseline[..baseline.len() - 1].to_owned(),
            same_size_replacement,
            format!("unexpected prefix\n{baseline}{}", marker()),
        ] {
            let error =
                parse_created_task_candidates(&[baseline.into()], &[observed], 0).unwrap_err();
            assert_eq!(
                error.to_string(),
                "backend log was replaced instead of appended"
            );
        }
    }

    #[test]
    fn rejects_replaced_logs_missing_duplicate_and_wrong_backend_markers() {
        assert!(parse_unique_created_task(&["old\n".into()], &[marker()], 0).is_err());
        assert!(parse_unique_created_task(&["".into()], &["".into()], 0).is_err());
        assert!(parse_unique_created_task(&["".into()], &[marker().repeat(2)], 0).is_err());
        assert!(
            parse_unique_created_task(&["".into(), "".into()], &[marker(), "".into()], 1).is_err()
        );
        assert!(parse_unique_created_task(&["".into()], &[marker()], 1).is_err());
        assert!(parse_unique_created_task(&["".into()], &[], 0).is_err());
        assert!(parse_unique_created_task(&[marker()], &[marker()], 0).is_err());
    }

    #[test]
    fn rejects_malformed_counter_uuid_layout_and_partial_markers() {
        for line in [
            marker().replace("stage=9", "stage=0"),
            marker().replace("task=17", "task=+17"),
            marker().replace("-7:23:2", "0:0:2"),
            marker().replace(":23:2", ":23:0"),
            marker().replace("-7000-", "-4000-"),
            marker().replace(" stage=9 task=17", " task=17 stage=9"),
            marker().replace(" task=17", " task=17 extra=1"),
            format!("prefix {}", marker()),
            marker().trim_end().to_owned(),
        ] {
            assert!(parse_unique_created_task(&["".into()], &[line], 0).is_err());
        }
    }

    #[test]
    fn candidates_keep_observed_tasks_only_on_the_occupied_backend() {
        let other = marker()
            .replace("task=17", "task=18")
            .replace("8000-000000000001", "8000-000000000002");
        let occupied = format!("{}{}", marker(), marker().replace("task=17", "task=19"));
        let before = vec!["".into(), "".into()];
        let after = vec![other, occupied];
        let candidates = parse_created_task_candidates(&before, &after, 1).unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|task| task.task_id)
                .collect::<Vec<_>>(),
            vec![17, 19]
        );
        assert!(parse_unique_created_task(&before, &after, 1).is_err());
    }

    #[test]
    fn candidate_discovery_rejects_duplicates_other_executions_and_overflow() {
        let before = vec!["".into(), "".into()];
        assert!(parse_created_task_candidates(&before, &[marker(), marker()], 0).is_err());
        let other_execution = marker().replace("-7:23:2", "-7:24:2");
        assert!(parse_created_task_candidates(&before, &[marker(), other_execution], 0).is_err());
        assert!(parse_created_task_candidates(&before, &["".into(), marker()], 0).is_err());
        let malformed_other = marker().replace("stage=9", "stage=0");
        assert!(parse_created_task_candidates(&before, &[marker(), malformed_other], 0).is_err());
        let mut log = String::new();
        for task in 1..=CANDIDATE_CAP {
            log.push_str(&marker().replace("task=17", &format!("task={task}")));
        }
        assert_eq!(
            parse_created_task_candidates(&["".into()], &[log.clone()], 0)
                .unwrap()
                .len(),
            CANDIDATE_CAP
        );
        log.push_str(&marker().replace("task=17", "task=9"));
        assert!(parse_created_task_candidates(&["".into()], &[log], 0).is_err());
    }

    #[test]
    fn grpc_frame_rejects_compression_truncation_multiple_messages_and_oversize() {
        assert_eq!(single_message(&[0, 0, 0, 0, 2, 7, 8]).unwrap(), &[7, 8]);
        for frame in [
            vec![0, 0, 0, 0],
            vec![1, 0, 0, 0, 0],
            vec![0, 0, 0, 0, 2, 7],
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![0; RESPONSE_CAP + 1],
        ] {
            assert!(single_message(&frame).is_err());
        }
    }

    #[test]
    fn grpc_status_requires_single_success_value() {
        let mut headers = HeaderMap::new();
        assert!(candidate_status(&headers).unwrap().is_none());
        headers.insert("grpc-status", "0".parse().unwrap());
        assert_eq!(
            candidate_status(&headers).unwrap(),
            Some(ProbeStatus::Success)
        );
        headers.append("grpc-status", "0".parse().unwrap());
        assert!(candidate_status(&headers).is_err());
        headers.insert("grpc-status", "8".parse().unwrap());
        assert!(candidate_status(&headers).is_err());
    }

    #[test]
    fn discovery_accepts_only_the_exact_typed_unknown_root_fact() {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        use novarocks_execution_contract::{
            SafeDetail,
            root_result::{RootReadDeferral, RootReadDeferred, RootReadRefusal},
        };
        let root = parse_unique_created_task(&["".into()], &[marker()], 0).unwrap();
        let request = proto::FetchRootResultRequest {
            root_task: Some(root),
            profile_id: 1,
            output_kind: Some(novarocks_proto_models::result::RootOutputKind {
                kind: Some(
                    novarocks_proto_models::result::root_output_kind::Kind::ClientRows(true),
                ),
            }),
            wanted_sequence: Some(1),
            consumed_sequence: 0,
            max_wait_millis: 100,
        };
        let read = decode_read(&request, FieldPath::root("test_read")).unwrap();
        for (namespace, code, class, accepted) in [
            (domain::WORKER, 165, FailureClass::Data, true),
            (domain::WORKER, 164, FailureClass::Data, false),
            (domain::WORKER, 165, FailureClass::Dependency, false),
            (domain::QUERY, 165, FailureClass::Data, false),
        ] {
            let fact = FailureFact::new(
                class,
                FailureReason::new(namespace, std::num::NonZeroU16::new(code).unwrap()),
            );
            let result = discovery_reply(RootReadResponse::Refused(Box::new(
                RootReadRefusal::for_request(
                    &read,
                    fact,
                    SafeDetail::truncating("arbitrary safe text is not a discriminator"),
                ),
            )));
            if accepted {
                assert!(result.unwrap().is_none());
            } else {
                assert!(result.is_err());
            }
        }
        assert!(
            discovery_reply(RootReadResponse::Deferred(RootReadDeferred::for_request(
                &read,
                RootReadDeferral::Preparing
            )))
            .is_err()
        );
        let mut headers = HeaderMap::new();
        headers.insert("grpc-status", "5".parse().unwrap());
        headers.insert("grpc-message", "unknown context root".parse().unwrap());
        assert!(candidate_status(&headers).is_err());
    }

    #[test]
    fn response_structure_requires_final_success_and_no_duplicate_status() {
        let success = Some(ProbeStatus::Success);
        let message = &[0, 0, 0, 0, 0];
        assert_eq!(
            response_status(None, success, message, true).unwrap(),
            ProbeStatus::Success
        );
        assert!(response_status(success, None, message, true).is_err());
        assert!(response_status(success, None, &[], false).is_err());
        assert!(response_status(success, success, message, true).is_err());
        assert!(response_status(None, success, &[], false).is_err());
        assert!(response_status(None, None, message, true).is_err());
        assert!(response_status(None, success, &[], true).is_err());
    }

    #[test]
    fn response_content_type_requires_one_exact_grpc_value() {
        let mut headers = HeaderMap::new();
        assert!(grpc_content_type(&headers).is_err());
        headers.insert(header::CONTENT_TYPE, "application/grpc".parse().unwrap());
        assert!(grpc_content_type(&headers).is_ok());
        headers.append(header::CONTENT_TYPE, "application/grpc".parse().unwrap());
        assert!(grpc_content_type(&headers).is_err());
        for value in [
            "application/grpc+proto",
            "application/grpc; charset=utf-8",
            "text/html",
        ] {
            headers.insert(header::CONTENT_TYPE, value.parse().unwrap());
            assert!(grpc_content_type(&headers).is_err());
        }
    }
}
