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

//! Core semantic-test runtime for sealed distributed query artifacts.
//!
//! This is deliberately not a coordinator implementation used by production.
//! It gives core unit tests a real native decode/pipeline/result/write path
//! after coordinator ownership moved to the frontend crate.  Frontend contract
//! tests and all-in-one production remain responsible for the real
//! Init/Stage/Start and gRPC boundaries.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use novarocks_spi::connector::{
    ConnectorExecutionBinding, ConnectorExecutionBindingKey, ConnectorExecutionDeclaration,
    ConnectorExecutionResolver,
};

use crate::cache::CacheOptions;
use crate::query_execution::artifact::{
    BackendPlacement, FragmentScheduleDraft, ValidatedFragmentSchedule,
};
use crate::query_execution::backend::LiveBackendTarget;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
    DistributedQueryRequest, QueryId,
};
use crate::query_execution::fragment_transport::FetchedQueryBatch;
use crate::query_execution::lifecycle::{AttemptId, FragmentTerminalSnapshot, QueryExecutionId};
use crate::query_execution::outcome::FragmentProfileSet;
use crate::query_execution::write::WriteTerminalBuilder;
use crate::runtime::fragment::fact::{FragmentCancelReason, FragmentOutcome, FragmentTerminalFact};
use crate::runtime::fragment::io::{NoopFragmentEventSink, UnavailableFragmentLookupClient};
use crate::runtime::fragment::prepare_fragment;
use crate::runtime::native_fragment_query::NativeFragmentQueryRuntime;
use crate::runtime::profile::Profiler;
use crate::runtime::query_options::query_expire_durations;
use crate::runtime::result_buffer::{TryFetchTypedResult, wait_fetch_typed};

static NEXT_TEST_QUERY_ID: AtomicI64 = AtomicI64::new(1);
const TEST_QUERY_ID_HIGH: i64 = i64::MIN + 0x4e52;

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

#[derive(Default)]
struct TestExecutionResolver {
    bindings: BTreeMap<ConnectorExecutionBindingKey, Arc<ConnectorExecutionBinding>>,
}

impl ConnectorExecutionResolver for TestExecutionResolver {
    fn resolve(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<Arc<ConnectorExecutionBinding>, novarocks_spi::connector::ConnectorError> {
        self.bindings.get(key).cloned().ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::NotFound,
                format!(
                    "in-process test runtime did not install connector instance `{}`",
                    key.instance_id.as_str()
                ),
            )
        })
    }
}

fn install_connector_bindings(
    declarations: &[ConnectorExecutionDeclaration],
) -> Result<Arc<dyn ConnectorExecutionResolver>, DistributedQueryError> {
    let installers =
        crate::connector::compose_backend_connector_execution_installers(None).map_err(failed)?;
    let context = crate::connector::test_request_context();
    let mut resolver = TestExecutionResolver::default();
    for declaration in declarations {
        let installer = installers
            .iter()
            .find(|installer| installer.provider_id() == &declaration.descriptor().provider_id)
            .ok_or_else(|| {
                failed(format!(
                    "in-process test runtime has no installer for connector provider `{}`",
                    declaration.descriptor().provider_id.as_str()
                ))
            })?;
        let binding = installer
            .install(declaration, &context)
            .map_err(|error| failed(error.to_string()))?;
        resolver
            .bindings
            .insert(binding.key().clone(), Arc::new(binding));
    }
    Ok(Arc::new(resolver))
}

pub(crate) fn execute(
    request: DistributedQueryRequest,
) -> Result<crate::query_execution::contract::DistributedQueryOutcome, DistributedQueryError> {
    crate::novarocks_config::install_default_for_test();
    let parts = request.into_parts();
    let query_id = QueryId::new(
        TEST_QUERY_ID_HIGH,
        NEXT_TEST_QUERY_ID.fetch_add(1, Ordering::Relaxed).max(1),
    );
    let execution_id = QueryExecutionId::new(
        query_id,
        AttemptId::new(1).expect("test attempt is nonzero"),
    )
    .map_err(|error| failed(error.to_string()))?;
    let endpoint = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
    let target = LiveBackendTarget::new(0, endpoint, 1);
    let live_backends = vec![target];
    let view = parts.artifacts.scheduling_view();
    let fragment_ids = view.fragment_ids().collect::<Vec<_>>();
    let mut draft = FragmentScheduleDraft::new();
    draft.freeze_live_backends(live_backends.clone())?;
    for fragment_id in fragment_ids {
        draft.assign_fragment(fragment_id, vec![BackendPlacement::new(0, endpoint)])?;
    }
    let schedule = ValidatedFragmentSchedule::validate(view, execution_id, draft)?;
    let artifact = parts
        .artifacts
        .bind_schedule(schedule)?
        .assemble_for_in_process_test(query_id, &parts.options, &live_backends)?;
    let resolver = install_connector_bindings(&artifact.declarations)?;
    let writer_ids = artifact
        .writer_registrations
        .writer_identities()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let connector_registry = Arc::new(crate::connector::ConnectorRegistry::new());
    let exchange = crate::runtime::fragment::io::exchange::in_process_test_exchange_transmitter();
    let result_writer = crate::runtime::fragment::io::result::in_process_test_result_writer();
    let lookup = Arc::new(UnavailableFragmentLookupClient);
    let events = Arc::new(NoopFragmentEventSink);
    let profile_enabled = parts.completion.intent() == DistributedQueryIntent::Profile;

    let mut dormant = Vec::with_capacity(artifact.submissions.len());
    for submission in artifact.submissions {
        let decoded =
            crate::protocol::native::decode::decode_fragment_submission_with_connectors_and_execution_resolver(
                &submission.plan,
                &submission.instance_params,
                Arc::clone(&connector_registry),
                Arc::clone(&resolver),
            )
            .map_err(|error| failed(error.to_string()))?;
        let (submission, _) = decoded.into_parts();
        let has_runtime_filter_bindings = submission.program().runtime_filters().has_bindings();
        if has_runtime_filter_bindings {
            return Err(failed(
                "in-process native test execution requires an injected runtime-filter session",
            ));
        }
        let fragment_instance_id = submission.fragment_instance_id();
        let (delivery_expire, query_expire) =
            query_expire_durations(Some(submission.query_options()));
        let cache_options =
            CacheOptions::from_query_options(Some(submission.query_options())).map_err(failed)?;
        let admission = NativeFragmentQueryRuntime::global()
            .prepare_admission(
                submission.query_id(),
                fragment_instance_id,
                delivery_expire,
                query_expire,
                cache_options,
                None,
            )
            .map_err(failed)?;
        let profiler = profile_enabled.then(|| {
            let root_node_id = submission.program().root_plan_node_id().get();
            let profiler = Profiler::new(format!(
                "execute_fragment_native (plan_node_id={root_node_id})"
            ));
            profiler.set_metadata(i64::from(root_node_id));
            profiler
        });
        let handle = prepare_fragment(
            submission,
            admission.into_prepare_context(
                profiler,
                Arc::clone(&exchange),
                lookup.clone(),
                Arc::clone(&result_writer),
                events.clone(),
            ),
        )
        .map_err(|error| failed(error.to_string()))?;
        dormant.push(handle);
    }

    let mut running = dormant
        .into_iter()
        .map(|handle| handle.start())
        .collect::<Vec<_>>();
    for handle in &running {
        if writer_ids.contains_key(&handle.fragment_instance_id()) {
            handle.handoff_sink_commit();
        }
    }

    let terminal_facts = std::thread::scope(|scope| {
        let (sender, receiver) = std::sync::mpsc::channel();
        for (index, handle) in running.iter().cloned().enumerate() {
            let sender = sender.clone();
            scope.spawn(move || {
                let _ = sender.send((index, handle.join()));
            });
        }
        drop(sender);

        let mut facts = vec![None; running.len()];
        let mut fail_close_started = false;
        for (index, fact) in receiver {
            if !matches!(fact.outcome(), FragmentOutcome::Succeeded) && !fail_close_started {
                fail_close_started = true;
                let reason = match fact.outcome() {
                    FragmentOutcome::Succeeded => unreachable!("checked terminal outcome"),
                    FragmentOutcome::Failed(error) => error.to_string(),
                    FragmentOutcome::Cancelled { reason } => reason.detail().to_string(),
                };
                for sibling in &running {
                    sibling.cancel(FragmentCancelReason::new(format!(
                        "sibling fragment terminated: {reason}"
                    )));
                }
            }
            facts[index] = Some(fact);
        }
        facts
            .into_iter()
            .map(|fact| fact.expect("every in-process fragment join reports exactly once"))
            .collect::<Vec<FragmentTerminalFact>>()
    });

    let mut profiles = Vec::new();
    let mut failed_message = None;
    for fact in &terminal_facts {
        if let Some(profile) = fact.profile().cloned() {
            profiles.push(profile);
        }
        match fact.outcome() {
            FragmentOutcome::Succeeded => {}
            FragmentOutcome::Failed(error) => {
                failed_message.get_or_insert_with(|| error.to_string());
            }
            FragmentOutcome::Cancelled { reason } => {
                failed_message.get_or_insert_with(|| reason.detail().to_string());
            }
        }
    }
    if let Some(message) = failed_message {
        return Err(failed(message));
    }

    let mut batches = Vec::new();
    if artifact.root_fetch.uses_result_buffer() {
        loop {
            match wait_fetch_typed(artifact.root_fetch.fragment_instance_id(), 100) {
                TryFetchTypedResult::Ready(result) if result.eos => break,
                TryFetchTypedResult::Ready(result) => {
                    let mut chunks = crate::runtime::exchange::decode_root_result_chunks(
                        &result.payload,
                        Some(artifact.expected_output.fetch_view().chunk_schema()),
                    )
                    .map_err(failed)?;
                    if chunks.len() != 1 {
                        return Err(failed(format!(
                            "in-process result batch decoded {} chunks, expected one",
                            chunks.len()
                        )));
                    }
                    batches.push(FetchedQueryBatch::new(chunks.remove(0)));
                }
                TryFetchTypedResult::NotReady => continue,
                TryFetchTypedResult::Error(error) => return Err(failed(error.message)),
            }
        }
    }
    let result = artifact.expected_output.into_query_result(batches)?;

    let outcome = match parts.completion.intent() {
        DistributedQueryIntent::Result => parts.completion.result(result),
        DistributedQueryIntent::Profile => parts
            .completion
            .profile(result, FragmentProfileSet::new(profiles)),
        DistributedQueryIntent::Write => {
            let mut builder = WriteTerminalBuilder::new(artifact.writer_registrations)?;
            for fact in terminal_facts {
                let fragment_instance_id = fact.fragment_instance_id();
                let Some(&backend_num) = writer_ids.get(&fragment_instance_id) else {
                    continue;
                };
                let snapshot = crate::runtime::sink_commit::report_snapshot(fragment_instance_id);
                let terminal = FragmentTerminalSnapshot::from_fact(fact, backend_num, snapshot)
                    .map_err(|error| failed(error.to_string()))?;
                builder.apply_terminal(&terminal)?;
                crate::runtime::sink_commit::unregister(fragment_instance_id);
            }
            let report = builder.finish()?;
            let (commit, abort) = report.into_payloads();
            parts.completion.write(result, commit, abort)
        }
        DistributedQueryIntent::Statistics => {
            let program = parts
                .statistics_program
                .as_ref()
                .ok_or_else(|| failed("statistics execution lost its typed collection program"))?;
            let result = program.finish_fragment_payloads(
                terminal_facts
                    .iter()
                    .map(|fragment| fragment.statistics_payload()),
            )?;
            parts.completion.statistics(program, result)
        }
    };
    running.clear();
    outcome
}
