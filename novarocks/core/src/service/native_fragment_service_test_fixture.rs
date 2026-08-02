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

//! Legacy native orchestration retained only as a core test fixture.
//!
//! Production native fragment orchestration is owned by `novarocks-backend`.

#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::num::NonZeroUsize;
#[cfg(test)]
use std::sync::Arc;

use crate::cache::CacheOptions;
use crate::common::app_config;
use crate::common::types::UniqueId;
use crate::exec::chunk::Chunk;
use crate::exec::fragment::program::{
    FragmentContractVersion, FragmentProgram, FragmentProgramOptions, FragmentSinkKind,
    FragmentSinkSpec, RuntimeFilterContract,
};
use crate::exec::fragment::sink::FragmentSinkProgram;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
use crate::novarocks_logging::{error, info, warn};
use crate::runtime::exchange;
use crate::runtime::fragment::error::{
    FragmentExecutionError, FragmentExecutionErrorKind, FragmentLaunchError,
    FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::instance::{
    BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
    FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
};
use crate::runtime::fragment::native_execution::{
    NativeExecutionContext, NativeExecutionStart, execute_native_submission,
    native_execution_readiness_channel,
};
use crate::runtime::fragment::submission::FragmentSubmission;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::{ProfileUnit, Profiler};
use crate::runtime::query_context::{QueryContextManager, QueryId, query_context_manager};
use crate::runtime::query_options::{QueryOptions, query_expire_durations};
use crate::runtime::result_buffer;
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;

fn profile_report_interval_ns(
    enable_profile: bool,
    query_opts: Option<&QueryOptions>,
) -> Option<i64> {
    if !enable_profile {
        return None;
    }
    let from_query = query_opts
        .and_then(|opts| opts.runtime_profile_report_interval)
        .filter(|v| *v > 0)
        .and_then(|v| v.checked_mul(1_000_000_000));
    from_query.or_else(|| {
        app_config::config()
            .ok()
            .map(|cfg| cfg.runtime.profile_report_interval.max(1) * 1_000_000_000)
    })
}

fn profiler_for_native_program(
    program: &crate::exec::fragment::program::FragmentProgram,
) -> Profiler {
    let root_plan_node_id = program.root_plan_node_id().get();
    let profiler = Profiler::new(format!(
        "execute_fragment_native (plan_node_id={root_plan_node_id})"
    ));
    profiler.set_metadata(i64::from(root_plan_node_id));
    profiler
}

fn spawn_exec_fragment_native(
    submission: FragmentSubmission,
    uses_fetch_result_buffer: bool,
    finst_id: UniqueId,
    query_id: QueryId,
    runtime_filter: Option<RuntimeFilterSessionRef>,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    mgr: Arc<QueryContextManager>,
) -> Result<(), FragmentLaunchError> {
    let (readiness, readiness_receiver) = native_execution_readiness_channel();
    let worker_readiness = readiness.clone();
    crate::runtime::sink_commit::register(finst_id);
    mgr.register_finst(finst_id, query_id);
    std::thread::spawn(move || {
        let wall_start = std::time::Instant::now();
        let profiler_for_wall = profiler.clone();
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_native_submission(
                submission,
                NativeExecutionContext {
                    profiler,
                    mem_tracker,
                    readiness,
                    runtime_filter,
                },
                crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
                crate::runtime::fragment::io::result::discard_result_writer(),
            )
        }))
        .unwrap_or_else(|payload| {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic payload".to_string()
            };
            Err(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Panic,
                format!("panic in native fragment execution: {msg}"),
            ))
        });
        if let Some(p) = profiler_for_wall.as_ref() {
            let elapsed_ns =
                crate::runtime::profile::clamp_u128_to_i64(wall_start.elapsed().as_nanos());
            p.counter_set("QueryExecutionWallTime", ProfileUnit::TimeNs, elapsed_ns);
        }
        let pre_ready_failure = out
            .as_ref()
            .err()
            .filter(|_| !worker_readiness.is_ready())
            .cloned();
        let rolled_back_pre_ready = pre_ready_failure.is_some()
            && mgr.rollback_pre_ready_native_fragment(query_id, finst_id);
        let mut report_error: Option<String> = None;
        if uses_fetch_result_buffer {
            match &out {
                Ok(out) => {
                    if let Some(json) = out.profile_json.as_deref() {
                        info!(
                            target: "novarocks::profile",
                            finst_id = %finst_id,
                            profile_bytes = json.len(),
                            "native_fragment_profile"
                        );
                    }
                }
                Err(e) => {
                    let error = e.to_string();
                    report_error = Some(error.clone());
                    error!(
                        target: "novarocks::exec",
                        finst_id = %finst_id,
                        error = %e,
                        "exec_plan_fragment_native failed"
                    );
                    if worker_readiness.is_ready() {
                        result_buffer::close_error(finst_id, error);
                    }
                }
            }
        } else if let Err(e) = &out {
            report_error = Some(e.to_string());
            error!(
                target: "novarocks::exec",
                finst_id = %finst_id,
                error = %e,
                "exec_plan_fragment_native failed"
            );
        }
        if let Some(ref err_msg) = report_error
            && !rolled_back_pre_ready
        {
            let finsts = mgr.cancel_query(query_id, err_msg.clone());
            for id in finsts {
                if id != finst_id || worker_readiness.is_ready() {
                    result_buffer::close_error(id, err_msg.clone());
                }
                exchange::cancel_fragment(id.high(), id.low());
            }
        }
        mgr.finish_fragment(query_id);
        exchange::remove_fragment(finst_id.high(), finst_id.low());
        mgr.unregister_finst(finst_id);
        if let Some(error) = pre_ready_failure {
            worker_readiness.fail_after_cleanup(error);
        }
    });

    match readiness_receiver.recv() {
        Ok(NativeExecutionStart::Ready) => Ok(()),
        Ok(NativeExecutionStart::Failed(error)) => Err(pre_ready_launch_error(error)),
        Err(_) => Err(FragmentLaunchError::new(
            FragmentLaunchStage::Start,
            FragmentLaunchErrorKind::ResourceUnavailable,
            "native fragment worker terminated before readiness",
        )),
    }
}

fn pre_ready_launch_error(error: FragmentExecutionError) -> FragmentLaunchError {
    let (stage, kind) = match error.kind() {
        FragmentExecutionErrorKind::Pipeline => (
            FragmentLaunchStage::BuildRuntimeState,
            FragmentLaunchErrorKind::ResourceUnavailable,
        ),
        FragmentExecutionErrorKind::Sink
        | FragmentExecutionErrorKind::Exchange
        | FragmentExecutionErrorKind::RuntimeFilter => (
            FragmentLaunchStage::Materialize,
            FragmentLaunchErrorKind::Materialization,
        ),
        FragmentExecutionErrorKind::Cancelled | FragmentExecutionErrorKind::Panic => {
            (FragmentLaunchStage::Start, FragmentLaunchErrorKind::Start)
        }
    };
    FragmentLaunchError::new(stage, kind, error.to_string())
}

fn prepare_native_query_before_fragment_registration(
    mgr: &QueryContextManager,
    query_id: QueryId,
    query_opts: &QueryOptions,
    runtime_filter: Option<RuntimeFilterSessionRef>,
) -> Result<Option<RuntimeFilterSessionRef>, String> {
    let (delivery_expire, query_expire) = query_expire_durations(Some(query_opts));
    mgr.ensure_native_context(query_id, false, delivery_expire, query_expire)?;
    let cache_options = CacheOptions::from_query_options(Some(query_opts))?;
    mgr.set_cache_options(query_id, cache_options)?;
    Ok(runtime_filter)
}

pub fn submit_exec_plan_fragment_native(submission: FragmentSubmission) -> Result<(), String> {
    submit_exec_plan_fragment_native_with_manager(submission, query_context_manager())
}

pub(crate) fn submit_exec_plan_fragment_native_with_manager(
    submission: FragmentSubmission,
    mgr: Arc<QueryContextManager>,
) -> Result<(), String> {
    let instance = submission.instance();
    let query_id = instance.query_id();
    let finst_id = instance.fragment_instance_id().get();
    let query_opts = instance.runtime_options().query_options().clone();
    let (delivery_expire, query_expire) = query_expire_durations(Some(&query_opts));
    if submission.program().runtime_filters().has_bindings() {
        return Err(
            "native fragment runtime-filter bindings require a Backend-injected execution session"
                .to_string(),
        );
    }
    let runtime_filter = prepare_native_query_before_fragment_registration(
        mgr.as_ref(),
        query_id,
        &query_opts,
        None,
    )?;

    let query_mem_tracker = mgr
        .query_mem_tracker(query_id)
        .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
    mgr.get_or_register_native(query_id, false, delivery_expire, query_expire)?;
    let fragment_label = format!("fragment_{:x}_{:x}", finst_id.high(), finst_id.low());
    let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
    let enable_profile = query_opts.enable_profile;
    let profiler = if enable_profile {
        Some(profiler_for_native_program(submission.program()))
    } else {
        None
    };
    let uses_fetch_result_buffer = submission.program().sink().kind() == FragmentSinkKind::Result;
    spawn_exec_fragment_native(
        submission,
        uses_fetch_result_buffer,
        finst_id,
        query_id,
        runtime_filter,
        profiler,
        Some(fragment_mem_tracker),
        Arc::clone(&mgr),
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
pub(crate) fn values_submission_for_test(
    query_id: QueryId,
    fragment_instance_id: UniqueId,
    root_node_id: i32,
    sink: FragmentSinkProgram,
) -> FragmentSubmission {
    let plan = ExecPlan {
        arena: Default::default(),
        root: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk: Chunk::default(),
                node_id: root_node_id,
            }),
        },
    };
    let program = Arc::new(FragmentProgram::new(
        plan,
        FragmentSinkSpec::try_new(sink).expect("valid test sink"),
        FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
        BTreeMap::new(),
        BTreeMap::new(),
        RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
    ));
    let instance = FragmentInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        query_id,
        FragmentInstanceId::new(fragment_instance_id),
        ScanAssignments::default(),
        ExchangeInputAssignments::default(),
        FragmentSinkAssignment::None,
        FragmentRuntimeOptions::new(QueryOptions::default(), false),
        NonZeroUsize::new(1).expect("non-zero DOP"),
        BackendNum::try_new(0).expect("backend number"),
    );
    FragmentSubmission::try_new(program, instance).expect("valid test values submission")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
    use crate::runtime::query_context::QueryContextManager;
    use crate::runtime::query_options::{QueryCacheOptions, QueryOptions};

    #[derive(Debug, Eq, PartialEq)]
    struct RuntimeRegistrationSnapshot {
        query_context: bool,
        finst_mapping: Option<QueryId>,
        result_buffer: &'static str,
        exchange_receiver: bool,
        runtime_filter_lifecycle: bool,
    }

    fn registration_snapshot(query_id: QueryId, finst_id: UniqueId) -> RuntimeRegistrationSnapshot {
        let exchange_key = crate::runtime::exchange::ExchangeKey {
            finst_id_hi: finst_id.high(),
            finst_id_lo: finst_id.low(),
            node_id: 404,
        };
        let result_buffer = match result_buffer::try_fetch(finst_id) {
            result_buffer::TryFetchResult::Ready(_) => "ready",
            result_buffer::TryFetchResult::NotReady => "registered",
            result_buffer::TryFetchResult::Error(error) => match error.kind {
                result_buffer::FetchErrorKind::NotFound => "missing",
                result_buffer::FetchErrorKind::Cancelled => "cancelled",
                result_buffer::FetchErrorKind::Failed => "failed",
            },
        };
        let query_key = crate::runtime::runtime_filter_observability::QueryKey::from_hi_lo(
            query_id.high(),
            query_id.low(),
        );

        RuntimeRegistrationSnapshot {
            query_context: query_context_manager()
                .with_context_mut(query_id, |_| Ok(()))
                .is_ok(),
            finst_mapping: query_context_manager().query_id_by_finst(finst_id),
            result_buffer,
            exchange_receiver: crate::runtime::exchange::snapshot_receiver_state(exchange_key)
                .is_some(),
            runtime_filter_lifecycle:
                crate::runtime::runtime_filter_observability::RuntimeFilterLifecycleRegistry::global()
                    .snapshot(query_key)
                    .is_some(),
        }
    }

    #[test]
    fn native_profiler_uses_true_program_root_plan_node_id() {
        let query_id = QueryId::new(73_901, 73_902);
        let submission = values_submission_for_test(
            query_id,
            UniqueId::new(query_id.high() + 1, query_id.low() + 1),
            99,
            FragmentSinkProgram::Result,
        );

        let profiler = profiler_for_native_program(submission.program());

        assert_eq!(profiler.name(), "execute_fragment_native (plan_node_id=99)");
        assert_eq!(profiler.metadata(), 99);
    }

    #[test]
    fn pre_ready_worker_panic_returns_synchronously_after_cleanup() {
        let query_id = QueryId::new(74_101, 74_102);
        let finst_id = UniqueId::new(74_103, 74_104);
        crate::runtime::fragment::native_execution::install_test_pre_ready_panic(finst_id);
        let before = registration_snapshot(query_id, finst_id);

        let error = submit_exec_plan_fragment_native(values_submission_for_test(
            query_id,
            finst_id,
            91,
            FragmentSinkProgram::Result,
        ))
        .expect_err("pre-ready worker panic must fail submission synchronously");
        let after = registration_snapshot(query_id, finst_id);

        assert!(
            error.contains("start") && error.contains("panic"),
            "{error}"
        );
        assert_eq!(
            after, before,
            "pre-ready panic must finish rollback before return"
        );
    }

    #[test]
    fn native_fragment_profile_report_interval_uses_query_options_before_config() {
        let query_opts = QueryOptions {
            enable_profile: true,
            runtime_profile_report_interval: Some(7),
            ..Default::default()
        };

        assert_eq!(
            profile_report_interval_ns(true, Some(&query_opts)),
            Some(7_000_000_000)
        );
        assert_eq!(profile_report_interval_ns(false, Some(&query_opts)), None);
    }

    #[test]
    fn conflicting_cache_preflight_after_strict_rf_context_does_not_register_fragment() {
        let manager = QueryContextManager::new_for_test();
        let query_id = QueryId::new(74_201, 74_202);
        manager
            .ensure_native_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("create native query context");
        manager
            .set_cache_options(
                query_id,
                CacheOptions::from_query_options(Some(&QueryOptions::default()))
                    .expect("default cache options"),
            )
            .expect("install initial cache options");
        let conflicting_query_options = QueryOptions {
            cache: QueryCacheOptions {
                enable_scan_datacache: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let error = match prepare_native_query_before_fragment_registration(
            manager.as_ref(),
            query_id,
            &conflicting_query_options,
            Some(crate::runtime_filter::test_support::fail_open_session()),
        ) {
            Ok(_) => panic!("cache conflict must fail after strict Service acquisition"),
            Err(error) => error,
        };

        assert!(error.contains("cache options mismatch"), "{error}");
        assert_eq!(manager.fragment_counts_for_test(query_id), Some((0, 0)));
        manager.cancel_query(query_id, "test cleanup".to_string());
    }
}
