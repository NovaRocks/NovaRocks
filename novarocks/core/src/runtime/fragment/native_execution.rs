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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver as ReadinessReceiver, SyncSender as ReadinessSender};
use std::time::Duration;

use crate::common::config::debug_exec_node_output;
use crate::exec::fragment::program::FragmentSinkKind;
use crate::exec::pipeline::executor::execute_native_plan_with_pipeline;
use crate::runtime::fragment::error::{FragmentExecutionError, FragmentExecutionErrorKind};
use crate::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentResultWriter, ResultPresentation, ResultWriteSpec,
};
use crate::runtime::fragment::runtime_state::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::fragment::sink::materialize_fragment_sink;
use crate::runtime::fragment::submission::FragmentSubmission;
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime_filter::service::NativeRuntimeFilterExecutionContext;

#[cfg(test)]
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::sync::mpsc::{Receiver, SyncSender};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
struct TestResultBufferCreationGateWorker {
    entered: SyncSender<()>,
    release: Receiver<()>,
}

#[cfg(test)]
fn test_result_buffer_creation_gates()
-> &'static Mutex<HashMap<crate::common::types::UniqueId, TestResultBufferCreationGateWorker>> {
    static GATES: OnceLock<
        Mutex<HashMap<crate::common::types::UniqueId, TestResultBufferCreationGateWorker>>,
    > = OnceLock::new();
    GATES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn test_pre_ready_panics() -> &'static Mutex<HashSet<crate::common::types::UniqueId>> {
    static PANICS: OnceLock<Mutex<HashSet<crate::common::types::UniqueId>>> = OnceLock::new();
    PANICS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(test)]
pub(crate) fn install_test_pre_ready_panic(finst_id: crate::common::types::UniqueId) {
    test_pre_ready_panics()
        .lock()
        .expect("pre-ready panic set lock")
        .insert(finst_id);
}

#[cfg(test)]
fn maybe_panic_before_ready(finst_id: crate::common::types::UniqueId) {
    if test_pre_ready_panics()
        .lock()
        .expect("pre-ready panic set lock")
        .remove(&finst_id)
    {
        panic!("injected native worker panic before readiness");
    }
}

#[cfg(test)]
pub(crate) struct TestResultBufferCreationGate {
    entered: Receiver<()>,
    release: Option<SyncSender<()>>,
}

#[cfg(test)]
impl TestResultBufferCreationGate {
    pub(crate) fn wait_until_worker_enters(&self) {
        self.entered
            .recv()
            .expect("native worker must reach result-buffer creation gate");
    }

    pub(crate) fn release(mut self) {
        self.release
            .take()
            .expect("result-buffer creation gate released once")
            .send(())
            .expect("native worker must wait for result-buffer gate release");
    }
}

#[cfg(test)]
impl Drop for TestResultBufferCreationGate {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

#[cfg(test)]
pub(crate) fn install_test_result_buffer_creation_gate(
    finst_id: crate::common::types::UniqueId,
) -> TestResultBufferCreationGate {
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    test_result_buffer_creation_gates()
        .lock()
        .expect("result-buffer creation gate lock")
        .insert(
            finst_id,
            TestResultBufferCreationGateWorker {
                entered: entered_tx,
                release: release_rx,
            },
        );
    TestResultBufferCreationGate {
        entered: entered_rx,
        release: Some(release_tx),
    }
}

#[cfg(test)]
fn wait_at_test_result_buffer_creation_gate(finst_id: crate::common::types::UniqueId) {
    let gate = test_result_buffer_creation_gates()
        .lock()
        .expect("result-buffer creation gate lock")
        .remove(&finst_id);
    if let Some(gate) = gate {
        gate.entered
            .send(())
            .expect("result-buffer gate observer must remain alive");
        gate.release
            .recv()
            .expect("result-buffer gate observer must release worker");
    }
}

pub(crate) struct NativeExecutionContext {
    pub(crate) profiler: Option<Profiler>,
    pub(crate) mem_tracker: Option<Arc<MemTracker>>,
    pub(crate) readiness: NativeExecutionReadiness,
    pub(crate) runtime_filter: Option<NativeRuntimeFilterExecutionContext>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum NativeExecutionStart {
    Ready,
    Failed(FragmentExecutionError),
}

#[derive(Clone, Debug)]
pub(crate) struct NativeExecutionReadiness {
    sender: ReadinessSender<NativeExecutionStart>,
    ready: Arc<AtomicBool>,
}

impl NativeExecutionReadiness {
    pub(crate) fn signal_ready(&self) {
        self.ready.store(true, Ordering::Release);
        let _ = self.sender.send(NativeExecutionStart::Ready);
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn fail_after_cleanup(&self, error: FragmentExecutionError) {
        if !self.is_ready() {
            let _ = self.sender.send(NativeExecutionStart::Failed(error));
        }
    }
}

pub(crate) fn native_execution_readiness_channel() -> (
    NativeExecutionReadiness,
    ReadinessReceiver<NativeExecutionStart>,
) {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    (
        NativeExecutionReadiness {
            sender,
            ready: Arc::new(AtomicBool::new(false)),
        },
        receiver,
    )
}

pub(crate) fn execute_native_submission(
    submission: FragmentSubmission,
    context: NativeExecutionContext,
    exchange_transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_writer: std::sync::Arc<dyn FragmentResultWriter>,
) -> Result<FragmentOutput, FragmentExecutionError> {
    let instance = submission.instance();
    let program = submission.program();
    let query_id = instance.query_id();
    let fragment_instance_id = instance.fragment_instance_id().get();
    let backend_num = instance.backend_num().get();
    let logical_pipeline_dop = i32::try_from(instance.pipeline_dop().get()).map_err(|_| {
        FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            format!(
                "pipeline DOP {} exceeds runtime representation",
                instance.pipeline_dop()
            ),
        )
    })?;
    let pipeline_dop = crate::runtime::exec_env::calc_pipeline_dop(logical_pipeline_dop);
    let query_options =
        apply_query_option_overrides(Some(instance.runtime_options().query_options().clone()));
    let runtime_state = build_runtime_state(
        RuntimeStateInputs {
            query_options,
            query_id: Some(query_id),
            fragment_instance_id: Some(fragment_instance_id),
            backend_num: Some(backend_num),
            mem_tracker: context.mem_tracker.clone(),
            native_runtime_filter_context: context.runtime_filter.clone(),
            connector_staged_report_collector: program
                .sink()
                .program()
                .connector_staged_report_collector(),
        },
        context.profiler.as_ref(),
    )
    .map_err(|error| FragmentExecutionError::new(FragmentExecutionErrorKind::Pipeline, error))?;

    if program.sink().kind() == FragmentSinkKind::Result {
        #[cfg(test)]
        wait_at_test_result_buffer_creation_gate(fragment_instance_id);
        #[cfg(test)]
        maybe_panic_before_ready(fragment_instance_id);
        context.readiness.signal_ready();
    }
    let result_session = if program.sink().kind() == FragmentSinkKind::Result {
        let spec = ResultWriteSpec::new(
            fragment_instance_id,
            ResultPresentation::MysqlText,
            None,
            instance.runtime_options().typed_result_sink(),
        );
        Some(result_writer.open(spec).map_err(|error| {
            FragmentExecutionError::new(FragmentExecutionErrorKind::Sink, error.to_string())
        })?)
    } else {
        None
    };
    let sink = materialize_fragment_sink(program, instance, exchange_transmitter, result_session)
        .map_err(|error| {
        FragmentExecutionError::new(FragmentExecutionErrorKind::Sink, error.to_string())
    })?;
    if program.sink().kind() != FragmentSinkKind::Result {
        context.readiness.signal_ready();
    }
    // PBF-2 launches each validated submission once. Instance-owned scan and
    // exchange state is materialized per-instance from the shared static
    // program (no bound ops baked into the plan nodes).
    let exec_plan = program.plan().clone();
    let exchange_bindings =
        crate::runtime::fragment::exchange::materialize_exchange_bindings(program, instance);
    let scan_bindings = crate::runtime::fragment::scan::materialize_scan_bindings(
        program, instance,
    )
    .map_err(|error| {
        FragmentExecutionError::new(FragmentExecutionErrorKind::Pipeline, error.to_string())
    })?;
    let _exec_timer = context
        .profiler
        .as_ref()
        .map(|profiler| profiler.scoped_timer("PipelineExecuteTime"));
    execute_native_plan_with_pipeline(
        exec_plan,
        debug_exec_node_output(),
        Duration::from_millis(50),
        sink,
        exchange_bindings,
        scan_bindings,
        Some((fragment_instance_id.high(), fragment_instance_id.low())),
        context.profiler,
        pipeline_dop,
        runtime_state,
        Some(query_id),
        None,
        Some(backend_num),
    )
    .map_err(|error| FragmentExecutionError::new(FragmentExecutionErrorKind::Pipeline, error))?;

    Ok(FragmentOutput { profile_json: None })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        FragmentContractVersion, FragmentProgram, FragmentProgramOptions, FragmentSinkSpec,
        RuntimeFilterContract,
    };
    use crate::exec::fragment::sink::FragmentSinkProgram;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::fragment::submission::FragmentSubmission;
    use crate::runtime::profile::Profiler;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;

    use super::{
        NativeExecutionContext, execute_native_submission, native_execution_readiness_channel,
    };

    fn noop_values_submission() -> FragmentSubmission {
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::default(),
                    node_id: 10,
                }),
            },
        };
        let program = Arc::new(FragmentProgram::new(
            plan,
            FragmentSinkSpec::try_new(FragmentSinkProgram::Noop).expect("noop sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        ));
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId::new(81, 82),
            FragmentInstanceId::new(UniqueId::new(83, 84)),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(1).expect("non-zero DOP"),
            BackendNum::try_new(1).expect("backend number"),
        );
        FragmentSubmission::try_new(program, instance).expect("valid submission")
    }

    #[test]
    fn executes_noop_values_submission_without_wire_inputs() {
        let profiler = Profiler::new("native submission");
        let (readiness, _receiver) = native_execution_readiness_channel();
        let output = execute_native_submission(
            noop_values_submission(),
            NativeExecutionContext {
                profiler: Some(profiler.clone()),
                mem_tracker: None,
                readiness,
                runtime_filter: None,
            },
            crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
            crate::runtime::fragment::io::result::discard_result_writer(),
        )
        .expect("noop submission executes");

        assert!(output.profile_json.is_none());
    }
}
