// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original queued-input job has one gate and one paid result leg.
use super::{
    queue_input::{AdmitQueuedInput, OriginalQueuedInput, SendKey},
    source_ports::{BodyOutcome, TransportLeg},
};
use crate::task_execution::{
    creation::{CreateTaskIntent, PricedTaskCreation},
    error::TaskExecutionError,
};
use novarocks_control_runtime::exit_work::{GatedOutcome, GatedWork};
use novarocks_control_runtime_core::identity::Generation;
use novarocks_execution_contract::OperationEnvelope;
use novarocks_workload_control::joint_admission::{
    NativeAsyncActionAuthority, NativeQueuedInputQuote,
};
use std::sync::{Arc, Mutex};

pub(super) type CreationSeed = Arc<Mutex<Option<PricedTaskCreation>>>;

pub(super) struct CreationAdmission {
    pub(super) envelope: OperationEnvelope,
    pub(super) seed: CreationSeed,
}
/// The same Source prepaid its canonical input/result cells before publishing
/// a passive request. The neutral kernel issues no author from identities.
pub(crate) struct BoundQueue {
    pub(crate) key: SendKey,
    pub(crate) scope_generation: Generation,
    pub(crate) control: bool,
    pub(crate) quote: NativeQueuedInputQuote,
    pub(crate) preflight: Result<usize, TaskExecutionError>,
    pub(crate) authority: NativeAsyncActionAuthority,
    pub(super) creation: Option<CreationAdmission>,
}
pub(crate) struct QueueJob {
    pub(crate) leg: TransportLeg,
    pub(crate) bound: Box<BoundQueue>,
}
pub(crate) fn future_layout() -> std::alloc::Layout {
    fn of<F: std::future::Future<Output = ()>>(_: fn(QueueJob) -> F) -> std::alloc::Layout {
        std::alloc::Layout::new::<F>()
    }
    of(run)
}
/// Actual generic GatedWork allocations plus this queue's one WC wait F.
/// Its inline values are included in the outer factory and tuple layouts.
pub(crate) fn owned_preparation_layouts() -> [std::alloc::Layout; 5] {
    fn of<P, F>(
        _: fn(
            SendKey,
            NativeAsyncActionAuthority,
            bool,
            NativeQueuedInputQuote,
            Result<usize, TaskExecutionError>,
        ) -> P,
        body: fn(Option<CreationAdmission>, P::Output) -> F,
    ) -> [std::alloc::Layout; 4]
    where
        P: std::future::Future,
        F: std::future::Future,
    {
        GatedWork::<Option<CreationAdmission>>::backing_layouts::<P, F>(body)
    }
    let gated = of(prepare_input, return_input);
    [
        gated[0],
        gated[1],
        gated[2],
        gated[3],
        super::proposal_wait::ProposalWait::owned_future_layout(),
    ]
}
async fn prepare_input(
    key: SendKey,
    authority: NativeAsyncActionAuthority,
    control: bool,
    quote: NativeQueuedInputQuote,
    preflight: Result<usize, TaskExecutionError>,
) -> Result<OriginalQueuedInput, TaskExecutionError> {
    preflight?;
    match AdmitQueuedInput::new(key, &authority, control, quote) {
        Ok(pre) => pre.await.map_err(TaskExecutionError::Workload),
        Err(error) => Err(TaskExecutionError::Workload(error)),
    }
}
pub(crate) fn run(job: QueueJob) -> impl std::future::Future<Output = ()> {
    let QueueJob { leg, bound } = job;
    let TransportLeg {
        exit,
        handoff,
        panic,
        result,
    } = leg;
    let BoundQueue {
        key,
        scope_generation,
        control,
        quote,
        preflight,
        authority,
        creation,
    } = *bound;
    let pre = prepare_input(key, authority, control, quote, preflight);
    let body: fn(Option<CreationAdmission>, Result<OriginalQueuedInput, TaskExecutionError>) -> _ =
        return_input;
    let running = GatedWork::new(exit, creation, handoff, panic).run(scope_generation, pre, body);
    async move {
        let outcome = match running.await {
            Ok(GatedOutcome::Completed(outcome)) => outcome,
            Ok(GatedOutcome::NotStarted(reason)) => BodyOutcome::Unused(reason),
            Err(error) => BodyOutcome::RawFailure(Box::new(error)),
        };
        result.body_finished(outcome);
    }
}

async fn return_input(
    creation: Option<CreationAdmission>,
    prepared: Result<OriginalQueuedInput, TaskExecutionError>,
) -> BodyOutcome {
    let input = match prepared {
        Ok(input) => input,
        Err(TaskExecutionError::Workload(error)) => return BodyOutcome::LocalFailure(error),
        Err(error) => return BodyOutcome::CreationFailed(error),
    };
    let creation = if let Some(CreationAdmission { envelope, seed }) = creation {
        let pin = match input.retain_payload() {
            Ok(pin) => pin,
            Err(error) => return BodyOutcome::RawFailure(Box::new((error, input))),
        };
        let original = seed
            .lock()
            .unwrap()
            .take()
            .expect("same first creation seed");
        let identity = original.seed.identity();
        let parts = match original.seed.freeze(original.sizing.lengths) {
            Ok(parts) => parts,
            Err(error) => return BodyOutcome::CreationFailed(error),
        };
        Some(CreateTaskIntent::new(envelope, identity, parts, Some(pin)))
    } else {
        None
    };
    BodyOutcome::InputAdmitted { creation, input }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_execution::error::CapacityBound;
    use novarocks_attempt_protocol::input::SendGeneration;
    use novarocks_coordination_contract::AttemptActionId;
    use novarocks_execution_contract::TaskOperationId;
    use std::num::NonZeroU64;

    #[tokio::test]
    async fn invalid_method_length_is_refused_before_wc_input_admission() {
        let wc = super::super::queue_input::tests::Governed::new();
        let key = SendKey {
            operation: TaskOperationId::new_v7(),
            generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::MIN)),
        };
        let quote =
            super::super::queued_input_quote(0, super::super::SourceLayouts { original: &[] })
                .unwrap();
        let bound = CapacityBound::OperationBytes {
            limit: 1024,
            actual: 1025,
        };
        let result = prepare_input(
            key,
            wc.authority(),
            false,
            quote,
            Err(TaskExecutionError::Capacity(bound)),
        )
        .await;
        assert!(matches!(result, Err(TaskExecutionError::Capacity(actual)) if actual == bound));
        assert!(
            wc.finish(),
            "a refused request creates no WC proposal or queue grant"
        );
    }
}
