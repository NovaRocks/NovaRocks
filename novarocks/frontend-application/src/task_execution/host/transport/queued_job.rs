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
use novarocks_control_runtime::exit_work::{GatedOutcome, GatedWork};
use novarocks_control_runtime_core::identity::Generation;
use novarocks_workload_control::joint_admission::{
    NativeAsyncActionAuthority, NativeQueuedInputQuote,
};
use std::any::Any;
/// The same Source prepaid its canonical input/result cells before publishing
/// a passive request. The neutral kernel issues no author from identities.
pub(crate) struct BoundQueue {
    pub(crate) key: SendKey,
    pub(crate) scope_generation: Generation,
    pub(crate) control: bool,
    pub(crate) quote: NativeQueuedInputQuote,
    pub(crate) authority: NativeAsyncActionAuthority,
    pub(crate) original_captures: Box<dyn Any + Send>,
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
        _: fn(SendKey, NativeAsyncActionAuthority, bool, NativeQueuedInputQuote) -> P,
        body: fn(Box<dyn Any + Send>, P::Output) -> F,
    ) -> [std::alloc::Layout; 4]
    where
        P: std::future::Future,
        F: std::future::Future,
    {
        GatedWork::<Box<dyn Any + Send>>::backing_layouts::<P, F>(body)
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
) -> Result<OriginalQueuedInput, novarocks_workload_control::WorkError> {
    match AdmitQueuedInput::new(key, &authority, control, quote) {
        Ok(pre) => pre.await,
        Err(error) => Err(error),
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
        authority,
        original_captures,
    } = *bound;
    let pre = prepare_input(key, authority, control, quote);
    let body: fn(
        Box<dyn Any + Send>,
        Result<OriginalQueuedInput, novarocks_workload_control::WorkError>,
    ) -> _ = return_input;
    let running =
        GatedWork::new(exit, original_captures, handoff, panic).run(scope_generation, pre, body);
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
    _captures: Box<dyn Any + Send>,
    prepared: Result<OriginalQueuedInput, novarocks_workload_control::WorkError>,
) -> BodyOutcome {
    match prepared {
        Ok(input) => BodyOutcome::InputAdmitted(input),
        Err(error) => BodyOutcome::LocalFailure(error),
    }
}
