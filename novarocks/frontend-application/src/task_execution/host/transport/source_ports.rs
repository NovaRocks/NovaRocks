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

//! Seams implemented only by P10a's original per-attempt Source. They are not
//! alternate protocol owners, result stores, Work factories or quota wallets.

use super::queue_input::OriginalQueuedInput;
use crate::task_execution::{creation::CreateTaskIntent, error::TaskExecutionError};
use novarocks_control_runtime::exit_custody::ExitObligation;
use novarocks_workload_control::WorkError;
use std::{any::Any, sync::Arc};

pub(crate) enum BodyOutcome {
    /// The same real WC grant moves to its original Join owner.
    InputAdmitted {
        creation: Option<CreateTaskIntent>,
        input: OriginalQueuedInput,
    },
    /// Individual original ACK cells contain every exact operation result.
    ReceiptsRecorded,
    /// GatedWork destroyed pre-handoff, captures and unused original authority.
    Unused(novarocks_control_runtime::exit_work::GatedNotStarted),
    /// Original first-party cause; the Source pins this same cause through its
    /// final logical/error consumer, independently of Core input consumption.
    LocalFailure(WorkError),
    CreationFailed(TaskExecutionError),
    Qa(crate::task_execution::bridge::qa::AttemptQaReply),
    RawFailure(Box<dyn Any + Send>),
}

/// Original per-job result obligation, supplied by the same admitted Source.
/// Actor adapters move their same CausalInput here; replacement adapters move
/// the QA original return leg. No constructor accepts a WorkId or mints Work.
/// Completing it reports body outcome only; Source independently retains its
/// actual Join, report/custody, physical-return and cleanup responsibilities.
pub(crate) trait TransportCompletionObligation: Send {
    fn body_finished(self: Box<Self>, outcome: BodyOutcome);
}
pub(crate) struct TransportLeg {
    pub(crate) exit: ExitObligation,
    pub(crate) handoff: Arc<novarocks_control_runtime::exit_work::HandoffCell>,
    pub(crate) panic: novarocks_control_runtime::exit_work::PanicSink,
    pub(crate) result: Box<dyn TransportCompletionObligation>,
}
