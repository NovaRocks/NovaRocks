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

pub(crate) mod error;
pub(crate) mod exchange;
pub(crate) mod fact;
pub(crate) mod handle;
pub(crate) mod instance;
pub(crate) mod io;
pub(crate) mod native_execution;
pub(crate) mod resources;
pub(crate) mod runtime_state;
pub(crate) mod scan;
pub(crate) mod sink;
pub(crate) mod submission;

#[cfg(test)]
mod io_contract_tests;

pub use error::{
    FragmentExecutionError, FragmentExecutionErrorKind, FragmentLaunchError,
    FragmentLaunchErrorKind, FragmentLaunchStage,
};
pub use fact::{FragmentCancelReason, FragmentOutcome, FragmentTerminalFact};
pub use handle::{
    DormantFragmentHandle, FragmentPrepareContext, RunningFragmentHandle, prepare_fragment,
};
pub use instance::*;
pub use io::result_format::{build_result_batch, build_statistic_result_batch, empty_result_batch};
pub use io::{
    ExchangeFrame, ExchangeFrameTransmitter, FragmentEvent, FragmentEventSink, FragmentIoError,
    FragmentIoErrorKind, FragmentIoOperation, FragmentLookupClient, FragmentProfileSnapshot,
    FragmentProgress, FragmentResultSession, FragmentResultWriter, LookupBatch, LookupColumn,
    LookupKind, LookupRequest, LookupTarget, NoopFragmentEventSink, ResultAbort,
    ResultPresentation, ResultProjection, ResultWriteSpec, UnavailableFragmentLookupClient,
};
pub use submission::FragmentSubmission;
