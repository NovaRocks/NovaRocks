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

//! Credentialed Native transport execution for the pure Attempt owner.
//!
//! Queue, encoding, WC input, action, reply, actual future and result-input
//! responsibilities remain the same original objects. Full returns a complete
//! PreparedEffectJob; protocol retries and terminal decisions stay in AP.
//!
//! Each physical send consumes a fresh original WC queued-input grant while
//! retaining its immutable operation/request/payload. The original Source owns
//! every result, Join, physical-return and financial retirement boundary; this
//! kernel neither creates a second transport window nor certifies those exits.

mod actor_adapter;
mod admission_source;
mod call_source;
mod capacity;
mod covered;
mod effect_source;
mod establish_material;
mod execute;
mod failure_abort;
mod granted_inputs;
mod prepare;
mod process_observation;
mod proposal_wait;
mod queue_input;
mod queued_job;
mod quote;
mod receipt_input;
mod release_wake;
mod releases;
mod send;
mod send_binding;
mod send_reports;
mod send_source;
mod settlement;
mod source_ports;

// Only these controlled original-object entry points are exposed to the host.
pub(crate) use actor_adapter::{
    ActorTransportExecutor, ActorTransportHandoff, ActorTransportWork, BoundActorTransport,
    actor_transport_layouts, handoff_actor_transport, prepare_actor_transport,
};
pub(crate) use admission_source::{AdmissionResultRefusal, AdmissionSource};
pub(crate) use call_source::{AttemptCallOwner, AttemptCallSource};
#[cfg(test)]
pub(crate) use capacity::EncodingCustody;
pub(crate) use capacity::{EncodingWindow, QueueCustody, QueueWindow, ReturnedQuota};
pub(crate) use covered::{
    AttemptSubscriptions, FrozenSubscriptionInputs, SubscriptionEndpoint,
    SubscriptionEndpointError, SubscriptionEndpoints, bind_subscription_endpoints,
    retire_unpublished_endpoints,
};
pub(crate) use effect_source::{
    AttemptFailureBinding, AttemptSendSource, AttemptTransportExecutor, AttemptTransportSource,
    FailedSendResult, NativeReportReferences,
};
pub(crate) use establish_material::AttemptEstablishMaterial;
pub(crate) use failure_abort::{FailureAbortAttempt, FailureResidualPort, NativeFailureCleanup};
pub(crate) use granted_inputs::GrantedInputs;
pub(crate) use process_observation::AttemptProcessObserver;
#[cfg(test)]
pub(crate) use queue_input::tests::Governed as GovernedQueuedInputFixture;
pub(crate) use queue_input::{OriginalQueuedInput, ReturnedQueuedInput, SendInputLoan, SendKey};
pub(crate) use queued_job::{BoundQueue, QueueJob, run as run_queue};
pub(crate) use quote::{
    SourceLayouts, original_run, original_run_for_factory, queued_input as queued_input_quote,
};
pub(crate) use receipt_input::NativeReceiptInput;
pub(in crate::task_execution::host) use releases::TransportReleases;
pub(crate) use send::{BoundSend, SendJob, run as run_send};
pub(crate) use send_binding::{
    FrozenSendRequests, PreparedSendBatch, ResolvedBatch, SendBindingError, SendBindingRefusal,
};
pub(crate) use send_reports::{
    ActorSendReports, NativeProgressInput, SendMemberReport, SendReportRetirement,
    SendReportRetirementStep, SendReportReturn, SendReportRows, actor_send_report_layouts,
};
pub(crate) use send_source::{PreparedActorSend, SendContext, send_queue_resources};
pub(crate) use source_ports::{BodyOutcome, TransportCompletionObligation, TransportLeg};
