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

//! Native Task wire projection for the frontend attempt host.
//!
//! The legacy TaskRound receipt intake and observation adapters remain during
//! fixture migration. Both carriers use the role-owned Native client stack.
#![allow(
    dead_code,
    reason = "The old TaskRound carrier remains compiled during fixture migration."
)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use once_cell::sync::Lazy;
#[cfg(test)]
use prometheus::Registry;
use prometheus::{HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts};

use crate::task_execution::operation::{OperationDispatchResult, WorkerReceiptOutcome};
use novarocks_attempt_protocol::config::DispatchLane;
#[cfg(test)]
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    AcquireParticipationAdmissionTicket, OperationKind, OperationOutcome,
    ParticipationConvergenceCursor, ParticipationConvergenceReceipt, ParticipationRef,
    TaskIdentity, TaskOperationId, TaskStatusCursor, UpdateParticipation,
};
use novarocks_execution_contract::BackendProcessDescriptor;
use novarocks_proto_codec::FieldPath;
#[cfg(test)]
use novarocks_proto_codec::native_rpc::NativeEndpointDomain;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_proto_models::catalog::CatalogSet;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::domain::{stored_credential, stored_message};
use novarocks_task_codec::operation as codec;
use novarocks_task_codec::operation::{
    CoveredStatusStreamEvent, CoveredStatusStreamFact, DecodedCoveredSubscription,
    ParticipationAwareStatusStreamEvent, ReceiptHeader, decode_covered_status_event,
    decode_participation_aware_status_event, decode_receipt_batch, encode_abort_participation,
    encode_acquire_participation_admission_ticket, encode_advance_participation_domain,
    encode_cancel_task, encode_control_operation_batch, encode_covered_subscribe_task_status,
    encode_create_task, encode_establish_participation, encode_operation_batch,
    encode_participation_aware_subscribe_task_status, encode_release_participation,
    encode_renew_lease, encode_update_task,
};
use novarocks_task_codec::{TransportBudget, domain as codec_domain};
use novarocks_types::identity::BackendProcessId;

use novarocks_execution::task_execution::operation::ParticipationReceipt;

use crate::task_execution::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
#[cfg(test)]
use crate::task_execution::intent::{
    DispatchBatch, TaskOperationQueueAdmission, TaskOperationSink, TaskOperationSubmit,
};
use crate::task_execution::participation_convergence::{
    ParticipationConvergenceIntakeHandle, ParticipationConvergencePublishError,
};
use crate::task_execution::status_intake::{
    ObservationFrame, ObservationIntake, ObservationPublisher, StatusEvent, StatusIntakeAdmission,
    StatusIntakeHandle, StatusIntakeWake,
};

#[cfg(test)]
use super::apply_send_owner::{ApplyReservations, OriginalDisposition};
use super::data_runtime::FrontendDataRuntime;
use super::transport::{ChannelAcquisitionError, Client};
use super::transport_supervisor::NativeTransportReadyWake;
#[cfg(test)]
use super::transport_supervisor::{
    NativeTransportBackpressure, NativeTransportLane, NativeTransportWaiter,
};

mod joint_action;
pub(crate) use joint_action::NativeCallSource;
pub(crate) use joint_action::{
    NativeBatchAdmission, NativePhysicalOffer, clone_input_backing, original_run_encoding,
};
mod queue_input;
pub(crate) use queue_input::{queued_creation_quote, queued_intent_quote, queued_update_quote};

mod covered;
mod legacy;
mod metrics;
#[cfg(test)]
mod send;
mod sink;
pub(crate) mod wire;
use covered::*;
#[cfg(test)]
use legacy::*;
use metrics::*;
#[cfg(test)]
use send::*;
use sink::*;
use wire::*;

#[cfg(test)]
pub(crate) use covered::covered_subscription_future_layout;
pub(crate) use covered::{CoveredTaskStatusSubscriber, SubscriptionState};
pub(crate) use legacy::TaskStatusSubscriber;
pub(crate) use metrics::observe_attempt_pump_installed;
#[cfg(test)]
pub(crate) use sink::NativeTaskOperationSink;
pub(crate) use sink::{
    NativeTaskTransportCreationError, TaskAckIntake, TaskAckIntakeHandle, TaskOperationIntakeEvent,
};
pub(crate) use wire::AttemptWireFacts;

fn is_small_control(intent: &OperationIntent) -> bool {
    intent.native_method() == NativeRpcMethod::ApplyTaskControlOperations
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
#[path = "task_transport/action_input.rs"]
mod action_input;
#[cfg(test)]
pub(crate) use action_input::{NativeApplyKnownInputLayouts, known_input_layouts_for_composition};
#[cfg(test)]
mod proposal_wait;
