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

//! The native carrier of the frontend task protocol.
//!
//! Two independent channels live here. [`NativeTaskOperationSink`] is the
//! mutation path: it encodes one released [`DispatchBatch`], sends exactly one
//! `ApplyTaskOperations` to the batch's own backend, and hands every receipt
//! back through [`TaskAckIntake`] in request order.
//! [`TaskStatusSubscriber`] is the observation path: one `SubscribeTaskStatus`
//! stream per participation, resubscribed by cursor inside a bounded error
//! budget.
//!
//! The two are deliberately unable to reach each other. The observation path
//! holds a [`StatusIntakeHandle`] and nothing else, so it can enqueue a
//! snapshot and wake the runner but cannot create a task, complete a stage,
//! settle an operation, or renew a lease. Nothing here renews a lease at all:
//! a renewal is minted by the participation owner from its own clock and
//! reaches the wire as an ordinary operation, so a healthy transport can never
//! keep an unhealthy query alive and a broken observation channel can never
//! stop a renewal.
//!
//! Both paths reuse the role's one native client stack — the cached channel,
//! the deployment JWT interceptor, and the typed channel-acquisition boundary
//! of [`super::transport`]. There is no second client here.
#![allow(
    dead_code,
    reason = "The native task protocol is not routed into production yet; the coordinator cutover constructs this carrier."
)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use once_cell::sync::Lazy;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
};

use crate::task_execution::operation::{OperationDispatchResult, WorkerReceiptOutcome};
use crate::task_protocol_config::DispatchLane;
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

use crate::task_execution::intent::{
    AckPayload, DispatchBatch, OperationAcknowledgement, OperationIntent,
    TaskOperationQueueAdmission, TaskOperationSink, TaskOperationSubmit,
};
use crate::task_execution::participation_convergence::{
    ParticipationConvergenceIntakeHandle, ParticipationConvergencePublishError,
};
use crate::task_execution::status_intake::{
    ObservationFrame, ObservationIntake, ObservationPublisher, StatusEvent, StatusIntakeAdmission,
    StatusIntakeHandle, StatusIntakeWake,
};

use super::apply_send_owner::{ApplyReservations, OriginalDisposition};
use super::data_runtime::FrontendDataRuntime;
use super::transport::{ChannelAcquisitionError, Client};
use super::transport_supervisor::{
    NativeTransportBackpressure, NativeTransportLane, NativeTransportReadyWake,
    NativeTransportWaiter,
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
mod send;
mod sink;
pub(crate) mod wire;
use covered::*;
#[cfg(test)]
use legacy::*;
use metrics::*;
use send::*;
use sink::*;
use wire::*;

#[cfg(test)]
pub(crate) use covered::covered_subscription_future_layout;
pub(crate) use covered::{CoveredTaskStatusSubscriber, SubscriptionState};
pub(crate) use legacy::TaskStatusSubscriber;
pub(crate) use metrics::{observe_attempt_pump_installed, register_metric_collectors};
pub(crate) use sink::{
    NativeTaskOperationSink, NativeTaskTransportCreationError, TaskAckIntake, TaskAckIntakeHandle,
    TaskOperationIntakeEvent,
};
pub(crate) use wire::AttemptWireFacts;

#[cfg(test)]
pub(super) mod tests;

#[path = "task_transport/action_input.rs"]
mod action_input;
pub(crate) use action_input::{NativeApplyKnownInputLayouts, known_input_layouts_for_composition};
mod proposal_wait;
