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

//! Query application contracts for the FE-owned logical actor carrier.
//! No control-runtime types cross this interface.

use crate::coordination::executor::LogicalExecutors;
use novarocks_coordination_contract::{AttemptActivationIdentity, QueryExecutionError};
use novarocks_execution_contract::ParticipationRef;
use novarocks_logical_execution_protocol::{
    LogicalRequestToken, LogicalSourceId,
    cancel::LogicalCancellationReason,
    input::{
        InternalRequest, LogicalInput, RegistryClose, RegistryParticipationConvergence,
        RelayRequest,
    },
    start::{LogicalStart, StartRefusal},
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub type LogicalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One standing source position. The variant determines its admitted input
/// class; participation fan-in is serialized by the original source owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalSourceKind {
    AttemptData(AttemptActivationIdentity),
    Cancellation,
    Consumer,
    Registry,
    Snapshot,
}

/// Only continuous sources use this envelope. One-shot executor results return
/// to their original pre-reserved action position, never through a new source.
#[derive(Clone, Debug)]
pub enum LogicalHostFact {
    Relay {
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        request: RelayRequest,
    },
    Internal {
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        request: InternalRequest,
    },
    Cancelled(LogicalCancellationReason),
    RegistryParticipation {
        activation: AttemptActivationIdentity,
        participation: ParticipationRef,
        convergence: RegistryParticipationConvergence,
    },
    RegistryJoinReadiness {
        token: LogicalRequestToken,
        close_ingress: bool,
    },
    ConsumerClosed,
    Snapshot {
        token: LogicalRequestToken,
    },
}

impl LogicalHostFact {
    /// Identity is attached by the registered inlet, never minted by a payload.
    pub fn into_input(self, source: LogicalSourceId) -> LogicalInput {
        match self {
            Self::Relay {
                token,
                activation,
                request,
            } => LogicalInput::Relay {
                source,
                token,
                activation,
                request,
            },
            Self::Internal {
                token,
                activation,
                request,
            } => LogicalInput::Internal {
                source,
                token,
                activation,
                request,
            },
            Self::Cancelled(reason) => LogicalInput::Cancelled(reason),
            Self::RegistryParticipation {
                activation,
                participation,
                convergence,
            } => LogicalInput::RegistryParticipation {
                activation,
                participation,
                convergence,
            },
            Self::RegistryJoinReadiness {
                token,
                close_ingress,
            } => LogicalInput::RegistryJoinReadiness {
                source,
                token,
                close_ingress,
            },
            Self::ConsumerClosed => LogicalInput::ConsumerClosed { source },
            Self::Snapshot { token } => LogicalInput::Snapshot { source, token },
        }
    }
    pub fn belongs_to(&self, kind: LogicalSourceKind) -> bool {
        match (self, kind) {
            (Self::Relay { activation, .. }, LogicalSourceKind::AttemptData(expected)) => {
                *activation == expected
            }
            (Self::Internal { .. } | Self::ConsumerClosed, LogicalSourceKind::Consumer)
            | (Self::Cancelled(_), LogicalSourceKind::Cancellation)
            | (
                Self::RegistryParticipation { .. } | Self::RegistryJoinReadiness { .. },
                LogicalSourceKind::Registry,
            )
            | (Self::Snapshot { .. }, LogicalSourceKind::Snapshot) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalSourceClosed;

/// Failure to acquire a source is a registration-contract error, never Busy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalSourceRefusal {
    AlreadyTaken,
    ForeignActivation,
    Closed,
}

pub trait LogicalFactSource: Send {
    fn id(&self) -> LogicalSourceId;
    /// Waits for this source's original standing position to retire. No retry,
    /// polling, fresh queue or caller-selected capacity exists on this port.
    fn send(&mut self, fact: LogicalHostFact)
    -> LogicalFuture<'_, Result<(), LogicalSourceClosed>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalInstallRefusal {
    InvalidStart(StartRefusal),
    RuntimeClosed,
    DuplicateExecution,
    Resource(QueryExecutionError),
}

/// Published only after the original actor Scope and children are removed and
/// all original work/captures have actually exited. Failure never claims a
/// positive remote closure absent from its retained protocol facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalExit {
    Completed,
    OwnerFailed(QueryExecutionError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalJoinReadiness {
    WaitingForConvergence,
    Ready,
}

pub trait InstalledLogicalExecution: Send + Sync {
    fn source(
        &self,
        kind: LogicalSourceKind,
    ) -> Result<Box<dyn LogicalFactSource>, LogicalSourceRefusal>;
    /// Coalescing close, with Shutdown stronger than Retire.
    fn request_close(&self, close: RegistryClose);
    /// Uses the SAME registered Registry source and its prepaid reply. Completion
    /// means the exact core reply arrived, not merely that input was handed off.
    fn join_readiness(
        &self,
        close_ingress: bool,
    ) -> LogicalFuture<'_, Result<LogicalJoinReadiness, LogicalSourceClosed>>;
    fn poll_exit(&self, cx: &mut Context<'_>) -> Poll<LogicalExit>;
}

pub trait LogicalRuntimeInstall: Send + Sync {
    fn install(
        &self,
        start: LogicalStart,
        executors: LogicalExecutors,
    ) -> Result<Arc<dyn InstalledLogicalExecution>, LogicalInstallRefusal>;
}
