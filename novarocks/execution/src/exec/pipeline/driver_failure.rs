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

//! Driver-owned contract failures and caught panic facts. Payload rendering
//! is bounded at capture; classification never inspects the panic message.

use crate::runtime::task::{ExecutionFailure, ExecutionReason};
use novarocks_failure_contract::{BoundedDetail, FailureClass, FailureFact, FailureProjection};
use std::{any::Any, error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExitHook {
    Cancel,
    FailureStop,
    BeginExit,
    Close,
    Observe,
    ReleaseInput,
    Dispose,
}
impl fmt::Display for ExitHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cancel => "cancel",
            Self::FailureStop => "failure stop",
            Self::BeginExit => "begin exit",
            Self::Close => "close",
            Self::Observe => "observe exit",
            Self::ReleaseInput => "release input",
            Self::Dispose => "dispose owner",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DriverFailure {
    Panicked(BoundedDetail),
    ExitHookPanicked(ExitHook),
    DuplicateRootGrant,
    MissingOperator { ordinal: usize },
    TerminalOutput,
    DroppedWithPendingExit,
    ExecutorShutdown,
    UnrequestedCancellation,
    DeadlineExceeded(BoundedDetail),
    MissingObservable { driver_id: i32 },
    DependencyScheduleRefused { driver_id: i32 },
}
impl DriverFailure {
    pub(crate) fn from_panic(payload: &(dyn Any + Send)) -> Self {
        let detail = if let Some(message) = payload.downcast_ref::<&str>() {
            *message
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.as_str()
        } else {
            "unknown panic payload"
        };
        Self::Panicked(BoundedDetail::truncating(detail))
    }
}
impl fmt::Display for DriverFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Panicked(detail) => write!(f, "panic in driver execution: {detail}"),
            Self::ExitHookPanicked(hook) => write!(f, "operator {hook} hook panicked"),
            Self::DuplicateRootGrant => {
                f.write_str("pipeline acquired more than one terminal root edge")
            }
            Self::MissingOperator { ordinal } => {
                write!(f, "pipeline operator index {ordinal} out of bounds")
            }
            Self::TerminalOutput => f.write_str("terminal sink produced an output"),
            Self::DroppedWithPendingExit => {
                f.write_str("driver owner was dropped before physical exit")
            }
            Self::ExecutorShutdown => f.write_str("driver executor is shutting down"),
            Self::UnrequestedCancellation => {
                f.write_str("pipeline driver canceled without an accepted stop")
            }
            Self::DeadlineExceeded(detail) => detail.fmt(f),
            Self::MissingObservable { driver_id } => {
                write!(f, "missing observable for blocked driver {driver_id}")
            }
            Self::DependencyScheduleRefused { driver_id } => write!(
                f,
                "event scheduler refused dependency-blocked driver {driver_id}"
            ),
        }
    }
}
impl Error for DriverFailure {}
impl FailureProjection for DriverFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, reason) = match self {
            Self::ExecutorShutdown => (
                FailureClass::Control,
                ExecutionReason::DriverExecutorShutdown,
            ),
            Self::UnrequestedCancellation => (
                FailureClass::Invariant,
                ExecutionReason::DriverUnrequestedCancellation,
            ),
            Self::DeadlineExceeded(_) => (
                FailureClass::Control,
                ExecutionReason::DriverDeadlineExceeded,
            ),
            Self::MissingObservable { .. } => (
                FailureClass::Invariant,
                ExecutionReason::DriverMissingObservable,
            ),
            Self::DependencyScheduleRefused { .. } => (
                FailureClass::Invariant,
                ExecutionReason::DriverDependencyScheduleRefused,
            ),
            Self::Panicked(_) => (FailureClass::Unexpected, ExecutionReason::DriverPanicked),
            Self::ExitHookPanicked(_) => (
                FailureClass::Unexpected,
                ExecutionReason::DriverExitHookPanicked,
            ),
            Self::DuplicateRootGrant => (
                FailureClass::Invariant,
                ExecutionReason::DriverDuplicateRootGrant,
            ),
            Self::MissingOperator { .. } => (
                FailureClass::Invariant,
                ExecutionReason::DriverMissingOperator,
            ),
            Self::TerminalOutput => (
                FailureClass::Invariant,
                ExecutionReason::DriverTerminalOutput,
            ),
            Self::DroppedWithPendingExit => (
                FailureClass::Invariant,
                ExecutionReason::DriverDroppedWithPendingExit,
            ),
        };
        FailureFact::new(class, reason.reason())
    }
}
impl From<DriverFailure> for ExecutionFailure {
    fn from(cause: DriverFailure) -> Self {
        Self::from_domain(cause)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::task::{ExecutionFailureCause, PipelineOperation};
    #[test]
    fn captured_panic_is_bounded_without_classifying_or_recreating_its_cause() {
        for message in ["resource exhausted".to_owned(), "界".repeat(1000)] {
            let cause = DriverFailure::from_panic(&message);
            let expected = cause.failure_fact();
            let DriverFailure::Panicked(detail) = &cause else {
                panic!("panic fact")
            };
            assert!(detail.as_str().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
            let original: ExecutionFailure = cause.into();
            let located = original.clone().at_operator(3, PipelineOperation::Control);
            assert!(located.same_occurrence(&original));
            assert_eq!(located.failure_fact(), expected);
            assert_eq!(expected.class(), FailureClass::Unexpected);
            let ExecutionFailureCause::Domain(record) = located.cause() else {
                panic!("domain cause")
            };
            assert!(record.downcast_ref::<DriverFailure>().is_some());
        }
        assert_eq!(
            DriverFailure::from_panic(&42u32),
            DriverFailure::Panicked(BoundedDetail::truncating("unknown panic payload"))
        );
    }
}
