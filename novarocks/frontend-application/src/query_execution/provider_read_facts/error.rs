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

//! Closed original errors of the Provider read creator. A diagnostic is never
//! parsed back into a producer fact or a compilation/control authority.

use novarocks_failure_contract::{FailureFact, FailureProjection, SharedFailureCause};
use novarocks_query_application::execution_error::ExecutionFailureInput;
use novarocks_spi::connector::{ConnectorCodecError, ConnectorError};
use novarocks_workload_control::WorkError;
use std::{error::Error, fmt};

/// The original CPU packet authors the execution fact; an admission refusal,
/// when present, is retained as its exact typed supporting source as well.
#[derive(Clone, Debug)]
pub(crate) struct ProviderReadCpuFailure {
    pub(crate) execution: ExecutionFailureInput,
    pub(crate) admission: Option<WorkError>,
}
impl fmt::Display for ProviderReadCpuFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.execution.detail().as_str())
    }
}
impl Error for ProviderReadCpuFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.admission.as_ref().map(|error| error as _)
    }
}
impl FailureProjection for ProviderReadCpuFailure {
    fn failure_fact(&self) -> FailureFact {
        self.execution.fact()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ProviderReadError {
    Connector {
        context: Option<String>,
        error: ConnectorError,
    },
    Codec {
        context: Option<String>,
        error: ConnectorCodecError,
    },
    Work {
        context: Option<String>,
        error: WorkError,
    },
    Cpu(ProviderReadCpuFailure),
    Original(SharedFailureCause),
    Contract(String),
    /// Raw health stays in the same original job and completion Source. This
    /// variant does not invent a domain failure fact or certify actual exit.
    ActualExit(novarocks_query_application::api::StatementNativeHealthFailure),
}
impl ProviderReadError {
    pub(crate) fn context(mut self, context: impl Into<String>) -> Self {
        let context = context.into();
        match &mut self {
            Self::Connector {
                context: target, ..
            }
            | Self::Codec {
                context: target, ..
            }
            | Self::Work {
                context: target, ..
            } => {
                *target = Some(match target.take() {
                    Some(inner) => format!("{context}: {inner}"),
                    None => context,
                })
            }
            Self::Contract(message) => *message = format!("{context}: {message}"),
            // Do not turn original CPU/health facts into contextual prose.
            Self::Cpu(_) | Self::Original(_) | Self::ActualExit(_) => {}
        }
        self
    }
    #[cfg(test)]
    pub(crate) fn fact(&self) -> Option<FailureFact> {
        match self {
            Self::Connector { error, .. } => Some(error.failure_fact()),
            Self::Codec { error, .. } => Some(error.failure_fact()),
            Self::Work { error, .. } => Some(error.failure_fact()),
            Self::Cpu(error) => Some(error.execution.fact()),
            Self::Original(source) => Some(source.failure_fact()),
            Self::Contract(_) | Self::ActualExit(_) => None,
        }
    }
    /// Existing producer SharedCause is reused, not wrapped in a new cause.
    /// Contract diagnostics and raw health must be handled as those families.
    pub(crate) fn into_original_cause(self) -> Result<SharedFailureCause, Self> {
        fn original<E>(error: E) -> SharedFailureCause
        where
            E: Error + FailureProjection + Send + Sync + 'static,
        {
            Error::source(&error)
                .and_then(|source| source.downcast_ref::<SharedFailureCause>())
                .cloned()
                .unwrap_or_else(|| SharedFailureCause::new(error))
        }
        match self {
            Self::Connector { error, .. } => Ok(original(error)),
            Self::Codec { error, .. } => {
                if let Some(novarocks_type_contract::CompileControlError::Source(source)) =
                    error.compile_control_error()
                {
                    Ok(source.clone())
                } else {
                    Ok(original(error))
                }
            }
            Self::Work { error, .. } => Ok(original(error)),
            Self::Cpu(error) => Ok(original(error)),
            Self::Original(source) => Ok(source),
            error @ (Self::Contract(_) | Self::ActualExit(_)) => Err(error),
        }
    }
}
impl From<String> for ProviderReadError {
    fn from(value: String) -> Self {
        Self::Contract(value)
    }
}
impl From<&str> for ProviderReadError {
    fn from(value: &str) -> Self {
        Self::Contract(value.to_owned())
    }
}
impl From<ConnectorError> for ProviderReadError {
    fn from(error: ConnectorError) -> Self {
        Self::Connector {
            context: None,
            error,
        }
    }
}
impl From<ConnectorCodecError> for ProviderReadError {
    fn from(error: ConnectorCodecError) -> Self {
        Self::Codec {
            context: None,
            error,
        }
    }
}
impl From<WorkError> for ProviderReadError {
    fn from(error: WorkError) -> Self {
        Self::Work {
            context: None,
            error,
        }
    }
}
impl fmt::Display for ProviderReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connector { context, error } => {
                if let Some(context) = context {
                    write!(f, "{context}: ")?;
                }
                fmt::Display::fmt(error, f)
            }
            Self::Codec { context, error } => {
                if let Some(context) = context {
                    write!(f, "{context}: ")?;
                }
                fmt::Display::fmt(error, f)
            }
            Self::Work { context, error } => {
                if let Some(context) = context {
                    write!(f, "{context}: ")?;
                }
                fmt::Display::fmt(error, f)
            }
            Self::Cpu(error) => fmt::Display::fmt(error, f),
            Self::Original(source) => fmt::Display::fmt(source, f),
            Self::Contract(message) => f.write_str(message),
            Self::ActualExit(_) => {
                f.write_str("original provider creator failure remains in its source")
            }
        }
    }
}
impl Error for ProviderReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connector { error, .. } => Some(error),
            Self::Codec { error, .. } => Some(error),
            Self::Work { error, .. } => Some(error),
            Self::Cpu(error) => Some(error),
            Self::Original(source) => Some(source),
            Self::ActualExit(error) => Some(error.cause()),
            Self::Contract(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::execution_error::{ExecutionFailureSite, QueryFailureReason};

    #[test]
    fn original_codec_domain_and_control_source_are_not_rewrapped() {
        use novarocks_spi::connector::{ConnectorCodecErrorKind, ConnectorFieldPath};
        let codec = ConnectorCodecError::from_domain(
            ConnectorFieldPath::root("relation"),
            WorkError::CapacityWaitTimeout,
        );
        let original = Error::source(&codec)
            .and_then(|source| source.downcast_ref::<SharedFailureCause>())
            .unwrap()
            .clone();
        let expected = codec.failure_fact();
        assert!(matches!(codec.kind(), ConnectorCodecErrorKind::Domain(_)));
        let typed = ProviderReadError::from(codec).context("actual relation encoder");
        assert_eq!(typed.fact(), Some(expected));
        assert_eq!(typed.into_original_cause().unwrap(), original);

        let codec = ConnectorCodecError::from(
            novarocks_type_contract::CompileControlError::Source(original.clone()),
        );
        assert_eq!(
            ProviderReadError::from(codec)
                .into_original_cause()
                .unwrap(),
            original
        );
    }

    #[test]
    fn cpu_result_retains_complete_selected_input_and_typed_admission_evidence() {
        let execution = ExecutionFailureInput::new(
            QueryFailureReason::BlockingQueueFull.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating("original CPU queue refusal"),
            ExecutionFailureSite::Coordinator,
        );
        // The selected execution fact is authoritative even if an independently
        // retained admission observation carries another class.
        let admission =
            WorkError::Cancelled(novarocks_workload_control::CancellationReason::Requested);
        let error = ProviderReadError::Cpu(ProviderReadCpuFailure {
            execution: execution.clone(),
            admission: Some(admission.clone()),
        });
        assert_eq!(error.fact(), Some(execution.fact()));
        let received = error.into_original_cause().unwrap();
        assert_eq!(received.failure_fact(), execution.fact());
        let retained = received.downcast_ref::<ProviderReadCpuFailure>().unwrap();
        assert_eq!(retained.execution, execution);
        assert_eq!(retained.execution.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(retained.admission.as_ref(), Some(&admission));
        assert_eq!(
            Error::source(retained).unwrap().downcast_ref::<WorkError>(),
            Some(&admission)
        );
    }
}
