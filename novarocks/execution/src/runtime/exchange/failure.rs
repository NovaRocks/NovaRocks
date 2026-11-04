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
//! Facts authored by the exchange operation that owns the failure.
use crate::exec::pipeline::operator::ConstructionFailure;
use crate::runtime::fragment::{ExecutionFailure, SharedDomainFailure};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExchangeFailure {
    Source(SharedDomainFailure),
    Execution(ExecutionFailure),
}
impl ExchangeFailure {
    pub fn into_execution(self) -> ExecutionFailure {
        self.into()
    }
}
impl fmt::Display for ExchangeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(source) => source.fmt(f),
            Self::Execution(source) => source.fmt(f),
        }
    }
}
impl Error for ExchangeFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Source(source) => source,
            Self::Execution(source) => source,
        })
    }
}
impl FailureProjection for ExchangeFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Source(source) => source.failure_fact(),
            Self::Execution(source) => source.failure_fact(),
        }
    }
}
impl From<ExecutionFailure> for ExchangeFailure {
    fn from(source: ExecutionFailure) -> Self {
        Self::Execution(source)
    }
}
impl From<crate::runtime::exchange_slots::ExchangeSlotError> for ExchangeFailure {
    fn from(source: crate::runtime::exchange_slots::ExchangeSlotError) -> Self {
        Self::Source(SharedDomainFailure::new(source))
    }
}
impl From<ConstructionFailure> for ExchangeFailure {
    fn from(source: ConstructionFailure) -> Self {
        Self::Execution(source.into())
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for ExchangeFailure
{
    fn from(source: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::from(ConstructionFailure::from(source))
    }
}
impl From<novarocks_type_contract::ValueTypeError> for ExchangeFailure {
    fn from(source: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Source(SharedDomainFailure::new(source))
    }
}
impl From<crate::exec::chunk::ChunkConstructionFailure> for ExchangeFailure {
    fn from(source: crate::exec::chunk::ChunkConstructionFailure) -> Self {
        Self::Source(SharedDomainFailure::new(source))
    }
}
impl From<novarocks_functions::array_projection::ArrayProjectionFailure> for ExchangeFailure {
    fn from(source: novarocks_functions::array_projection::ArrayProjectionFailure) -> Self {
        Self::Source(SharedDomainFailure::new(source))
    }
}
#[derive(Clone, Copy, Debug)]
pub(super) enum ArrowOperation {
    Decode,
    Encode,
    Construct,
}
#[derive(Debug)]
enum ExchangeCause {
    Contract(BoundedDetail),
    Payload(BoundedDetail),
    Stopped,
    Poisoned(&'static str),
    Arrow {
        operation: ArrowOperation,
        source: arrow::error::ArrowError,
    },
    #[cfg(test)]
    Read {
        operation: &'static str,
        source: std::io::Error,
    },
    Write {
        operation: &'static str,
        source: std::io::Error,
    },
}
impl fmt::Display for ExchangeCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) | Self::Payload(detail) => detail.fmt(f),
            Self::Stopped => f.write_str("exchange receiver was stopped"),
            Self::Poisoned(lock) => write!(f, "exchange {lock} lock is poisoned"),
            Self::Arrow { operation, source } => write!(f, "exchange {operation:?}: {source}"),
            #[cfg(test)]
            Self::Read { operation, source } => write!(f, "exchange {operation}: {source}"),
            Self::Write { operation, source } => {
                write!(f, "exchange {operation}: {source}")
            }
        }
    }
}
impl Error for ExchangeCause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Arrow { source, .. } => Some(source),
            #[cfg(test)]
            Self::Read { source, .. } => Some(source),
            Self::Write { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for ExchangeCause {
    fn failure_fact(&self) -> FailureFact {
        // Only this writer's nominal marker can cross the std/Arrow I/O
        // boundary with its fact. Diagnostic text grants no classification.
        let writer = match self {
            Self::Write { source, .. } => writer_failure(source),
            Self::Arrow {
                operation: ArrowOperation::Encode,
                source: arrow::error::ArrowError::IoError(_, source),
            } => writer_failure(source),
            _ => None,
        };
        if let Some(source) = writer {
            return source.failure_fact();
        }
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, 100),
            Self::Payload(_) => (FailureClass::Data, 101),
            Self::Stopped => (FailureClass::Control, 102),
            Self::Poisoned(_) => (FailureClass::Unexpected, 103),
            Self::Arrow {
                source: arrow::error::ArrowError::MemoryError(_),
                ..
            } => (FailureClass::Resource, 108),
            Self::Arrow {
                operation: ArrowOperation::Decode,
                ..
            } => (FailureClass::Data, 104),
            Self::Arrow {
                operation: ArrowOperation::Encode,
                ..
            } => (FailureClass::Invariant, 105),
            Self::Arrow {
                operation: ArrowOperation::Construct,
                ..
            } => (FailureClass::Invariant, 106),
            #[cfg(test)]
            Self::Read { .. } => (FailureClass::Data, 107),
            Self::Write { .. } => (FailureClass::Invariant, 111),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::EXCHANGE, NonZeroU16::new(code).unwrap()),
        )
    }
}
fn from_cause(source: ExchangeCause) -> ExchangeFailure {
    ExchangeFailure::Source(SharedDomainFailure::new(source))
}
pub(super) fn contract(detail: impl fmt::Display) -> ExchangeFailure {
    from_cause(ExchangeCause::Contract(BoundedDetail::truncating_display(
        detail,
    )))
}
pub(super) fn payload(detail: impl fmt::Display) -> ExchangeFailure {
    from_cause(ExchangeCause::Payload(BoundedDetail::truncating_display(
        detail,
    )))
}
/// This is an ingress-control fact, not a new Task cancellation conclusion.
pub(super) fn stopped() -> ExchangeFailure {
    from_cause(ExchangeCause::Stopped)
}
pub(super) fn poisoned(lock: &'static str) -> ExchangeFailure {
    from_cause(ExchangeCause::Poisoned(lock))
}
pub(super) fn arrow(
    operation: ArrowOperation,
    source: arrow::error::ArrowError,
) -> ExchangeFailure {
    from_cause(ExchangeCause::Arrow { operation, source })
}
#[cfg(test)]
pub(super) fn read(operation: &'static str, source: std::io::Error) -> ExchangeFailure {
    from_cause(ExchangeCause::Read { operation, source })
}

#[derive(Debug)]
struct MetadataFailure<E> {
    operation: BoundedDetail,
    source: E,
}
impl<E: Error> fmt::Display for MetadataFailure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exchange {}: {}", self.operation, self.source)
    }
}
impl<E: Error + 'static> Error for MetadataFailure<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl<E> FailureProjection for MetadataFailure<E> {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(domain::EXCHANGE, NonZeroU16::new(110).unwrap()),
        )
    }
}
pub(super) fn metadata<E: Error + Send + Sync + 'static>(
    operation: impl fmt::Display,
    source: E,
) -> ExchangeFailure {
    ExchangeFailure::Source(SharedDomainFailure::new(MetadataFailure {
        operation: BoundedDetail::truncating_display(operation),
        source,
    }))
}
impl From<crate::exec::chunk::type_compatibility::TypeMismatch> for ExchangeFailure {
    fn from(source: crate::exec::chunk::type_compatibility::TypeMismatch) -> Self {
        Self::Source(SharedDomainFailure::new(source))
    }
}
pub(super) fn allocation(source: std::collections::TryReserveError) -> ExchangeFailure {
    ExchangeFailure::Source(SharedDomainFailure::new(
        novarocks_functions::allocation_failure::AllocationRefusal::new(
            "reserve exchange payload",
            source,
        ),
    ))
}

pub(super) fn write(operation: &'static str, source: std::io::Error) -> ExchangeFailure {
    from_cause(ExchangeCause::Write { operation, source })
}
impl ExchangeFailure {
    pub(crate) fn receiver_unavailable() -> Self {
        contract("exchange receiver port is unavailable")
    }
}

/// This marker belongs to the exchange writer, including when std/Arrow
/// temporarily transports it through an I/O error. It carries no row prefix
/// proof and does not turn an output-format bound into a memory grant.
#[derive(Debug)]
pub(super) enum PayloadWriterFailure {
    Packet {
        required: u128,
        limit: usize,
    },
    Storage {
        required: u128,
        source: Option<std::alloc::LayoutError>,
    },
    SlotCount {
        required: usize,
        source: std::num::TryFromIntError,
    },
    Allocation(
        novarocks_functions::allocation_failure::AllocationRefusal<
            std::collections::TryReserveError,
        >,
    ),
}
impl fmt::Display for PayloadWriterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Packet { required, limit } => write!(
                f,
                "exchange payload requires {required} bytes and exceeds packet cap {limit}"
            ),
            Self::Storage { required, .. } => write!(
                f,
                "exchange payload requires {required} bytes beyond the addressable storage layout"
            ),
            Self::SlotCount { required, .. } => write!(
                f,
                "exchange slot count {required} exceeds the u32 envelope format"
            ),
            Self::Allocation(source) => source.fmt(f),
        }
    }
}
impl Error for PayloadWriterFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Packet { .. } => None,
            Self::Storage { source, .. } => source
                .as_ref()
                .map(|source| source as &(dyn Error + 'static)),
            Self::SlotCount { source, .. } => Some(source),
            Self::Allocation(source) => Some(source),
        }
    }
}
impl FailureProjection for PayloadWriterFailure {
    fn failure_fact(&self) -> FailureFact {
        let (code, required, limit) = match self {
            Self::Packet { required, limit } => (112, *required, *limit as u64),
            Self::Storage { required, .. } => (113, *required, isize::MAX as u64),
            Self::SlotCount { required, .. } => (114, *required as u128, u32::MAX as u64),
            Self::Allocation(source) => return source.failure_fact(),
        };
        let code = NonZeroU16::new(code).unwrap();
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::EXCHANGE, code),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::EXCHANGE,
                code,
            },
            required: u64::try_from(required).ok(),
            extent_limit: Some(limit),
            representable_prefix: None,
        })
    }
}
impl PayloadWriterFailure {
    pub(super) fn allocation(source: std::collections::TryReserveError) -> Self {
        Self::Allocation(
            novarocks_functions::allocation_failure::AllocationRefusal::new(
                "reserve exchange payload",
                source,
            ),
        )
    }
    pub(super) fn into_exchange(self) -> ExchangeFailure {
        ExchangeFailure::Source(SharedDomainFailure::new(self))
    }
}
fn writer_failure(source: &std::io::Error) -> Option<&PayloadWriterFailure> {
    source.get_ref()?.downcast_ref()
}
pub(super) fn payload_storage(required: u128) -> Result<usize, PayloadWriterFailure> {
    let count = usize::try_from(required).map_err(|_| PayloadWriterFailure::Storage {
        required,
        source: None,
    })?;
    std::alloc::Layout::array::<u8>(count).map_err(|source| PayloadWriterFailure::Storage {
        required,
        source: Some(source),
    })?;
    Ok(count)
}
pub(super) fn envelope_slot_count(required: usize) -> Result<u32, ExchangeFailure> {
    u32::try_from(required)
        .map_err(|source| PayloadWriterFailure::SlotCount { required, source }.into_exchange())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn writer_marker_survives_arrow_but_identical_io_text_has_no_authority() {
        let io = || {
            std::io::Error::other(PayloadWriterFailure::Packet {
                required: 11,
                limit: 10,
            })
        };
        let direct = write("payload", io()).failure_fact();
        let wrapped = arrow(ArrowOperation::Encode, io().into()).failure_fact();
        assert_eq!(direct, wrapped);
        let representation = direct.representation().unwrap();
        assert_eq!(representation.required, Some(11));
        assert_eq!(representation.extent_limit, Some(10));
        assert_eq!(representation.representable_prefix, None);
        assert_eq!(
            representation.boundary,
            RepresentationBoundary::SingleOutput
        );
        assert!(direct.resource().is_none());
        let text = io().to_string();
        assert_eq!(
            write("payload", std::io::Error::other(text.clone()))
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
        assert_eq!(
            arrow(ArrowOperation::Encode, std::io::Error::other(text).into())
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
    }
    #[test]
    fn exact_envelope_storage_and_slot_limits_precede_allocation() {
        use std::error::Error;
        assert_eq!(
            payload_storage(isize::MAX as u128).unwrap(),
            isize::MAX as usize
        );
        let source = payload_storage(isize::MAX as u128 + 1).unwrap_err();
        assert!(source.source().unwrap().is::<std::alloc::LayoutError>());
        let fact = source.failure_fact().representation().unwrap();
        assert_eq!(fact.required, Some(isize::MAX as u64 + 1));
        assert_eq!(fact.extent_limit, Some(isize::MAX as u64));
        if let Some(oversized) = (u32::MAX as usize).checked_add(1) {
            assert_eq!(envelope_slot_count(u32::MAX as usize).unwrap(), u32::MAX);
            let error = envelope_slot_count(oversized).unwrap_err();
            assert_eq!(
                error.failure_fact().representation().unwrap().required,
                Some(u32::MAX as u64 + 1)
            );
        }
    }
}
