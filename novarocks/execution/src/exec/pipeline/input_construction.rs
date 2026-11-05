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

//! BE-local consumer input construction. These installed capabilities never
//! form part of a frontend wire declaration or create a memory grant authority.

use std::alloc::Layout;

use arrow::datatypes::DataType;
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_types::SlotId;

use crate::exec::chunk::{ArrayNormalizationStep, ArrayPathStep, Chunk, ChunkSchema};
use crate::runtime::task::ExecutionFailure;

#[path = "input_construction/arrow_model.rs"]
pub mod arrow_model;
#[path = "input_construction/runtime.rs"]
pub mod runtime;
#[path = "input_construction/scoped.rs"]
pub mod scoped;
pub use arrow_model::LockedArrowInputConstructionModel;
pub use runtime::{
    ReceiverInputConstructionStopOwner, try_receiver_input_construction_factory,
    try_runtime_input_construction_factory,
};
pub use scoped::{
    InputConstructionOperationModel, InputConstructionProfile, InputConstructionRequestHost,
    InputConstructionWorkControl, ObservedInputConstructionFactory, OpaqueConstructionBound,
};

/// Construction failures retain the original caller payload. Source and
/// admission failures have explicit variants and are never inferred from text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstructionFailure {
    MissingOwner,
    Memory(crate::runtime::mem_tracker::MemTrackerLimitExceeded),
    InvalidSource(String),
    Source(crate::runtime::task::SharedDomainFailure),
    Control(novarocks_type_contract::CompileControlError),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Caller(Box<ExecutionFailure>),
}
impl ConstructionFailure {
    pub fn detail(&self) -> &str {
        match self {
            Self::MissingOwner => "typed input construction owner is missing",
            Self::Memory(error) => error.detail(),
            Self::InvalidSource(error) => error,
            Self::Source(error) => error.detail(),
            Self::Control(novarocks_type_contract::CompileControlError::Cancelled) => {
                "input construction was cancelled"
            }
            Self::Control(novarocks_type_contract::CompileControlError::DeadlineExceeded) => {
                "input construction deadline was exceeded"
            }
            Self::Control(novarocks_type_contract::CompileControlError::ResourceExhausted) => {
                "input construction work resources were exhausted"
            }
            Self::Control(novarocks_type_contract::CompileControlError::Source(source)) => {
                source.detail()
            }
            Self::Caller(error) => error.detail(),
            Self::Resource(_) => "input construction storage layout cannot be represented",
        }
    }
}
impl From<novarocks_functions::array_projection::ArrayProjectionFailure> for ConstructionFailure {
    fn from(source: novarocks_functions::array_projection::ArrayProjectionFailure) -> Self {
        Self::Source(crate::runtime::task::SharedDomainFailure::new(source))
    }
}
impl From<crate::exec::chunk::ChunkConstructionFailure> for ConstructionFailure {
    fn from(source: crate::exec::chunk::ChunkConstructionFailure) -> Self {
        Self::Source(crate::runtime::task::SharedDomainFailure::new(source))
    }
}
impl From<novarocks_type_contract::CompileControlError> for ConstructionFailure {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExecutionFailure> for ConstructionFailure {
    fn from(error: ExecutionFailure) -> Self {
        Self::Caller(Box::new(error))
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for ConstructionFailure
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}
impl std::fmt::Display for ConstructionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingOwner => f.write_str("typed input construction owner is missing"),
            Self::Memory(error) => error.fmt(f),
            Self::InvalidSource(error) => f.write_str(error),
            Self::Source(error) => error.fmt(f),
            Self::Control(error) => error.fmt(f),
            Self::Caller(error) => error.fmt(f),
            Self::Resource(error) => write!(f, "input construction storage layout: {error:?}"),
        }
    }
}
impl std::error::Error for ConstructionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Memory(error) => Some(error),
            Self::Caller(error) => Some(error.as_ref()),
            Self::Source(error) => Some(error),
            _ => None,
        }
    }
}

/// A separately installed source-custody operation. Its implementation owns a
/// real allocation host; numerical construction observations grant no host.
/// Only known decoder-produced built-in arrays enter this continuation.
pub trait DecodedInputSourceCustody: Send + Sync {
    fn retain(
        &self,
        array: arrow::array::ArrayRef,
        storage: ValueTypeStorageLifetime,
        work: &dyn InputConstructionOwner,
    ) -> Result<arrow::array::ArrayRef, ConstructionFailure>;
}

/// Preparation installs this factory once. Each actual transformation obtains
/// an independent construction scope. The scope's Box/holder creation itself
/// is observed by the original factory before allocation; a caller cannot reset
/// a shared current scope or reuse a preparation-wide cumulative byte allowance.
pub trait InputConstructionFactory: Send + Sync {
    /// Attach the original decoded storage to every result Buffer/Array alias,
    /// including buffers newly allocated by Arrow alignment repair. An absent
    /// explicitly installed custody capability fails before publication.
    fn retain_decoded_source(
        &self,
        _array: arrow::array::ArrayRef,
        _storage: ValueTypeStorageLifetime,
        _work: &dyn InputConstructionOwner,
    ) -> Result<arrow::array::ArrayRef, ConstructionFailure> {
        Err(ConstructionFailure::MissingOwner)
    }
    fn observe_input_work(&self, units: usize) -> Result<(), ConstructionFailure>;
    /// Begin one actual construction from an existing explicit binding. This
    /// does not retain the schema or fabricate an input chunk for source and
    /// aggregate authors. The scope owns only newly constructed storage.
    fn begin_typed_construction(
        &self,
        schema: &ChunkSchema,
    ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure>;
    /// Begin schema construction from an already frozen complete declaration.
    /// No runtime ChunkSchema is manufactured before the creating owner exists.
    fn begin_compiled_layout_construction(
        &self,
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure>;
    /// Create the runtime schema through the same actual preparation owner.
    /// The resulting schema retains only its finite storage responsibility;
    /// it does not keep this factory or the whole compiled program alive.
    fn create_compiled_chunk_schema(
        &self,
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<crate::exec::chunk::ChunkSchemaRef, ConstructionFailure> {
        let owner = self.begin_compiled_layout_construction(layout)?;
        ChunkSchema::from_compiled_layout_owned_observed::<ConstructionFailure>(
            layout,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )
    }
    fn begin_input_construction(
        &self,
        source: &Chunk,
    ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
        self.begin_typed_construction(source.chunk_schema())
    }
}

/// One actual encoded occurrence. Slot identity and output occurrence are both
/// explicit; a parent path cannot authorize an independently encoded child.
#[derive(Clone, Copy, Debug)]
pub struct EncodedValueOccurrence<'a> {
    pub slot: SlotId,
    pub occurrence: usize,
    pub path: &'a [ArrayPathStep],
    pub carrier: &'a DataType,
}

/// Installed by the actual pipeline preparation owner. Implementations check
/// their finite construction request counts/bytes and cooperative control
/// before each operation. They retain only their constructed storage's real
/// responsibility, never QueryContext, MemoryAuthority, or a whole input Chunk.
/// Observation grants no capacity and publishes no allocator facts.
pub trait InputConstructionOwner: Send + Sync {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime;

    /// One concrete constructor request, before the allocation it describes.
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure>;

    /// A locked library's operation bound remains opaque; it is never turned
    /// into a fabricated Layout or a unique allocator fact.
    fn observe_opaque_upper_bound(&self, bytes_upper: usize) -> Result<(), ConstructionFailure>;

    /// Actual borrowed work and representation operation. Decode/rebuild events
    /// precede their allocations; the owner validates their concrete shapes and
    /// requests before allowing them. It must not estimate unknown backing from
    /// ArrayRef size or derive semantics from Field metadata.
    fn observe(
        &self,
        occurrence: usize,
        path: &[ArrayPathStep],
        step: ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure>;
}

impl novarocks_failure_contract::FailureProjection for ConstructionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        match self {
            Self::MissingOwner => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::CONSTRUCTION, std::num::NonZeroU16::new(1).unwrap()),
            ),
            Self::InvalidSource(_) => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::CONSTRUCTION, std::num::NonZeroU16::new(2).unwrap()),
            ),
            Self::Memory(cause) => cause.failure_fact(),
            Self::Control(cause) => cause.failure_fact(),
            Self::Resource(cause) => cause.failure_fact(),
            Self::Caller(cause) => cause.failure_fact(),
            Self::Source(cause) => cause.failure_fact(),
        }
    }
}
