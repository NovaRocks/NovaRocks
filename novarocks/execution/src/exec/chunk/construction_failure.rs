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

//! Chunk schema construction owns its contract refusals. Dependencies keep
//! their original facts; callback failures return unchanged to their caller.
use crate::runtime::task::{ExecutionReason, SharedDomainFailure};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use novarocks_functions::array_projection::ArrayProjectionFailure;
use novarocks_type_contract::{
    ValueTypeError,
    owned_resources::{hashmap::HashMapResourceError, layout::LayoutResourceError},
};
use novarocks_types::SlotId;
use std::{error::Error, fmt};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkConstructionFailure(SharedDomainFailure);
impl ChunkConstructionFailure {
    pub(crate) fn missing_hydration_binding() -> Self {
        Self::source(ChunkViolation::HydrationBinding)
    }
    pub(crate) fn record_batch(source: arrow::error::ArrowError) -> Self {
        Self::source(ChunkRecordBatchFailure(source))
    }
    pub(crate) fn source<E: Error + FailureProjection + Send + Sync + 'static>(source: E) -> Self {
        Self(SharedDomainFailure::new(source))
    }
    pub(super) fn contract(reason: ChunkSchemaViolation) -> Self {
        Self::source(reason)
    }
    pub(super) fn array_request<T>(count: usize, source: std::alloc::LayoutError) -> Self {
        Self::source(SchemaArrayRequest::new::<T>(count, source))
    }
}
impl fmt::Display for ChunkConstructionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for ChunkConstructionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for ChunkConstructionFailure {
    fn failure_fact(&self) -> FailureFact {
        self.0.failure_fact()
    }
}
impl From<ArrayProjectionFailure> for ChunkConstructionFailure {
    fn from(source: ArrayProjectionFailure) -> Self {
        Self::source(source)
    }
}
impl From<ValueTypeError> for ChunkConstructionFailure {
    fn from(source: ValueTypeError) -> Self {
        Self::source(source)
    }
}
#[derive(Debug)]
enum SchemaModelCause {
    Layout(LayoutResourceError),
    Table(HashMapResourceError),
}
impl fmt::Display for SchemaModelCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "schema construction model: {self:?}")
    }
}
impl Error for SchemaModelCause {}
impl FailureProjection for SchemaModelCause {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Layout(source) => source.failure_fact(),
            Self::Table(source) => source.failure_fact(),
        }
    }
}
impl From<LayoutResourceError> for ChunkConstructionFailure {
    fn from(source: LayoutResourceError) -> Self {
        Self::source(SchemaModelCause::Layout(source))
    }
}
impl From<HashMapResourceError> for ChunkConstructionFailure {
    fn from(source: HashMapResourceError) -> Self {
        Self::source(SchemaModelCause::Table(source))
    }
}
#[derive(Debug)]
pub enum ChunkSchemaViolation {
    FieldBinding,
    SlotRename,
    LegacySlot,
    DuplicateSlot { slot: SlotId },
    CanonicalLayout,
    IncompleteOccurrence { index: usize },
    MissingDerivedBinding { index: usize },
    DictionaryEncoding,
    RunEndUnsupported,
    ViewUnsupported,
}
impl fmt::Display for ChunkSchemaViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldBinding => {
                f.write_str("typed chunk field does not match its explicit value binding")
            }
            Self::SlotRename => f.write_str("typed slot rename requires its complete declaration"),
            Self::LegacySlot => f.write_str("typed chunk schema contains a legacy imported slot"),
            Self::DuplicateSlot { slot } => {
                write!(f, "duplicate slot id {slot} in typed chunk schema")
            }
            Self::CanonicalLayout => {
                f.write_str("typed layout does not have a canonical execution schema")
            }
            Self::IncompleteOccurrence { index } => {
                write!(f, "typed layout occurrence {index} is not completely bound")
            }
            Self::MissingDerivedBinding { index } => write!(
                f,
                "derived schema occurrence {index} has no complete binding"
            ),
            Self::DictionaryEncoding => f.write_str(
                "typed chunk dictionary encoding requires Int32 keys and Utf8/LargeUtf8 values",
            ),
            Self::RunEndUnsupported => {
                f.write_str("run-end encoding is not supported by the current execution engine")
            }
            Self::ViewUnsupported => {
                f.write_str("view carriers are not supported by the current execution engine")
            }
        }
    }
}
impl Error for ChunkSchemaViolation {}
impl FailureProjection for ChunkSchemaViolation {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self {
            Self::FieldBinding => ExecutionReason::SchemaFieldBinding,
            Self::SlotRename => ExecutionReason::SchemaSlotRename,
            Self::LegacySlot => ExecutionReason::SchemaLegacySlot,
            Self::DuplicateSlot { .. } => ExecutionReason::SchemaDuplicateSlot,
            Self::CanonicalLayout => ExecutionReason::SchemaCanonicalLayout,
            Self::IncompleteOccurrence { .. } => ExecutionReason::SchemaIncompleteOccurrence,
            Self::MissingDerivedBinding { .. } => ExecutionReason::SchemaMissingDerivedBinding,
            Self::DictionaryEncoding => ExecutionReason::SchemaDictionaryEncoding,
            Self::RunEndUnsupported => ExecutionReason::SchemaRunEndUnsupported,
            Self::ViewUnsupported => ExecutionReason::SchemaViewUnsupported,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
#[derive(Debug)]
pub(super) struct SchemaArrayRequest {
    count: usize,
    width: usize,
    source: std::alloc::LayoutError,
}
impl SchemaArrayRequest {
    pub(super) fn new<T>(count: usize, source: std::alloc::LayoutError) -> Self {
        Self {
            count,
            width: std::mem::size_of::<T>(),
            source,
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for SchemaArrayRequest {}
impl fmt::Display for SchemaArrayRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "schema array request cannot represent {} elements of {} bytes",
            self.count, self.width
        )
    }
}
impl Error for SchemaArrayRequest {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for SchemaArrayRequest {
    fn failure_fact(&self) -> FailureFact {
        use novarocks_failure_contract::{
            RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
        };
        FailureFact::new(
            FailureClass::Representation,
            ExecutionReason::SchemaArrayRequest.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: std::num::NonZeroU16::new(243).unwrap(),
            },
            required: u64::try_from((self.count as u128) * (self.width as u128)).ok(),
            extent_limit: Some(isize::MAX as u64),
            representable_prefix: None,
        })
    }
}

pub(super) enum ChunkObservationFailure<E> {
    Caller(E),
    Source(ChunkConstructionFailure),
}
impl<E> From<ArrayProjectionFailure> for ChunkObservationFailure<E> {
    fn from(source: ArrayProjectionFailure) -> Self {
        Self::Source(source.into())
    }
}
impl<E> From<ValueTypeError> for ChunkObservationFailure<E> {
    fn from(source: ValueTypeError) -> Self {
        Self::Source(source.into())
    }
}
impl<E> ChunkObservationFailure<E> {
    pub(super) fn into_caller(self) -> E
    where
        E: From<ChunkConstructionFailure>,
    {
        match self {
            Self::Caller(error) => error,
            Self::Source(error) => E::from(error),
        }
    }
}
impl From<ChunkConstructionFailure> for crate::runtime::task::ExecutionFailure {
    fn from(source: ChunkConstructionFailure) -> Self {
        Self::from_domain(source)
    }
}

impl From<novarocks_types::arrow_metadata_owner::MetadataOwnerError> for ChunkConstructionFailure {
    fn from(source: novarocks_types::arrow_metadata_owner::MetadataOwnerError) -> Self {
        Self::source(source)
    }
}
#[derive(Clone, Copy, Debug)]
pub(super) enum SchemaContractSite {
    Metadata,
    Mutation,
    Layout,
    Projection,
    Reconciliation,
    Assembly,
}
#[derive(Debug)]
struct SchemaContractRefusal {
    site: SchemaContractSite,
    detail: novarocks_failure_contract::BoundedDetail,
}
impl fmt::Display for SchemaContractRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}
impl Error for SchemaContractRefusal {}
impl FailureProjection for SchemaContractRefusal {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self.site {
            SchemaContractSite::Metadata => ExecutionReason::SchemaMetadataContract,
            SchemaContractSite::Mutation => ExecutionReason::SchemaMutationContract,
            SchemaContractSite::Layout => ExecutionReason::SchemaLayoutContract,
            SchemaContractSite::Projection => ExecutionReason::SchemaProjectionContract,
            SchemaContractSite::Reconciliation => ExecutionReason::SchemaReconciliationContract,
            SchemaContractSite::Assembly => ExecutionReason::SchemaAssemblyContract,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
pub(super) fn schema_contract(
    site: SchemaContractSite,
    detail: impl fmt::Display,
) -> ChunkConstructionFailure {
    ChunkConstructionFailure::source(SchemaContractRefusal {
        site,
        detail: novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    })
}

impl From<novarocks_type_contract::ExecutionValueTypeError> for ChunkConstructionFailure {
    fn from(source: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::source(source)
    }
}
#[derive(Debug)]
struct SchemaTypeMismatch {
    root: novarocks_failure_contract::BoundedDetail,
    source: super::type_compatibility::TypeMismatch,
}
impl fmt::Display for SchemaTypeMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "chunk schema type mismatch at {}: expected {}, got {} ({:?})",
            super::type_compatibility::nested_path_label(
                self.root.as_str(),
                &self.source.nested_path
            ),
            self.source.expected,
            self.source.actual,
            self.source.kind
        )
    }
}
impl Error for SchemaTypeMismatch {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for SchemaTypeMismatch {
    fn failure_fact(&self) -> FailureFact {
        self.source.failure_fact()
    }
}
pub(super) fn schema_type_mismatch(
    root: &str,
    source: super::type_compatibility::TypeMismatch,
) -> ChunkConstructionFailure {
    ChunkConstructionFailure::source(SchemaTypeMismatch {
        root: novarocks_failure_contract::BoundedDetail::truncating(root),
        source,
    })
}

impl<E> From<LayoutResourceError> for ChunkObservationFailure<E> {
    fn from(source: LayoutResourceError) -> Self {
        Self::Source(source.into())
    }
}
#[derive(Debug)]
pub(super) enum ChunkViolation {
    TypedChannels {
        typed: bool,
        expected: usize,
        actual: usize,
    },
    ColumnRows {
        column: usize,
        expected: usize,
        actual: usize,
    },
    ColumnLayout {
        column: usize,
    },
    ColumnBinding {
        column: usize,
    },
    BatchOccurrence {
        column: usize,
    },
    Slice {
        typed: bool,
        offset: usize,
        length: usize,
        rows: usize,
    },
    BatchLabels,
    AccountingOwner,
    OutputMetadata,
    HydrationBinding,
    HydrationMapEntries,
    HydrationMapArity,
}
impl fmt::Display for ChunkViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypedChannels {
                typed,
                expected,
                actual,
            } => write!(
                f,
                "typed chunk channel contract differs (typed={typed}, expected={expected}, actual={actual})"
            ),
            Self::ColumnRows {
                column,
                expected,
                actual,
            } => write!(
                f,
                "typed chunk channel {column} has {actual} rows, expected {expected}"
            ),
            Self::ColumnLayout { column } => write!(
                f,
                "typed chunk channel {column} carrier does not match its explicit value layout"
            ),
            Self::ColumnBinding { column } => write!(
                f,
                "typed chunk channel {column} carrier does not match its explicit binding"
            ),
            Self::BatchOccurrence { column } => write!(
                f,
                "typed chunk batch occurrence {column} differs from its explicit label or nullability"
            ),
            Self::Slice {
                typed,
                offset,
                length,
                rows,
            } => write!(
                f,
                "typed chunk slice is outside its explicit source contract (typed={typed}, offset={offset}, length={length}, rows={rows})"
            ),
            Self::BatchLabels => {
                f.write_str("typed chunk batch occurrence labels differ from its explicit contract")
            }
            Self::AccountingOwner => {
                f.write_str("chunk output memory already has an accounting owner")
            }
            Self::OutputMetadata => {
                f.write_str("output column metadata requires the original frozen column container")
            }
            Self::HydrationBinding => {
                f.write_str("typed hydration requires explicit value bindings")
            }
            Self::HydrationMapEntries => {
                f.write_str("dictionary hydration Map entries are not a Struct")
            }
            Self::HydrationMapArity => {
                f.write_str("dictionary hydration Map entries have invalid arity")
            }
        }
    }
}
impl Error for ChunkViolation {}
impl FailureProjection for ChunkViolation {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self {
            Self::TypedChannels { .. } => ExecutionReason::ChunkTypedChannels,
            Self::ColumnRows { .. } => ExecutionReason::ChunkColumnRows,
            Self::ColumnLayout { .. } => ExecutionReason::ChunkColumnLayout,
            Self::ColumnBinding { .. } => ExecutionReason::ChunkColumnBinding,
            Self::BatchOccurrence { .. } => ExecutionReason::ChunkBatchOccurrence,
            Self::Slice { .. } => ExecutionReason::ChunkSlice,
            Self::BatchLabels => ExecutionReason::ChunkBatchLabels,
            Self::AccountingOwner => ExecutionReason::ChunkAccountingOwner,
            Self::OutputMetadata => ExecutionReason::ChunkOutputMetadata,
            Self::HydrationBinding => ExecutionReason::ChunkHydrationBinding,
            Self::HydrationMapEntries => ExecutionReason::ChunkHydrationMapEntries,
            Self::HydrationMapArity => ExecutionReason::ChunkHydrationMapArity,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
#[derive(Debug)]
pub(super) struct ChunkRecordBatchFailure(pub arrow::error::ArrowError);
impl fmt::Display for ChunkRecordBatchFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "build chunk record batch failed: {}", self.0)
    }
}
impl Error for ChunkRecordBatchFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for ChunkRecordBatchFailure {
    fn failure_fact(&self) -> FailureFact {
        // RecordBatch construction only validates the supplied schema/columns;
        // it does not cast values, concatenate payload or change offset widths.
        match self.0 {
            arrow::error::ArrowError::MemoryError(_) => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::ChunkRecordBatchMemory.reason(),
            ),
            _ => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::ChunkRecordBatch.reason(),
            ),
        }
    }
}
