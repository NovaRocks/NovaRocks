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

//! The connector `SourcePage` to execution `Chunk` boundary.
//!
//! Responsibilities:
//! - Turns each page a scan's page stream produces into a `Chunk` bound to
//!   the scan's ordered output slot ids.
//! - Keeps the distinction the page contract makes and a naive adapter would
//!   lose: a page with zero channels and a positive position count is a real
//!   result, never end of stream.
//!
//! Key exported interfaces:
//! - Types: `SourcePageConverter`, `PageAdapterError`, `PageAdapterErrorKind`.
//!
//! Every page binds the scan's immutable complete typed output declaration.
//! Runtime construction uses the installed scan factory; page arrays never
//! author a competing schema.
//!
//! Provider neutrality: nothing here names a provider or inspects a provider
//! variant, so this file compiles with no provider crate in the dependency
//! graph.

use std::sync::Arc;

use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::SourcePage;

use crate::exec::chunk::{ArrayNormalizationStep, Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

/// The source diagnostic and creating owner's original cause stay distinct.
enum PageConversionFailure {
    Page(PageAdapterError),
    Construction(ConstructionFailure),
}
impl From<PageAdapterError> for PageConversionFailure {
    fn from(error: PageAdapterError) -> Self {
        Self::Page(error)
    }
}
impl From<ConstructionFailure> for PageConversionFailure {
    fn from(error: ConstructionFailure) -> Self {
        Self::Construction(error)
    }
}
impl PageConversionFailure {
    fn into_execution(self) -> ExecutionFailure {
        match self {
            Self::Page(error) => ExecutionFailure::from_domain(error),
            Self::Construction(error) => error.into(),
        }
    }
    #[cfg(test)]
    fn into_page(self) -> PageAdapterError {
        match self {
            Self::Page(error) => error,
            // Only the explicit pure adapter uses this conversion. Its body
            // has no construction owner and cannot originate this variant.
            Self::Construction(error) => {
                PageAdapterError::new(PageAdapterCause::Chunk, error.to_string())
            }
        }
    }
}

/// Why one page could not become a chunk.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PageAdapterErrorKind {
    /// The page carries fewer channels than the output binds.
    ChannelMismatch,
    /// A materialized channel does not agree with the page's position count.
    PositionMismatch,
    /// The connector failed while producing, materializing, or closing.
    Connector,
    /// The materialized columns could not form a chunk.
    Chunk,
}

impl std::fmt::Display for PageAdapterErrorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ChannelMismatch => "channel mismatch",
            Self::PositionMismatch => "position mismatch",
            Self::Connector => "connector failure",
            Self::Chunk => "chunk construction failure",
        })
    }
}

/// A page-conversion cause. Connector failures always retain their original
/// typed payload; local contract failures cannot impersonate a Connector.
#[derive(Clone, Debug, Eq, PartialEq)]
enum PageAdapterCause {
    ChannelMismatch,
    PositionMismatch,
    Chunk,
    Connector(ConnectorError),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageAdapterError {
    cause: PageAdapterCause,
    detail: novarocks_failure_contract::BoundedDetail,
}
impl PageAdapterError {
    fn new(cause: PageAdapterCause, detail: impl std::fmt::Display) -> Self {
        Self {
            cause,
            detail: novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        }
    }
    pub fn from_connector(error: ConnectorError, context: &str) -> Self {
        let detail = novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
            "{context}: {error}"
        ));
        Self {
            cause: PageAdapterCause::Connector(error),
            detail,
        }
    }
    pub const fn kind(&self) -> PageAdapterErrorKind {
        match &self.cause {
            PageAdapterCause::ChannelMismatch => PageAdapterErrorKind::ChannelMismatch,
            PageAdapterCause::PositionMismatch => PageAdapterErrorKind::PositionMismatch,
            PageAdapterCause::Chunk => PageAdapterErrorKind::Chunk,
            PageAdapterCause::Connector(_) => PageAdapterErrorKind::Connector,
        }
    }
    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
    pub const fn connector_error(&self) -> Option<&ConnectorError> {
        match &self.cause {
            PageAdapterCause::Connector(error) => Some(error),
            _ => None,
        }
    }
}
impl std::fmt::Display for PageAdapterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.kind(), self.detail)
    }
}
impl std::error::Error for PageAdapterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.connector_error()
            .map(|error| error as &dyn std::error::Error)
    }
}
impl novarocks_failure_contract::FailureProjection for PageAdapterError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match &self.cause {
            PageAdapterCause::Connector(error) => return error.failure_fact(),
            PageAdapterCause::ChannelMismatch => ExecutionReason::SourcePageChannels,
            PageAdapterCause::PositionMismatch => ExecutionReason::SourcePagePositions,
            PageAdapterCause::Chunk => ExecutionReason::SourcePageBinding,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}

/// Converts pages against the scan's frozen complete typed declaration.
/// Channels beyond its bound prefix remain provider working columns and are
/// dropped. A page's accounted output memory moves onto its chunk.
pub struct SourcePageConverter {
    output_schema: ChunkSchemaRef,
}

impl SourcePageConverter {
    /// Borrow the scan's frozen ordered slots and complete typed declarations.
    /// This converter creates no competing slot-ID vector or type author.
    pub fn with_output_schema(output_schema: ChunkSchemaRef) -> Self {
        Self { output_schema }
    }

    #[cfg(test)]
    fn convert(&mut self, page: SourcePage) -> Result<Chunk, PageAdapterError> {
        convert_page(page, &self.output_schema, None).map_err(PageConversionFailure::into_page)
    }

    /// The installed scan factory creates only the new batch/type scaffolds.
    /// The provider's column vector and output-memory token transfer unchanged.
    pub fn convert_with_factory(
        &mut self,
        page: SourcePage,
        factory: &dyn InputConstructionFactory,
    ) -> ExecutionResult<Chunk> {
        let owner = factory.begin_typed_construction(&self.output_schema)?;
        convert_page(page, &self.output_schema, Some(owner.as_ref()))
            .map_err(PageConversionFailure::into_execution)
    }
}

fn convert_page(
    mut page: SourcePage,
    output_schema: &ChunkSchemaRef,
    owner: Option<&dyn InputConstructionOwner>,
) -> Result<Chunk, PageConversionFailure> {
    if let Some(owner) = owner {
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork { units: 1 },
        )?;
    }
    if !output_schema.is_typed() {
        return Err(PageAdapterError::new(
            PageAdapterCause::Chunk,
            "connector page output schema lacks complete typed bindings",
        )
        .into());
    }
    let slot_ids = output_schema.slot_ids();
    let positions = page.position_count();
    let channel_count = page.channel_count();
    if slot_ids.len() > channel_count {
        return Err(PageAdapterError::new(
            PageAdapterCause::ChannelMismatch,
            format!(
                "scan output binds {} channels but the page carries {channel_count}",
                slot_ids.len()
            ),
        )
        .into());
    }

    // Discard provider working channels before extracting the page. The
    // accounted extraction moves the provider reservation together with the
    // visible Arrow buffers.
    page.truncate_channels(slot_ids.len()).map_err(|error| {
        PageAdapterError::from_connector(error, "connector page channel projection")
    })?;
    let (extracted_positions, columns, output_memory) =
        page.into_accounted_columns().map_err(|error| {
            PageAdapterError::from_connector(error, "connector page channel materialization")
        })?;
    debug_assert_eq!(positions, extracted_positions);
    for (index, column) in columns.iter().enumerate() {
        if let Some(owner) = owner {
            owner.observe(
                index,
                &[],
                ArrayNormalizationStep::InspectConstructionWork { units: 1 },
            )?;
        }
        if column.len() != positions {
            return Err(PageAdapterError::new(
                PageAdapterCause::PositionMismatch,
                format!(
                    "channel {index} produced {} values for a page of {positions} positions",
                    column.len()
                ),
            )
            .into());
        }
    }

    let mut visible_bytes = 0_u64;
    for (index, column) in columns.iter().enumerate() {
        if let Some(owner) = owner {
            owner.observe(index, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
        }
        visible_bytes = visible_bytes.saturating_add(column.get_array_memory_size() as u64);
        if let Some(owner) = owner {
            owner.observe(index, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
        }
    }
    let mut chunk =
        if let Some(owner) = owner {
            // The incoming Vec already belongs to the SourcePage. Only actual
            // normalized wrappers and the new RecordBatch Arc are constructed.
            Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
                Arc::clone(output_schema),
                columns,
                positions,
                owner.storage_lifetime(),
                |step| owner.observe(0, &[], step),
            )?
        } else {
            #[cfg(not(test))]
            return Err(ConstructionFailure::MissingOwner.into());
            #[cfg(test)]
            {
                // The explicit pure fixture path uses the same complete declaration,
                // including row count for a zero-channel page; it never derives types.
                Chunk::try_new_typed_with_columns_observed::<
                    crate::runtime::task::ExecutionFailure,
                    _,
                >(Arc::clone(output_schema), columns, positions, |_| Ok(()))
                .map_err(|error| PageAdapterError::new(PageAdapterCause::Chunk, error))?
            }
        };

    let output_memory = if let Some(mut output_memory) = output_memory {
        output_memory.shrink_to(visible_bytes).map_err(|error| {
            PageAdapterError::from_connector(error, "connector page visible output accounting")
        })?;
        if visible_bytes > 0 {
            Some(output_memory)
        } else {
            None
        }
    } else {
        None
    };
    if let Some(output_memory) = output_memory {
        chunk
            .attach_connector_output_memory(output_memory)
            .map_err(|error| PageAdapterError::new(PageAdapterCause::Chunk, error))?;
    }
    Ok(chunk)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use arrow::array::{Array, ArrayRef, Int64Array};
    use arrow::datatypes::Field;
    use novarocks_spi::connector::{
        ConnectorError, ConnectorExecutionResources, ConnectorReservationBackend,
        ConnectorResourceCheckpoint, ConnectorResourceClass, ConnectorResourceLedger,
    };
    use novarocks_types::SlotId;

    use super::*;
    use crate::runtime::mem_tracker::MemTracker;

    fn declared_schema(with_column: bool) -> ChunkSchemaRef {
        declared_schema_at(with_column, SlotId::new(17))
    }

    fn declared_schema_at(with_column: bool, slot: SlotId) -> ChunkSchemaRef {
        use arrow::datatypes::DataType;
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        let storage = ValueTypeStorageLifetime::pure();
        let slots = if with_column {
            let declaration = FunctionValueType::new(DataType::Int64, false);
            vec![
                ChunkSlotSchema::try_new_typed(
                    slot,
                    Arc::new(Field::new("v", DataType::Int64, false)),
                    declaration.typed_binding().clone(),
                    None,
                    storage.clone(),
                )
                .unwrap(),
            ]
        } else {
            vec![]
        };
        Arc::new(ChunkSchema::try_new_typed(slots, storage).unwrap())
    }

    fn actual_factory(
        tracker: &Arc<MemTracker>,
        error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    ) -> Arc<dyn InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            error,
        )
        .unwrap()
    }

    #[test]
    fn typed_page_initial_chunk_keeps_only_its_finite_creator_and_frozen_slots() {
        let tracker = MemTracker::new_root("initial typed page");
        let error = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default());
        let controller = Arc::downgrade(&error);
        let factory = actual_factory(&tracker, error);
        let schema = declared_schema(true);
        let mut converter = SourcePageConverter::with_output_schema(schema.clone());
        let chunk = converter
            .convert_with_factory(int_page(vec![1, 2]), factory.as_ref())
            .unwrap();
        assert_eq!(chunk.chunk_schema().slot_ids(), &[SlotId::new(17)]);
        assert_eq!(chunk.len(), 2);
        assert!(Arc::ptr_eq(&chunk.chunk_schema_ref(), &schema));
        drop(converter);
        drop(factory);
        assert!(
            controller.upgrade().is_none(),
            "late chunk does not retain the control/factory"
        );
        assert!(tracker.current() > 0);
        let late = chunk.clone();
        drop(chunk);
        assert!(tracker.current() > 0);
        drop(late);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn typed_count_only_page_and_final_wrapper_refusal_use_the_actual_owner() {
        use crate::runtime::task::ExecutionFailureCause;
        let tracker = MemTracker::new_root("count-only page constructor");
        let factory = actual_factory(
            &tracker,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );
        let initial = tracker.current();
        let mut converter = SourcePageConverter::with_output_schema(declared_schema(false));
        let chunk = converter
            .convert_with_factory(SourcePage::zero_channel(7), factory.as_ref())
            .unwrap();
        assert_eq!(chunk.len(), 7);
        assert!(chunk.columns().is_empty());
        assert!(chunk.chunk_schema().is_typed());
        let request_bytes = tracker.current() - initial;
        assert!(request_bytes > 1);
        drop(chunk);
        assert_eq!(tracker.current(), initial);
        // This same constructor has only scope metadata and its final batch
        // Arc requests: one byte below its real successful total refuses that
        // final wrapper without changing the provider's row-count semantics.
        tracker
            .install_limit_once(initial + request_bytes - 1)
            .unwrap();
        let error = converter
            .convert_with_factory(SourcePage::zero_channel(7), factory.as_ref())
            .unwrap_err();
        assert!(matches!(
            error.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
        ));
        assert_eq!(
            tracker.current(),
            initial,
            "all successful prefix requests roll back"
        );
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn typed_page_original_stop_precedes_source_errors_without_constructing_a_chunk() {
        let tracker = MemTracker::new_root("page original stop");
        let error = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default());
        let factory = actual_factory(&tracker, error.clone());
        let initial = tracker.current();
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("original scan stop"),
        ));
        error.set_error(original.clone());
        let mut converter = SourcePageConverter::with_output_schema(declared_schema(true));
        let refused = converter
            .convert_with_factory(SourcePage::zero_channel(1), factory.as_ref())
            .unwrap_err();
        assert_eq!(
            refused, original,
            "the missing source channel must not replace the original stop"
        );
        assert_eq!(tracker.current(), initial);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    struct OutputLedger {
        retained: Arc<AtomicU64>,
    }

    impl ConnectorResourceLedger for OutputLedger {
        fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
            Ok(ConnectorResourceCheckpoint::new(0))
        }

        fn try_reserve(
            &self,
            class: ConnectorResourceClass,
            bytes: u64,
        ) -> Result<Box<dyn ConnectorReservationBackend>, ConnectorError> {
            assert_eq!(class, ConnectorResourceClass::ReaderOutput);
            self.retained.fetch_add(bytes, Ordering::AcqRel);
            Ok(Box::new(OutputReservation {
                retained: Arc::clone(&self.retained),
                bytes,
            }))
        }
    }

    struct OutputReservation {
        retained: Arc<AtomicU64>,
        bytes: u64,
    }

    impl ConnectorReservationBackend for OutputReservation {
        fn bytes(&self) -> u64 {
            self.bytes
        }

        fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
            self.retained.fetch_add(additional, Ordering::AcqRel);
            self.bytes += additional;
            Ok(())
        }

        fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
            assert!(bytes <= self.bytes);
            self.retained
                .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
            self.bytes = bytes;
            Ok(())
        }
    }

    impl Drop for OutputReservation {
        fn drop(&mut self) {
            self.retained.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }

    fn int_page(values: Vec<i64>) -> SourcePage {
        let positions = values.len();
        let column: ArrayRef = Arc::new(Int64Array::from(values));
        SourcePage::try_new(positions, vec![column]).expect("valid page")
    }

    fn convert_declared(page: SourcePage, with_column: bool) -> Result<Chunk, PageAdapterError> {
        SourcePageConverter::with_output_schema(declared_schema_at(with_column, SlotId::new(1)))
            .convert(page)
    }

    #[test]
    fn a_zero_channel_page_converts_to_a_row_counted_chunk() {
        let chunk = convert_declared(SourcePage::zero_channel(1024), false).expect("chunk");
        assert_eq!(chunk.len(), 1024);
        assert_eq!(chunk.columns().len(), 0);
        // It is a real result, never end of stream.
        assert!(!chunk.is_empty());
    }

    #[test]
    fn accounted_page_moves_one_existing_charge_to_the_chunk() {
        let retained = Arc::new(AtomicU64::new(0));
        let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
            retained: Arc::clone(&retained),
        }));
        let column: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let bytes = column.get_array_memory_size() as u64;
        let output_memory = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
            .expect("reserve provider output")
            .into_output(bytes)
            .expect("freeze provider output charge");
        let page = SourcePage::try_new_accounted(3, vec![column], output_memory)
            .expect("accounted source page");
        let tracker = MemTracker::new_root("converted connector chunk");
        let mut chunk =
            SourcePageConverter::with_output_schema(declared_schema_at(true, SlotId::new(1)))
                .convert(page)
                .expect("convert accounted page");
        chunk.transfer_to(&tracker);
        assert_eq!(retained.load(Ordering::Acquire), bytes);
        assert_eq!(tracker.current(), 0, "the page must not be charged twice");

        let shared = chunk.clone();
        drop(chunk);
        assert_eq!(retained.load(Ordering::Acquire), bytes);
        drop(shared);
        assert_eq!(retained.load(Ordering::Acquire), 0);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn accounted_page_releases_hidden_materialized_channels_before_chunk_handoff() {
        let retained = Arc::new(AtomicU64::new(0));
        let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
            retained: Arc::clone(&retained),
        }));
        let visible: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let hidden: ArrayRef = Arc::new(Int64Array::from((0_i64..1024).collect::<Vec<_>>()));
        let hidden = hidden.slice(0, 3);
        let visible_bytes = visible.get_array_memory_size() as u64;
        let total_bytes = visible_bytes + hidden.get_array_memory_size() as u64;
        let output_memory = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, total_bytes)
            .expect("reserve visible and hidden provider output")
            .into_output(total_bytes)
            .expect("freeze full provider output charge");
        let page = SourcePage::try_new_accounted(3, vec![visible, hidden], output_memory)
            .expect("accounted source page");
        let chunk = convert_declared(page, true).expect("convert projected page");
        assert_eq!(retained.load(Ordering::Acquire), visible_bytes);
        drop(chunk);
        assert_eq!(retained.load(Ordering::Acquire), 0);
    }

    #[test]
    fn accounted_count_only_projection_releases_the_complete_page_charge() {
        let retained = Arc::new(AtomicU64::new(0));
        let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
            retained: Arc::clone(&retained),
        }));
        let hidden: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let bytes = hidden.get_array_memory_size() as u64;
        let output_memory = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
            .expect("reserve hidden provider output")
            .into_output(bytes)
            .expect("freeze hidden provider output charge");
        let page = SourcePage::try_new_accounted(3, vec![hidden], output_memory)
            .expect("accounted source page");
        let chunk = convert_declared(page, false).expect("convert count-only page");
        assert_eq!(chunk.len(), 3);
        assert_eq!(retained.load(Ordering::Acquire), 0);
    }

    #[test]
    fn an_unbound_trailing_channel_is_dropped() {
        let visible: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let hidden: ArrayRef = Arc::new(Int64Array::from(vec![5_i64, 6]));
        let page = SourcePage::try_new(2, vec![visible, hidden]).expect("valid page");

        let chunk = convert_declared(page, true).expect("chunk");
        assert_eq!(chunk.columns().len(), 1);
        assert_eq!(chunk.len(), 2);
    }

    #[test]
    fn binding_more_channels_than_the_page_carries_is_rejected() {
        let error =
            convert_declared(SourcePage::zero_channel(4), true).expect_err("channel mismatch");
        assert_eq!(error.kind(), PageAdapterErrorKind::ChannelMismatch);
    }

    #[test]
    fn the_derived_schema_is_reused_while_the_arrow_types_hold() {
        let mut converter =
            SourcePageConverter::with_output_schema(declared_schema_at(true, SlotId::new(1)));
        let first = converter
            .convert(int_page(vec![1]))
            .expect("first chunk")
            .chunk_schema_ref();
        let second = converter
            .convert(int_page(vec![2, 3]))
            .expect("second chunk")
            .chunk_schema_ref();
        assert!(Arc::ptr_eq(&first, &second));
    }

    fn frozen_map_schema(value_type: arrow::datatypes::DataType) -> ChunkSchemaRef {
        use arrow::datatypes::DataType;
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Field::new("value", value_type, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        let storage = ValueTypeStorageLifetime::pure();
        let declaration = FunctionValueType::new(map_type, true);
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(17),
            Arc::new(Field::new("mi", declaration.data_type().clone(), true)),
            declaration.typed_binding().clone(),
            None,
            storage.clone(),
        )
        .expect("frozen map slot");
        Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).expect("frozen map schema"))
    }

    fn physical_map() -> ArrayRef {
        use arrow::array::{Int32Array, MapArray, StructArray};
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        let entries = StructArray::new(
            vec![
                Arc::new(Field::new("key", arrow::datatypes::DataType::Int32, false)),
                Arc::new(Field::new("value", arrow::datatypes::DataType::Int32, true)),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![None, Some(7)])),
            ],
            None,
        );
        Arc::new(MapArray::new(
            Arc::new(Field::new("key_value", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0_i32, 0, 0, 1, 2].into()),
            entries,
            Some(NullBuffer::from(vec![false, true, true, true])),
            false,
        ))
    }

    #[test]
    fn frozen_scan_map_retags_buffers_and_keeps_parent_value_nulls_and_one_lease() {
        use arrow::array::MapArray;
        let retained = Arc::new(AtomicU64::new(0));
        let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
            retained: Arc::clone(&retained),
        }));
        let source = physical_map();
        let physical = source
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("source map");
        let key_pointer = physical.keys().to_data().buffers()[0].as_ptr();
        let value_pointer = physical.values().to_data().buffers()[0].as_ptr();
        let offset_pointer = physical.offsets().inner().inner().as_ptr();
        let bytes = source.get_array_memory_size() as u64;
        let lease = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
            .expect("reserve")
            .into_output(bytes)
            .expect("output lease");
        let page = SourcePage::try_new_accounted(4, vec![source], lease).expect("page");
        let schema = frozen_map_schema(arrow::datatypes::DataType::Int32);
        let mut converter = SourcePageConverter::with_output_schema(Arc::clone(&schema));
        let mut chunk = converter.convert(page).expect("frozen map output");
        assert_eq!(
            chunk.columns()[0].data_type(),
            schema.slots()[0].data_type()
        );
        assert_eq!(chunk.chunk_schema().slot_ids(), &[SlotId::new(17)]);
        let map = chunk.columns()[0]
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map");
        assert_eq!(map.keys().to_data().buffers()[0].as_ptr(), key_pointer);
        assert_eq!(map.values().to_data().buffers()[0].as_ptr(), value_pointer);
        assert_eq!(map.offsets().inner().inner().as_ptr(), offset_pointer);
        assert!(map.is_null(0));
        assert!(!map.is_null(1));
        assert_eq!(map.value_length(1), 0);
        assert_eq!(map.values().null_count(), 1);
        assert_eq!(map.value_length(2), 1);
        let tracker = MemTracker::new_root("frozen map output");
        chunk.transfer_to(&tracker);
        assert_eq!(
            tracker.current(),
            0,
            "retag must keep the existing provider lease"
        );
        assert_eq!(retained.load(Ordering::Acquire), bytes);
        let cloned = chunk.clone();
        drop(chunk);
        assert_eq!(retained.load(Ordering::Acquire), bytes);
        drop(cloned);
        assert_eq!(retained.load(Ordering::Acquire), 0);
        let second = converter
            .convert(SourcePage::try_new(4, vec![physical_map()]).expect("page"))
            .expect("second page uses the same frozen contract");
        assert_eq!(
            second.columns()[0].data_type(),
            schema.slots()[0].data_type()
        );
    }

    #[test]
    fn frozen_scan_rejects_nested_value_drift_instead_of_rebinding_from_the_page() {
        let mut converter = SourcePageConverter::with_output_schema(frozen_map_schema(
            arrow::datatypes::DataType::Int64,
        ));
        let error = converter
            .convert(SourcePage::try_new(4, vec![physical_map()]).expect("page"))
            .expect_err("Int32 must not become Int64 by metadata retag");
        assert_eq!(error.kind(), PageAdapterErrorKind::Chunk);
    }

    #[test]
    fn frozen_zero_channel_scan_preserves_count_and_output_contract() {
        let schema = declared_schema(false);
        let chunk = SourcePageConverter::with_output_schema(Arc::clone(&schema))
            .convert(SourcePage::zero_channel(19))
            .expect("count-only page");
        assert_eq!(chunk.len(), 19);
        assert!(chunk.columns().is_empty());
        assert_eq!(chunk.chunk_schema().slot_ids(), schema.slot_ids());
    }
}

#[cfg(test)]
mod failure_projection_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn page_conversion_preserves_connector_cause_and_does_not_classify_diagnostics() {
        use novarocks_spi::connector::ConnectorErrorKind;
        for kind in [
            ConnectorErrorKind::Unavailable,
            ConnectorErrorKind::Cancelled,
        ] {
            let original = ConnectorError::new(kind, "ResourceExhausted: secret".repeat(200));
            let expected = original.failure_fact();
            let failure = PageConversionFailure::Page(PageAdapterError::from_connector(
                original.clone(),
                "page input",
            ))
            .into_execution();
            assert_eq!(failure.failure_fact(), expected);
            let crate::runtime::task::ExecutionFailureCause::Domain(domain) = failure.cause()
            else {
                panic!("page cause")
            };
            let actual = domain.downcast_ref::<PageAdapterError>().unwrap();
            assert_eq!(actual.connector_error(), Some(&original));
            assert!(actual.detail().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
            assert_eq!(
                std::error::Error::source(actual)
                    .unwrap()
                    .downcast_ref::<ConnectorError>(),
                Some(&original)
            );
        }
        let local = PageAdapterError::new(PageAdapterCause::ChannelMismatch, "resource exhausted");
        assert_eq!(local.failure_fact().class(), FailureClass::Invariant);
    }
}
