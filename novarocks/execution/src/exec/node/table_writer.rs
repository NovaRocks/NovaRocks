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

//! Native runtime binding and value obligations of a compiled TableWriter.
//!
//! `TableWriter` is an ordinary unary processor *with output*, not a terminal
//! sink. Every pipeline driver opens, owns, appends to, and finishes or aborts
//! its own [`ConnectorBatchWriter`]. Nothing about the writer is shared between
//! drivers, so this node carries no driver count, no writer mutex, and no
//! "last driver finishes" protocol: only an immutable logical recipe plus the
//! attempt-local facts a driver needs to build its own physical context.

use std::sync::Arc;

#[cfg(test)]
use arrow::datatypes::DataType;

use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::write_stack::{
    ConnectorWriteExecution, ConnectorWriterHandle, ConnectorWriterPhysicalContext,
};

use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::exec::node::table_write_relation::ConnectorCommitFragmentEncoder;
#[cfg(novarocks_dev_controls)]
use crate::exec::node::table_write_relation::{
    AllowTableWriteAggregates, TableWriteAggregateGuard,
};

/// The attempt-local facts every driver of one `TableWriter` shares.
///
/// The driver id is deliberately absent: it is the one field that differs per
/// driver, and it is supplied by `OperatorFactory::create` when that driver
/// opens its own writer. These facts are never a commit authority and never a
/// recovery token.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TableWriterPhysicalContextTemplate {
    execution_query_id: [u8; 16],
    execution_attempt_id: u64,
    fragment_instance_id: [u8; 16],
    writer_ordinal: u32,
}

impl TableWriterPhysicalContextTemplate {
    pub const fn new(
        execution_query_id: [u8; 16],
        execution_attempt_id: u64,
        fragment_instance_id: [u8; 16],
        writer_ordinal: u32,
    ) -> Self {
        Self {
            execution_query_id,
            execution_attempt_id,
            fragment_instance_id,
            writer_ordinal,
        }
    }

    /// Complete this template for the exact driver that is opening a writer.
    pub const fn for_driver(self, driver_id: u32) -> ConnectorWriterPhysicalContext {
        ConnectorWriterPhysicalContext::new(
            self.execution_query_id,
            self.execution_attempt_id,
            self.fragment_instance_id,
            driver_id,
            self.writer_ordinal,
        )
    }

    pub const fn writer_ordinal(self) -> u32 {
        self.writer_ordinal
    }
}

/// Provider requiredness is a value obligation after expression guards; it is
/// neither a schema repair nor a maskable expression-row journal entry.
pub(crate) fn validate_writer_projection_requiredness(
    schema: &arrow::datatypes::Schema,
    columns: &[arrow::array::ArrayRef],
) -> crate::runtime::fragment::ExecutionResult<()> {
    for (ordinal, (field, column)) in schema.fields().iter().zip(columns).enumerate() {
        if !field.is_nullable() && column.null_count() > 0 {
            return Err(crate::runtime::fragment::ExecutionFailure::from_domain(
                WriterRequiredValue {
                    column: ordinal,
                    row: (0..column.len()).find(|row| column.is_null(*row)),
                    field: novarocks_failure_contract::BoundedDetail::truncating(field.name()),
                },
            ));
        }
    }
    Ok(())
}
#[derive(Debug)]
struct WriterRequiredValue {
    column: usize,
    row: Option<usize>,
    field: novarocks_failure_contract::BoundedDetail,
}
impl std::fmt::Display for WriterRequiredValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "build table writer projected batch: Invalid argument error: Column '{}' (column={}, row={:?}) is declared as non-nullable but contains null values",
            self.field, self.column, self.row
        )
    }
}
impl std::error::Error for WriterRequiredValue {}
impl novarocks_failure_contract::FailureProjection for WriterRequiredValue {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            crate::runtime::fragment::ExecutionReason::WriterRequiredValue.reason(),
        )
    }
}

#[cfg(test)]
mod required_value_source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn actual_required_value_failure_keeps_its_column_and_row_without_input_owners() {
        use arrow::array::{ArrayRef, Int32Array};
        use arrow::datatypes::{Field, Schema};
        let field = Arc::new(Field::new("ResourceExhausted", DataType::Int32, false));
        let field_owner = Arc::downgrade(&field);
        let schema = Schema::new(vec![field.clone()]);
        let column: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        let column_owner = Arc::downgrade(&column);
        let failure =
            validate_writer_projection_requiredness(&schema, std::slice::from_ref(&column))
                .unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 498);
        let crate::runtime::fragment::ExecutionFailureCause::Domain(source) = failure.cause()
        else {
            panic!("writer value owner required")
        };
        let source = source.downcast_ref::<WriterRequiredValue>().unwrap();
        assert_eq!(source.column, 0);
        assert_eq!(source.row, Some(1));
        assert!(failure.detail().contains("column=0"));
        assert!(failure.detail().contains("row=Some(1)"));
        drop(column);
        drop(schema);
        drop(field);
        assert!(column_owner.upgrade().is_none());
        assert!(field_owner.upgrade().is_none());
    }
}

/// Provider and attempt capabilities moved out of the transient decoder node.
/// A pure local program never owns any of these values.
pub struct TableWriterRuntimeBinding {
    pub(crate) handle: ConnectorWriterHandle,
    pub(crate) execution: Arc<dyn ConnectorWriteExecution>,
    pub(crate) physical_template: TableWriterPhysicalContextTemplate,
    pub(crate) request_context: ConnectorRequestContext,
    pub(crate) fragment_encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
}

impl TableWriterRuntimeBinding {
    /// One Task's write capability for one compiled writer: the decoded
    /// handle, the query-leased execution of the handle's own catalog, the
    /// attempt-local physical template, the request context and the
    /// commit-fragment encoder.
    pub fn try_new(
        handle: ConnectorWriterHandle,
        execution: Arc<dyn ConnectorWriteExecution>,
        physical_template: TableWriterPhysicalContextTemplate,
        request_context: ConnectorRequestContext,
        fragment_encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
    ) -> Result<Self, ExecPlanBuildError> {
        if execution.catalog_handle() != handle.binding().catalog_handle() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Node,
                "table writer catalog handle does not match its query-leased write execution",
            ));
        }
        Ok(Self {
            handle,
            execution,
            physical_template,
            request_context,
            fragment_encoder,
            #[cfg(novarocks_dev_controls)]
            aggregate_guard: Arc::new(AllowTableWriteAggregates),
        })
    }

    /// Bind the application-owned, query-scoped aggregate rejection guard.
    #[cfg(novarocks_dev_controls)]
    pub fn with_aggregate_guard(mut self, guard: Arc<dyn TableWriteAggregateGuard>) -> Self {
        self.aggregate_guard = guard;
        self
    }

    pub const fn handle(&self) -> &ConnectorWriterHandle {
        &self.handle
    }

    pub const fn physical_template(&self) -> TableWriterPhysicalContextTemplate {
        self.physical_template
    }
}

#[cfg(test)]
mod runtime_projection_tests {

    use crate::exec::chunk::Chunk;
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::runtime::fragment::ExecutionFailure;
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    use std::sync::Arc;

    #[test]
    fn typed_writer_projection_keeps_nominal_input_through_freeze_and_last_output_exit() {
        let tracker = MemTracker::new_root("typed compiled writer projection output");
        let errors = Arc::new(RuntimeErrorState::default());
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),errors.clone(),
        ).unwrap();
        let ty =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "document",
            DataType::Utf8,
            false,
        )]));
        let (frozen, projection, input_layout) =
            crate::exec::operators::table_writer::tests::compiled_projection(
                ty.clone(),
                provider_schema,
                errors.clone(),
            );
        let mut runtime = projection.create();
        runtime.set_mem_tracker(tracker.clone());
        runtime.prepare(Some(&factory)).unwrap();
        let source_schema = factory.create_compiled_chunk_schema(&input_layout).unwrap();
        assert!(source_schema.is_typed());
        assert_eq!(source_schema.slots()[0].typed_binding(), ty.typed_binding());
        let owner = factory.begin_typed_construction(&source_schema).unwrap();
        owner
            .request(std::alloc::Layout::array::<ArrayRef>(1).unwrap())
            .unwrap();
        let input = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            source_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["{}", "[]"])) as ArrayRef],
            2,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap();
        drop(owner);
        let output = runtime.project(&input).unwrap();
        assert_eq!(
            output.chunk_schema().slots()[0].typed_binding(),
            ty.typed_binding()
        );
        assert!(
            output.chunk_schema().slots()[0]
                .field()
                .metadata()
                .as_arc()
                .is_none()
        );
        assert_eq!(
            output.columns()[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "[]"
        );
        let weak = Arc::downgrade(&factory);
        let output = output.into_batch_projection();
        drop((
            runtime,
            projection,
            frozen,
            input_layout,
            source_schema,
            input,
            factory,
            errors,
            ty,
        ));
        assert!(weak.upgrade().is_none());
        assert!(tracker.current() > 0);
        assert_eq!(output.record_batch().num_rows(), 2);
        drop(output);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn static_writer_projection_binds_its_exact_fragment_stop_owner() {
        // A real compiled slot root observes RuntimeKernelControl/Frame entry
        // checkpoints. The retired SLEEP(0) arena was only a cancellation
        // stimulus; no new pure SLEEP owner is claimed here.
        let first = Arc::new(RuntimeErrorState::default());
        let sibling = Arc::new(RuntimeErrorState::default());
        let ty = FunctionValueType::new(DataType::Boolean, false);
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Boolean,
            false,
        )]));
        let (_frozen, projection, input_layout) =
            crate::exec::operators::table_writer::tests::compiled_projection(
                ty.clone(),
                provider_schema.clone(),
                first.clone(),
            );
        let (_sibling_frozen, sibling_projection, _) =
            crate::exec::operators::table_writer::tests::compiled_projection(
                ty.clone(),
                provider_schema,
                sibling,
            );
        let tracker = MemTracker::new_root("writer projection runtime fixture");
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),Arc::new(RuntimeErrorState::default()),
        ).unwrap();
        let mut projection = projection.create();
        let mut sibling_projection = sibling_projection.create();
        projection.set_mem_tracker(tracker.clone());
        sibling_projection.set_mem_tracker(tracker);
        projection.prepare(Some(&factory)).unwrap();
        sibling_projection.prepare(Some(&factory)).unwrap();
        let batch = arrow::record_batch::RecordBatch::try_new(
            input_layout.schema().clone(),
            vec![Arc::new(BooleanArray::from(vec![true])) as ArrayRef],
        )
        .unwrap();
        let chunk = crate::exec::chunk::typed_fixture::chunk(batch, input_layout.slots(), &[ty]);
        assert!(projection.project(&chunk).is_ok());
        let cancelled = ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled);
        first.set_error(cancelled.clone());
        let actual = projection.project(&chunk).unwrap_err();
        assert_eq!(actual, cancelled);
        assert!(actual.same_occurrence(&cancelled));
        assert!(sibling_projection.project(&chunk).is_ok());
        // RuntimeBoundArena refusal to re-freeze protected only the removed
        // arena wrapper. It is not a compiled-program contract or this test's
        // fragment-stop/source property.
    }
}
