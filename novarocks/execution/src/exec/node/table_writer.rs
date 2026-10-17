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

//! The `TableWriter` plan node.
//!
//! `TableWriter` is an ordinary unary processor *with output*, not a terminal
//! sink. Every pipeline driver opens, owns, appends to, and finishes or aborts
//! its own [`ConnectorBatchWriter`]. Nothing about the writer is shared between
//! drivers, so this node carries no driver count, no writer mutex, and no
//! "last driver finishes" protocol: only an immutable logical recipe plus the
//! attempt-local facts a driver needs to build its own physical context.

use std::sync::Arc;

use arrow::compute::cast;
use arrow::datatypes::{DataType, SchemaRef};

use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::write_stack::{
    ConnectorWriteExecution, ConnectorWriterHandle, ConnectorWriterPhysicalContext,
    WriteTargetOrdinal,
};

#[cfg(test)]
use crate::exec::chunk::ChunkSchema;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprId, cast_with_special_rules};
use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::exec::node::ExecNode;
use crate::exec::node::table_write_aggregate::WriterPartialAggregatePlan;
use crate::exec::node::table_write_relation::ConnectorCommitFragmentEncoder;
#[cfg(novarocks_dev_controls)]
use crate::exec::node::table_write_relation::{
    AllowTableWriteAggregates, TableWriteAggregateGuard,
};
#[cfg(test)]
use novarocks_types::SlotId;

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

/// Expression projection from the writer's input chunk onto the exact Arrow
/// schema the provider writer expects.
///
/// This mirrors the pre-NCP-6 sink projection instead of borrowing it, so the
/// old terminal-sink path stays independently deletable.
#[derive(Clone)]
pub struct TableWriterInputProjection {
    arena: ExprArena,
    exprs: Vec<ExprId>,
    schema: SchemaRef,
    chunk_schema: Option<ChunkSchemaRef>,
    provider_chunk_schema: Option<ChunkSchemaRef>,
    _provider_layout: Option<novarocks_local_program::StaticLayout>,
    static_layout: Option<novarocks_local_program::StaticLayout>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    evaluation_control: Option<crate::exec::operators::compiled_expression::RuntimeKernelControl>,
}

impl TableWriterInputProjection {
    pub(crate) fn from_static(
        projection: &novarocks_local_program::StaticWriterProjection,
        provider_layout: &novarocks_local_program::StaticLayout,
        runtime_error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    ) -> Result<Self, String> {
        let mut arena = ExprArena::from_immutable(&projection.arena)?;
        arena.bind_runtime_error(runtime_error.clone());
        let exprs: Vec<ExprId> = projection
            .expressions
            .iter()
            .map(|id| ExprId(id.index()))
            .collect();
        if !projection.layout.is_typed() || !provider_layout.is_typed() {
            return Err("writer projection lacks its complete static bindings".into());
        }
        Self::validate_expressions(&arena, &exprs, provider_layout.schema())
            .map_err(|error| error.to_string())?;
        Ok(Self {
            arena,
            exprs,
            schema: Arc::clone(provider_layout.schema()),
            chunk_schema: None,
            provider_chunk_schema: None,
            _provider_layout: Some(provider_layout.clone()),
            static_layout: Some(projection.layout.clone()),
            construction: None,
            evaluation_control: Some(
                crate::exec::operators::compiled_expression::RuntimeKernelControl::new(
                    runtime_error,
                ),
            ),
        })
    }

    #[cfg(test)]
    pub fn try_new(
        arena: ExprArena,
        exprs: Vec<ExprId>,
        schema: SchemaRef,
    ) -> Result<Self, ExecPlanBuildError> {
        Self::validate_expressions(&arena, &exprs, &schema)?;
        let slot_ids = (0..schema.fields().len())
            .map(|ordinal| {
                ordinal
                    .checked_add(1)
                    .and_then(|slot| u32::try_from(slot).ok())
                    .map(SlotId::new)
                    .ok_or_else(|| {
                        ExecPlanBuildError::new(
                            ExecPlanInvariant::Schema,
                            "table writer projected input slot ID overflowed",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &slot_ids)
                .map_err(|error| ExecPlanBuildError::new(ExecPlanInvariant::Schema, error))?;
        Ok(Self {
            arena,
            exprs,
            schema,
            provider_chunk_schema: Some(chunk_schema.clone()),
            chunk_schema: Some(chunk_schema),
            _provider_layout: None,
            static_layout: None,
            construction: None,
            evaluation_control: None,
        })
    }

    fn validate_expressions(
        arena: &ExprArena,
        exprs: &[ExprId],
        schema: &SchemaRef,
    ) -> Result<(), ExecPlanBuildError> {
        if exprs.is_empty() || exprs.len() != schema.fields().len() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Node,
                "table writer expression projection does not match its output schema",
            ));
        }
        if let Some(expr_id) = exprs.iter().find(|expr_id| arena.node(**expr_id).is_none()) {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Expression,
                format!(
                    "table writer input expression id {} is missing from its arena",
                    expr_id.0
                ),
            ));
        }
        Ok(())
    }

    /// The decoder supplies its already-created complete execution schema.
    /// No type or logical domain is reconstructed from presentation fields.
    pub fn try_new_typed(
        arena: ExprArena,
        exprs: Vec<ExprId>,
        provider_chunk_schema: ChunkSchemaRef,
        chunk_schema: ChunkSchemaRef,
    ) -> Result<Self, ExecPlanBuildError> {
        if !chunk_schema.is_typed()
            || !provider_chunk_schema.is_typed()
            || chunk_schema.slot_ids() != provider_chunk_schema.slot_ids()
        {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Schema,
                "writer projection requires its complete execution bindings",
            ));
        }
        let schema = provider_chunk_schema.arrow_schema_ref();
        Self::validate_expressions(&arena, &exprs, &schema)?;
        Ok(Self {
            arena,
            exprs,
            schema,
            chunk_schema: Some(chunk_schema),
            provider_chunk_schema: Some(provider_chunk_schema),
            _provider_layout: None,
            static_layout: None,
            construction: None,
            evaluation_control: None,
        })
    }

    pub(crate) fn prepare_created(
        &mut self,
        factory: Option<&Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        if let Some(layout) = self.static_layout.as_ref() {
            let factory = factory.ok_or(ConstructionFailure::MissingOwner)?;
            self.chunk_schema = Some(factory.create_compiled_chunk_schema(layout)?);
            self.static_layout = None;
        }
        if self.chunk_schema().is_typed() {
            self.construction = Some(factory.ok_or(ConstructionFailure::MissingOwner)?.clone());
        }
        Ok(())
    }

    pub(crate) fn bind_evaluation_runtime_state(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) {
        let mut control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(
            state.error_state(),
        );
        control.bind_runtime_memory(state);
        if let Some(factory) = &self.construction {
            control.bind_construction_factory(factory.clone());
        }
        self.evaluation_control = Some(control);
    }

    pub(crate) fn project_created(
        &self,
        chunk: &Chunk,
    ) -> crate::runtime::fragment::ExecutionResult<Chunk> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        let schema = self.chunk_schema();
        if !schema.is_typed() {
            #[cfg(test)]
            return Ok(self.project(chunk)?);
            #[cfg(not(test))]
            return Err(ConstructionFailure::InvalidSource(
                "writer projection lacks its complete execution bindings".into(),
            )
            .into());
        }
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_typed_construction(schema)?;
        let layout = std::alloc::Layout::array::<arrow::array::ArrayRef>(self.exprs.len())
            .map_err(|_| ConstructionFailure::from(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?;
        owner.request(layout)?;
        let control = self
            .evaluation_control
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let evaluation =
            crate::exec::expr::ExprEvaluationContext::new(&self.arena, owner.as_ref(), control);
        let columns = self.project_columns(&evaluation, chunk)?;
        validate_writer_projection_requiredness(&self.schema, &columns)?;
        let rows = columns.first().map_or(chunk.len(), |array| array.len());
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ConstructionFailure,
            _,
        >(
            schema.clone(),
            columns,
            rows,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?)
    }

    pub const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn arena_mut(&mut self) -> &mut ExprArena {
        &mut self.arena
    }

    pub fn chunk_schema(&self) -> &ChunkSchemaRef {
        self.chunk_schema
            .as_ref()
            .expect("writer projection schema is prepared")
    }

    pub(crate) fn provider_chunk_schema(&self) -> &ChunkSchemaRef {
        self.provider_chunk_schema
            .as_ref()
            .expect("decoded provider schema is present")
    }

    /// Consume decoder-owned expression backing when freezing the local
    /// program. No writer or request capability crosses this boundary.
    pub(crate) fn into_static(
        self,
        layout: novarocks_local_program::StaticLayout,
    ) -> Result<novarocks_local_program::StaticWriterProjection, String> {
        Ok(novarocks_local_program::StaticWriterProjection {
            arena: Arc::new(
                self.arena
                    .into_immutable()
                    .map_err(|error| error.to_string())?,
            ),
            expressions: self
                .exprs
                .into_iter()
                .map(|id| novarocks_local_program::ProgramExprId::new(id.0))
                .collect(),
            layout,
        })
    }

    /// Evaluate the projection once and expose the exact same target-typed
    /// Arrow buffers to both the connector writer and embedded aggregates.
    #[cfg(test)]
    pub fn project(&self, chunk: &Chunk) -> Result<Chunk, String> {
        if self.chunk_schema().is_typed() {
            return Err("typed writer projection requires its actual construction owner".into());
        }
        let arrays = self.project_columns(&self.arena, chunk)?;
        let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|error| format!("build table writer projected batch: {error}"))?;
        Chunk::try_new_with_chunk_schema(batch, Arc::clone(self.chunk_schema()))
            .map_err(|error| format!("build table writer projected chunk: {error}"))
    }
    fn project_columns<E: crate::exec::expr::ExprEvaluation + ?Sized>(
        &self,
        evaluation: &E,
        chunk: &Chunk,
    ) -> Result<Vec<arrow::array::ArrayRef>, E::Error> {
        // Preserve the original ordering: evaluate every expression, then cast
        // each result in ordinal order. Reuse this actual output Vec rather
        // than creating a second container whose lifetime could be ambiguous.
        let mut arrays = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            arrays.push(evaluation.eval(*expr, chunk)?);
        }
        for (index, (array, field)) in arrays.iter_mut().zip(self.schema.fields()).enumerate() {
            if array.data_type() == field.data_type() {
                continue;
            }
            let casted = if matches!(field.data_type(), DataType::FixedSizeBinary(width)
                if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
            {
                cast_with_special_rules(array, field.data_type())
            } else {
                cast(array.as_ref(), field.data_type()).map_err(|error| error.to_string())
            };
            *array = casted.map_err(|error| format!(
                "table writer projection cast failed at column {index} from {:?} to {:?}: {error}",
                array.data_type(), field.data_type(),
            ))?;
        }
        Ok(arrays)
    }
}

pub(crate) fn validate_writer_projection_requiredness(
    schema: &arrow::datatypes::Schema,
    columns: &[arrow::array::ArrayRef],
) -> Result<(), String> {
    for (field, column) in schema.fields().iter().zip(columns) {
        if !field.is_nullable() && column.null_count() > 0 {
            return Err(format!(
                "build table writer projected batch: Invalid argument error: Column '{}' is declared as non-nullable but contains null values",
                field.name(),
            ));
        }
    }
    Ok(())
}

impl std::fmt::Debug for TableWriterInputProjection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TableWriterInputProjection")
            .field("columns", &self.exprs.len())
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// A normal unary processor that writes its input and reports what it wrote.
#[derive(Clone)]
pub struct TableWriterNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    handle: ConnectorWriterHandle,
    target: WriteTargetOrdinal,
    execution: Arc<dyn ConnectorWriteExecution>,
    expected_schema: SchemaRef,
    projection: TableWriterInputProjection,
    physical_template: TableWriterPhysicalContextTemplate,
    request_context: ConnectorRequestContext,
    fragment_encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
    writer_multiplex_schema: crate::exec::node::table_write_relation::WriterMultiplexRelationSchema,
    partial_aggregate_plan: WriterPartialAggregatePlan,
    #[cfg(novarocks_dev_controls)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
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

impl TableWriterNode {
    #[expect(
        clippy::too_many_arguments,
        reason = "A table writer joins independently-owned plan, provider, attempt, and codec facts."
    )]
    pub fn try_new(
        input: Box<ExecNode>,
        node_id: i32,
        handle: ConnectorWriterHandle,
        target: WriteTargetOrdinal,
        execution: Arc<dyn ConnectorWriteExecution>,
        expected_schema: SchemaRef,
        projection: TableWriterInputProjection,
        physical_template: TableWriterPhysicalContextTemplate,
        request_context: ConnectorRequestContext,
        fragment_encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
    ) -> Result<Self, ExecPlanBuildError> {
        Self::try_new_with_relation(
            input,
            node_id,
            handle,
            target,
            execution,
            expected_schema,
            projection,
            physical_template,
            request_context,
            fragment_encoder,
            crate::exec::node::table_write_relation::WriterMultiplexRelationSchema::empty(),
            WriterPartialAggregatePlan::default(),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "A table writer joins independently-owned plan, provider, attempt, codec, and relation facts."
    )]
    pub fn try_new_with_relation(
        input: Box<ExecNode>,
        node_id: i32,
        handle: ConnectorWriterHandle,
        target: WriteTargetOrdinal,
        execution: Arc<dyn ConnectorWriteExecution>,
        expected_schema: SchemaRef,
        projection: TableWriterInputProjection,
        physical_template: TableWriterPhysicalContextTemplate,
        request_context: ConnectorRequestContext,
        fragment_encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
        writer_multiplex_schema: crate::exec::node::table_write_relation::WriterMultiplexRelationSchema,
        partial_aggregate_plan: WriterPartialAggregatePlan,
    ) -> Result<Self, ExecPlanBuildError> {
        if execution.catalog_handle() != handle.binding().catalog_handle() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Node,
                "table writer catalog handle does not match its query-leased write execution",
            ));
        }
        if projection.schema().as_ref() != expected_schema.as_ref() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Schema,
                "table writer input projection does not produce the expected writer schema",
            ));
        }
        Ok(Self {
            input,
            node_id,
            handle,
            target,
            execution,
            expected_schema,
            projection,
            physical_template,
            request_context,
            fragment_encoder,
            writer_multiplex_schema,
            partial_aggregate_plan,
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

    pub const fn target(&self) -> WriteTargetOrdinal {
        self.target
    }

    pub const fn execution(&self) -> &Arc<dyn ConnectorWriteExecution> {
        &self.execution
    }

    pub const fn expected_schema(&self) -> &SchemaRef {
        &self.expected_schema
    }

    pub const fn projection(&self) -> &TableWriterInputProjection {
        &self.projection
    }

    /// Decoder-only patch point for the node-owned expression arena.
    pub fn projection_arena_mut(&mut self) -> &mut ExprArena {
        self.projection.arena_mut()
    }

    pub const fn physical_template(&self) -> TableWriterPhysicalContextTemplate {
        self.physical_template
    }

    pub const fn request_context(&self) -> &ConnectorRequestContext {
        &self.request_context
    }

    pub const fn fragment_encoder(&self) -> &Arc<dyn ConnectorCommitFragmentEncoder> {
        &self.fragment_encoder
    }

    pub const fn writer_multiplex_schema(
        &self,
    ) -> &crate::exec::node::table_write_relation::WriterMultiplexRelationSchema {
        &self.writer_multiplex_schema
    }

    pub const fn partial_aggregate_plan(&self) -> &WriterPartialAggregatePlan {
        &self.partial_aggregate_plan
    }

    pub(crate) fn into_static_parts_with_binding(
        self,
    ) -> (
        TableWriterRuntimeBinding,
        (
            Box<ExecNode>,
            WriteTargetOrdinal,
            SchemaRef,
            TableWriterInputProjection,
            crate::exec::node::table_write_relation::WriterMultiplexRelationSchema,
            WriterPartialAggregatePlan,
        ),
    ) {
        (
            TableWriterRuntimeBinding {
                handle: self.handle,
                execution: self.execution,
                physical_template: self.physical_template,
                request_context: self.request_context,
                fragment_encoder: self.fragment_encoder,
                #[cfg(novarocks_dev_controls)]
                aggregate_guard: self.aggregate_guard,
            },
            (
                self.input,
                self.target,
                self.expected_schema,
                self.projection,
                self.writer_multiplex_schema,
                self.partial_aggregate_plan,
            ),
        )
    }

    #[cfg(novarocks_dev_controls)]
    pub const fn aggregate_guard(&self) -> &Arc<dyn TableWriteAggregateGuard> {
        &self.aggregate_guard
    }
}

impl std::fmt::Debug for TableWriterNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TableWriterNode")
            .field("node_id", &self.node_id)
            .field("target", &self.target)
            .field("handle", &self.handle)
            .field("expected_schema", &self.expected_schema)
            .field("physical_template", &self.physical_template)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod runtime_projection_tests {
    use super::*;
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprNode, LiteralValue};
    use crate::runtime::runtime_state::RuntimeErrorState;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn created_projection_layout(schema: SchemaRef) -> novarocks_local_program::StaticLayout {
        use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
        use crate::runtime::mem_tracker::MemTracker;
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        use novarocks_type_contract::{FunctionValueType, TypedValueBinding};
        use std::alloc::Layout;

        let ty = FunctionValueType::new(DataType::Boolean, false);
        let declared = crate::exec::chunk::typed_fixture::layout(
            Arc::clone(&schema),
            &[SlotId::new(1)],
            &[ty.clone()],
        );
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            MemTracker::new_root("writer projection fixture layout"),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let owner = factory
            .begin_compiled_layout_construction(&declared)
            .unwrap();
        owner
            .request(arc_layout(Layout::array::<SlotId>(1).unwrap()).unwrap())
            .unwrap();
        let slots = Arc::from([SlotId::new(1)]);
        owner
            .request(Layout::array::<(TypedValueBinding, Option<i32>)>(1).unwrap())
            .unwrap();
        let bindings = vec![(ty.typed_binding().clone(), None)];
        novarocks_local_program::StaticLayout::try_new_typed_with_creator(
            schema,
            slots,
            bindings,
            owner.storage_lifetime(),
            |request| {
                owner
                    .request(request)
                    .map_err(novarocks_local_program::LayoutCreationFailure::Creator)
            },
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
    }

    #[test]
    fn typed_writer_projection_keeps_nominal_input_through_freeze_and_last_output_exit() {
        use crate::exec::pipeline::operator::ConstructionFailure;
        use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
        use crate::runtime::mem_tracker::MemTracker;
        use arrow::array::{ArrayRef, StringArray};
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        let tracker = MemTracker::new_root("typed writer projection output");
        let errors = Arc::new(RuntimeErrorState::default());
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            errors.clone(),
        )
        .unwrap();
        let ty =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let layout = crate::exec::chunk::typed_fixture::layout(
            Arc::new(Schema::new(vec![Field::new(
                "document",
                DataType::Utf8,
                false,
            )])),
            &[SlotId::new(1)],
            &[ty.clone()],
        );
        let source_schema = factory.create_compiled_chunk_schema(&layout).unwrap();
        let mut arena = ExprArena::default();
        let expr = arena.push_value_typed(ExprNode::SlotId(SlotId::new(1)), ty.clone());
        let decoded = TableWriterInputProjection::try_new_typed(
            arena,
            vec![expr],
            source_schema.clone(),
            source_schema.clone(),
        )
        .unwrap();
        assert!(decoded.chunk_schema().is_typed());
        assert_eq!(
            decoded.chunk_schema().slots()[0].typed_binding(),
            ty.typed_binding()
        );
        let frozen = decoded.into_static(layout.clone()).unwrap();
        let mut runtime =
            TableWriterInputProjection::from_static(&frozen, &frozen.layout, errors.clone())
                .unwrap();
        runtime.prepare_created(Some(&factory)).unwrap();
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
        let output = runtime.project_created(&input).unwrap();
        assert_eq!(
            output.chunk_schema().slots()[0].typed_binding(),
            ty.typed_binding()
        );
        assert_eq!(
            output.chunk_schema().slots()[0]
                .field()
                .metadata()
                .capacity(),
            0
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
            frozen,
            layout,
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
        let mut arena = ExprArena::default();
        let seconds = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(0)), DataType::Int64);
        let sleep = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Object("sleep"),
                args: vec![seconds],
            },
            DataType::Boolean,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "sleep",
            DataType::Boolean,
            false,
        )]));
        let layout = created_projection_layout(Arc::clone(&schema));
        let frozen = TableWriterInputProjection::try_new(arena, vec![sleep], schema)
            .unwrap()
            .into_static(layout)
            .unwrap();
        let first = Arc::new(RuntimeErrorState::default());
        let sibling = Arc::new(RuntimeErrorState::default());
        let mut projection =
            TableWriterInputProjection::from_static(&frozen, &frozen.layout, Arc::clone(&first))
                .unwrap();
        let mut sibling_projection =
            TableWriterInputProjection::from_static(&frozen, &frozen.layout, sibling).unwrap();
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("writer projection runtime fixture"),
            Arc::new(RuntimeErrorState::default()),
        ).unwrap();
        projection.prepare_created(Some(&factory)).unwrap();
        sibling_projection.prepare_created(Some(&factory)).unwrap();
        let input_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&input_schema, &[SlotId::new(1)])
                .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        assert!(projection.project_created(&chunk).is_ok());
        first.set_error("writer fragment cancelled".to_string());
        assert_eq!(
            projection.project_created(&chunk).unwrap_err(),
            crate::runtime::fragment::ExecutionFailure::from(
                "writer fragment cancelled".to_string()
            )
        );
        assert!(sibling_projection.project_created(&chunk).is_ok());
        assert!(
            projection
                .into_static(frozen.layout.clone())
                .unwrap_err()
                .contains("RuntimeBoundArena")
        );
    }
}
