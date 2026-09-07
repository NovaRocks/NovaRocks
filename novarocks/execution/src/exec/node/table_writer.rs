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

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprId, cast_with_special_rules};
use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::exec::node::ExecNode;
use crate::exec::node::table_write_aggregate::WriterPartialAggregatePlan;
use crate::exec::node::table_write_relation::ConnectorCommitFragmentEncoder;
#[cfg(debug_assertions)]
use crate::exec::node::table_write_relation::{
    AllowTableWriteAggregates, TableWriteAggregateGuard,
};
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
    chunk_schema: ChunkSchemaRef,
}

impl TableWriterInputProjection {
    pub fn try_new(
        arena: ExprArena,
        exprs: Vec<ExprId>,
        schema: SchemaRef,
    ) -> Result<Self, ExecPlanBuildError> {
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
            chunk_schema,
        })
    }

    pub const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn arena_mut(&mut self) -> &mut ExprArena {
        &mut self.arena
    }

    pub const fn chunk_schema(&self) -> &ChunkSchemaRef {
        &self.chunk_schema
    }

    /// Evaluate the projection once and expose the exact same target-typed
    /// Arrow buffers to both the connector writer and embedded aggregates.
    pub fn project(&self, chunk: &Chunk) -> Result<Chunk, String> {
        let arrays = self
            .exprs
            .iter()
            .map(|expr| self.arena.eval(*expr, chunk))
            .collect::<Result<Vec<_>, _>>()?;
        let arrays = arrays
            .into_iter()
            .zip(self.schema.fields())
            .enumerate()
            .map(|(index, (array, field))| {
                if array.data_type() == field.data_type() {
                    return Ok(array);
                }
                let casted = if matches!(
                    field.data_type(),
                    DataType::FixedSizeBinary(width)
                        if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH
                ) {
                    cast_with_special_rules(&array, field.data_type())
                } else {
                    cast(array.as_ref(), field.data_type()).map_err(|error| error.to_string())
                };
                casted.map_err(|error| {
                    format!(
                        "table writer projection cast failed at column {index} from {:?} to {:?}: {error}",
                        array.data_type(),
                        field.data_type()
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|error| format!("build table writer projected batch: {error}"))?;
        Chunk::try_new_with_chunk_schema(batch, Arc::clone(&self.chunk_schema))
            .map_err(|error| format!("build table writer projected chunk: {error}"))
    }
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
    #[cfg(debug_assertions)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
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
            #[cfg(debug_assertions)]
            aggregate_guard: Arc::new(AllowTableWriteAggregates),
        })
    }

    /// Bind the application-owned, query-scoped aggregate rejection guard.
    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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
