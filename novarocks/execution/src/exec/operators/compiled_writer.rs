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

//! The compiled writer family: a `TableWriter` and a `TableFinish` built from
//! one compiled LocalProgram and the Task's exact write capabilities.
//!
//! Both reuse the existing operators' writer actor, abort and activation core
//! and the finish's carrier and row-count core. What differs is where their
//! facts come from:
//!
//! * the writer's projection evaluates the program's `WriterProjection` roots,
//!   one compiled instance per root and driver, and builds each page under the
//!   provider recipe's exact input schema. There is no expression arena, no
//!   name dispatch and no `arrow::cast`: the provider-link law already proved
//!   every carrier equal. A nullable value feeding a NOT NULL provider field
//!   is the writer's row obligation, checked here before any provider I/O;
//! * both relations are the program's positional layouts, carried by its own
//!   slots rather than by the SPI's reserved relation slot IDs.
//!
//! Writer statistics run the program's prepared writer calls: the writer's
//! partial processor feeds the existing sparse `AGGREGATE_PARTIAL` packer, and
//! the finish's compiled statistics owner replaces the plan-tree final
//! aggregate and grouped Unpivot behind the finish's own coverage checks.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_local_program::{
    LocalProgram, ProgramChannelLayoutRole, ProgramExpressionRootSite, ProgramNodeId,
    ProgramNodeKind, ProgramRootInput, root_input_layout,
};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::node::table_finish::TableFinishRuntimeBinding;
use crate::exec::node::table_write_relation::{
    RootWriteResultRelationSchema, WriterMultiplexRelationSchema,
};
use crate::exec::node::table_writer::TableWriterRuntimeBinding;
use crate::exec::operators::compiled_expression::{
    RuntimeKernelControl, evaluate_chunk_all, instances,
};
use crate::exec::operators::compiled_writer_statistics::{
    CompiledFinishStatistics, CompiledWriterPartialFactory,
};
use crate::exec::operators::table_writer::{WriterPageProjection, WriterProjectionFactory};
use crate::exec::operators::{TableFinishOperatorFactory, TableWriterOperatorFactory};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::ExecutionResult;
use crate::runtime::runtime_state::RuntimeErrorState;

/// Build the compiled writer of local node `id` over the Task's exact write
/// capability.
pub(crate) fn compiled_table_writer_factory(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    display_id: i32,
    binding: &TableWriterRuntimeBinding,
    error: &Arc<RuntimeErrorState>,
) -> Result<TableWriterOperatorFactory, String> {
    let node = program
        .graph()
        .nodes()
        .get(id.index())
        .ok_or_else(|| format!("missing compiled table writer node {}", id.index()))?;
    let ProgramNodeKind::TableWriter {
        target,
        writer_multiplex_layout,
        ..
    } = node.kind()
    else {
        return Err(format!("compiled node {} is not a TableWriter", id.index()));
    };
    let partial_aggregate_factory =
        CompiledWriterPartialFactory::try_new(program, id, Arc::clone(error))?
            .map(|factory| Arc::new(factory) as Arc<dyn OperatorFactory>);
    let projection = CompiledWriterProjection::try_new(Arc::clone(program), id, Arc::clone(error))?;
    let relation =
        WriterMultiplexRelationSchema::try_from_compiled_layout(writer_multiplex_layout)?;
    if relation.chunk_schema().slot_ids() != node.output_layout().slots() {
        return Err(format!(
            "compiled table writer at local node {} does not publish its multiplex relation",
            id.index()
        ));
    }
    TableWriterOperatorFactory::try_new_compiled(
        display_id,
        *target,
        Arc::clone(&projection.schema),
        Arc::new(projection),
        relation,
        partial_aggregate_factory,
        binding,
    )
}

/// Build the compiled finish of local node `id` over the Task's exact
/// validation authority.
pub(crate) fn compiled_table_finish_factory(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    display_id: i32,
    binding: &TableFinishRuntimeBinding,
    error: &Arc<RuntimeErrorState>,
) -> Result<TableFinishOperatorFactory, String> {
    let node = program
        .graph()
        .nodes()
        .get(id.index())
        .ok_or_else(|| format!("missing compiled table finish node {}", id.index()))?;
    let ProgramNodeKind::TableFinish {
        inputs,
        expected_targets,
        writer_multiplex_layout,
        root_result_layout,
        ..
    } = node.kind()
    else {
        return Err(format!("compiled node {} is not a TableFinish", id.index()));
    };
    let [input] = inputs.as_slice() else {
        return Err(format!(
            "compiled table finish at local node {} reads more than one writer input",
            id.index()
        ));
    };
    let input_layout = program
        .graph()
        .nodes()
        .get(input.index())
        .ok_or_else(|| format!("missing compiled table finish input {}", input.index()))?
        .output_layout();
    if input_layout.slots() != writer_multiplex_layout.slots()
        || input_layout.schema() != writer_multiplex_layout.schema()
    {
        return Err(format!(
            "compiled table finish at local node {} does not read its input relation unchanged",
            id.index()
        ));
    }
    let statistics = CompiledFinishStatistics::try_new(program, id, Arc::clone(error))?.map(
        |(coverage, statistics)| {
            (
                coverage,
                statistics
                    as Arc<dyn crate::exec::operators::table_finish::FinishStatisticsFactory>,
            )
        },
    );
    TableFinishOperatorFactory::new_compiled(
        display_id,
        expected_targets.clone(),
        WriterMultiplexRelationSchema::try_from_compiled_layout(writer_multiplex_layout)?,
        RootWriteResultRelationSchema::try_from_compiled_layout(root_result_layout)?,
        statistics,
        binding,
    )
}

/// The compiled projection of one writer: one `WriterProjection` root per
/// provider input field, read over the writer's input port.
pub(crate) struct CompiledWriterProjection {
    program: Arc<LocalProgram>,
    sites: Arc<[ProgramExpressionRootSite]>,
    /// The provider recipe's exact input schema, provider nullability
    /// included.
    schema: SchemaRef,
    chunk_schema: ChunkSchemaRef,
    /// Ordinals of NOT NULL provider fields: each is a row obligation.
    obligations: Arc<[usize]>,
    error: Arc<RuntimeErrorState>,
}

impl CompiledWriterProjection {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        id: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let node = program
            .graph()
            .nodes()
            .get(id.index())
            .ok_or_else(|| format!("missing compiled table writer node {}", id.index()))?;
        let ProgramNodeKind::TableWriter {
            input, projection, ..
        } = node.kind()
        else {
            return Err(format!("compiled node {} is not a TableWriter", id.index()));
        };
        let recipe = program.write_recipes().get(&id).ok_or_else(|| {
            format!(
                "compiled table writer at local node {} has no provider write recipe",
                id.index()
            )
        })?;
        let fields = recipe
            .draft()
            .input()
            .fields_iter()
            .map(|binding| binding.field().clone())
            .collect::<Vec<_>>();
        if fields.len() != projection.expressions.len()
            || fields.len() != projection.layout.slots().len()
        {
            return Err(format!(
                "compiled table writer at local node {} projects {} values for {} provider fields",
                id.index(),
                projection.expressions.len(),
                fields.len()
            ));
        }
        let mut sites = Vec::with_capacity(fields.len());
        for ordinal in 0..fields.len() {
            let site = ProgramExpressionRootSite::WriterProjection {
                node: id,
                expression: u32::try_from(ordinal)
                    .map_err(|_| "writer projection width exceeds u32".to_string())?,
            };
            // Every projected value reads the writer's input port, the batch
            // the writer is pushed.
            match root_input_layout(program.graph(), site) {
                Ok(ProgramRootInput::Layout {
                    node,
                    role: ProgramChannelLayoutRole::NodeOutput,
                }) if node == *input => {}
                other => {
                    return Err(format!(
                        "compiled writer projection {ordinal} at local node {} does not read its input port: {other:?}",
                        id.index()
                    ));
                }
            }
            sites.push(site);
        }
        let obligations = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| !field.is_nullable())
            .map(|(ordinal, _)| ordinal)
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, projection.layout.slots())?;
        Ok(Self {
            program,
            sites: Arc::from(sites),
            schema,
            chunk_schema,
            obligations: Arc::from(obligations),
            error,
        })
    }
}

impl WriterProjectionFactory for CompiledWriterProjection {
    fn create(&self) -> Box<dyn WriterPageProjection> {
        Box::new(CompiledWriterProjector {
            program: Arc::clone(&self.program),
            sites: Arc::clone(&self.sites),
            schema: Arc::clone(&self.schema),
            chunk_schema: Arc::clone(&self.chunk_schema),
            obligations: Arc::clone(&self.obligations),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
        })
    }
}

/// One driver's projection: its own compiled instance per root.
struct CompiledWriterProjector {
    program: Arc<LocalProgram>,
    sites: Arc<[ProgramExpressionRootSite]>,
    schema: SchemaRef,
    chunk_schema: ChunkSchemaRef,
    obligations: Arc<[usize]>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
}

#[cfg(test)]
pub(crate) fn source_loan_test_projector(
    program: Arc<LocalProgram>,
    observer: Option<Arc<crate::exec::expr::compiled_program::input_source::SlotSourceObserver>>,
) -> Box<dyn WriterPageProjection> {
    use novarocks_local_program::ProgramNodeExpressionRole;
    // Reuse the original checked Slot fixture, through the actual projector's
    // project() method. This fixture tests input borrowing and handoff, not
    // provider recipe construction or WriterProjection root admission.
    let site = ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(3),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 1 },
    };
    let field = program.graph().nodes()[2]
        .output_layout()
        .schema()
        .field(1)
        .clone();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[novarocks_types::SlotId(100)])
            .unwrap();
    let control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    let mut slots = None;
    instances(&mut slots, &program, &[site], &control).unwrap();
    if let Some(observer) = observer {
        slots.as_mut().unwrap()[0].observe_slot_source(observer);
    }
    Box::new(CompiledWriterProjector {
        program,
        sites: Arc::from([site]),
        schema,
        chunk_schema,
        obligations: Arc::from([]),
        control,
        instances: slots,
    })
}

impl WriterPageProjection for CompiledWriterProjector {
    fn bind_runtime_state(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.control.bind_mem_tracker(tracker);
    }
    fn project(&mut self, chunk: &Chunk) -> ExecutionResult<Chunk> {
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.sites.len());
        for (instance, site) in instances.iter_mut().zip(self.sites.iter()) {
            columns.push(evaluate_chunk_all(instance, *site, chunk, &self.control)?);
        }
        // The row obligation: a NOT NULL provider field fed a NULL refuses the
        // page before any provider write, at the same failure point and with
        // the same text as the projection-built page of the plan-tree writer.
        for &ordinal in self.obligations.iter() {
            if columns[ordinal].null_count() > 0 {
                return Err(format!(
                    "build table writer projected batch: Invalid argument error: Column '{}' is declared as non-nullable but contains null values",
                    self.schema.field(ordinal).name()
                )
                .into());
            }
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|error| format!("build table writer projected batch: {error}"))?;
        Ok(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.chunk_schema),
        )?)
    }
}
