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
//!   one compiled instance per root and driver, and builds each page under its
//!   complete execution projection. There is no expression arena, no
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
use arrow::datatypes::SchemaRef;
use novarocks_local_program::{
    LocalProgram, ProgramChannelLayoutRole, ProgramExpressionRootSite, ProgramNodeId,
    ProgramNodeKind, ProgramRootInput, root_input_layout,
};

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::node::table_finish::TableFinishRuntimeBinding;
use crate::exec::node::table_write_relation::{
    RootWriteResultRelationTemplate, WriterMultiplexRelationTemplate,
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
    let relation = WriterMultiplexRelationTemplate::try_new(writer_multiplex_layout)?;
    if relation.slots() != node.output_layout().slots() {
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
        WriterMultiplexRelationTemplate::try_new(writer_multiplex_layout)?,
        RootWriteResultRelationTemplate::try_new(root_result_layout)?,
        statistics,
        binding,
    )
}

/// The compiled projection of one writer: one `WriterProjection` root per
/// provider input field, read over the writer's input port.
pub(crate) struct CompiledWriterProjection {
    program: Arc<LocalProgram>,
    sites: Arc<[ProgramExpressionRootSite]>,
    /// The complete execution projection. Provider requiredness remains an
    /// independent per-row obligation below.
    schema: SchemaRef,
    layout: novarocks_local_program::StaticLayout,
    /// Ordinals of NOT NULL provider fields: each is a row obligation.
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
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let layout = projection.layout.clone();
        Ok(Self {
            program,
            sites: Arc::from(sites),
            schema,
            layout,
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
            layout: self.layout.clone(),
            chunk_schema: None,
            construction: None,
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
    layout: novarocks_local_program::StaticLayout,
    chunk_schema: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
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
    let input = program.graph().nodes()[2].output_layout();
    let field = Arc::clone(&input.schema().fields()[1]);
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
    let value_type = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
        input
            .value_binding_at(1)
            .expect("original Slot declaration")
            .clone(),
    )
    .unwrap();
    let layout = crate::exec::chunk::typed_fixture::layout(
        schema.clone(),
        &[novarocks_types::SlotId(100)],
        &[value_type],
    );
    let tracker =
        crate::runtime::mem_tracker::MemTracker::new_root("writer source-loan construction");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    let mut control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    control.bind_mem_tracker(tracker.clone());
    let mut slots = None;
    instances(&mut slots, &program, &[site], &control).unwrap();
    if let Some(observer) = observer {
        slots.as_mut().unwrap()[0].observe_slot_source(observer);
    }
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker,
        Arc::new(RuntimeErrorState::default()),
    ).unwrap();
    let mut projector = CompiledWriterProjector {
        program,
        sites: Arc::from([site]),
        schema,
        layout,
        chunk_schema: None,
        construction: None,
        control,
        instances: slots,
    };
    projector.prepare(Some(&factory)).unwrap();
    Box::new(projector)
}

impl WriterPageProjection for CompiledWriterProjector {
    fn prepare(
        &mut self,
        factory: Option<&Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    ) -> ExecutionResult<()> {
        let factory =
            factory.ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        self.chunk_schema = Some(factory.create_compiled_chunk_schema(&self.layout)?);
        self.construction = Some(factory.clone());
        Ok(())
    }
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
        use crate::exec::pipeline::operator::ConstructionFailure;
        let schema = self
            .chunk_schema
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_typed_construction(schema)?;
        owner.request(std::alloc::Layout::array::<ArrayRef>(self.sites.len())
            .map_err(|_| ConstructionFailure::from(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.sites.len());
        for (instance, site) in instances.iter_mut().zip(self.sites.iter()) {
            columns.push(evaluate_chunk_all(instance, *site, chunk, &self.control)?);
        }
        crate::exec::node::table_writer::validate_writer_projection_requiredness(
            &self.schema,
            &columns,
        )?;
        let rows = columns.first().map_or(chunk.len(), |array| array.len());
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ConstructionFailure,
            _,
        >(
            Arc::clone(schema),
            columns,
            rows,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?)
    }
}
