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

//! Single-fragment fixtures for compiled node families: a physical fragment
//! authored with the real builder, published as a checked package, compiled by
//! local-compiler and run through `prepare_compiled_program_pipeline_execution`.
//! Every expression root in these fixtures is a leaf (literal or value read),
//! so each root gets one eager use in one evaluation domain.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::ConstantPolicy;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_physical_plan::{
    ConstantPools, ExprKind, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage,
    FragmentPackageAdmission, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, LiteralValue, NodeId, PhysicalExpressionRoots, PhysicalRootUses,
    PipelineDopDomain, PlanLimits, PlanVersionId, PropertyProofProjectionLimits, RequiredContracts,
    ResultField, ResultPort, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, PureCompileControl, SemanticParameters,
};

use crate::exec::chunk::{Chunk, ChunkSchema, OutputColumnMetadata};
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::runtime::fragment::io::NoopFragmentEventSink;
use crate::runtime::runtime_state::RuntimeState;

pub(super) struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

impl novarocks_functions::KernelEvaluationControl for FixtureControl {
    fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), novarocks_functions::KernelFailure> {
        panic!("column metadata fixture never waits")
    }
}

/// A declared constant input with an admitted original columns Vec. Only
/// container metadata is paid here; this does not grant payload authority.
pub(super) fn paid_input(
    program: &LocalProgram,
    tracker: &Arc<crate::runtime::mem_tracker::MemTracker>,
) -> Chunk {
    let node = &program.graph().nodes()[0];
    let novarocks_local_program::ProgramNodeKind::Values { values } = node.kind() else {
        panic!("fixture input is not Values");
    };
    let source = values.batch().expect("fixture Values are constant");
    let schema = ChunkSchema::from_compiled_layout(node.output_layout()).unwrap();
    let metadata = OutputColumnMetadata::try_new(
        source.num_columns(),
        crate::exec::operators::compiled_aggregate::expression_allocation_host(tracker.clone()),
        &FixtureControl,
    )
    .unwrap();
    let mut columns = Vec::new();
    columns.try_reserve_exact(source.num_columns()).unwrap();
    columns.extend(source.columns().iter().cloned());
    let batch =
        arrow::record_batch::RecordBatch::try_new(schema.arrow_schema_ref(), columns).unwrap();
    Chunk::try_new_with_output_column_metadata(batch, schema, metadata).unwrap()
}

pub(super) fn int64(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}

pub(super) fn boolean(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Boolean, nullable)
}

/// One nullable or non-null Int64 literal cell.
pub(super) fn cell(value: Option<i64>) -> LiteralValue {
    value.map_or(LiteralValue::Null, LiteralValue::Int64)
}

/// A Values leaf at `node` whose columns have the given complete types and
/// whose rows are literal cells of exactly those types.
pub(super) fn values(
    builder: &mut FragmentBuilder,
    node: NodeId,
    types: &[FunctionValueType],
    rows: &[Vec<LiteralValue>],
) -> Vec<ValueId> {
    let columns = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: u32::try_from(ordinal).unwrap(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let cells = rows
        .iter()
        .map(|row| {
            assert_eq!(row.len(), types.len());
            row.iter()
                .zip(types)
                .map(|(literal, ty)| {
                    builder
                        .add_expression(node, ty.clone(), ExprKind::Literal(literal.clone()))
                        .unwrap()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        })
        .collect::<Vec<_>>();
    builder
        .add_values(
            node,
            cells.into_boxed_slice(),
            columns.clone().into_boxed_slice(),
        )
        .unwrap();
    columns
}

// Explicit small-fixture admission; these are test inputs, not defaults.
fn admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

/// Publish the fragment rooted at `root` with a Result sink. Every root is a
/// leaf and gets one eager use; there is no function call to freeze.
pub(super) fn package(
    builder: FragmentBuilder,
    root: NodeId,
    constants: ConstantPools,
    max_dop: u32,
) -> Arc<FragmentPackage> {
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: max_dop,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = ExpressionUseId::new(u32::try_from(ordinal).unwrap());
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain,
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &FixtureControl).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &FixtureControl).unwrap();
    let output = fragment.nodes()[&root].output.clone();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: output.clone(),
        fields: output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: crate::test_result_domain::result_value_domain(
                    &fragment.values()[value].ty,
                ),
                name: format!("c{ordinal}").into_boxed_str(),
                alias: None,
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let id: FragmentId = fragment.id();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                constants,
                version: PlanVersionId::try_new([91; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment,
                expression_uses: uses,
                calls,
                pruning: FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            admission(),
            &FixtureControl,
        )
        .unwrap(),
    )
}

/// Explicit fixture constant admission; these values are not production
/// defaults. Constant pools authored by a test use the same policy.
pub(super) fn constant_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}

fn options(dop: usize) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(dop).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: constant_policy(),
        exchange_wait: Duration::from_secs(120),
    }
}

/// Compile with the real local compiler. The catalog is the sealed RAND
/// subset because an empty catalog is refused; these fixtures bind no call.
pub(super) fn try_compile(
    package: Arc<FragmentPackage>,
    dop: usize,
) -> Result<Arc<LocalProgram>, String> {
    let functions = crate::exec::expr::compiled_program::tests::rng_subset();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated = validate_fragment_providers(package, &providers, &FixtureControl).unwrap();
    compile_fragment(validated, &functions, options(dop), &FixtureControl)
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

pub(super) fn compile(package: Arc<FragmentPackage>, dop: usize) -> Arc<LocalProgram> {
    try_compile(package, dop).unwrap_or_else(|error| panic!("fixture fragment compiles: {error}"))
}

/// Prepare and run the program with its frozen driver count; a preparation
/// or execution failure is returned as text.
pub(super) fn try_run(program: &Arc<LocalProgram>) -> Result<Vec<Chunk>, String> {
    try_run_in(program, None)
}

/// A declared original query tracker for resource-owning component fixtures.
/// Other family fixtures keep their original untracked inputs unchanged.
pub(super) fn run_with_tracker(
    program: &Arc<LocalProgram>,
    tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
) -> Vec<Chunk> {
    try_run_in(program, Some(tracker))
        .unwrap_or_else(|error| panic!("tracked compiled program runs: {error}"))
}
fn try_run_in(
    program: &Arc<LocalProgram>,
    tracker: Option<Arc<crate::runtime::mem_tracker::MemTracker>>,
) -> Result<Vec<Chunk>, String> {
    let tracked = tracker.is_some();
    let state = Arc::new(RuntimeState::new(
        None,
        None,
        None,
        None,
        None,
        tracker,
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ));
    let output = ResultSinkHandle::new();
    let dop = i32::try_from(program.graph().profile().pipeline_dop().get()).unwrap();
    let prepared = prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        ExchangeBindings::default(),
        None,
        dop,
        state,
        Arc::new(NoopFragmentEventSink),
    )
    .map_err(|error| error.to_string())?;
    let running = prepared.start();
    running.join().map_err(|error| error.to_string())?;
    if tracked {
        assert!(
            running.stopped_fact().is_some(),
            "successful join proves actual driver exit"
        );
    }
    Ok(output.take_chunks())
}

pub(super) fn run(program: &Arc<LocalProgram>) -> Vec<Chunk> {
    try_run(program).unwrap_or_else(|error| panic!("compiled program runs: {error}"))
}

/// Every output row as nullable Int64 cells, in emission order.
pub(super) fn int64_rows(chunks: &[Chunk]) -> Vec<Vec<Option<i64>>> {
    let mut rows = Vec::new();
    for chunk in chunks {
        let columns = chunk
            .batch
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 column")
                    .clone()
            })
            .collect::<Vec<_>>();
        for row in 0..chunk.len() {
            rows.push(
                columns
                    .iter()
                    .map(|column| (!column.is_null(row)).then(|| column.value(row)))
                    .collect(),
            );
        }
    }
    rows
}
