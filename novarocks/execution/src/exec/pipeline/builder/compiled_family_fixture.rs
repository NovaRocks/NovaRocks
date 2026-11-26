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
//! No-call expression drafts retain explicit roots and input scopes. The unit
//! SDK authors their actual occurrences and control regions before publication.

#[path = "compiled_family_fixture/constant_sources.rs"]
pub(crate) mod constant_sources;
use constant_sources as cv;
pub(crate) use constant_sources::FragmentBuilder;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_functions::ConstantValidation;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_physical_plan::{
    ConstantPools, FragmentCuts, FragmentId, FragmentPackage, FragmentPackageAdmission,
    FragmentPackageInput, FragmentSink, FrozenFragmentPruning, NodeId, PipelineDopDomain,
    PlanLimits, PlanVersionId, PropertyProofProjectionLimits, RequiredContracts, ResultField,
    ResultPort, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, FunctionValueType,
    PureCompileControl, SemanticParameters,
};

use crate::exec::chunk::{Chunk, OutputColumnMetadata};
use crate::exec::operators::{ResultSinkBuffer, ResultSinkFactory};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::io::NoopTaskEventSink;

pub(crate) struct FixtureControl;
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
pub(crate) fn paid_input(
    program: &LocalProgram,
    tracker: &Arc<crate::runtime::mem_tracker::MemTracker>,
) -> Chunk {
    let node = &program.graph().nodes()[0];
    let novarocks_local_program::ProgramNodeKind::Values { values } = node.kind() else {
        panic!("fixture input is not Values");
    };
    let source = values.batch().expect("fixture Values are constant");
    let construction_tracker =
        crate::runtime::mem_tracker::MemTracker::new_root("paid-input-typed-construction");
    construction_tracker
        .install_limit_once(64 * 1024 * 1024)
        .unwrap();
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        construction_tracker,
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let schema = factory
        .create_compiled_chunk_schema(node.output_layout())
        .unwrap();
    let metadata = OutputColumnMetadata::try_new(
        source.num_columns(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
        &FixtureControl,
    )
    .unwrap();
    let mut columns = Vec::new();
    columns.try_reserve_exact(source.num_columns()).unwrap();
    columns.extend(source.columns().iter().cloned());
    let batch =
        arrow::record_batch::RecordBatch::try_new(schema.arrow_schema_ref(), columns).unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    Chunk::try_new_with_output_column_metadata(batch, schema, metadata, owner.as_ref()).unwrap()
}

pub(crate) fn int64(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}

pub(crate) fn boolean(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Boolean, nullable)
}

/// One nullable or non-null Int64 literal cell.
pub(crate) fn cell(value: Option<i64>) -> novarocks_functions::ConstantValue {
    value.map_or_else(cv::null, cv::i64)
}

/// A Values leaf at `node` whose columns have the given complete types and
/// whose rows are literal cells of exactly those types.
pub(crate) fn values(
    builder: &mut FragmentBuilder,
    node: NodeId,
    types: &[FunctionValueType],
    rows: &[Vec<novarocks_functions::ConstantValue>],
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
                    let definition = builder.constant(node, ty.clone(), literal.clone());
                    builder
                        .unit(node)
                        .unwrap()
                        .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
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

/// Seal the original drafts of fixtures that contain no function calls. The SDK owns all
/// occurrence identities and control regions; the empty claim map admits no call.
pub(crate) fn seal_no_call_units(builder: &mut novarocks_physical_plan::FragmentBuilder) {
    let nodes = builder
        .construction_nodes()
        .filter_map(|(&id, node)| {
            let mut roots = Vec::new();
            node.kind.root_references(&mut roots);
            (node.expressions.is_none() && !roots.is_empty()).then_some(id)
        })
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap();
    for node in nodes {
        let ports = builder.node_ports(node).unwrap();
        let unit = builder
            .take_unit_builder(node)
            .unwrap()
            .seal(ports, &mut work)
            .unwrap()
            .finish(BTreeMap::new(), &mut work)
            .unwrap();
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
    work.finish().unwrap();
}

/// Freeze calls over the actual writer/finish materialized channels. These
/// inputs have already left scalar evaluation; no expression use is invented.
pub(crate) fn freeze_materialized_writer_calls(
    fragment: &novarocks_physical_plan::Fragment,
    catalog: &novarocks_functions::PureEngineFunctionCatalog,
    parameters: &SemanticParameters,
    decimal_policy: novarocks_type_contract::DecimalOverflowPolicy,
    control: &dyn PureCompileControl,
) -> novarocks_physical_plan::FrozenFragmentCalls {
    use novarocks_functions::{
        AggregateBindingSelection, AggregateKernelPhase, AggregatePreparationOptions,
        CallArgumentUses, CallEffectInput, FunctionArgument, FunctionBindingRequest,
        FunctionBindingSelection, FunctionKind, FunctionResultType, PureCallPreparation,
        ScopedRelationalEffects,
    };
    use novarocks_physical_plan::{
        FrozenPhysicalCall, NodeKind, PhysicalCallSite, StaticFunctionArgument,
    };
    use novarocks_type_contract::{
        CallProofScope, ExpressionEffects, RelationCallId, RelationEffectContext,
    };
    let mut frozen = Vec::new();
    for node in fragment.nodes().values() {
        let (calls, partial) = match &node.kind {
            NodeKind::TableWriter { target } => (target.partial_aggregates.as_ref(), true),
            NodeKind::TableFinish(spec) => (spec.final_aggregates.as_ref(), false),
            _ => continue,
        };
        for (ordinal, item) in calls.iter().enumerate() {
            let call = u32::try_from(ordinal).unwrap();
            let site = if partial {
                PhysicalCallSite::WriterPartial {
                    node: node.id,
                    call,
                }
            } else {
                PhysicalCallSite::WriterFinal {
                    node: node.id,
                    call,
                }
            };
            let relation = RelationCallId::new(u32::try_from(frozen.len()).unwrap());
            let context = RelationEffectContext::call(relation);
            let input = RelationEffectContext::materialized_input(relation);
            let binding = &item.binding;
            let selected = Arc::new(FunctionBindingSelection {
                overload: binding.function.overload.clone(),
                argument_types: binding.function.argument_types.clone(),
                result_type: FunctionResultType::Scalar(binding.function.result_type.clone()),
                aggregate: Some(AggregateBindingSelection {
                    state_argument_contract: binding.state_argument_contract,
                    intermediate_type: binding.intermediate_type.clone(),
                    state_format: binding.state_format.clone(),
                }),
            });
            let original = fragment
                .call_requests()
                .get(site)
                .expect("original writer request");
            let arguments = original
                .arguments
                .iter()
                .map(|argument| match argument {
                    StaticFunctionArgument::Value {
                        value_type,
                        constant: None,
                    } => FunctionArgument::Value {
                        value_type: value_type.clone(),
                        constant: None,
                    },
                    _ => panic!("writer requests retain their original materialized value types"),
                })
                .collect::<Vec<_>>();
            let input_type = &fragment.values()[&item.input].ty;
            let phase = if partial {
                AggregateKernelPhase::Partial
            } else {
                AggregateKernelPhase::Final
            };
            let token = catalog
                .prepare_fresh(
                    CallEffectInput {
                        context: context.into(),
                        argument_uses: if partial {
                            CallArgumentUses::MaterializedInput {
                                context: input,
                                input_type,
                            }
                        } else {
                            CallArgumentUses::AggregateMerge {
                                phase,
                                state_context: input.into(),
                                state_input_type: input_type,
                            }
                        },
                        function_id: &binding.function.function_id,
                        kind: FunctionKind::Aggregate,
                        selected: selected.as_ref(),
                        request: FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: original.logical_argument_count,
                            expected_result_type: original.expected_result_type.as_ref(),
                        },
                        environment: &[],
                        parameters,
                        decimal_overflow_policy: decimal_policy,
                        proof_scope: CallProofScope::Relation(context.identity),
                    },
                    selected.clone(),
                    PureCallPreparation::Aggregate {
                        arguments: ScopedRelationalEffects::primitive(
                            context,
                            ExpressionEffects::PURE_VALUE,
                        ),
                        options: AggregatePreparationOptions {
                            state_interpretation: None,
                            phase,
                            distinct: false,
                            order_keys: Arc::from([]),
                            state_input_type: (!partial).then(|| input_type.clone()),
                        },
                    },
                    control,
                )
                .unwrap_or_else(|error| {
                    panic!("the original writer owner prepares {site:?}: {error}")
                });
            frozen.push(FrozenPhysicalCall {
                site,
                context,
                materialized_writer_input: Some(input),
                effects: token.call_contract().effects().clone(),
                decimal_overflow_policy: decimal_policy,
            });
        }
    }
    novarocks_physical_plan::FrozenFragmentCalls::try_new(fragment, frozen, control).unwrap()
}

/// Publish the fragment rooted at `root` with a Result sink. Its original
/// no-call expression drafts are sealed by the unit SDK before publication.
pub(crate) fn package(
    builder: FragmentBuilder,
    root: NodeId,
    constants: ConstantPools,
    max_dop: u32,
) -> Arc<FragmentPackage> {
    package_with_sink(builder, root, constants, max_dop, FragmentSink::Result)
}

pub(crate) fn package_with_sink(
    builder: FragmentBuilder,
    root: NodeId,
    constants: ConstantPools,
    max_dop: u32,
    sink: FragmentSink,
) -> Arc<FragmentPackage> {
    package_with_sink_and_names(builder, root, constants, max_dop, sink, None)
}

pub(crate) fn package_with_sink_and_names(
    mut builder: FragmentBuilder,
    root: NodeId,
    constants: ConstantPools,
    max_dop: u32,
    sink: FragmentSink,
    names: Option<&[&str]>,
) -> Arc<FragmentPackage> {
    seal_no_call_units(&mut builder);
    let fragment = builder
        .finish_definition(
            root,
            sink,
            PipelineDopDomain {
                min: 1,
                max: max_dop,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let (fragment, sources) = fragment.into_parts();
    let mut constants = constants;
    cv::merge(&mut constants, &sources);
    let output = fragment.nodes()[&root].output.clone();
    if let Some(names) = names {
        assert_eq!(names.len(), output.columns.len());
    }
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
                name: names.map_or_else(
                    || format!("c{ordinal}").into_boxed_str(),
                    |names| names[ordinal].into(),
                ),
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

                pruning: FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: Default::default(),
            },
            admission(),
            &FixtureControl,
        )
        .unwrap(),
    )
}

/// Explicit fixture constant admission; these values are not production
/// defaults. Constant pools authored by a test use the same policy.
pub(crate) fn constant_policy() -> ConstantValidation {
    novarocks_functions::ConstantValidation::bounded(novarocks_functions::ConstantPolicy {
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
    })
}

fn options(dop: usize) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(dop).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: constant_policy(),
    }
}

/// Compile with the real local compiler. The catalog is the sealed RAND
/// subset because an empty catalog is refused; these fixtures bind no call.
pub(crate) fn try_compile(
    package: Arc<FragmentPackage>,
    dop: usize,
) -> Result<Arc<LocalProgram>, String> {
    let functions = novarocks_functions::testkit::rng_subset();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated = validate_fragment_providers(package, &providers, &FixtureControl).unwrap();
    compile_fragment(validated, &functions, options(dop), &FixtureControl)
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

pub(crate) fn compile(package: Arc<FragmentPackage>, dop: usize) -> Arc<LocalProgram> {
    try_compile(package, dop).unwrap_or_else(|error| panic!("fixture fragment compiles: {error}"))
}

/// Prepare and run the program with its frozen driver count; a preparation
/// or execution failure is returned as text.
pub(crate) fn try_run(program: &Arc<LocalProgram>) -> Result<Vec<Chunk>, String> {
    try_run_in(program, None)
}

/// A declared original query tracker for resource-owning component fixtures.
/// Other family fixtures keep their original untracked inputs unchanged.
pub(crate) fn run_with_tracker(
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
    let state = Arc::new(RuntimeState::for_local_execution(
        None,
        None,
        None,
        None,
        None,
        Some(tracker.unwrap_or_else(|| {
            crate::runtime::mem_tracker::MemTracker::new_root("compiled family fixture")
        })),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ));
    let output = ResultSinkBuffer::new();
    let dop = i32::try_from(program.graph().profile().pipeline_dop().get()).unwrap();
    let prepared = prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        ExchangeBindings::default(),
        None,
        dop,
        state,
        Arc::new(NoopTaskEventSink),
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

pub(crate) fn run(program: &Arc<LocalProgram>) -> Vec<Chunk> {
    try_run(program).unwrap_or_else(|error| panic!("compiled program runs: {error}"))
}

/// Every output row as nullable Int64 cells, in emission order.
pub(crate) fn int64_rows(chunks: &[Chunk]) -> Vec<Vec<Option<i64>>> {
    let mut rows = Vec::new();
    for chunk in chunks {
        let columns = chunk
            .record_batch()
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

/// Compile the fixture's complete declared columns as constant Values. This
/// authors a checked package; lifecycle tests do not retain a decoder bridge.
pub(crate) fn values_chunk_program(chunk: &Chunk, sink: FragmentSink) -> Arc<LocalProgram> {
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();

    use novarocks_functions::ConstantPool;
    use novarocks_physical_plan::{ConstantPoolId, ConstantReference};
    let mut builder = FragmentBuilder::new(FragmentId::new(19), &fixture_sources);
    let node = novarocks_physical_plan::NodeId::new(19);
    let mut constants = ConstantPools::empty();
    let types = chunk
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| {
            FunctionValueType::try_from_typed_binding(slot.typed_binding().clone()).unwrap()
        })
        .collect::<Vec<_>>();
    for (ordinal, (column, ty)) in chunk
        .record_batch()
        .columns()
        .iter()
        .zip(&types)
        .enumerate()
    {
        let pool = ConstantPool::try_new(
            Arc::clone(chunk.chunk_schema().slots()[ordinal].field_ref()),
            ty.clone(),
            column.to_data(),
            constant_policy(),
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
        constants
            .insert(ConstantPoolId::new(ordinal as u32), pool)
            .unwrap();
    }
    let columns = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let rows = (0..chunk.len())
        .map(|row| {
            types
                .iter()
                .enumerate()
                .map(|(ordinal, ty)| {
                    let definition = builder
                        .unit(node)
                        .unwrap()
                        .add(
                            None,
                            ty.clone(),
                            ExprKind::Constant(ConstantReference {
                                pool: ConstantPoolId::new(ordinal as u32),
                                ordinal: row as u32,
                            }),
                        )
                        .unwrap();
                    builder
                        .unit(node)
                        .unwrap()
                        .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
                        .unwrap()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        })
        .collect::<Vec<_>>();
    builder
        .add_values(node, rows.into_boxed_slice(), columns.into_boxed_slice())
        .unwrap();
    let names = chunk
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();
    compile(
        package_with_sink_and_names(builder, node, constants, 1, sink, Some(&names)),
        1,
    )
}
