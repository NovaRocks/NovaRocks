// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Table function lowering: the argument Project, the local TableFunction,
//! its prepared call and its channels. Every binding comes from the real
//! builtin UNNEST resolver and the frozen call effects from a fresh
//! preparation by its installed owner, as the FE freezes them.

use super::*;
use arrow_array::{ListArray, types::Int64Type};
use arrow_schema::{DataType, Field};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantPool, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType,
    InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, ScopedExpressionEffects,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalProgram, ProgramCallSite, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind,
    ProgramStateTemplate, StaticExprKind, TableFunctionOutputSlot,
};
use novarocks_physical_plan::{
    BoundTableFunction, ConstantPoolId, ConstantPools, ConstantReference, ExprId, ExprKind,
    ExpressionRootRole, ExpressionRootSite, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackage, FragmentPackageAdmission, FragmentPackageInput, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, FrozenPhysicalCall, FunctionArgumentType,
    LegacyBindingMetadata, LiteralValue, NodeId, NodeKind, OutputPort, PhysicalCallDefinition,
    PhysicalCallRequest, PhysicalCallSite, PhysicalExpressionRoots, PhysicalNode,
    PhysicalProperties, PhysicalRootUses, PipelineDopDomain, PlanLimits, PlanVersionId,
    PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort,
    StaticFunctionArgument, TableFunctionOutput, ValueId, ValueOrigin, passthrough_requirement,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, DomainGuard,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEffects, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, PureCompileControl, SemanticParameters, control_argument_semantics,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

const VALUES: NodeId = NodeId::new(1);
const TABLE: NodeId = NodeId::new(2);
/// Relational use IDs start far above every expression use of a fixture.
const RELATIONAL_USE: u32 = 1_000_000;
const POLICY: DecimalOverflowPolicy = DecimalOverflowPolicy::ReportError;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn constant_policy() -> ConstantPolicy {
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

fn options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: constant_policy(),
    }
}

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

/// The sealed builtin UNNEST owner with its actually installed TableV1 kernel.
fn unnest_catalog() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("unnest", FunctionKind::Table)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.table/unnest/v1").unwrap(),
            kind: FunctionKind::Table,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new("builtin.table/unnest/array-variadic-v1")
                    .unwrap(),
                implementation: PureImplementationId::try_new("builtin.table/unnest/selected-v1")
                    .unwrap(),
                abi: PureKernelAbi::TableV1,
            },
            aggregate_state_format: None,
        }])
        .unwrap()
}

/// A sealed catalog with no table kernel at all: only RAND is installed.
fn rand_catalog() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.scalar/rand/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}

fn int64(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}

fn list_type() -> FunctionValueType {
    FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )
}

/// One row: its key and one optional array per list column.
type Row = (i64, [Option<Vec<Option<i64>>>; 2]);

/// One output occurrence of the table function.
#[derive(Clone, Copy)]
enum Out {
    Key,
    Result(u32),
}

struct Shape {
    lists: usize,
    left_outer: bool,
    outputs: Vec<Out>,
    /// A table function with no outer input reads one constant list.
    standalone: bool,
}
impl Shape {
    fn inner(outputs: Vec<Out>) -> Self {
        Self {
            lists: 1,
            left_outer: false,
            outputs,
            standalone: false,
        }
    }
}

/// One eager use per actual occurrence; a control child gets its own guarded
/// domain exactly as the shape's argument semantics require.
struct Author<'a> {
    fragment: &'a Fragment,
    next_use: u32,
    next_domain: u32,
    uses: Vec<ExpressionInvocation<ExprId>>,
    domains: Vec<ExpressionEvaluationDomain>,
}
impl Author<'_> {
    fn visit(
        &mut self,
        expr: ExprId,
        domain: EvaluationDomainId,
        demand: EvaluationDemand,
    ) -> ExpressionUseId {
        let id = ExpressionUseId::new(self.next_use);
        self.next_use += 1;
        let kind = &self.fragment.expressions().get(expr).unwrap().kind;
        let shape = kind
            .intrinsic_control_shape()
            .unwrap()
            .expect("fixture authors no function call");
        let mut children = Vec::new();
        kind.expression_references_observed::<std::convert::Infallible>(|child| {
            children.push(child);
            Ok(())
        })
        .unwrap();
        let mut arguments = Vec::new();
        for (ordinal, child) in children.iter().enumerate() {
            let (child_demand, guard) =
                control_argument_semantics(shape, children.len(), ordinal, demand).unwrap();
            let child_domain = match guard {
                Some(kind) => {
                    let child_domain = EvaluationDomainId::new(self.next_domain);
                    self.next_domain += 1;
                    self.domains.push(ExpressionEvaluationDomain {
                        id: child_domain,
                        parent: Some(domain),
                        guard: Some(DomainGuard { owner: id, kind }),
                    });
                    child_domain
                }
                None => domain,
            };
            arguments.push(self.visit(*child, child_domain, child_demand));
        }
        self.uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain,
                demand,
            },
            definition: expr,
            control: shape,
            arguments: arguments.into_boxed_slice(),
        });
        id
    }
}

struct Fixture {
    package: Arc<FragmentPackage>,
    output: Vec<ValueId>,
}

/// `Values(k, l0, l1, ...)` and `TableFunction(UNNEST(l0, l1, ...))` with the
/// shape's outputs, published through a Result sink.
fn fixture(shape: &Shape, catalog: &PureEngineFunctionCatalog) -> Fixture {
    let rows: [Row; 3] = [
        (1, [Some(vec![Some(10), Some(11)]), Some(vec![Some(7)])]),
        (2, [None, Some(vec![])]),
        (3, [Some(vec![]), None]),
    ];
    let mut constants = ConstantPools::empty();
    for list in 0..shape.lists.max(1) {
        let array = ListArray::from_iter_primitive::<Int64Type, _, _>(
            rows.iter().map(|(_, lists)| lists[list].clone()),
        );
        let ty = list_type();
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("analyzed").unwrap()),
            ty,
            arrow_array::Array::to_data(&array),
            constant_policy(),
            CompilePhase::Validate,
            &Control,
        )
        .unwrap();
        constants
            .insert(ConstantPoolId::new(u32::try_from(list).unwrap()), pool)
            .unwrap();
    }
    let mut builder = FragmentBuilder::new(FragmentId::new(83));
    let mut key = None;
    let mut list_values = Vec::new();
    if !shape.standalone {
        let k = builder
            .add_value(
                int64(false),
                ValueOrigin::NodeOutput {
                    node: VALUES,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        for list in 0..shape.lists {
            list_values.push(
                builder
                    .add_value(
                        list_type(),
                        ValueOrigin::NodeOutput {
                            node: VALUES,
                            output_ordinal: u32::try_from(list + 1).unwrap(),
                        },
                    )
                    .unwrap(),
            );
        }
        let mut cells = Vec::new();
        for (row, (k, _)) in rows.iter().enumerate() {
            let mut cell = vec![
                builder
                    .add_expression(
                        VALUES,
                        int64(false),
                        ExprKind::Literal(LiteralValue::Int64(*k)),
                    )
                    .unwrap(),
            ];
            for list in 0..shape.lists {
                cell.push(
                    builder
                        .add_expression(
                            VALUES,
                            list_type(),
                            ExprKind::Constant(ConstantReference {
                                pool: ConstantPoolId::new(u32::try_from(list).unwrap()),
                                ordinal: u32::try_from(row).unwrap(),
                            }),
                        )
                        .unwrap(),
                );
            }
            cells.push(cell.into_boxed_slice());
        }
        let mut columns = vec![k];
        columns.extend(list_values.iter().copied());
        builder
            .add_values(VALUES, cells.into_boxed_slice(), columns.into_boxed_slice())
            .unwrap();
        key = Some(k);
    }
    // The real UNNEST binding for the actual argument types.
    let request = (0..shape.lists)
        .map(|_| FunctionArgument::Value {
            value_type: list_type(),
            constant: None,
        })
        .collect::<Vec<_>>();
    let bound = catalog
        .metadata()
        .resolve_bound_user(
            "unnest",
            FunctionKind::Table,
            FunctionBindingRequest {
                arguments: &request,
                logical_argument_count: request.len(),
                expected_result_type: None,
            },
            &Control,
        )
        .unwrap();
    let FunctionResultType::Relation(results) = &bound.selected.result_type else {
        panic!("UNNEST binds a relation")
    };
    let mut function = BoundTableFunction::from_exact_signature(
        bound.function_id.clone(),
        bound.selected.overload.clone(),
        bound.selected.argument_types.clone(),
        results.clone(),
    );
    function.legacy_metadata = Some(LegacyBindingMetadata {
        volatility: bound.semantics.volatility,
        argument_evaluation: bound.semantics.argument_evaluation,
        failure_behavior: bound.semantics.failure_behavior,
        intrinsic_row_error: bound.semantics.intrinsic_row_error,
        semantic_parameters: Box::default(),
    });
    let arguments = (0..shape.lists)
        .map(|list| {
            let kind = match key {
                Some(_) => ExprKind::Value(list_values[list]),
                None => ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(0),
                    ordinal: 0,
                }),
            };
            builder.add_expression(TABLE, list_type(), kind).unwrap()
        })
        .collect::<Vec<_>>();
    let mut outputs = Vec::new();
    let mut columns = Vec::new();
    for (ordinal, out) in shape.outputs.iter().enumerate() {
        let output = match out {
            Out::Key => TableFunctionOutput::PassThrough(key.expect("pass-through needs an input")),
            Out::Result(result) => {
                let mut ty = results[*result as usize].clone();
                ty = ty.with_nullable(ty.nullable() || shape.left_outer).unwrap();
                let value = builder
                    .add_value(
                        ty,
                        ValueOrigin::NodeOutput {
                            node: TABLE,
                            output_ordinal: u32::try_from(ordinal).unwrap(),
                        },
                    )
                    .unwrap();
                TableFunctionOutput::FunctionResult {
                    result_ordinal: *result,
                    value,
                }
            }
        };
        columns.push(output.value());
        outputs.push(output);
    }
    let (inputs, required_inputs, output_properties) = if shape.standalone {
        (
            Box::default(),
            Box::default(),
            PhysicalProperties {
                distribution: novarocks_physical_plan::Distribution::Singleton,
                row_multiplicity: novarocks_physical_plan::RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
        )
    } else {
        let input = builder.node_output_properties(VALUES).unwrap().clone();
        (
            Box::from([VALUES]),
            Box::from([passthrough_requirement(&input)]),
            PhysicalProperties {
                distribution: input.distribution.clone(),
                row_multiplicity: input.row_multiplicity,
                ordering: Box::default(),
            },
        )
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: TABLE,
            inputs,
            required_inputs,
            output_properties,
            output: OutputPort {
                node: TABLE,
                columns: columns.clone().into_boxed_slice(),
            },
            kind: NodeKind::TableFunction {
                function: function.clone(),
                arguments: arguments.into_boxed_slice(),
                outputs: outputs.into_boxed_slice(),
                left_outer: shape.left_outer,
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            TABLE,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    // The original relational request: its bound Value arguments.
    let mut entries = fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(definition, request)| (*definition, request.clone()))
        .collect::<Vec<_>>();
    entries.push((
        PhysicalCallDefinition::Relational(PhysicalCallSite::Table { node: TABLE }),
        PhysicalCallRequest {
            arguments: function
                .argument_types
                .iter()
                .map(|argument| match argument {
                    FunctionArgumentType::Value(value_type) => StaticFunctionArgument::Value {
                        value_type: value_type.clone(),
                        constant: None,
                    },
                    FunctionArgumentType::Lambda { .. } => panic!("UNNEST takes values"),
                })
                .collect(),
            logical_argument_count: function.argument_types.len(),
            expected_result_type: None,
            constant_policy: constant_policy(),
        },
    ));
    let fragment = fragment
        .with_call_requests_observed(entries, &Control)
        .unwrap();
    let package = package(fragment, constants, &function, &request);
    Fixture {
        package,
        output: columns,
    }
}

fn package(
    fragment: Fragment,
    constants: ConstantPools,
    function: &BoundTableFunction,
    request: &[FunctionArgument],
) -> Arc<FragmentPackage> {
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut author = Author {
        fragment: &fragment,
        next_use: 0,
        next_domain: 2,
        uses: Vec::new(),
        domains: vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
    };
    let mut bindings = Vec::new();
    for (site, root) in roots.sites() {
        bindings.push((*site, author.visit(root.expr, domain, root.demand)));
    }
    // The table call owns its own relational use and unguarded domain.
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(RELATIONAL_USE),
        domain: EvaluationDomainId::new(1),
        demand: EvaluationDemand::Value,
    };
    let Author { uses, domains, .. } = author;
    let mut domains = domains;
    domains.push(ExpressionEvaluationDomain {
        id: context.domain,
        parent: None,
        guard: None,
    });
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let root_uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control).unwrap();
    let NodeKind::TableFunction { arguments, .. } = &fragment.nodes()[&TABLE].kind else {
        unreachable!()
    };
    let argument_uses = (0..arguments.len())
        .map(|argument| {
            Some(
                root_uses.bindings()[&ExpressionRootSite {
                    node: TABLE,
                    role: ExpressionRootRole::TableFunctionArgument {
                        argument: u32::try_from(argument).unwrap(),
                    },
                }],
            )
        })
        .collect::<Vec<_>>();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let selection = Arc::new(novarocks_functions::FunctionBindingSelection {
        overload: function.overload.clone(),
        argument_types: function.argument_types.clone(),
        result_type: FunctionResultType::Relation(function.result_types.clone()),
        aggregate: None,
    });
    let token = unnest_catalog()
        .prepare_fresh(
            CallEffectInput {
                context,
                argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                    &argument_uses,
                ),
                function_id: &function.function_id,
                kind: FunctionKind::Table,
                selected: selection.as_ref(),
                request: FunctionBindingRequest {
                    arguments: request,
                    logical_argument_count: request.len(),
                    expected_result_type: None,
                },
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: POLICY,
                proof_scope: CallProofScope::Domain(context.domain),
            },
            Arc::clone(&selection),
            PureCallPreparation::Table {
                arguments: ScopedExpressionEffects::primitive(
                    context,
                    ExpressionEffects::PURE_VALUE,
                ),
            },
            &Control,
        )
        .unwrap_or_else(|error| panic!("fixture table call prepares: {error}"));
    let calls = FrozenFragmentCalls::try_new(
        &fragment,
        &root_uses,
        vec![FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Table { node: TABLE },
            context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: POLICY,
        }],
        &Control,
    )
    .unwrap();
    let output = fragment.nodes()[&fragment.root()].output.clone();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: output.clone(),
        fields: output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("c{ordinal}").into_boxed_str(),
                alias: None,
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let id = fragment.id();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                constants,
                version: PlanVersionId::try_new([83; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment,
                expression_uses: root_uses,
                calls,
                pruning: FrozenFragmentPruning::try_new(id, vec![], &Control).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            admission(),
            &Control,
        )
        .unwrap_or_else(|error| panic!("table function package publishes: {error:?}")),
    )
}

fn try_compile(
    package: Arc<FragmentPackage>,
    catalog: &PureEngineFunctionCatalog,
) -> Result<LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let validated = validate_fragment_providers(package, &providers, &Control).unwrap();
    compile_fragment(validated, catalog, options(), &Control)
}

fn compile(shape: &Shape) -> (LocalProgram, Fixture) {
    let catalog = unnest_catalog();
    let fixture = fixture(shape, &catalog);
    let program = try_compile(Arc::clone(&fixture.package), &catalog)
        .unwrap_or_else(|error| panic!("table function fragment compiles: {error}"));
    (program, fixture)
}

/// The argument Project and the TableFunction of a compiled fixture.
struct Lowered<'a> {
    project: ProgramNodeId,
    table: ProgramNodeId,
    project_kind: &'a ProgramNodeKind,
    table_kind: &'a ProgramNodeKind,
}
fn lowered(program: &LocalProgram) -> Lowered<'_> {
    let nodes = program.graph().nodes();
    let table = program.graph().root();
    let ProgramNodeKind::TableFunction { input, .. } = nodes[table.index()].kind() else {
        panic!("the table function is the root");
    };
    Lowered {
        project: *input,
        table,
        project_kind: nodes[input.index()].kind(),
        table_kind: nodes[table.index()].kind(),
    }
}

/// The slot a Project output root reads, when that root is a plain slot read.
fn root_slot(
    program: &LocalProgram,
    node: ProgramNodeId,
    expression: u32,
) -> Option<novarocks_types::SlotId> {
    let site = ProgramExpressionRootSite::Node {
        node,
        role: ProgramNodeExpressionRole::ProjectOutput { expression },
    };
    let expressions = program.checked().channels().expressions();
    let root = &expressions.resolved_calls().snapshot().roots().sites()[&site];
    match program
        .graph()
        .expressions()
        .node(root.definition)
        .unwrap()
        .kind()
    {
        StaticExprKind::SlotId(slot) => Some(*slot),
        _ => None,
    }
}

#[test]
fn table_function_lowers_to_an_argument_project_and_a_local_table_function() {
    let (program, _) = compile(&Shape::inner(vec![Out::Key, Out::Result(0)]));
    let nodes = program.graph().nodes();
    assert_eq!(nodes.len(), 3);
    let lowered = lowered(&program);
    assert_eq!(lowered.project, ProgramNodeId::new(1));
    assert_eq!(lowered.table, ProgramNodeId::new(2));
    let ProgramNodeKind::Project {
        input,
        is_subordinate,
        exprs,
        ..
    } = lowered.project_kind
    else {
        panic!("argument Project");
    };
    assert_eq!(*input, ProgramNodeId::new(0));
    assert!(*is_subordinate);
    assert_eq!(exprs.len(), 2, "one pass-through read and one argument");
    let project_slots = nodes[1].output_layout().slots().to_vec();
    let values_slots = nodes[0].output_layout().slots().to_vec();
    let ProgramNodeKind::TableFunction {
        input,
        function_name,
        param_slots,
        outer_slots,
        fn_result_slots,
        fn_result_required,
        is_left_join,
        param_types,
        ret_types,
        output_slot_sources,
    } = lowered.table_kind
    else {
        panic!("local TableFunction");
    };
    assert_eq!(*input, lowered.project);
    assert_eq!(function_name.as_ref(), "builtin.table/unnest/v1");
    assert_eq!(param_slots, &project_slots[1..]);
    assert_eq!(outer_slots, &project_slots[..1]);
    assert_eq!(fn_result_slots.len(), 1);
    assert!(*fn_result_required);
    assert!(!*is_left_join);
    assert_eq!(param_types, &[list_type().data_type().clone()]);
    assert_eq!(ret_types, &[DataType::Int64]);
    assert_eq!(
        output_slot_sources,
        &[
            TableFunctionOutputSlot::Outer {
                slot: project_slots[0]
            },
            TableFunctionOutputSlot::Result { index: 0 },
        ]
    );
    // The pass-through read is a compiler-authored slot read of the outer
    // key; the argument root moved onto the Project reads the outer list.
    assert_eq!(
        root_slot(&program, lowered.project, 0),
        Some(values_slots[0])
    );
    assert_eq!(
        root_slot(&program, lowered.project, 1),
        Some(values_slots[1])
    );
    // No expression root belongs to the table function itself.
    let expressions = program.checked().channels().expressions();
    assert!(
        expressions
            .resolved_calls()
            .snapshot()
            .roots()
            .sites()
            .keys()
            .all(|site| !matches!(
                site,
                ProgramExpressionRootSite::Node { node, .. } if *node == lowered.table
            ))
    );
    // The prepared call is the table function's own cursor lifecycle.
    let Some(ProgramStateTemplate::TableCursor { node, kernel }) =
        program.state_template(ProgramCallSite::Table {
            node: lowered.table,
        })
    else {
        panic!("prepared table cursor");
    };
    assert_eq!(node, lowered.table);
    assert_eq!(kernel.contract().argument_types().len(), 1);
    assert_eq!(kernel.contract().result_types(), &[int64(true)]);
    let channels = program.checked().channels();
    assert_eq!(
        channels.channel_type(ProgramChannelSite::TableResult {
            node: lowered.table,
            result: 0
        }),
        Some(&int64(true))
    );
    assert_eq!(
        channels.channel_type(ProgramChannelSite::Layout {
            node: lowered.table,
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0
        }),
        Some(&int64(false))
    );
    // The published result labels are the root's.
    let fields = nodes[2].output_layout().schema();
    assert_eq!(fields.field(0).name(), "c0");
    assert_eq!(fields.field(1).name(), "c1");
}

#[test]
fn left_outer_table_function_publishes_nullable_results_over_exact_relation_channels() {
    let shape = Shape {
        left_outer: true,
        ..Shape::inner(vec![Out::Result(0), Out::Key])
    };
    let (program, fixture) = compile(&shape);
    let lowered = lowered(&program);
    let ProgramNodeKind::TableFunction {
        is_left_join,
        output_slot_sources,
        outer_slots,
        ..
    } = lowered.table_kind
    else {
        panic!("local TableFunction");
    };
    assert!(*is_left_join);
    assert_eq!(
        output_slot_sources,
        &[
            TableFunctionOutputSlot::Result { index: 0 },
            TableFunctionOutputSlot::Outer {
                slot: outer_slots[0]
            },
        ]
    );
    let channels = program.checked().channels();
    let output = |ordinal| {
        channels
            .channel_type(ProgramChannelSite::Layout {
                node: lowered.table,
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal,
            })
            .cloned()
    };
    assert_eq!(output(0), Some(int64(true)), "a NULL-extended result");
    assert_eq!(
        output(1),
        Some(int64(false)),
        "the outer key keeps its type"
    );
    assert_eq!(
        channels.channel_type(ProgramChannelSite::TableResult {
            node: lowered.table,
            result: 0
        }),
        Some(&int64(true))
    );
    let layout = program.graph().nodes()[lowered.table.index()].output_layout();
    assert!(layout.schema().field(0).is_nullable());
    assert_eq!(fixture.output.len(), 2);
}

#[test]
fn zipped_arguments_share_one_pass_through_read_and_map_reordered_results() {
    let shape = Shape {
        lists: 2,
        ..Shape::inner(vec![Out::Key, Out::Result(1), Out::Key, Out::Result(0)])
    };
    let (program, _) = compile(&shape);
    let lowered = lowered(&program);
    let ProgramNodeKind::Project { exprs, .. } = lowered.project_kind else {
        panic!("argument Project");
    };
    assert_eq!(
        exprs.len(),
        3,
        "one shared pass-through read and two arguments"
    );
    let ProgramNodeKind::TableFunction {
        param_slots,
        outer_slots,
        fn_result_slots,
        output_slot_sources,
        ..
    } = lowered.table_kind
    else {
        panic!("local TableFunction");
    };
    assert_eq!(param_slots.len(), 2);
    assert_eq!(outer_slots.len(), 1);
    assert_eq!(fn_result_slots.len(), 2, "every produced relation column");
    assert_eq!(
        output_slot_sources,
        &[
            TableFunctionOutputSlot::Outer {
                slot: outer_slots[0]
            },
            TableFunctionOutputSlot::Result { index: 1 },
            TableFunctionOutputSlot::Outer {
                slot: outer_slots[0]
            },
            TableFunctionOutputSlot::Result { index: 0 },
        ]
    );
    let layout = program.graph().nodes()[lowered.table.index()].output_layout();
    let mut slots = layout.slots().to_vec();
    slots.sort();
    slots.dedup();
    assert_eq!(
        slots.len(),
        4,
        "each published occurrence is its own channel"
    );
    let channels = program.checked().channels();
    for result in 0..2 {
        assert_eq!(
            channels.channel_type(ProgramChannelSite::TableResult {
                node: lowered.table,
                result
            }),
            Some(&int64(true))
        );
    }
}

#[test]
fn table_function_unknown_identity_is_not_a_capability_refusal() {
    let fixture = fixture(
        &Shape::inner(vec![Out::Key, Out::Result(0)]),
        &unnest_catalog(),
    );
    let error = try_compile(fixture.package, &rand_catalog()).unwrap_err();
    assert!(!error.is_unsupported());
    assert!(matches!(
        &error,
        FragmentCompileError::Owner { error, .. }
            if matches!(error.downcast_ref::<crate::expressions::ExpressionLoweringError>(),
                Some(crate::expressions::ExpressionLoweringError::Specialization(
                    novarocks_functions::FunctionSpecializationFailure::Binding(
                        novarocks_functions::FunctionBindingError::UnknownFunction))))
    ));
}

#[test]
fn table_function_unknown_overload_is_not_a_capability_refusal() {
    let catalog = unnest_catalog();
    let fixture = fixture(&Shape::inner(vec![Out::Result(0)]), &catalog);
    let mut node = fixture.package.fragment().nodes()[&TABLE].clone();
    let NodeKind::TableFunction { function, .. } = &mut node.kind else {
        panic!("expected the fixture table function")
    };
    let unknown = FunctionOverloadId::try_new("test.table/unknown-overload/v1").unwrap();
    function.overload = unknown.clone();
    let mut work =
        novarocks_type_contract::CompileCheckpoints::try_new(&Control, CompilePhase::LowerProgram)
            .unwrap();
    let error =
        crate::table_function::admit_table_function(&node, &catalog, &mut work).unwrap_err();
    assert!(!error.is_unsupported());
    assert!(matches!(
        &error,
        FragmentCompileError::Owner { error, .. }
            if matches!(error.downcast_ref::<crate::expressions::ExpressionLoweringError>(),
                Some(crate::expressions::ExpressionLoweringError::Specialization(
                    novarocks_functions::FunctionSpecializationFailure::Binding(
                        novarocks_functions::FunctionBindingError::UnknownOverload(actual))))
                    if actual == &unknown)
    ));
}

/// Lookup-level coverage: a pure seal cannot contain a metadata-only entry.
#[test]
fn table_function_metadata_without_pure_owner_remains_a_capability_refusal() {
    let catalog = unnest_catalog();
    let fixture = fixture(&Shape::inner(vec![Out::Result(0)]), &catalog);
    let NodeKind::TableFunction { function, .. } = &fixture.package.fragment().nodes()[&TABLE].kind
    else {
        panic!("expected the fixture table function")
    };
    let original = catalog
        .metadata()
        .definition("unnest", FunctionKind::Table)
        .unwrap();
    let metadata_only = novarocks_functions::FunctionDefinition::try_new_bound(
        original.canonical_name(),
        original.visibility(),
        original.binding_declaration().unwrap().clone(),
        Arc::new(original.binding_resolver().unwrap().clone()),
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(metadata_only).unwrap();
    let metadata = builder.seal_bound().unwrap();
    let error =
        crate::table_function::table_function_abi(function, &metadata, &Control).unwrap_err();
    assert!(error.is_unsupported());
    assert!(matches!(
        &error,
        FragmentCompileError::Owner { error, .. }
            if matches!(error.downcast_ref::<crate::expressions::ExpressionLoweringError>(),
                Some(crate::expressions::ExpressionLoweringError::Specialization(
                    novarocks_functions::FunctionSpecializationFailure::MissingPureImplementation(actual)))
                    if actual == &function.overload)
    ));
}

#[test]
fn standalone_table_function_without_an_outer_input_is_refused_by_name() {
    let shape = Shape {
        standalone: true,
        ..Shape::inner(vec![Out::Result(0)])
    };
    let fixture = fixture(&shape, &unnest_catalog());
    let error = try_compile(fixture.package, &unnest_catalog()).unwrap_err();
    assert!(
        matches!(
            error,
            FragmentCompileError::Unsupported {
                node: Some(TABLE),
                feature: "standalone table function without an outer input",
            }
        ),
        "{error}"
    );
}
