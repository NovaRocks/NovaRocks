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

use super::*;
use arrow_array::{Array, ListArray, MapArray, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    ConstantError, ConstantPolicy, ConstantPool, EngineFunctionCatalogBuilder, FunctionId,
    FunctionKind, FunctionOverloadId, InstalledPureKernel, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalOperatorId, LocalOperatorOrigin, ProgramChannelLayoutRole,
    ProgramChannelSite, ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind, StaticExprKind, UnpivotConstant as LocalConstant,
};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, ConstantReferenceError, ExprKind,
    FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage, FragmentPackageAdmission,
    FragmentPackageError, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, LiteralValue, NodeId, NodeKind, PhysicalExpressionRoots,
    PhysicalRootUses, PipelineDopDomain, PlanLimits, PlanVersionId, PropertyProofProjectionLimits,
    RequiredContracts, ResultField, ResultPort, UnpivotConstant, UnpivotSpec, UnpivotValueMapping,
    ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, PureCompileControl, SemanticParameters,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn functions() -> PureEngineFunctionCatalog {
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
    // Actual unused RAND owner, with independently authored two receipts.
    // This fixture is not a Server catalogue closure claim.
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
fn admission() -> FragmentPackageAdmission {
    // Explicit small-fixture invoice and independent projection ceilings.
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
fn options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: std::time::Duration::from_secs(120),
        constants: ConstantPolicy {
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
        },
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Valid,
    NestedConstant,
    LiteralSource,
    EmptyMappings,
    BadPassthrough,
    BadValueNullability,
    BadScalar,
    BadMapOrder,
    ZeroBounds,
    JoinedCollectionNullable,
    MissingCollectionPool,
    BadCollectionOrdinal,
    BadCollectionType,
    BadCollectionNullable,
}
struct Fixture {
    package: Arc<FragmentPackage>,
    unpivot: NodeId,
    downstream: bool,
    selected: ConstantReference,
}
fn project(
    builder: &mut FragmentBuilder,
    node: NodeId,
    input: NodeId,
    items: Vec<(ExprKind, FunctionValueType)>,
) -> Vec<ValueId> {
    let mut definitions = Vec::new();
    let mut output = Vec::new();
    for (kind, ty) in items {
        let expr = builder.add_expression(node, ty.clone(), kind).unwrap();
        let value = builder
            .add_value(ty, ValueOrigin::Expr { node, expr })
            .unwrap();
        definitions.push((expr, value));
        output.push(value);
    }
    builder
        .add_project(
            node,
            input,
            definitions.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )
        .unwrap();
    output
}
fn list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
}
fn map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Utf8, false)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}
fn checked_list_pool(first: i32, nullable: bool, wrong_type: bool) -> ConstantPool {
    let (ty, data) = if wrong_type {
        let field = Arc::new(Field::new("item", DataType::Int64, false));
        let raw = ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>([
            Some(vec![Some(9999)]),
            Some(vec![Some(i64::from(first)), Some(-2)]),
            Some(vec![Some(i64::from(first) + 1), Some(-2)]),
        ]);
        let array = ListArray::try_new(
            field.clone(),
            raw.offsets().clone(),
            raw.values().clone(),
            None,
        )
        .unwrap();
        (
            FunctionValueType::new(DataType::List(field), nullable),
            array.to_data(),
        )
    } else {
        let DataType::List(field) = list_type() else {
            unreachable!()
        };
        let raw = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
            Some(vec![Some(9999)]),
            Some(vec![Some(first), Some(-2)]),
            Some(vec![Some(first + 1), Some(-2)]),
        ]);
        let array = ListArray::try_new(
            field.clone(),
            raw.offsets().clone(),
            raw.values().clone(),
            None,
        )
        .unwrap();
        (
            FunctionValueType::new(DataType::List(field), nullable),
            array.to_data(),
        )
    };
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("original-special-list").unwrap()),
        ty,
        data,
        options().constants,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap()
}
fn checked_map_pool(bad_order: bool) -> ConstantPool {
    let ty = FunctionValueType::new(map_type(), false);
    let DataType::Map(entries, _) = &ty.data_type() else {
        unreachable!()
    };
    let DataType::Struct(fields) = entries.data_type() else {
        unreachable!()
    };
    let keys = if bad_order {
        vec!["unused", "zz", "z", "a", "z", "a"]
    } else {
        vec!["unused", "zz", "a", "z", "a", "z"]
    };
    let children = StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(StringArray::from(vec![
                "unused", "unused", "λ\0", "tail", "λ\0", "tail",
            ])),
        ],
        None,
    )
    .unwrap();
    let offsets = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(0), Some(0)]),
        Some(vec![Some(0), Some(0)]),
        Some(vec![Some(0), Some(0)]),
    ]);
    let array = MapArray::try_new(
        entries.clone(),
        offsets.offsets().clone(),
        children,
        None,
        false,
    )
    .unwrap();
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("original-special-map").unwrap()),
        ty,
        array.to_data(),
        options().constants,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap()
}

fn fixture(
    shape: Shape,
    mappings: usize,
    downstream: bool,
) -> Result<Fixture, Box<dyn std::error::Error>> {
    let fid = FragmentId::new(79);
    let empty = NodeId::new(u32::MAX);
    let initial = NodeId::new(0);
    let duplicate = NodeId::new(43);
    let unpivot = NodeId::new(901);
    let integer = FunctionValueType::new(DataType::Int64, false);
    let nullable = FunctionValueType::new(DataType::Int64, true);
    let mut builder = FragmentBuilder::new(fid);
    builder
        .add_values(empty, Box::from([Box::default()]), Box::default())
        .unwrap();
    let initial_values = project(
        &mut builder,
        initial,
        empty,
        vec![
            (ExprKind::Literal(LiteralValue::Int64(7)), integer.clone()),
            (ExprKind::Literal(LiteralValue::Null), nullable.clone()),
            (
                ExprKind::Literal(LiteralValue::Utf8("source".into())),
                FunctionValueType::new(DataType::Utf8, false),
            ),
        ],
    );
    let mut projected = Vec::new();
    let mut child = Vec::new();
    for (index, (source, ty)) in [
        (initial_values[0], integer.clone()),
        (initial_values[1], nullable.clone()),
        (
            initial_values[2],
            FunctionValueType::new(DataType::Utf8, false),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let expr = builder
            .add_expression(duplicate, ty.clone(), ExprKind::Value(source))
            .unwrap();
        let value = builder
            .add_value(
                ty,
                ValueOrigin::Expr {
                    node: duplicate,
                    expr,
                },
            )
            .unwrap();
        projected.push((expr, value));
        child.push(value);
        if index == 0 {
            projected.push((expr, value));
            child.push(value);
        }
    }
    builder
        .add_project(
            duplicate,
            initial,
            projected.into_boxed_slice(),
            child.clone().into_boxed_slice(),
        )
        .unwrap();
    let pool_id = ConstantPoolId::new(u32::MAX);
    let scalar_type;
    let data;
    if matches!(shape, Shape::NestedConstant) {
        let array = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
            Some(vec![Some(99)]),
            Some(vec![Some(2), None]),
            None,
        ]);
        let field = Arc::new(Field::new("item", DataType::Int32, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "original-child".to_owned(),
                "preserved".to_owned(),
            )]),
        ));
        let array = ListArray::try_new(
            field.clone(),
            array.offsets().clone(),
            array.values().clone(),
            array.nulls().cloned(),
        )
        .unwrap();
        scalar_type = FunctionValueType::new(DataType::List(field), true);
        data = array.to_data();
    } else if matches!(shape, Shape::JoinedCollectionNullable) {
        let DataType::List(field) = list_type() else {
            unreachable!()
        };
        let raw = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
            Some(vec![Some(999)]),
            Some(vec![Some(2), Some(-2)]),
            None,
        ]);
        let array = ListArray::try_new(
            field.clone(),
            raw.offsets().clone(),
            raw.values().clone(),
            raw.nulls().cloned(),
        )
        .unwrap();
        scalar_type = FunctionValueType::new(DataType::List(field), true);
        data = array.to_data();
    } else {
        scalar_type = FunctionValueType::new(DataType::Utf8, true);
        data = StringArray::from(vec![Some("unused"), Some("selected-λ\0"), None]).to_data();
    }
    let field = Arc::new(Field::new(
        "original-pool",
        data.data_type().clone(),
        scalar_type.nullable(),
    ));
    let pool = ConstantPool::try_new(
        field,
        scalar_type.clone(),
        data,
        options().constants,
        CompilePhase::Validate,
        &Control,
    )?;
    let selected = ConstantReference {
        pool: pool_id,
        ordinal: 1,
    };
    let null = ConstantReference {
        pool: pool_id,
        ordinal: 2,
    };
    let selected_expr = if matches!(shape, Shape::LiteralSource) {
        builder
            .add_expression(
                unpivot,
                FunctionValueType::new(DataType::Utf8, false),
                ExprKind::Literal(LiteralValue::Utf8("authored-literal".into())),
            )
            .unwrap()
    } else {
        builder
            .add_expression(unpivot, scalar_type.clone(), ExprKind::Constant(selected))
            .unwrap()
    };
    let null_expr = builder
        .add_expression(unpivot, scalar_type.clone(), ExprKind::Constant(null))
        .unwrap();
    let bad_expr = if matches!(shape, Shape::BadScalar) {
        Some(
            builder
                .add_expression(unpivot, scalar_type.clone(), ExprKind::Value(child[3]))
                .unwrap(),
        )
    } else {
        None
    };
    let mut output = Vec::new();
    // The original Physical owner requires passthroughs, value, then literals.
    // Both passthrough outputs still name the same proven child source.
    let types = [
        FunctionValueType::new(DataType::Utf8, false),
        FunctionValueType::new(DataType::Utf8, false),
        if matches!(shape, Shape::BadValueNullability) {
            integer.clone()
        } else {
            nullable.clone()
        },
        scalar_type.clone(),
        FunctionValueType::new(
            list_type(),
            matches!(shape, Shape::JoinedCollectionNullable),
        ),
        FunctionValueType::new(map_type(), false),
    ];
    for (ordinal, ty) in types.into_iter().enumerate() {
        output.push(
            builder
                .add_value(
                    ty,
                    ValueOrigin::NodeOutput {
                        node: unpivot,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
        );
    }
    let passthrough = vec![
        (
            if matches!(shape, Shape::BadPassthrough) {
                child[0]
            } else {
                child[3]
            },
            output[0],
        ),
        (child[3], output[1]),
    ];
    let actual_mappings = if matches!(shape, Shape::EmptyMappings) {
        0
    } else {
        mappings
    };
    let mut constants = ConstantPools::empty();
    constants.insert(pool_id, pool)?;
    let map_pool_id = ConstantPoolId::new(u32::MAX - 1);
    if actual_mappings != 0 {
        constants.insert(
            map_pool_id,
            checked_map_pool(matches!(shape, Shape::BadMapOrder)),
        )?;
    }
    let mut items = Vec::new();
    for index in 0..actual_mappings {
        // Each pair shares the original three-row backing and selects a
        // different nonzero ordinal. The unused row is never copied to LP.
        let list_pool_id = ConstantPoolId::new((index / 2) as u32);
        if index % 2 == 0 {
            constants.insert(
                list_pool_id,
                checked_list_pool(
                    index as i32,
                    matches!(shape, Shape::BadCollectionNullable),
                    matches!(shape, Shape::BadCollectionType),
                ),
            )?;
        }
        let list_reference = ConstantReference {
            pool: if matches!(shape, Shape::MissingCollectionPool) {
                ConstantPoolId::new(u32::MAX - 2)
            } else {
                list_pool_id
            },
            ordinal: if matches!(shape, Shape::BadCollectionOrdinal) {
                42
            } else {
                1 + (index % 2) as u32
            },
        };
        items.push(UnpivotValueMapping {
            input: if index % 2 == 0 { child[0] } else { child[2] },
            constants: vec![
                UnpivotConstant::Scalar(bad_expr.unwrap_or(if index % 2 == 0 {
                    selected_expr
                } else {
                    null_expr
                })),
                if matches!(shape, Shape::JoinedCollectionNullable) && index % 2 == 1 {
                    UnpivotConstant::Scalar(null_expr)
                } else {
                    UnpivotConstant::Int32List(list_reference)
                },
                UnpivotConstant::Utf8Map(ConstantReference {
                    pool: map_pool_id,
                    ordinal: 1 + (index % 2) as u32,
                }),
            ]
            .into_boxed_slice(),
        });
    }
    let spec = UnpivotSpec {
        passthrough: passthrough.clone().into_boxed_slice(),
        value_output: output[2],
        literal_outputs: vec![output[3], output[4], output[5]].into_boxed_slice(),
        mappings: items.into_boxed_slice(),
        max_output_rows: if matches!(shape, Shape::ZeroBounds) {
            0
        } else {
            1024
        },
        max_output_bytes: 1 << 20,
    };
    let passthrough_map = passthrough.into_iter().collect();
    builder
        .add_row_rewriting(
            unpivot,
            duplicate,
            Some(&passthrough_map),
            output.clone().into_boxed_slice(),
            NodeKind::Unpivot { spec },
        )
        .unwrap();
    let (root, result_values) = if downstream {
        let root = NodeId::new(7);
        let values = project(
            &mut builder,
            root,
            unpivot,
            vec![
                (
                    ExprKind::Value(output[0]),
                    FunctionValueType::new(DataType::Utf8, false),
                ),
                (ExprKind::Value(output[2]), nullable),
            ],
        );
        (root, values)
    } else {
        (unpivot, output)
    };
    let fragment = builder.finish_structure(
        root,
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        PlanLimits::FROZEN,
        &Control,
        novarocks_physical_plan::StagePlacementPolicy::Singleton,
    )?;
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control)?;
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = ExpressionUseId::new(u32::MAX - ordinal as u32);
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
        &Control,
    )?;
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control)?;
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control)?;
    let result = ResultPort {
        scalar_schema: None,
        fragment: fid,
        output: fragment.nodes()[&root].output.clone(),
        fields: result_values
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("original_{ordinal}").into_boxed_str(),
                alias: Some(format!("selected_{ordinal}").into_boxed_str()),
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            constants,
            version: PlanVersionId::try_new([79; 16]).unwrap(),
            required: RequiredContracts::default(),
            fragment,
            expression_uses: uses,
            calls,
            pruning: FrozenFragmentPruning::try_new(fid, vec![], &Control)?,
            cuts: FragmentCuts::default(),
            result: Some(result),
            parameters: SemanticParameters::try_new([])?,
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        admission(),
        &Control,
    )?;
    Ok(Fixture {
        package: Arc::new(package),
        unpivot,
        downstream,
        selected,
    })
}
fn compile(
    fixture: &Fixture,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let validated =
        validate_fragment_providers(fixture.package.clone(), &providers, &Control).unwrap();
    compile_fragment(validated, functions, options(), control)
}

#[test]
fn unpivot_complete_lowering_preserves_ordered_roles_repeated_sources_constants_and_provenance() {
    let source = fixture(Shape::Valid, 2, false).unwrap();
    let program = compile(&source, &functions(), &Control).unwrap();
    let graph = program.graph();
    assert_eq!(graph.nodes().len(), 4);
    assert_eq!(graph.root(), ProgramNodeId::new(3));
    let child = &graph.nodes()[2];
    let node = &graph.nodes()[3];
    let ProgramNodeKind::Unpivot {
        input,
        passthrough_columns,
        value_output_slot_id,
        literal_output_slot_ids,
        value_mappings,
        max_output_rows,
        max_output_bytes,
    } = node.kind()
    else {
        panic!("actual compiled Unpivot");
    };
    assert_eq!(*input, ProgramNodeId::new(2));
    assert_eq!((*max_output_rows, *max_output_bytes), (1024, 1 << 20));
    let slots = node.output_layout().slots();
    let old = child.output_layout().slots();
    assert_ne!(old[0], old[1]);
    assert_eq!(
        (
            passthrough_columns[0].input_slot_id,
            passthrough_columns[0].output_slot_id
        ),
        (old[3], slots[0])
    );
    assert_eq!(
        (
            passthrough_columns[1].input_slot_id,
            passthrough_columns[1].output_slot_id
        ),
        (old[3], slots[1])
    );
    assert_eq!(*value_output_slot_id, slots[2]);
    assert_eq!(literal_output_slot_ids, &vec![slots[3], slots[4], slots[5]]);
    assert_eq!(value_mappings.len(), 2);
    let NodeKind::Unpivot { spec } = &source.package.fragment().nodes()[&source.unpivot].kind
    else {
        panic!("original checked source")
    };
    for position in [1, 2] {
        let references = spec
            .mappings
            .iter()
            .map(|mapping| match &mapping.constants[position] {
                UnpivotConstant::Int32List(reference) | UnpivotConstant::Utf8Map(reference) => {
                    *reference
                }
                _ => panic!("actual checked collection reference"),
            })
            .collect::<Vec<_>>();
        assert_eq!(references[0].pool, references[1].pool);
        assert_eq!((references[0].ordinal, references[1].ordinal), (1, 2));
        let original = &source.package.constants().entries()[&references[0].pool];
        assert_eq!(original.resource_facts().rows, 3);
        assert!(!original.value_type().nullable());
    }
    assert_eq!(value_mappings[0].input_value_slot_id, old[0]);
    assert_eq!(value_mappings[1].input_value_slot_id, old[2]);
    for (index, mapping) in value_mappings.iter().enumerate() {
        let LocalConstant::Scalar { expr_id, nullable } = &mapping.constants[0] else {
            panic!("actual Scalar constant");
        };
        assert!(*nullable);
        let StaticExprKind::Constant(value) = graph.expressions().node(*expr_id).unwrap().kind()
        else {
            panic!("original checked CV");
        };
        assert_eq!(value.ordinal(), if index == 0 { 1 } else { 2 });
        assert_eq!(
            value.value_type(),
            source.package.constants().entries()[&source.selected.pool].value_type()
        );
        assert!(
            value
                .equals_observed(
                    &source.package.constants().entries()[&source.selected.pool]
                        .value(value.ordinal())
                        .unwrap(),
                    CompilePhase::Validate,
                    &Control
                )
                .unwrap()
        );
        assert_eq!(
            value.try_utf8().unwrap(),
            if index == 0 {
                Some("selected-λ\0")
            } else {
                None
            }
        );
        let LocalConstant::Int32List(values) = &mapping.constants[1] else {
            panic!("actual Int32List");
        };
        assert_eq!(values, &vec![index as i32, -2]);
        let LocalConstant::Utf8Map(entries) = &mapping.constants[2] else {
            panic!("actual Utf8Map");
        };
        assert_eq!(
            entries
                .iter()
                .map(|(k, v)| (k.as_ref(), v.as_ref()))
                .collect::<Vec<_>>(),
            vec![("a", "λ\0"), ("z", "tail")]
        );
    }
    for ordinal in 0..6 {
        let field = node.output_layout().schema().field(ordinal);
        assert_eq!(field.name(), &format!("selected_{ordinal}"));
        assert_eq!(field.is_nullable(), ordinal == 2 || ordinal == 3);
    }
    assert_eq!(node.physical_sources()[0].get(), source.unpivot.get());
    assert_eq!(graph.nodes()[0].physical_sources()[0].get(), u32::MAX);
    let provenance = program.provenance().get(LocalOperatorId::new(3)).unwrap();
    assert!(matches!(provenance.origin, LocalOperatorOrigin::Direct));
    assert_eq!(provenance.cost_owner, LocalOperatorId::new(3));
    let bindings = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot()
        .bindings();
    for mapping in 0..2 {
        assert!(bindings.contains_key(&ProgramExpressionRootSite::Node {
            node: ProgramNodeId::new(3),
            role: ProgramNodeExpressionRole::UnpivotConstant {
                mapping,
                constant: 0
            }
        }));
    }
    assert_eq!(graph.profile().pipeline_dop().get(), 1);
}
#[test]
fn unpivot_selected_nested_constant_keeps_original_ordinal_complete_field_and_success_null() {
    let source = fixture(Shape::NestedConstant, 2, false).unwrap();
    let program = compile(&source, &functions(), &Control).unwrap();
    let graph = program.graph();
    let node = &graph.nodes()[3];
    let ProgramNodeKind::Unpivot { value_mappings, .. } = node.kind() else {
        panic!("Unpivot");
    };
    for (index, mapping) in value_mappings.iter().enumerate() {
        let LocalConstant::Scalar { expr_id, .. } = &mapping.constants[0] else {
            panic!("scalar");
        };
        let StaticExprKind::Constant(value) = graph.expressions().node(*expr_id).unwrap().kind()
        else {
            panic!("CV");
        };
        assert_eq!(value.ordinal(), if index == 0 { 1 } else { 2 });
        assert!(Arc::ptr_eq(
            value.pool().field_ref(),
            source.package.constants().entries()[&source.selected.pool].field_ref()
        ));
        assert_eq!(
            value
                .is_null_observed(CompilePhase::Validate, &Control)
                .unwrap(),
            index == 1
        );
        let DataType::List(field) = &value.value_type().data_type() else {
            panic!("exact List");
        };
        assert!(field.metadata().is_empty());
        assert!(field.is_nullable());
        let DataType::List(source_field) = value.pool().field_ref().data_type() else {
            panic!("original source List");
        };
        assert_eq!(
            source_field.metadata().get("original-child").unwrap(),
            "preserved"
        );
    }
    let expected = &source.package.fragment().values()[&source.package.fragment().nodes()
        [&source.unpivot]
        .output
        .columns[3]]
        .ty;
    let actual = program
        .checked()
        .channels()
        .channel_type(ProgramChannelSite::Layout {
            node: ProgramNodeId::new(3),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 3,
        })
        .unwrap();
    assert!(
        actual
            .exactly_equals_observed::<crate::unpivot::UnpivotLoweringError>(expected, || Control
                .checkpoint(CompilePhase::Validate, 0)
                .map_err(Into::into))
            .unwrap()
    );
    let DataType::List(field) = node.output_layout().schema().field(3).data_type() else {
        panic!("original metadata");
    };
    assert!(field.metadata().is_empty());
}
#[test]
fn unpivot_downstream_value_uses_actual_output_ordinal_and_keeps_independent_passthrough_slot() {
    let source = fixture(Shape::Valid, 2, true).unwrap();
    assert!(source.downstream);
    let program = compile(&source, &functions(), &Control).unwrap();
    let graph = program.graph();
    let node = &graph.nodes()[3];
    let final_node = &graph.nodes()[4];
    let ProgramNodeKind::Project { exprs, .. } = final_node.kind() else {
        panic!("actual downstream Project");
    };
    assert!(
        matches!(graph.expressions().node(exprs[0]).unwrap().kind(),StaticExprKind::SlotId(slot)if *slot==node.output_layout().slots()[0])
    );
    assert!(
        matches!(graph.expressions().node(exprs[1]).unwrap().kind(),StaticExprKind::SlotId(slot)if *slot==node.output_layout().slots()[2])
    );
    for ordinal in [0, 2] {
        assert!(program.checked().slots().values().any(|source| *source
            == ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                node: ProgramNodeId::new(3),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal
            })));
    }
    assert_ne!(
        node.output_layout().slots()[0],
        node.output_layout().slots()[1]
    );
    assert_eq!(
        final_node.output_layout().schema().field(0).name(),
        "selected_0"
    );
    assert!(final_node.output_layout().schema().field(1).is_nullable());
}
#[test]
fn unpivot_real_static_author_refuses_nonliteral_constant_domain_nullability_map_order_and_zero_bounds()
 {
    for shape in [
        Shape::EmptyMappings,
        Shape::BadPassthrough,
        Shape::BadValueNullability,
        Shape::BadScalar,
        Shape::BadMapOrder,
        Shape::ZeroBounds,
    ] {
        let error = match fixture(shape, 2, false) {
            Ok(_) => panic!("malformed static source must be refused by its original owner"),
            Err(error) => error,
        };
        let expected = match shape {
            Shape::EmptyMappings => "unpivot requires mappings",
            Shape::BadPassthrough => "unpivot passthrough changes the value type",
            Shape::BadValueNullability => "unpivot value output nullability differs",
            Shape::BadScalar => "unpivot constant type differs",
            Shape::BadMapOrder => "unpivot map keys must be non-empty and strictly increasing",
            Shape::ZeroBounds => "unpivot row/byte bounds are zero",
            _ => unreachable!(),
        };
        assert!(
            error.to_string().contains(expected),
            "original semantic fault must remain: {error}"
        );
        assert!(
            error
                .downcast_ref::<novarocks_physical_plan::FragmentStructureError>()
                .is_some()
                || error
                    .downcast_ref::<novarocks_physical_plan::FragmentPackageError>()
                    .is_some(),
            "unexpected original rejection: {error}"
        );
    }
}
#[test]
fn unpivot_special_reference_borrows_nonnullable_source_under_nullable_joined_output() {
    let source = fixture(Shape::JoinedCollectionNullable, 2, false).unwrap();
    let source_node = &source.package.fragment().nodes()[&source.unpivot];
    let NodeKind::Unpivot { spec } = &source_node.kind else {
        panic!("physical Unpivot")
    };
    let UnpivotConstant::Int32List(reference) = &spec.mappings[0].constants[1] else {
        panic!("source reference")
    };
    let original = &source.package.constants().entries()[&reference.pool];
    assert_eq!(reference.ordinal, 1);
    assert!(!original.value_type().nullable());
    assert!(
        source.package.fragment().values()[&spec.literal_outputs[1]]
            .ty
            .nullable()
    );
    assert_eq!(original.resource_facts().rows, 3);
    let program = compile(&source, &functions(), &Control).unwrap();
    let graph = program.graph();
    let node = &graph.nodes()[3];
    let ProgramNodeKind::Unpivot { value_mappings, .. } = node.kind() else {
        panic!("compiled Unpivot")
    };
    assert!(
        matches!(&value_mappings[0].constants[1], LocalConstant::Int32List(values) if values == &vec![0,-2])
    );
    let LocalConstant::Scalar { expr_id, nullable } = &value_mappings[1].constants[1] else {
        panic!("individual Scalar NULL")
    };
    assert!(*nullable);
    let StaticExprKind::Constant(value) = graph.expressions().node(*expr_id).unwrap().kind() else {
        panic!("original CV")
    };
    assert_eq!(value.ordinal(), 2);
    assert!(
        value
            .is_null_observed(CompilePhase::Validate, &Control)
            .unwrap()
    );
    assert!(node.output_layout().schema().field(4).is_nullable());
    // The source promise and original field/backing have not been retagged to
    // the joined output's true nullable metadata.
    assert!(!original.value_type().nullable());
    assert_eq!(original.field_ref().name(), "original-special-list");
}

#[test]
fn unpivot_special_reference_missing_pool_and_ordinal_are_exact_package_errors() {
    for shape in [Shape::MissingCollectionPool, Shape::BadCollectionOrdinal] {
        let error = match fixture(shape, 2, false) {
            Ok(_) => panic!("invalid actual source reference must not publish a package"),
            Err(error) => error,
        };
        let Some(FragmentPackageError::Constant(error)) =
            error.downcast_ref::<FragmentPackageError>()
        else {
            panic!("reference fault must reach its exact package owner: {error}");
        };
        match shape {
            Shape::MissingCollectionPool => assert!(matches!(error,
                ConstantReferenceError::MissingPool(id) if id.get()==u32::MAX-2)),
            Shape::BadCollectionOrdinal => assert!(matches!(
                error,
                ConstantReferenceError::Constant(ConstantError::Invalid(
                    "constant ordinal is outside its pool"
                ))
            )),
            _ => unreachable!(),
        }
    }
}

#[test]
fn unpivot_special_reference_source_full_type_and_nullable_promise_are_mandatory() {
    for shape in [Shape::BadCollectionType, Shape::BadCollectionNullable] {
        let error = match fixture(shape, 2, false) {
            Ok(_) => panic!("an invalid original special source must not publish a package"),
            Err(error) => error,
        };
        assert!(
            matches!(
                error.downcast_ref::<FragmentPackageError>(),
                Some(FragmentPackageError::Constant(
                    ConstantReferenceError::InvalidConsumer(
                        "Unpivot collection source differs from its exact non-null special type"
                    )
                ))
            ),
            "actual source profile must refuse before output or selected payload: {error}"
        );
    }
}

struct Trace {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    at: Option<usize>,
    cause: CompileControlError,
    refused: AtomicBool,
}
impl Trace {
    fn new(at: Option<usize>, cause: CompileControlError) -> Self {
        Self {
            events: Mutex::new(vec![]),
            at,
            cause,
            refused: AtomicBool::new(false),
        }
    }
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(
            !self.refused.load(Ordering::SeqCst),
            "no checkpoint after original refusal"
        );
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        events.push((phase, units));
        if self.at == Some(at) {
            self.refused.store(true, Ordering::SeqCst);
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}

#[test]
fn unpivot_full_compiler_preserves_each_original_control_prefix_and_ordinary_failure_tail() {
    let source = fixture(Shape::Valid, 2, false).unwrap();
    let functions = functions();
    for invalid_dop in [false, true] {
        let invoke = |control: &dyn PureCompileControl| {
            if invalid_dop {
                let providers =
                    PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control)
                        .unwrap();
                let validated =
                    validate_fragment_providers(source.package.clone(), &providers, &Control)
                        .unwrap();
                let mut options = options();
                options.pipeline_dop = NonZeroUsize::new(2).unwrap();
                compile_fragment(validated, &functions, options, control)
            } else {
                compile(&source, &functions, control)
            }
        };
        let trace = Trace::new(None, CompileControlError::Cancelled);
        let result = invoke(&trace);
        if invalid_dop {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
        let expected = trace.events.into_inner().unwrap();
        assert!(!expected.is_empty());
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..expected.len() {
                let control = Trace::new(Some(at), cause.clone());
                assert!(
                    matches!(invoke(&control),Err(FragmentCompileError::Control(actual))if actual==cause)
                );
                assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
            }
        }
    }
}
#[test]
fn unpivot_wide_actual_mapping_compilation_crosses_quantum_and_keeps_entry_tail_first_refusal() {
    let source = fixture(Shape::Valid, 320, false).unwrap();
    let functions = functions();
    let trace = Trace::new(None, CompileControlError::Cancelled);
    let program = compile(&source, &functions, &trace).unwrap();
    let ProgramNodeKind::Unpivot { value_mappings, .. } = program.graph().nodes()[3].kind() else {
        panic!("actual Unpivot");
    };
    assert_eq!(value_mappings.len(), 320);
    for index in [0, 1, 255, 256, 319] {
        assert!(
            matches!(&value_mappings[index].constants[1],LocalConstant::Int32List(values)if values==&vec![index as i32,-2])
        );
    }
    let expected = trace.events.into_inner().unwrap();
    let quantum = expected
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("real mapping compilation crosses 256");
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, quantum, expected.len() - 1] {
            let control = Trace::new(Some(at), cause.clone());
            assert!(
                matches!(compile(&source,&functions,&control),Err(FragmentCompileError::Control(actual))if actual==cause)
            );
            assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
        }
    }
}

#[test]
fn unpivot_literal_source_keeps_individual_nullability_under_joined_nullable_output() {
    let source = fixture(Shape::LiteralSource, 2, false).unwrap();
    let program = compile(&source, &functions(), &Control).unwrap();
    let graph = program.graph();
    let ProgramNodeKind::Unpivot { value_mappings, .. } = graph.nodes()[3].kind() else {
        panic!("actual Unpivot");
    };
    let LocalConstant::Scalar { expr_id, nullable } = &value_mappings[0].constants[0] else {
        panic!("literal");
    };
    assert!(!*nullable);
    let StaticExprKind::Constant(value) = graph.expressions().node(*expr_id).unwrap().kind() else {
        panic!("shared literal admission owner");
    };
    assert_eq!(value.try_utf8().unwrap(), Some("authored-literal"));
    assert!(!value.value_type().nullable());
    let LocalConstant::Scalar { expr_id, nullable } = &value_mappings[1].constants[0] else {
        panic!("CV NULL");
    };
    assert!(*nullable);
    let StaticExprKind::Constant(value) = graph.expressions().node(*expr_id).unwrap().kind() else {
        panic!("original CV");
    };
    assert_eq!(value.ordinal(), 2);
    assert!(
        value
            .is_null_observed(CompilePhase::Validate, &Control)
            .unwrap()
    );
    assert!(
        graph.nodes()[3]
            .output_layout()
            .schema()
            .field(3)
            .is_nullable()
    );
}
