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

use super::*;
use crate::values::lower_values;
use crate::{
    FragmentCompileError, LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use arrow_array::{Float32Array, StringArray};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_local_program::{
    KernelAbiVersion, LocalProgram, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramExpressionArena, ProgramNodeId, ProgramNodeKind,
};
use novarocks_physical_plan::{ExprKind, FragmentPackage};
use novarocks_type_contract::{FunctionArgumentType, ValueLogicalType};
use novarocks_types::SlotId;
use std::num::NonZeroUsize;

pub(crate) fn checked_pool(
    array: ArrayRef,
    nullable: bool,
    logical: ValueLogicalType,
) -> ConstantPool {
    let ty = FunctionValueType::try_with_logical_type(array.data_type().clone(), nullable, logical)
        .unwrap();
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("source-original").unwrap()),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap()
}
pub(crate) fn compile_options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: std::time::Duration::from_secs(120),
        constants: policy(),
    }
}
pub(crate) fn providers(package: Arc<FragmentPackage>) -> crate::ProviderValidatedFragment {
    let catalog =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control::good())
            .unwrap();
    validate_fragment_providers(package, &catalog, &Control::good()).unwrap()
}
pub(crate) fn build(
    pools: &[ConstantPool],
    rows: &[Vec<(usize, u32)>],
    column_pools: &[usize],
    chain: bool,
) -> Arc<FragmentPackage> {
    let types: Vec<_> = column_pools
        .iter()
        .map(|&index| pools[index].value_type().clone())
        .collect();
    build_typed(pools, rows, &types, chain)
}
pub(super) fn build_typed(
    pools: &[ConstantPool],
    rows: &[Vec<(usize, u32)>],
    column_types: &[FunctionValueType],
    chain: bool,
) -> Arc<FragmentPackage> {
    let mut constants = ConstantPools::empty();
    for (index, pool) in pools.iter().enumerate() {
        // The real package namespace is closed: empty Values has output types
        // but no selected constants, so do not publish an unused pool.
        if !rows
            .iter()
            .any(|row| row.iter().any(|(source, _)| *source == index))
        {
            continue;
        }
        let id = if index == 0 {
            0
        } else if index == 1 {
            u32::MAX
        } else {
            index as u32
        };
        constants
            .insert(ConstantPoolId::new(id), pool.clone())
            .unwrap();
    }
    let values_node = NodeId::new(u32::MAX);
    let mut builder = FragmentBuilder::new(FragmentId::new(171));
    let mut outputs = Vec::new();
    for (ordinal, ty) in column_types.iter().enumerate() {
        outputs.push(
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: values_node,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap(),
        );
    }
    let mut actual_rows = Vec::new();
    for row in rows {
        let mut cells = Vec::new();
        for &(index, ordinal) in row {
            let pool_id = if index == 0 {
                0
            } else if index == 1 {
                u32::MAX
            } else {
                index as u32
            };
            cells.push(
                builder
                    .add_expression(
                        values_node,
                        pools[index].value_type().clone(),
                        ExprKind::Constant(ConstantReference {
                            pool: ConstantPoolId::new(pool_id),
                            ordinal,
                        }),
                    )
                    .unwrap(),
            );
        }
        actual_rows.push(cells.into_boxed_slice());
    }
    builder
        .add_values(
            values_node,
            actual_rows.into_boxed_slice(),
            outputs.clone().into_boxed_slice(),
        )
        .unwrap();
    let root = if chain {
        let filter = NodeId::new(0);
        let predicate = builder
            .add_expression(
                filter,
                FunctionValueType::new(DataType::Boolean, false),
                ExprKind::Literal(novarocks_physical_plan::LiteralValue::Boolean(true)),
            )
            .unwrap();
        builder
            .add_filter(filter, values_node, Box::from([predicate]))
            .unwrap();
        let project = NodeId::new(7);
        let mut assignments = Vec::new();
        let mut projected = Vec::new();
        for &input in &outputs {
            let ty = builder.value(input).unwrap().ty.clone();
            let expr = builder
                .add_expression(project, ty.clone(), ExprKind::Value(input))
                .unwrap();
            let output = builder
                .add_value(
                    ty,
                    ValueOrigin::Expr {
                        node: project,
                        expr,
                    },
                )
                .unwrap();
            assignments.push((expr, output));
            projected.push(output);
        }
        builder
            .add_project(
                project,
                filter,
                assignments.into_boxed_slice(),
                projected.into_boxed_slice(),
            )
            .unwrap();
        project
    } else {
        values_node
    };
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control::good()).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut invocations = Vec::new();
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(position, (&site, root))| {
            let id = match position {
                0 => 0,
                1 => u32::MAX,
                n => n as u32 - 1,
            };
            let use_id = ExpressionUseId::new(id);
            invocations.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain,
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, use_id)
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let expression_uses =
        PhysicalRootUses::try_new(&fragment, flow, bindings, &Control::good()).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &Control::good())
        .unwrap();
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &Control::good()).unwrap();
    let fields = fragment.nodes()[&root]
        .output
        .columns
        .iter()
        .enumerate()
        .map(|(index, &value)| ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: format!("result-{index}").into(),
            alias: Some(format!("alias-{index}").into()),
            value,
            ty: fragment.values()[&value].ty.clone(),
        })
        .collect();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: fragment.nodes()[&root].output.clone(),
        fields,
    };
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([171; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment,
                expression_uses,
                calls,
                constants,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning,
            },
            package_admission(),
            &Control::good(),
        )
        .unwrap(),
    )
}
fn compile(
    package: Arc<FragmentPackage>,
    control: &dyn PureCompileControl,
) -> Result<LocalProgram, FragmentCompileError> {
    compile_fragment(providers(package), &functions(), compile_options(), control)
}
fn batch(program: &LocalProgram) -> &arrow_array::RecordBatch {
    let ProgramNodeKind::Values { values } = program.graph().nodes()[0].kind() else {
        panic!("Values")
    };
    values.batch().unwrap()
}
fn direct(
    package: &FragmentPackage,
    lowered: &LoweredExpressions,
    control: &dyn PureCompileControl,
) -> Result<(ProgramNodeKind, novarocks_local_program::StaticLayout), FragmentCompileError> {
    let node = &package.fragment().nodes()[&NodeId::new(u32::MAX)];
    let slots: Vec<_> = (0..node.output.columns.len())
        .map(|index| SlotId::new(index as u32))
        .collect();
    lower_values(package, node, lowered, &slots, control)
}
fn is_control(error: FragmentCompileError, cause: CompileControlError) {
    assert!(matches!(error, FragmentCompileError::Control(actual) if actual == cause));
}

#[test]
fn values_materialize_real_rows_columns_selected_ordinals_float_bits_and_json() {
    let integers = checked_pool(
        Arc::new(Int64Array::from(vec![Some(900), None, Some(-7), Some(42)])),
        true,
        ValueLogicalType::Physical,
    );
    let bits = [0x7f80_4321, 0x8000_0000, 0x0000_0000, 0x3f80_0000];
    let floats = checked_pool(
        Arc::new(Float32Array::from(bits.map(f32::from_bits).to_vec())),
        false,
        ValueLogicalType::Physical,
    );
    let strings = checked_pool(
        Arc::new(StringArray::from(vec!["{}", "{\"x\":1}", "null", "[2]"])),
        false,
        ValueLogicalType::Json,
    );
    let package = build(
        &[integers.clone(), floats.clone(), strings.clone()],
        &[
            vec![(0, 3), (1, 0), (2, 1)],
            vec![(0, 1), (1, 1), (2, 2)],
            vec![(0, 2), (1, 2), (2, 3)],
        ],
        &[0, 1, 2],
        false,
    );
    let program = compile(package.clone(), &Control::good()).unwrap();
    let batch = batch(&program);
    assert_eq!((batch.num_rows(), batch.num_columns()), (3, 3));
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        column.iter().collect::<Vec<_>>(),
        vec![Some(42), None, Some(-7)]
    );
    let column = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(
        column
            .values()
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        vec![bits[0], bits[1], bits[2]]
    );
    let column = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        column.iter().collect::<Vec<_>>(),
        vec![Some("{\"x\":1}"), Some("null"), Some("[2]")]
    );
    assert_eq!(batch.schema().field(0).name(), "alias-0");
    assert_eq!(
        FunctionValueType::try_from_field(batch.schema().field(2))
            .unwrap()
            .logical_type,
        ValueLogicalType::Json
    );
    assert!(
        program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot()
            .bindings()
            .is_empty()
    );
    // Original CV sources remain in the expression owner; output copying never
    // reauthors a pool or changes the selected row/source-field identity.
    for source in program.graph().expressions().nodes() {
        if let StaticExprKind::Constant(value) = source.kind() {
            assert!(
                [
                    integers.backing_identity(),
                    floats.backing_identity(),
                    strings.backing_identity()
                ]
                .contains(&value.pool().backing_identity())
            );
            assert_ne!(value.ordinal(), u32::MAX);
        }
    }
}

#[test]
fn values_multiple_backings_use_actual_interleave_choices_and_nulls() {
    let first = checked_pool(
        Arc::new(Int64Array::from(vec![Some(91), Some(12), None])),
        true,
        ValueLogicalType::Physical,
    );
    let second = checked_pool(
        Arc::new(Int64Array::from(vec![Some(81), Some(-4), None])),
        true,
        ValueLogicalType::Physical,
    );
    let package = build(
        &[first, second],
        &[vec![(1, 1)], vec![(0, 1)], vec![(1, 2)], vec![(0, 2)]],
        &[0],
        false,
    );
    let program = compile(package, &Control::good()).unwrap();
    assert_eq!(
        batch(&program)
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-4), Some(12), None, None]
    );
    let first: ArrayRef = Arc::new(StringArray::from(vec![
        Some("skip"),
        Some("雪"),
        None,
        Some("é"),
    ]));
    let second: ArrayRef = Arc::new(StringArray::from(vec![Some("skip"), Some("λ"), None]));
    let first = checked_pool(first.slice(1, 3), true, ValueLogicalType::Physical);
    let second = checked_pool(second.slice(1, 2), true, ValueLogicalType::Physical);
    let program = compile(
        build(
            &[first, second],
            &[vec![(0, 2)], vec![(1, 0)], vec![(0, 1)]],
            &[0],
            false,
        ),
        &Control::good(),
    )
    .unwrap();
    assert_eq!(
        batch(&program)
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("é"), Some("λ"), None]
    );
}

#[test]
fn values_nested_fields_nulls_and_empty_rows_preserve_exact_admitted_types() {
    let child = Arc::new(
        Field::new("child-original", DataType::Int64, true)
            .with_metadata(HashMap::from([("nested.unknown".into(), "keep".into())])),
    );
    let nested: ArrayRef = Arc::new(StructArray::new(
        vec![child.clone()].into(),
        vec![Arc::new(Int64Array::from(vec![Some(90), None, Some(7)]))],
        None,
    ));
    let original = checked_pool(nested, false, ValueLogicalType::Physical);
    let package = build(
        std::slice::from_ref(&original),
        &[vec![(0, 2)], vec![(0, 1)]],
        &[0],
        false,
    );
    let program = compile(package, &Control::good()).unwrap();
    let array = batch(&program)
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        array
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(7), None]
    );
    let DataType::Struct(fields) = batch(&program).schema().field(0).data_type().clone() else {
        panic!("struct")
    };
    assert!(Arc::ptr_eq(&fields[0], &child));
    let empty = compile(
        build(std::slice::from_ref(&original), &[], &[0], false),
        &Control::good(),
    )
    .unwrap();
    assert_eq!(
        (batch(&empty).num_rows(), batch(&empty).num_columns()),
        (0, 1)
    );
    assert_eq!(
        batch(&empty).column(0).data_type(),
        original.array().data_type()
    );
    for rows in [0, 1, 3] {
        let program = compile(
            build(&[], &vec![vec![]; rows], &[], false),
            &Control::good(),
        )
        .unwrap();
        assert_eq!(
            (batch(&program).num_rows(), batch(&program).num_columns()),
            (rows, 0)
        );
    }
}

#[test]
fn values_project_filter_keep_sparse_source_channels_and_remaining_runtime_roots() {
    let source = integer_pool();
    let package = build(
        std::slice::from_ref(&source),
        &[vec![(0, 1)], vec![(0, 2)]],
        &[0],
        true,
    );
    let program = compile(package, &Control::good()).unwrap();
    assert_eq!(
        program
            .graph()
            .nodes()
            .iter()
            .map(|node| node.physical_sources()[0].get())
            .collect::<Vec<_>>(),
        vec![u32::MAX, 0, 7]
    );
    assert!(matches!(
        program.graph().nodes()[1].kind(),
        ProgramNodeKind::Filter { .. }
    ));
    assert!(matches!(
        program.graph().nodes()[2].kind(),
        ProgramNodeKind::Project { .. }
    ));
    let resolved = program.checked().channels().expressions().resolved_calls();
    assert_eq!(resolved.snapshot().bindings().len(), 2);
    let channels = program.checked().channels();
    assert_eq!(
        channels.channel_type(ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0
        }),
        Some(source.value_type())
    );
    let types = channels.expressions().types();
    assert!(
        types[&ProgramExpressionArena::Main]
            .iter()
            .all(|ty| matches!(ty, FunctionArgumentType::Value(_)))
    );
}

#[test]
fn values_original_control_every_materialization_callback_success_and_ordinary_tail() {
    let source = integer_pool();
    let package = build(
        std::slice::from_ref(&source),
        &[vec![(0, 1)], vec![(0, 2)]],
        &[0],
        false,
    );
    let lowered = lower(&package, &Control::good()).unwrap();
    let mut wrong = lower(&package, &Control::good()).unwrap();
    let first = *wrong.ids.values().next().unwrap();
    let ty = FunctionValueType::new(DataType::Int64, true);
    replace_constant(
        &mut wrong,
        first,
        ConstantValue::from_i64(
            Arc::new(ty.try_to_field("wrong").unwrap()),
            ty,
            42,
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap(),
    );
    for (lowered, ordinary) in [(&lowered, false), (&wrong, true)] {
        let baseline = Control::good();
        let result = direct(&package, lowered, &baseline);
        if ordinary {
            assert!(matches!(
                result,
                Err(FragmentCompileError::Invalid(
                    "Values cell full type differs from output"
                ))
            ));
        } else {
            assert!(result.is_ok());
        }
        let trace = baseline.trace();
        assert!(trace.last().unwrap().1 > 0 || !ordinary);
        for position in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refused = Control::refusing(position, cause);
                is_control(direct(&package, lowered, &refused).unwrap_err(), cause);
                assert_eq!(refused.trace(), trace[..position]);
            }
        }
    }
}

#[test]
fn values_wide_real_rows_observe_quantum_and_preserve_all_rows() {
    let source = integer_pool();
    let rows = vec![vec![(0, 1)]; 320];
    let package = build(std::slice::from_ref(&source), &rows, &[0], false);
    let lowered = lower(&package, &Control::good()).unwrap();
    let baseline = Control::good();
    let (kind, _) = direct(&package, &lowered, &baseline).unwrap();
    let ProgramNodeKind::Values { values } = kind else {
        panic!("values")
    };
    assert_eq!(values.batch().unwrap().num_rows(), 320);
    assert!(
        values
            .batch()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .iter()
            .all(|value| *value == 42)
    );
    let trace = baseline.trace();
    let positions: Vec<_> = trace
        .iter()
        .enumerate()
        .filter_map(|(index, (_, units))| (*units == 256).then_some(index + 1))
        .collect();
    assert!(!positions.is_empty());
    for position in positions.into_iter().chain([1, trace.len()]) {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refused = Control::refusing(position, cause);
            is_control(direct(&package, &lowered, &refused).unwrap_err(), cause);
            assert_eq!(refused.trace(), trace[..position]);
        }
    }
}

#[test]
fn values_cross_pool_nested_copy_refuses_explicitly_without_resource_or_control_fallback() {
    let field = Arc::new(Field::new("payload", DataType::Int64, false));
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![field].into(),
        vec![Arc::new(Int64Array::from(vec![10, 20]))],
        None,
    ));
    let first = checked_pool(array.clone(), false, ValueLogicalType::Physical);
    let second = checked_pool(array, false, ValueLogicalType::Physical);
    assert_ne!(first.backing_identity(), second.backing_identity());
    let package = build(&[first, second], &[vec![(0, 1)], vec![(1, 0)]], &[0], false);
    let lowered = lower(&package, &Control::good()).unwrap();
    let baseline = Control::good();
    assert!(
        matches!(direct(&package, &lowered, &baseline), Err(FragmentCompileError::Unsupported {
        node: Some(node), feature: "Values combined copy carrier lacks selected-copy preflight",
    }) if node == NodeId::new(u32::MAX))
    );
    let trace = baseline.trace();
    for position in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::refusing(position, cause);
            is_control(direct(&package, &lowered, &control).unwrap_err(), cause);
            assert_eq!(control.trace(), trace[..position]);
        }
    }
}

#[test]
fn values_empty_union_and_nested_empty_union_refuse_before_arrow_constructor() {
    let empty = DataType::Union(
        arrow_schema::UnionFields::empty(),
        arrow_schema::UnionMode::Sparse,
    );
    for data_type in [
        empty.clone(),
        DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            empty.clone(),
            true,
        ))),
        DataType::Struct(vec![Arc::new(arrow_schema::Field::new("child", empty, true))].into()),
    ] {
        let ty = FunctionValueType::new(data_type, true);
        let package = build_typed(&[], &[], &[ty], false);
        assert!(package.constants().entries().is_empty());
        let control = Control::good();
        assert!(
            matches!(compile(package, &control), Err(FragmentCompileError::Unsupported {
            node: Some(node), feature: "Values empty Union has no Arrow constructor child",
        }) if node == NodeId::new(u32::MAX))
        );
    }
}

#[test]
fn values_empty_malformed_map_refuses_before_arrow_constructor() {
    let ty = FunctionValueType::new(
        DataType::Map(
            Arc::new(arrow_schema::Field::new("entries", DataType::Int64, false)),
            false,
        ),
        true,
    );
    let package = build_typed(&[], &[], &[ty], false);
    assert!(package.constants().entries().is_empty());
    assert!(matches!(
        compile(package, &Control::good()),
        Err(FragmentCompileError::Unsupported {
            feature: "Values empty carrier has invalid Arrow constructor parameters",
            ..
        })
    ));
}

#[cfg(test)]
#[path = "values_cell_lowering_tests.rs"]
mod values_cell_lowering_tests;
