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

// Mechanical, local test inputs use the original checked constant factories.
use novarocks_functions::{ConstantPolicy, ConstantValue};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::Arc;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
pub(crate) fn boolean(value: bool) -> ConstantValue {
    let ty = FunctionValueType::new(arrow_schema::DataType::Boolean, false);
    ConstantValue::from_boolean(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty,
        value,
        policy(),
        CompilePhase::LowerProgram,
        &Control,
    )
    .unwrap()
}
pub(crate) fn integer(value: i64) -> ConstantValue {
    let ty = FunctionValueType::new(arrow_schema::DataType::Int64, false);
    ConstantValue::from_i64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty,
        value,
        policy(),
        CompilePhase::LowerProgram,
        &Control,
    )
    .unwrap()
}
pub(crate) fn int32(value: i32) -> ConstantValue {
    let ty = FunctionValueType::new(arrow_schema::DataType::Int32, false);
    ConstantValue::from_i32(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty,
        value,
        policy(),
        CompilePhase::LowerProgram,
        &Control,
    )
    .unwrap()
}
pub(crate) fn utf8(value: &str) -> ConstantValue {
    let ty = FunctionValueType::new(arrow_schema::DataType::Utf8, false);
    ConstantValue::from_utf8(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty,
        value,
        policy(),
        CompilePhase::LowerProgram,
        &Control,
    )
    .unwrap()
}
pub(crate) fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1_000_000,
        max_array_nodes: 4096,
        max_logical_elements: 16_000_000,
        max_retained_buffer_bytes: 64 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 1024 * 1024 * 1024,
        max_library_validation_bytes: 64 * 1024 * 1024,
    }
}

pub(crate) fn integer_typed(value: i64, ty: FunctionValueType) -> ConstantValue {
    ConstantValue::from_i64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty,
        value,
        policy(),
        CompilePhase::LowerProgram,
        &Control,
    )
    .unwrap()
}
pub(crate) fn int32_typed(value: i32, ty: FunctionValueType) -> ConstantValue {
    ConstantValue::from_i32(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty,
        value,
        policy(),
        CompilePhase::LowerProgram,
        &Control,
    )
    .unwrap()
}
/// A real checked scalar unit used by fragment fixtures. Inputs carry explicit
/// complete value facts; no function claim or runtime binding is synthesized.
pub(crate) fn read_unit(
    layout: &crate::StaticLayout,
    ordinal_types: &[FunctionValueType],
    values: &[(novarocks_expression_contract::ValueId, FunctionValueType)],
    roots: &[(usize, novarocks_type_contract::EvaluationDemand)],
) -> Arc<novarocks_expression::CompiledExprSet> {
    use novarocks_expression::*;
    use novarocks_expression_contract::*;
    use novarocks_type_contract::*;
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::LowerProgram).unwrap();
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: MAX_CONTROL_DEPTH,
    });
    let mut definitions = std::collections::BTreeMap::new();
    for (definition, demand) in roots {
        let id = *definitions.entry(*definition).or_insert_with(|| {
            let (value, ty) = &values[*definition];
            builder
                .add(None, ty.clone(), ExprKind::Value(*value))
                .unwrap()
        });
        builder.root(id, *demand, RootScope::Port(0)).unwrap();
    }
    let unit = builder
        .seal(
            vec![ExprPort {
                values: values.to_vec().into_boxed_slice(),
            }],
            &mut work,
        )
        .unwrap()
        .finish(std::collections::BTreeMap::new(), &mut work)
        .unwrap();
    let scope = ScopeLayout::try_new(
        layout.schema().clone(),
        Arc::from(ordinal_types),
        values
            .iter()
            .enumerate()
            .map(|(ordinal, (id, ty))| ScopeColumn {
                value: *id,
                ordinal: ordinal as u32,
                ty: ty.clone(),
            })
            .collect(),
        ValueTypeStorageLifetime::pure(),
        &mut work,
    )
    .unwrap();
    let pools = novarocks_constant_contract::ConstantPools::empty();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let catalog = actual_rng_catalogue();
    compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &pools,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[scope],
            all_ports: None,
        },
        policy(),
        &catalog,
        &mut work,
    )
    .unwrap()
}
pub(crate) fn constant_unit(
    values: &[ConstantValue],
    roots: &[(usize, novarocks_type_contract::EvaluationDemand)],
) -> Arc<novarocks_expression::CompiledExprSet> {
    use novarocks_constant_contract::*;
    use novarocks_expression::*;
    use novarocks_expression_contract::*;
    use novarocks_type_contract::*;
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::LowerProgram).unwrap();
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: MAX_CONTROL_DEPTH,
    });
    let mut pools = ConstantPools::empty();
    let mut definitions = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let pool = ConstantPoolId::new(index as u32);
        pools.insert(pool, value.pool().clone()).unwrap();
        definitions.push(
            builder
                .add(
                    None,
                    value.value_type().clone(),
                    ExprKind::Constant(ConstantReference {
                        pool,
                        ordinal: value.ordinal(),
                    }),
                )
                .unwrap(),
        );
    }
    for (definition, demand) in roots {
        builder
            .root(definitions[*definition], *demand, RootScope::AllPorts)
            .unwrap();
    }
    let unit = builder
        .seal(vec![], &mut work)
        .unwrap()
        .finish(std::collections::BTreeMap::new(), &mut work)
        .unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let catalog = actual_rng_catalogue();
    compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &pools,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        policy(),
        &catalog,
        &mut work,
    )
    .unwrap()
}
pub(crate) fn root(unit: usize, root: u32) -> crate::ProgramRootRef {
    crate::ProgramRootRef::new(
        crate::ProgramUnitId::new(unit),
        novarocks_expression_contract::RootOrdinal::new(root),
    )
}

pub(crate) fn actual_rng_catalogue() -> novarocks_functions::PureEngineFunctionCatalog {
    use novarocks_functions::*;
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let definition = actual
        .definition("rand", FunctionKind::Scalar)
        .unwrap()
        .clone();
    // Retain the actual owner/resolver from production registration. These two
    // independent installed records describe the real RNG CPU implementation,
    // not declaration iteration masquerading as a Server installation seal.
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    let function = FunctionId::try_new("builtin.scalar/rand/v1").unwrap();
    let implementation = PureImplementationId::try_new("builtin.scalar/rand/selected-v1").unwrap();
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i8)->i8;strict;coerce",
                "builtin.scalar/rand/(i16)->i16;strict;coerce",
                "builtin.scalar/rand/(i32)->i32;strict;coerce",
                "builtin.scalar/rand/(i64)->i64;strict;coerce",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: function.clone(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: implementation.clone(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}

/// Materialize the real input roots used by relation-call fixtures.
pub(crate) fn argument_unit(
    types: &[FunctionValueType],
) -> Arc<novarocks_expression::CompiledExprSet> {
    let values: Vec<_> = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            (
                novarocks_expression_contract::ValueId::new(ordinal as u32),
                ty.clone(),
            )
        })
        .collect();
    let schema = Arc::new(arrow_schema::Schema::new(
        types
            .iter()
            .enumerate()
            .map(|(ordinal, ty)| ty.try_to_field(format!("argument_{ordinal}")).unwrap())
            .collect::<Vec<_>>(),
    ));
    let layout = crate::StaticLayout::try_new(
        schema,
        types
            .iter()
            .enumerate()
            .map(|(ordinal, _)| novarocks_types::SlotId::new(ordinal as u32))
            .collect::<Vec<_>>()
            .into(),
    )
    .unwrap();
    let roots: Vec<_> = types
        .iter()
        .enumerate()
        .map(|(ordinal, _)| (ordinal, novarocks_type_contract::EvaluationDemand::Value))
        .collect();
    read_unit(&layout, types, &values, &roots)
}

/// A real invocation-only owner keeps a large reachable static subtree inert.
/// This isolates the original fragment definition sum from the use-reference sum.
pub(crate) fn large_static_unit(count: usize) -> Arc<novarocks_expression::CompiledExprSet> {
    use arrow_schema::{DataType, Field};
    use novarocks_constant_contract::*;
    use novarocks_expression::*;
    use novarocks_expression_contract::*;
    use novarocks_functions::*;
    use novarocks_type_contract::*;
    use std::collections::BTreeMap;
    assert!(count >= 6);
    use novarocks_functions::{
        CallArgumentUses, CallEffectInput, FunctionArgument, FunctionBindingRequest,
        PureCallPreparation, ScopedExpressionEffects,
    };
    let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let mut registry = EngineFunctionCatalogBuilder::new();
    registry
        .register(
            actual
                .definition("__array_struct_subfield", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let catalog = registry
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/__array_struct_subfield/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(
                    "builtin.scalar/__array_struct_subfield/dynamic-v1",
                )
                .unwrap(),
                implementation: PureImplementationId::try_new(
                    "builtin.scalar/__array_struct_subfield/selected-v1",
                )
                .unwrap(),
                abi: PureKernelAbi::ScalarInvocationV1,
            },
            aggregate_state_format: None,
        }])
        .unwrap();
    let source_type = ValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::new(Field::new("item", DataType::Int32, true))].into()),
            true,
        ))),
        true,
    );
    let field_type = ValueType::new(DataType::Utf8, true);
    let extra_type = ValueType::new(DataType::Boolean, false);
    let field = ConstantValue::from_utf8(
        Arc::new(Field::new("field", DataType::Utf8, true)),
        field_type.clone(),
        "item",
        policy(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(12), field.pool().clone())
        .unwrap();
    let arguments = [
        FunctionArgument::Value {
            value_type: source_type.clone(),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: field_type.clone(),
            constant: Some(field.clone()),
        },
        FunctionArgument::Value {
            value_type: extra_type.clone(),
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 3,
        expected_result_type: None,
    };
    let c = Control;
    let bound = catalog
        .metadata()
        .resolve_bound_user("__array_struct_subfield", FunctionKind::Scalar, request, &c)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("scalar")
    };
    let mut b = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: MAX_CONTROL_DEPTH,
    });
    let deciding = crate::test_support::boolean(true);
    pools
        .insert(ConstantPoolId::new(13), deciding.pool().clone())
        .unwrap();
    let static_children = (0..count - 4)
        .map(|_| {
            b.add(
                None,
                extra_type.clone(),
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(13),
                    ordinal: deciding.ordinal(),
                }),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let ids = [
        b.add(None, source_type.clone(), ExprKind::Value(ValueId::new(1)))
            .unwrap(),
        b.add(
            None,
            field_type.clone(),
            ExprKind::Constant(ConstantReference {
                pool: ConstantPoolId::new(12),
                ordinal: field.ordinal(),
            }),
        )
        .unwrap(),
        b.add(
            None,
            extra_type.clone(),
            ExprKind::Conjunction {
                args: static_children.into_boxed_slice(),
            },
        )
        .unwrap(),
    ];
    let call = b
        .add(
            None,
            result.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction::from_exact_signature(
                    bound.function_id.clone(),
                    selected.overload.clone(),
                    FunctionKind::Scalar,
                    selected.argument_types.clone(),
                    result.clone(),
                ),
                args: Box::new(ids),
            },
        )
        .unwrap();
    b.request(
        call,
        PhysicalCallRequest {
            arguments: Box::new([
                novarocks_function_contract::FunctionArgument::Value {
                    value_type: source_type.clone(),
                    constant: None,
                },
                novarocks_function_contract::FunctionArgument::Value {
                    value_type: field_type,
                    constant: Some(ConstantReference {
                        pool: ConstantPoolId::new(12),
                        ordinal: field.ordinal(),
                    }),
                },
                novarocks_function_contract::FunctionArgument::Value {
                    value_type: extra_type.clone(),
                    constant: None,
                },
            ]),
            logical_argument_count: 3,
            expected_result_type: None,
        },
    )
    .unwrap();
    b.definition_control(
        call,
        DefinitionControl {
            shape: ControlShape::NoArguments,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b
        .seal(
            vec![ExprPort {
                values: Box::new([(ValueId::new(1), source_type.clone())]),
            }],
            &mut w,
        )
        .unwrap();
    let use_id = sealed.root_use_ids()[0];
    let context = sealed.uses()[&use_id].context;
    let parameters = SemanticParameters::try_new([]).unwrap();
    let token = catalog
        .prepare_fresh(
            CallEffectInput {
                context: context.into(),
                argument_uses: CallArgumentUses::SelectedChannels(&[None, None, None]),
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                proof_scope: CallProofScope::Region(context.region),
            },
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &c,
        )
        .unwrap();
    assert_eq!(
        token.call_contract().effects().argument_control,
        ArgumentControl::NoArguments
    );
    let u = sealed
        .finish(
            BTreeMap::from([(
                use_id,
                ExpressionCallClaim {
                    context,
                    effects: token.call_contract().effects().clone(),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            )]),
            &mut w,
        )
        .unwrap();
    w.finish().unwrap();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let scope = ScopeLayout::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            source_type.try_to_field("source").unwrap(),
        ])),
        Arc::from([source_type.clone()]),
        Arc::from([ScopeColumn {
            value: ValueId::new(1),
            ordinal: 0,
            ty: source_type,
        }]),
        ValueTypeStorageLifetime::pure(),
        &mut w,
    )
    .unwrap();
    let scopes = [scope];
    let p = compile_expr_set_pure(
        &u,
        EvaluationEnvironment {
            constants: &pools,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &scopes,
            all_ports: None,
        },
        policy(),
        &catalog,
        &mut w,
    )
    .unwrap();
    w.finish().unwrap();
    assert_eq!(p.definitions().nodes().len(), count);
    assert_eq!(p.types().len(), count);
    assert_eq!(p.control_flow().uses().len(), 1);
    assert!(p.control_flow().uses()[&use_id].arguments.is_empty());
    assert!(p.input(use_id).is_none());
    assert_eq!(p.control_flow().use_reference_count(), 1);
    p
}
