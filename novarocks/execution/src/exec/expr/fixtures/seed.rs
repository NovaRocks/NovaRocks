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

//! Checked RAND fixture with the original physical Project, Filter and Limit.
use super::{FixtureControl, options, package_admission};
use arrow::datatypes::DataType;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_expression::testkit::{
    ClaimAuthor, add_call, declare_call_controls, finish_unit, register_call_requests,
    register_constant,
};
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_functions::{
    ConstantValue, FunctionArgument, PureEngineFunctionCatalog, testkit::rng_subset,
};
use novarocks_local_compiler::{compile_fragment, validate_fragment_providers};
use novarocks_local_program::LocalProgram;
use novarocks_physical_plan::{
    FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage, FragmentPackageInput, FragmentSink,
    FrozenFragmentPruning, NodeId, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
pub(crate) enum RandArgument {
    Input,
    DirectConstant,
    Unbounded,
}

fn checked_source(
    pools: &mut novarocks_physical_plan::ConstantPools,
    value: &ConstantValue,
) -> ExprKind {
    ExprKind::Constant(register_constant(pools, value))
}
fn i64_source(
    pools: &mut novarocks_physical_plan::ConstantPools,
    ty: &FunctionValueType,
    value: i64,
) -> ExprKind {
    let value = ConstantValue::from_i64(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        value,
        options(1).constants,
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    checked_source(pools, &value)
}
fn bool_source(
    pools: &mut novarocks_physical_plan::ConstantPools,
    ty: &FunctionValueType,
    value: bool,
) -> ExprKind {
    let value = ConstantValue::from_boolean(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        value,
        options(1).constants,
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    checked_source(pools, &value)
}
fn null_source(
    pools: &mut novarocks_physical_plan::ConstantPools,
    ty: &FunctionValueType,
) -> ExprKind {
    let value = ConstantValue::null(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        options(1).constants,
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    checked_source(pools, &value)
}

fn package(
    functions: &PureEngineFunctionCatalog,
    mode: RandArgument,
    twin: bool,
) -> Arc<FragmentPackage> {
    package_with_dictionary(functions, mode, twin, false)
}

fn package_with_dictionary(
    functions: &PureEngineFunctionCatalog,
    mode: RandArgument,
    twin: bool,
    dictionary: bool,
) -> Arc<FragmentPackage> {
    let bound_type = FunctionValueType::new(
        DataType::Int64,
        !matches!(mode, RandArgument::DirectConstant),
    );
    let constant = matches!(mode, RandArgument::DirectConstant).then(|| {
        ConstantValue::from_i64(
            Arc::new(bound_type.try_to_field("fixture").unwrap()),
            bound_type.clone(),
            1,
            options(1).constants,
            CompilePhase::FunctionSpecialization,
            &FixtureControl,
        )
        .unwrap()
    });
    let arguments = match mode {
        RandArgument::Unbounded => Vec::new(),
        RandArgument::Input | RandArgument::DirectConstant => vec![FunctionArgument::Value {
            value_type: bound_type.clone(),
            constant: constant.clone(),
        }],
    };
    let mut constants = novarocks_physical_plan::ConstantPools::empty();
    if let Some(value) = &constant {
        constants
            .insert(
                novarocks_physical_plan::ConstantPoolId::new(u32::MAX),
                value.pool().clone(),
            )
            .unwrap();
    }
    let owner = ClaimAuthor::with_legacy_binding(
        functions,
        "rand",
        arguments,
        ControlShape::Eager,
        None,
        options(1).constants,
        &FixtureControl,
    )
    .with_decimal_policy(DecimalOverflowPolicy::OutputNull);
    let result_type = owner.result();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let fragment_id = FragmentId::new(72);
    let values_node = NodeId::new(u32::MAX);
    let first_project = NodeId::new(21);
    let filter = NodeId::new(3);
    let second_project = NodeId::new(8);
    let limit = NodeId::new(1);
    let bound_value = ValueId::new(100);
    let bool_value = ValueId::new(7);
    let sample_value = ValueId::new(200);
    let extra_value = ValueId::new(0);
    let twin_value = ValueId::new(201);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(values_node, Box::from([Box::default()]), Box::default())
        .unwrap();
    let extra_type = FunctionValueType::new(DataType::Int64, false);
    let extra_expr = builder
        .unit(first_project)
        .unwrap()
        .add(
            None,
            extra_type.clone(),
            i64_source(&mut constants, &extra_type, 999),
        )
        .unwrap();
    let bound_expr = builder
        .unit(first_project)
        .unwrap()
        .add(
            None,
            bound_type.clone(),
            i64_source(&mut constants, &bound_type, 1),
        )
        .unwrap();
    let bool_type = FunctionValueType::new(DataType::Boolean, false);
    let bool_expr = builder
        .unit(first_project)
        .unwrap()
        .add(
            None,
            bool_type.clone(),
            bool_source(&mut constants, &bool_type, true),
        )
        .unwrap();
    let mut first_definitions = vec![
        (extra_value, extra_type, extra_expr),
        (bound_value, bound_type.clone(), bound_expr),
        (bool_value, bool_type.clone(), bool_expr),
    ];
    if dictionary {
        let ty = FunctionValueType::new(
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        );
        let expr = builder
            .unit(first_project)
            .unwrap()
            .add(None, ty.clone(), null_source(&mut constants, &ty))
            .unwrap();
        first_definitions.push((ValueId::new(400), ty, expr));
    }
    let mut first_outputs = Vec::new();
    let mut first_values = Vec::new();
    for (id, ty, definition) in first_definitions {
        let root = builder
            .unit(first_project)
            .unwrap()
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        first_outputs.push((root, id));
        first_values.push(id);
        builder
            .insert_value(ValueDef {
                id,
                ty,
                origin: ValueOrigin::Expr {
                    node: first_project,
                    root,
                },
            })
            .unwrap();
    }
    // The extra actual output shifts the bound channel to ordinal one. ValueId
    // 100 must never become either its ordinal or a local SlotId.
    builder
        .add_project(
            first_project,
            values_node,
            first_outputs.into_boxed_slice(),
            first_values.into_boxed_slice(),
        )
        .unwrap();
    let predicate = builder
        .unit(filter)
        .unwrap()
        .add(None, bool_type, ExprKind::input(bool_value))
        .unwrap();
    let predicate = builder
        .unit(filter)
        .unwrap()
        .root(predicate, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    builder
        .add_filter(filter, first_project, Box::from([predicate]))
        .unwrap();
    let call_arguments = match mode {
        RandArgument::Unbounded => vec![],
        RandArgument::Input | RandArgument::DirectConstant => {
            let argument = match constant {
                Some(value) => checked_source(&mut constants, &value),
                None => ExprKind::input(bound_value),
            };
            vec![
                builder
                    .unit(second_project)
                    .unwrap()
                    .add(None, bound_type.clone(), argument)
                    .unwrap(),
            ]
        }
    };
    let mut authors = BTreeMap::new();
    let call = add_call(
        builder.unit(second_project).unwrap(),
        &mut authors,
        owner,
        call_arguments,
    );
    register_call_requests(
        builder.unit(second_project).unwrap(),
        &authors,
        &mut constants,
    );
    let call_root = builder
        .unit(second_project)
        .unwrap()
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    builder
        .insert_value(ValueDef {
            id: sample_value,
            ty: result_type.clone(),
            origin: ValueOrigin::Expr {
                node: second_project,
                root: call_root,
            },
        })
        .unwrap();
    let identity_a = builder
        .unit(second_project)
        .unwrap()
        .add(None, bound_type.clone(), ExprKind::input(bound_value))
        .unwrap();
    let identity_b = builder
        .unit(second_project)
        .unwrap()
        .add(None, bound_type.clone(), ExprKind::input(bound_value))
        .unwrap();
    let mut project_expressions = vec![(call_root, sample_value)];
    let mut project_output = vec![sample_value];
    if twin {
        let twin_root = builder
            .unit(second_project)
            .unwrap()
            .root(call, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: twin_value,
                ty: result_type.clone(),
                origin: ValueOrigin::Expr {
                    node: second_project,
                    root: twin_root,
                },
            })
            .unwrap();
        // One definition, two independent root uses and produced values.
        project_expressions.push((twin_root, twin_value));
        project_output.push(twin_value);
    }
    let identity_a = builder
        .unit(second_project)
        .unwrap()
        .root(identity_a, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let identity_b = builder
        .unit(second_project)
        .unwrap()
        .root(identity_b, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    project_expressions.extend([(identity_a, bound_value), (identity_b, bound_value)]);
    project_output.extend([bound_value, bound_value]);
    builder
        .add_project(
            second_project,
            filter,
            project_expressions.into_boxed_slice(),
            project_output.into_boxed_slice(),
        )
        .unwrap();
    builder
        .add_limit(limit, second_project, Some(1), 0)
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap();
    declare_call_controls(
        builder.unit(second_project).unwrap(),
        functions,
        &constants,
        &parameters,
        &mut work,
    );
    let no_authors = BTreeMap::new();
    for node in [first_project, filter, second_project] {
        let ports = builder.node_ports(node).unwrap();
        let sealed = builder
            .take_unit_builder(node)
            .unwrap()
            .seal(ports, &mut work)
            .unwrap();
        let unit = finish_unit(
            functions,
            sealed,
            if node == second_project {
                &authors
            } else {
                &no_authors
            },
            &constants,
            &parameters,
            &mut work,
        );
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
    work.finish().unwrap();
    let fragment = builder
        .finish_definition(
            limit,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut result_fields = vec![ResultField {
        domain: crate::test_result_domain::result_value_domain(&result_type),
        name: "sample".into(),
        alias: None,
        value: sample_value,
        ty: result_type.clone(),
    }];
    if twin {
        result_fields.push(ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "sample_twin".into(),
            alias: None,
            value: twin_value,
            ty: result_type,
        });
    }
    result_fields.extend([
        ResultField {
            domain: crate::test_result_domain::result_value_domain(&bound_type),
            name: "bound_a".into(),
            alias: None,
            value: bound_value,
            ty: bound_type.clone(),
        },
        ResultField {
            domain: crate::test_result_domain::result_value_domain(&bound_type),
            name: "bound_b".into(),
            alias: None,
            value: bound_value,
            ty: bound_type,
        },
    ]);
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&limit].output.clone(),
        fields: result_fields.into_boxed_slice(),
    };
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([72; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &FixtureControl)
                    .unwrap(),
                fragment,

                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: Default::default(),
            },
            package_admission(),
            &FixtureControl,
        )
        .unwrap(),
    )
}

pub(crate) fn program(mode: RandArgument, twin: bool) -> Arc<LocalProgram> {
    let functions = rng_subset();
    let source = package(&functions, mode, twin);
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated = validate_fragment_providers(source, &providers, &FixtureControl).unwrap();
    Arc::new(compile_fragment(validated, &functions, options(1), &FixtureControl).unwrap())
}
