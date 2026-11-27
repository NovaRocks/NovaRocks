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

use super::fragment_fixture::{compile, install_claims, options, package};
use arrow_schema::DataType;
use novarocks_constant_contract::{ConstantPoolId, ConstantPools};
use novarocks_expression::StaticExprKind;
use novarocks_expression::testkit::{
    ClaimAuthor, HarnessControl, add_call, checked_constants, register_constant,
};
use novarocks_expression_contract::{ExprId, ExprKind, RootScope};
use novarocks_functions::testkit::rng_subset;
use novarocks_functions::{ConstantValue, FunctionArgument, PureEngineFunctionCatalog};
use novarocks_local_program::{LocalProgram, ProgramNodeKind};
use novarocks_physical_plan::{
    FragmentBuilder, FragmentId, NodeId, ResultField, ResultValueDomain, ValueDef, ValueId,
    ValueOrigin,
};
use novarocks_type_contract::{
    CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand, FunctionValueType,
    SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

fn source_columns(
    builder: &mut FragmentBuilder,
    source: NodeId,
    input: NodeId,
    root: NodeId,
    source_pools: &mut ConstantPools,
) -> Vec<ExprId> {
    // Leave room for the builder's generated Project result identities.
    let values = [
        ValueId::new(3),
        ValueId::new(100),
        ValueId::new(u32::MAX - 32),
    ];
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    let mut operands = vec![];
    for &value in &values {
        let ty = FunctionValueType::new(DataType::Boolean, true);
        let definition = builder
            .unit(input)
            .unwrap()
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    source_pools,
                    checked_constants::null(&ty, options().constants),
                ),
            )
            .unwrap();
        let input_root = builder
            .unit(input)
            .unwrap()
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    root: input_root,
                },
            })
            .unwrap();
        items.push((input_root, value));
        operands.push(
            builder
                .unit(root)
                .unwrap()
                .add(None, ty, ExprKind::input(value))
                .unwrap(),
        );
    }
    builder
        .add_project(input, source, items.into_boxed_slice(), Box::from(values))
        .unwrap();
    operands
}
fn filter_program(width: usize) -> Arc<LocalProgram> {
    let mut constants = ConstantPools::empty();
    let root = NodeId::new(0);
    let input = NodeId::new(41);
    let mut builder = FragmentBuilder::new(FragmentId::new(91));
    let operands = source_columns(
        &mut builder,
        NodeId::new(u32::MAX),
        input,
        root,
        &mut constants,
    );
    let order = [operands[2], operands[0], operands[1]];
    let predicates = (0..width)
        .map(|ordinal| {
            builder
                .unit(root)
                .unwrap()
                .root(
                    order[ordinal % 3],
                    EvaluationDemand::TruthOnly,
                    RootScope::Port(0),
                )
                .unwrap()
        })
        .collect::<Box<[_]>>();
    builder.add_filter(root, input, predicates).unwrap();
    let fields = builder
        .construction_node(root)
        .unwrap()
        .output
        .columns
        .iter()
        .copied()
        .enumerate()
        .map(|(ordinal, value)| ResultField {
            domain: ResultValueDomain::Plain,
            name: format!("result_{ordinal}").into(),
            alias: None,
            value,
            ty: FunctionValueType::new(DataType::Boolean, true),
        })
        .collect();
    let package = package(
        91,
        builder,
        root,
        fields,
        SemanticParameters::try_new([]).unwrap(),
        constants,
    );
    compile(&rng_subset(), package, &HarnessControl).unwrap()
}

#[test]
fn filter_compiler_keeps_ordered_truth_only_roots_at_actual_node() {
    for width in [3, 4, 320] {
        let program = filter_program(width);
        let ProgramNodeKind::Filter { predicates, .. } =
            program.graph().nodes()[program.graph().root().index()].kind()
        else {
            panic!("actual Filter")
        };
        let unit = program.unit(predicates[0].unit).unwrap();
        let mut distinct = BTreeMap::new();
        let mut definitions = vec![];
        for ordinal in 0..width {
            let reference = predicates[ordinal];
            assert_eq!(reference.unit, predicates[0].unit);
            let use_id = unit.root(reference.root).unwrap().use_id;
            assert!(distinct.insert(use_id, ordinal).is_none());
            let invocation = &unit.control_flow().uses()[&use_id];
            assert_eq!(invocation.context.demand, EvaluationDemand::TruthOnly);
            definitions.push(invocation.definition);
        }
        if width > 3 {
            assert_eq!(definitions[0], definitions[3]);
        }
    }
}

fn checked_source(pools: &mut ConstantPools, value: &ConstantValue) -> ExprKind {
    ExprKind::Constant(register_constant(pools, value))
}

fn i64_source(pools: &mut ConstantPools, ty: &FunctionValueType, value: i64) -> ExprKind {
    let value = ConstantValue::from_i64(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        value,
        options().constants,
        CompilePhase::Validate,
        &HarnessControl,
    )
    .unwrap();
    checked_source(pools, &value)
}
fn bool_source(pools: &mut ConstantPools, ty: &FunctionValueType, value: bool) -> ExprKind {
    let value = ConstantValue::from_boolean(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        value,
        options().constants,
        CompilePhase::Validate,
        &HarnessControl,
    )
    .unwrap();
    checked_source(pools, &value)
}
fn null_source(pools: &mut ConstantPools, ty: &FunctionValueType) -> ExprKind {
    let value = ConstantValue::null(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        options().constants,
        CompilePhase::Validate,
        &HarnessControl,
    )
    .unwrap();
    checked_source(pools, &value)
}

fn rng_program(
    functions: &PureEngineFunctionCatalog,
    bound_is_literal: bool,
    twin: bool,
    dictionary: bool,
) -> Arc<LocalProgram> {
    let bound_type = FunctionValueType::new(DataType::Int64, !bound_is_literal);
    let bound_constant = bound_is_literal.then(|| {
        ConstantValue::from_i64(
            Arc::new(bound_type.try_to_field("fixture").unwrap()),
            bound_type.clone(),
            1,
            options().constants,
            CompilePhase::FunctionSpecialization,
            &HarnessControl,
        )
        .unwrap()
    });
    let mut constants = ConstantPools::empty();
    if let Some(value) = &bound_constant {
        constants
            .insert(ConstantPoolId::new(u32::MAX), value.pool().clone())
            .unwrap();
    }
    let author = ClaimAuthor::with_legacy_binding(
        functions,
        "rand",
        vec![FunctionArgument::Value {
            value_type: bound_type.clone(),
            constant: bound_constant.clone(),
        }],
        ControlShape::Eager,
        None,
        options().constants,
        &HarnessControl,
    )
    .with_decimal_policy(DecimalOverflowPolicy::OutputNull);
    let result_type = author.result();
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
    let argument = bound_constant.as_ref().map_or_else(
        || ExprKind::input(bound_value),
        |value| checked_source(&mut constants, value),
    );
    let call_argument = builder
        .unit(second_project)
        .unwrap()
        .add(None, bound_type.clone(), argument)
        .unwrap();
    let mut authors = BTreeMap::new();
    let call = add_call(
        builder.unit(second_project).unwrap(),
        &mut authors,
        author,
        vec![call_argument],
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

    install_claims(
        &mut builder,
        second_project,
        &mut constants,
        functions,
        &authors,
        &parameters,
    );
    let mut result_fields = vec![ResultField {
        domain: ResultValueDomain::Plain,
        name: "sample".into(),
        alias: None,
        value: sample_value,
        ty: result_type.clone(),
    }];
    if twin {
        result_fields.push(ResultField {
            domain: ResultValueDomain::Plain,
            name: "sample_twin".into(),
            alias: None,
            value: twin_value,
            ty: result_type,
        });
    }
    result_fields.extend([
        ResultField {
            domain: ResultValueDomain::Plain,
            name: "bound_a".into(),
            alias: None,
            value: bound_value,
            ty: bound_type.clone(),
        },
        ResultField {
            domain: ResultValueDomain::Plain,
            name: "bound_b".into(),
            alias: None,
            value: bound_value,
            ty: bound_type,
        },
    ]);
    let package = package(
        72,
        builder,
        limit,
        result_fields.into_boxed_slice(),
        parameters,
        constants,
    );
    compile(functions, package, &HarnessControl).unwrap()
}

#[test]
fn project_compiler_keeps_distinct_call_root_occurrences() {
    let program = rng_program(&rng_subset(), true, true, false);
    let graph = program.graph();
    let ProgramNodeKind::Project { exprs, .. } = graph.nodes()[3].kind() else {
        panic!("actual second Project")
    };
    assert_eq!(exprs[0].unit, exprs[1].unit);
    assert_ne!(exprs[0].root, exprs[1].root);
    let unit = program.unit(exprs[0].unit).unwrap();
    let root_a = unit.root(exprs[0].root).unwrap();
    let root_b = unit.root(exprs[1].root).unwrap();
    assert_eq!(root_a.definition, root_b.definition);
    assert!(matches!(
        unit.definitions().node(root_a.definition).unwrap().kind(),
        StaticExprKind::BoundCall { .. }
    ));
    let root_a = root_a.use_id;
    let root_b = root_b.use_id;
    assert_ne!(root_a, root_b);
    let flow = unit.control_flow();
    assert_eq!(
        flow.uses()[&root_a].definition,
        flow.uses()[&root_b].definition
    );
    assert_ne!(
        flow.uses()[&root_a].arguments[0],
        flow.uses()[&root_b].arguments[0]
    );
}

#[test]
fn filter_output_layout_normalizes_unused_dictionary_carrier_and_keeps_nullability() {
    let functions = rng_subset();
    let program = rng_program(&functions, false, false, true);
    let schema = program.graph().nodes()[2].output_layout().schema();
    assert_eq!(schema.fields().len(), 4);
    let dictionary = schema.field(3);
    assert_eq!(dictionary.data_type(), &DataType::Utf8);
    assert!(dictionary.is_nullable());
}
