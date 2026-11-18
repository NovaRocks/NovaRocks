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
//! Real catalogue -> sealed expression unit -> LocalCompiler -> runtime Frame fixture.
use super::*;
use crate::exec::expr::compiled_program::checked_constants;
use arrow::array::StringArray;
fn functions() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = vec![];
    for name in ["ds_hll_count_distinct_state", "if"] {
        let definition = actual.definition(name, FunctionKind::Scalar).unwrap();
        let declaration = definition.binding_declaration().unwrap();
        for overload in declaration.overloads() {
            let entry = actual
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &Control,
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: entry.implementation().clone(),
                aggregate_state_format: overload.aggregate.as_ref().map(|a| a.state_format.clone()),
            });
        }
        builder.register(definition.clone()).unwrap();
    }
    builder.seal_pure(installed).unwrap()
}
pub(super) fn program() -> Arc<LocalProgram> {
    program_with_target(FunctionValueType::new(
        DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        ))),
        true,
    ))
}
pub(super) fn row_data_program() -> Arc<LocalProgram> {
    program_with_target(FunctionValueType::new(DataType::Utf8, true))
}
fn program_with_target(target: FunctionValueType) -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = functions();
    let fragment_id = FragmentId::new(198);
    let source = NodeId::new(0);
    let input = NodeId::new(1);
    let output = NodeId::new(2);
    let ty = FunctionValueType::new(DataType::Int64, true);
    let mut b = FragmentBuilder::new(fragment_id);
    b.add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = vec![];
    let mut output_values = vec![];
    let mut children = vec![];
    for i in 0..4 {
        let ty = match i {
            0 | 1 => FunctionValueType::new(DataType::Int64, true),
            2 => target.clone(),
            _ => FunctionValueType::new(DataType::Boolean, true),
        };
        let expr = b
            .unit(input)
            .unwrap()
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(ty.clone()), options().constants),
                ),
            )
            .unwrap();
        let input_root = b
            .unit(input)
            .unwrap()
            .root(
                expr,
                EvaluationDemand::Value,
                novarocks_expression_contract::RootScope::Port(0),
            )
            .unwrap();
        let value = ValueId::new(91 + i as u32);
        b.insert_value(ValueDef {
            id: value,
            ty: ty.clone(),
            origin: ValueOrigin::Expr {
                node: input,
                root: input_root,
            },
        })
        .unwrap();
        projections.push((input_root, value));
        output_values.push(value);
        children.push(
            b.unit(output)
                .unwrap()
                .add(None, ty.clone(), ExprKind::Value(value))
                .unwrap(),
        );
    }
    b.add_project(
        input,
        source,
        projections.into_boxed_slice(),
        output_values.into_boxed_slice(),
    )
    .unwrap();
    let owner = author(
        &functions,
        "ds_hll_count_distinct_state",
        vec![
            argument(ty.clone(), None),
            argument(ty.clone(), None),
            argument(target.clone(), None),
        ],
        ControlShape::Eager,
    );
    let result_ty = owner.result();
    assert_eq!(result_ty.data_type().clone(), DataType::Binary);
    let call = b
        .unit(output)
        .unwrap()
        .add(
            None,
            result_ty.clone(),
            ExprKind::FunctionCall {
                function: owner.function().clone(),
                args: children[..3].to_vec().into_boxed_slice(),
            },
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    authors.insert(call, owner);
    let fallback = b
        .unit(output)
        .unwrap()
        .add(
            None,
            result_ty.clone(),
            checked_constants::kind(
                &mut source_pools,
                checked_constants::null(&(result_ty.clone()), options().constants),
            ),
        )
        .unwrap();
    let conditional = author(
        &functions,
        "if",
        vec![
            argument(FunctionValueType::new(DataType::Boolean, true), None),
            argument(result_ty.clone(), None),
            argument(result_ty.clone(), None),
        ],
        ControlShape::If,
    );
    let conditional_call = b
        .unit(output)
        .unwrap()
        .add(
            None,
            result_ty.clone(),
            ExprKind::FunctionCall {
                function: conditional.function().clone(),
                args: Box::from([children[3], call, fallback]),
            },
        )
        .unwrap();
    authors.insert(conditional_call, conditional);
    let call = conditional_call;

    let output_root = b
        .unit(output)
        .unwrap()
        .root(
            call,
            EvaluationDemand::Value,
            novarocks_expression_contract::RootScope::Port(0),
        )
        .unwrap();
    let result_value = b
        .add_value(
            result_ty.clone(),
            ValueOrigin::Expr {
                node: output,
                root: output_root,
            },
        )
        .unwrap();
    b.add_project(
        output,
        input,
        Box::from([(output_root, result_value)]),
        Box::from([result_value]),
    )
    .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: b.construction_node(output).unwrap().output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_ty),
            name: "decoded".into(),
            alias: None,
            value: result_value,
            ty: result_ty,
        }]),
    };
    compile_checked_fragment(&functions, b, output, &authors, result, source_pools)
}
pub(super) fn source_batch(p: &LocalProgram) -> RecordBatch {
    use arrow::array::builder::{ListBuilder, StringBuilder};
    use arrow::array::{BooleanArray, Int64Array};
    let mut b = ListBuilder::new(StringBuilder::new());
    for _ in 0..3 {
        for _ in 0..90 {
            b.values().append_value("full-original-诊断");
        }
        b.append(true);
    }
    RecordBatch::try_new(
        p.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![17, 17, 17])),
            Arc::new(b.finish()),
            Arc::new(BooleanArray::from(vec![false, true, false])),
        ],
    )
    .unwrap()
}

pub(super) fn row_data_batch(p: &LocalProgram) -> RecordBatch {
    use arrow::array::{BooleanArray, Int64Array};
    RecordBatch::try_new(
        p.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![17, 999, 17])),
            Arc::new(StringArray::from(vec!["HLL_6", "HLL_6", "HLL_6"])),
            Arc::new(BooleanArray::from(vec![false, true, false])),
        ],
    )
    .unwrap()
}
