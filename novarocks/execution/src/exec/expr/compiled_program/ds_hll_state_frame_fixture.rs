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
use arrow::array::{ArrayRef, StringArray};
fn functions() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let definition = actual
        .definition("ds_hll_count_distinct_state", FunctionKind::Scalar)
        .unwrap();
    let declaration = definition.binding_declaration().unwrap();
    // All three actual fixed generic ANY records.
    // Sealing borrows every actual installed declaration, never resolver samples.
    assert_eq!(declaration.overloads().len(), 3);
    let installed: Vec<_> = declaration
        .overloads()
        .iter()
        .map(|overload| {
            let installed = actual
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &Control,
                )
                .unwrap();
            InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: installed.implementation().clone(),
                aggregate_state_format: overload.aggregate.as_ref().map(|a| a.state_format.clone()),
            }
        })
        .collect();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition.clone()).unwrap();
    builder.seal_pure(installed).unwrap()
}
pub(super) fn program(count: usize) -> Arc<LocalProgram> {
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
    for i in 0..count {
        let ty = if i == 2 {
            FunctionValueType::new(DataType::Utf8, true)
        } else {
            ty.clone()
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
        (0..count)
            .map(|i| {
                argument(
                    if i == 2 {
                        FunctionValueType::new(DataType::Utf8, true)
                    } else {
                        ty.clone()
                    },
                    None,
                )
            })
            .collect(),
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
                function: owner.function.clone(),
                args: children.into_boxed_slice(),
            },
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    authors.insert(call, owner);
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
pub(super) fn source_batch(program: &LocalProgram, count: usize) -> RecordBatch {
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(arrow::array::Int64Array::from(vec![
        Some(1),
        None,
        Some(-7),
    ]))];
    if count >= 2 {
        arrays.push(Arc::new(arrow::array::Int64Array::from(vec![
            Some(10),
            Some(999),
            None,
        ])));
    }
    if count >= 3 {
        arrays.push(Arc::new(StringArray::from(vec![
            Some("HLL_6"),
            None,
            Some("HLL_6"),
        ])));
    }
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        arrays,
    )
    .unwrap()
}
