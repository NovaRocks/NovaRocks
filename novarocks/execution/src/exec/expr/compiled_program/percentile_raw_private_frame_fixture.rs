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
use arrow::array::{ArrayRef, BinaryArray};
fn functions() -> PureEngineFunctionCatalog {
    let actual = novarocks_functions::builtin::catalogue::percentile_raw_private_test_catalog();
    let definition = actual
        .definition("percentile_approx_raw", FunctionKind::Scalar)
        .unwrap();
    let declaration = definition.binding_declaration().unwrap();
    // The actual complete fixed ANY2 declaration.
    // Sealing borrows every actual installed declaration, never resolver samples.
    assert_eq!(declaration.overloads().len(), 1);
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
pub(super) fn program() -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = functions();
    let fragment_id = FragmentId::new(198);
    let source = NodeId::new(0);
    let input = NodeId::new(1);
    let output = NodeId::new(2);
    let types = [
        FunctionValueType::new(DataType::Binary, true),
        FunctionValueType::new(DataType::Float64, true),
    ];
    let mut b = FragmentBuilder::new(fragment_id);
    b.add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = vec![];
    let mut output_values = vec![];
    let mut children = vec![];
    for (i, ty) in types.iter().enumerate() {
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
        "percentile_approx_raw",
        types.iter().cloned().map(|ty| argument(ty, None)).collect(),
        ControlShape::Eager,
    );
    let result_ty = owner.result();
    assert_eq!(result_ty.data_type().clone(), DataType::Float64);
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
pub(super) fn source_arrays() -> Vec<ArrayRef> {
    let mut state = novarocks_functions::approx_percentile_core::PercentileState::default();
    for value in [1.0, 2.0, 3.0, 4.0] {
        novarocks_functions::approx_percentile_core::add_value(&mut state, value).unwrap();
    }
    let bytes = novarocks_functions::approx_percentile_core::encode_state(&state)
        .expect("percentile frame fixture");
    vec![
        Arc::new(BinaryArray::from(vec![
            Some(b"bad".as_slice()),
            None,
            Some(bytes.as_slice()),
        ])),
        Arc::new(arrow::array::Float64Array::from(vec![0.5, 0.5, 0.5])),
    ]
}
pub(super) fn source_batch(program: &LocalProgram) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        source_arrays(),
    )
    .unwrap()
}
