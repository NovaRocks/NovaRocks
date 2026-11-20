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

use super::fragment_fixture::{compile, empty_values, numeric_catalogue, options, package};
use arrow_schema::DataType;
use novarocks_constant_contract::ConstantPools;
use novarocks_expression::testkit::HarnessControl;
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_local_program::{AssertRowsMode, ProgramNodeKind};
use novarocks_physical_plan::{
    FragmentBuilder, FragmentId, NullOrdering, ResultField, ResultValueDomain, RowCountAssertion,
    RowCountAssertionSpec, SortDirection, SortExpr, SortMode, ValueType,
};
use novarocks_type_contract::EvaluationDemand;

#[test]
fn sorted_global_row_assertion_has_a_local_order_preserving_owner() {
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let ty = ValueType::new(DataType::Int64, false);
    let (rows, values) = empty_values(&mut builder, std::slice::from_ref(&ty));
    let value = values[0];
    let sort = builder.reserve_node_id().unwrap();
    let unit = builder.unit(sort).unwrap();
    let read = unit.add(None, ty.clone(), ExprKind::Value(value)).unwrap();
    let expr = unit
        .root(read, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    builder
        .add_sort(
            sort,
            rows,
            Box::new([SortExpr {
                expr,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            SortMode::Global,
        )
        .unwrap();
    let root = builder.reserve_node_id().unwrap();
    builder
        .add_assert_one_row(
            root,
            sort,
            RowCountAssertionSpec::Global {
                subject: "scalar".into(),
                desired_rows: 1,
                comparison: RowCountAssertion::Le,
            },
        )
        .unwrap();
    let package = package(
        25,
        builder,
        root,
        Box::new([ResultField {
            name: "quantity".into(),
            alias: None,
            domain: ResultValueDomain::Plain,
            value,
            ty,
        }]),
        Default::default(),
        ConstantPools::empty(),
        options().constants,
    );
    let program = compile(&numeric_catalogue(), package, &HarnessControl).unwrap();
    let graph = program.graph();
    let ProgramNodeKind::AssertNumRows {
        input,
        mode: AssertRowsMode::Global { .. },
    } = graph.nodes()[graph.root().index()].kind()
    else {
        panic!("compiled global assertion");
    };
    assert!(matches!(
        graph.nodes()[input.index()].kind(),
        ProgramNodeKind::Sort { .. }
    ));
}
