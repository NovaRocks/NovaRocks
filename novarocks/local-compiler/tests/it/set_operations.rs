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

use super::fragment_fixture::{
    compile, empty_values, numeric_catalogue, options, output_value, package,
};
use arrow_schema::DataType;
use novarocks_constant_contract::ConstantPools;
use novarocks_expression::testkit::HarnessControl;
use novarocks_local_compiler::FragmentCompileError;
use novarocks_local_program::{ProgramNodeKind, SetOpKind};
use novarocks_physical_plan::{
    Distribution, FragmentBuilder, FragmentId, FragmentPackage, NodeKind, RequiredInputs,
    ResultField, ResultValueDomain, SetOperationKind, ValueType,
};
use std::sync::Arc;

fn set_operation(kind: SetOperationKind, shared: bool) -> Arc<FragmentPackage> {
    let int64 = ValueType::new(DataType::Int64, false);
    let nullable = ValueType::new(DataType::Int64, true);
    let boolean = ValueType::new(DataType::Boolean, false);
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let (left, left_values) = empty_values(&mut builder, &[int64, boolean.clone()]);
    let (right, right_values) = if shared {
        (left, vec![left_values[1], left_values[0]])
    } else {
        empty_values(&mut builder, &[boolean.clone(), nullable.clone()])
    };
    let root = builder.reserve_node_id().unwrap();
    let first = output_value(&mut builder, root, 0, boolean.clone());
    let second = output_value(&mut builder, root, 1, nullable.clone());
    builder
        .add_row_consuming(
            root,
            Box::new([left, right]),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            Box::new([first, second]),
            NodeKind::SetOp {
                kind,
                input_mappings: Box::new([
                    Box::new([left_values[1], left_values[0]]),
                    Box::new([right_values[0], right_values[1]]),
                ]),
            },
        )
        .unwrap();
    package(
        17,
        builder,
        root,
        Box::new([
            ResultField {
                name: "present".into(),
                alias: None,
                domain: ResultValueDomain::Plain,
                value: first,
                ty: boolean,
            },
            ResultField {
                name: "amount".into(),
                alias: None,
                domain: ResultValueDomain::Plain,
                value: second,
                ty: nullable,
            },
        ]),
        Default::default(),
        ConstantPools::empty(),
        options().constants,
    )
}

#[test]
fn distinct_set_operations_lower_to_ordered_normalized_branches() {
    let functions = numeric_catalogue();
    for (kind, expected) in [
        (SetOperationKind::Intersect, SetOpKind::Intersect),
        (SetOperationKind::Except, SetOpKind::Except),
    ] {
        let program = compile(&functions, set_operation(kind, false), &HarnessControl).unwrap();
        let graph = program.graph();
        let root = &graph.nodes()[graph.root().index()];
        let ProgramNodeKind::SetOp { kind, inputs } = root.kind() else {
            panic!("compiled set operation: {:?}", root.kind());
        };
        assert_eq!(*kind, expected);
        assert_eq!(inputs.len(), 2);
        for (&branch, expected_ordinals) in inputs.iter().zip([[1, 0], [0, 1]]) {
            let branch = &graph.nodes()[branch.index()];
            let ProgramNodeKind::Project {
                is_subordinate: true,
                exprs,
                ..
            } = branch.kind()
            else {
                panic!("compiled branch normalizer: {:?}", branch.kind());
            };
            assert_eq!(branch.output_layout().slots(), root.output_layout().slots());
            assert_eq!(
                branch.output_layout().schema(),
                root.output_layout().schema()
            );
            let ordinals: Vec<_> = exprs
                .iter()
                .map(|reference| {
                    let unit = program.unit(reference.unit).unwrap();
                    let root = unit.root(reference.root).unwrap();
                    unit.input(root.use_id).unwrap().ordinal
                })
                .collect();
            assert_eq!(ordinals, expected_ordinals);
        }
        assert!(root.output_layout().schema().field(1).is_nullable());
    }
}

#[test]
fn union_all_rejects_shared_physical_input() {
    assert!(matches!(
        compile(
            &numeric_catalogue(),
            set_operation(SetOperationKind::UnionAll, true),
            &HarnessControl,
        ),
        Err(FragmentCompileError::Unsupported {
            feature: "shared or cyclic physical input",
            ..
        })
    ));
}
