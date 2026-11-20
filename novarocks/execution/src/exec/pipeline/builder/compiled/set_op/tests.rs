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

#[path = "tests/parallelism.rs"]
mod parallelism;
#[path = "tests/semantics.rs"]
mod semantics;

use novarocks_physical_plan::{
    ConstantPools, Distribution, FragmentId, NodeId, NodeKind, RequiredInputs, SetOperationKind,
    ValueOrigin,
};

use super::super::family_fixture::{cell, compile, int64, int64_rows, package, run, values};
use std::collections::BTreeSet;

fn execute(kind: SetOperationKind, branches: &[Vec<Option<i64>>], dop: usize) -> Vec<Option<i64>> {
    let sources = super::super::family_fixture::constant_sources::Sources::new();
    let mut builder = super::super::family_fixture::constant_sources::FragmentBuilder::new(
        FragmentId::new(5),
        &sources,
    );
    let ty = int64(true);
    let mut inputs = Vec::new();
    let mut mappings = Vec::new();
    for (branch, input) in branches.iter().enumerate() {
        let node = NodeId::new(u32::try_from(branch).unwrap());
        let rows = input.iter().map(|&row| vec![cell(row)]).collect::<Vec<_>>();
        let columns = values(&mut builder, node, std::slice::from_ref(&ty), &rows);
        inputs.push(node);
        mappings.push(columns.into_boxed_slice());
    }
    let root = NodeId::new(u32::try_from(branches.len()).unwrap());
    let output = builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node: root,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_row_consuming(
            root,
            inputs.into_boxed_slice(),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            Box::new([output]),
            NodeKind::SetOp {
                kind,
                input_mappings: mappings.into_boxed_slice(),
            },
        )
        .unwrap();
    let program = compile(
        package(
            builder,
            root,
            ConstantPools::empty(),
            u32::try_from(dop).unwrap(),
        ),
        dop,
    );
    assert_eq!(program.graph().profile().pipeline_dop().get(), dop);
    let mut rows = int64_rows(&run(&program))
        .into_iter()
        .map(|row| {
            assert_eq!(row.len(), 1);
            row[0]
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn oracle(kind: SetOperationKind, branches: &[Vec<Option<i64>>]) -> Vec<Option<i64>> {
    branches[0]
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| match kind {
            SetOperationKind::Intersect => branches[1..].iter().all(|branch| branch.contains(key)),
            SetOperationKind::Except => branches[1..].iter().all(|branch| !branch.contains(key)),
            SetOperationKind::UnionAll => panic!("independent DISTINCT set semantics only"),
        })
        .collect()
}
