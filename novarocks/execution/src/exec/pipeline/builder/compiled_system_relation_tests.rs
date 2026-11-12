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

//! The actual compiled pipeline proves ordered one-batch pulls and terminal
//! completion. A higher requested query DOP still produces one source driver.
use super::family_fixture::{compile, int64_rows, package, run};
use arrow::{
    array::{Int64Array, RecordBatch, RecordBatchOptions},
    datatypes::{DataType, Field, Schema},
};
use novarocks_physical_plan::{
    ConstantPools, FragmentBuilder, FragmentId, NodeId, SystemRelationData, ValueOrigin,
};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use std::sync::Arc;
#[test]
fn system_relation_outputs_original_batches_in_order_then_finishes() {
    let fields = Arc::from([Arc::new(Field::new("value", DataType::Int64, false))]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batches: Arc<[RecordBatch]> = vec![vec![1i64, 2], vec![3], vec![4, 5, 6]]
        .into_iter()
        .map(|rows| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(rows))]).unwrap()
        })
        .collect::<Vec<_>>()
        .into();
    let mut builder = FragmentBuilder::new(FragmentId::new(21));
    let node = NodeId::new(0);
    let value = builder
        .add_value(
            FunctionValueType::new(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_system_relation(
            node,
            Box::from([value]),
            fields,
            SystemRelationData::Batches {
                batches,
                storage: ValueTypeStorageLifetime::pure(),
            },
        )
        .unwrap();
    let program = compile(package(builder, node, ConstantPools::empty(), 4), 4);
    let chunks = run(&program);
    assert_eq!(
        chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
        vec![2, 1, 3]
    );
    assert_eq!(
        int64_rows(&chunks),
        (1..=6).map(|i| vec![Some(i)]).collect::<Vec<_>>()
    );
}
#[test]
fn system_relation_zero_column_batch_and_empty_snapshot_keep_rows() {
    for rows in [0, 5001] {
        let batches: Arc<[RecordBatch]> = if rows == 0 {
            Arc::from([])
        } else {
            Arc::from([RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap()])
        };
        let mut builder = FragmentBuilder::new(FragmentId::new(22));
        let node = NodeId::new(0);
        builder
            .add_system_relation(
                node,
                Box::default(),
                Arc::from([]),
                SystemRelationData::Batches {
                    batches,
                    storage: ValueTypeStorageLifetime::pure(),
                },
            )
            .unwrap();
        let program = compile(package(builder, node, ConstantPools::empty(), 4), 4);
        let chunks = run(&program);
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), rows);
        assert_eq!(chunks.len(), usize::from(rows != 0));
    }
}
