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

//! Exact grouped native witness, using genuine published plans and prepared factories.
use super::*;
use arrow::array::{Int32Array, StringArray};
use std::collections::BTreeMap;
fn grouped_program(two_phase: bool, nullable: bool) -> Arc<LocalProgram> {
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();
    let parameters = novarocks_type_contract::SemanticParameters::try_new([]).unwrap();
    let catalog = aggregate_catalog(&[("ds_hll_count_distinct", PureKernelAbi::AggregateV1)]);
    let key_type = novarocks_type_contract::FunctionValueType::new(DataType::Int32, nullable);
    let argument_types = [
        int64(nullable),
        int64(false),
        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
    ];
    let bound = bind(&catalog, "ds_hll_count_distinct", &argument_types);
    let fragment = FragmentId::new(1);
    let mut builder =
        crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
            fragment,
            &fixture_sources,
        );
    let source = builder.reserve_node_id().unwrap();
    let input = values(
        &mut builder,
        source,
        &[
            key_type.clone(),
            argument_types[0].clone(),
            argument_types[1].clone(),
            argument_types[2].clone(),
        ],
        &[], // Exact typed source; the real factory probe supplies its runtime Chunk.
    );
    let sequence = AggregateSequenceId::new(1);
    let (first, state) = add_aggregate(
        &mut builder,
        source,
        &input[..1],
        &[CallSpec {
            bound: &bound,
            phase: if two_phase {
                AggregatePhase::Partial { sequence }
            } else {
                AggregatePhase::Single
            },
            id: AggregateCallId::new(1),
            arguments: input[1..].to_vec(),
            distinct: false,
        }],
        if two_phase {
            AggregateGrouping::Partial
        } else {
            AggregateGrouping::Complete
        },
    );
    let root = if two_phase {
        add_aggregate(
            &mut builder,
            first,
            &state[..1],
            &[CallSpec {
                bound: &bound,
                phase: AggregatePhase::Final { sequence },
                id: AggregateCallId::new(2),
                arguments: state[1..].to_vec(),
                distinct: false,
            }],
            AggregateGrouping::Complete,
        )
        .0
    } else {
        first
    };
    let definition = finish(
        builder,
        root,
        FragmentSink::Result,
        2,
        &catalog,
        &parameters,
    );
    let output = definition.nodes()[&root].output.clone();
    let mut plan = crate::exec::operators::family_fixture::constant_sources::PlanBuilder::new(
        PlanVersionId::try_new([117; 16]).unwrap(),
        &fixture_sources,
    )
    .with_semantic_parameters(parameters.clone());
    plan.add_fragment(definition).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment,
        fields: output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: crate::test_result_domain::result_value_domain(&if ordinal == 0 {
                    key_type.clone()
                } else {
                    bound.result_type()
                }),
                name: if ordinal == 0 { "grp" } else { "estimate" }.into(),
                alias: None,
                value: *value,
                ty: if ordinal == 0 {
                    key_type.clone()
                } else {
                    bound.result_type()
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        output,
    })
    .unwrap();
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    compile(
        packages(&plan).remove(&fragment).unwrap(),
        &catalog,
        2,
        true,
    )
}
fn grouped_input(program: &LocalProgram, length: usize, nullable: bool) -> Chunk {
    let node = program
        .graph()
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), ProgramNodeKind::Values { .. }))
        .unwrap();
    let schema = ChunkSchema::from_compiled_layout(node.output_layout()).unwrap();
    let keys = Int32Array::from(
        (1..=length)
            .map(|id| {
                if nullable && id % 11 == 0 {
                    None
                } else {
                    Some((id % 8) as i32)
                }
            })
            .collect::<Vec<_>>(),
    );
    let values = Int64Array::from(
        (1..=length)
            .map(|id| {
                if nullable && id % 13 == 0 {
                    None
                } else {
                    Some(id as i64)
                }
            })
            .collect::<Vec<_>>(),
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(keys),
        Arc::new(values),
        Arc::new(Int64Array::from(vec![10; length])),
        Arc::new(StringArray::from(vec!["HLL_6"; length])),
    ];
    Chunk::try_new_with_chunk_schema(
        RecordBatch::try_new(schema.arrow_schema_ref(), columns).unwrap(),
        schema,
    )
    .unwrap()
}
fn estimates(output: &Chunk) -> BTreeMap<Option<i32>, i64> {
    let keys = output
        .record_batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let values = output
        .record_batch()
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..output.len())
        .map(|row| {
            (
                (if keys.is_null(row) {
                    None
                } else {
                    Some(keys.value(row))
                }),
                values.value(row),
            )
        })
        .collect()
}
fn two_partials(
    program: &Arc<LocalProgram>,
    source: &Chunk,
    tracker: &Arc<MemTracker>,
) -> [Chunk; 2] {
    let partial = factory(program, novarocks_functions::AggregateKernelPhase::Partial);
    let split = source.len() / 2;
    [
        run(
            &partial,
            &[Chunk::new_like(
                source.record_batch().slice(0, split),
                source,
            )],
            tracker.clone(),
        ),
        run(
            &partial,
            &[Chunk::new_like(
                source.record_batch().slice(split, source.len() - split),
                source,
            )],
            tracker.clone(),
        ),
    ]
}
#[test]
fn ds_hll_grouped_local_before_real_factories_native_100k() {
    use novarocks_functions::AggregateKernelPhase as P;
    let tracker = MemTracker::new_root("DsHllGroupedBefore");
    {
        let single = grouped_program(false, false);
        let raw = run(
            &factory(&single, P::Single),
            &[grouped_input(&single, 100_000, false)],
            tracker.clone(),
        );
        println!("DS_GROUPED PURE_RAW {:?}", estimates(&raw));
        println!(
            "DS_GROUPED LOCAL_STAGE_AVAILABLE {}",
            factory(&single, P::Single).requires_local_update_stages()
        );
        let program = grouped_program(true, false);
        let parts = two_partials(&program, &grouped_input(&program, 100_000, false), &tracker);
        for (driver, part) in parts.iter().enumerate() {
            let payloads = part
                .record_batch()
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for row in 0..part.len() {
                let bytes = payloads.value(row);
                assert_eq!(bytes[3], 10);
                assert_eq!((bytes[7] >> 2) & 3, 1);
                println!(
                    "DS_GROUPED PART driver={driver} row={row} flags={} bytes={}",
                    bytes[5],
                    bytes.len()
                );
            }
        }
        for reverse in [false, true] {
            let inputs = if reverse {
                [parts[1].clone(), parts[0].clone()]
            } else {
                parts.clone()
            };
            let result = run(&factory(&program, P::Final), &inputs, tracker.clone());
            println!(
                "DS_GROUPED PURE_TWO reverse={reverse} {:?}",
                estimates(&result)
            );
        }
    }
    assert_eq!(tracker.current(), 0);
}

#[cfg(test)]
#[path = "ds_hll_grouped_local_after_tests.rs"]
mod ds_hll_grouped_local_after_tests;
