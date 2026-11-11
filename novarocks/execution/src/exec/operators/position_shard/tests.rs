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

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::{DataType, Field, Fields, Schema};
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::FunctionValueType;
use novarocks_types::SlotId;

use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
use crate::exec::pipeline::test_driver::TestDriver;

use super::*;

fn partition_fields() -> Fields {
    vec![Arc::new(Field::new(
        "partition_field",
        DataType::Int32,
        true,
    ))]
    .into()
}

fn output_layout() -> StaticLayout {
    let columns = [
        ("file", DataType::Utf8, false),
        ("spec_id", DataType::Int32, false),
        ("partition", DataType::Struct(partition_fields()), true),
        ("driver", DataType::Int64, false),
        ("high32", DataType::Int32, false),
        ("container", DataType::Int32, false),
        ("index", DataType::Int64, false),
        ("count", DataType::Int64, false),
        ("bitmap", DataType::Binary, false),
    ];
    let types = columns
        .iter()
        .map(|(_, ty, nullable)| FunctionValueType::new(ty.clone(), *nullable))
        .collect::<Vec<_>>();
    let schema = Schema::new(
        columns
            .map(|(name, ty, nullable)| Field::new(name, ty, nullable))
            .to_vec(),
    );
    crate::exec::chunk::typed_fixture::layout(
        Arc::new(schema),
        &(10..19).map(SlotId::new).collect::<Vec<_>>(),
        &types,
    )
}

fn contribution(rows: &[(&str, i64, i32, Option<i32>)], first_slot: u32) -> Chunk {
    let fields = partition_fields();
    let part = StructArray::new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(
            rows.iter().map(|row| row.3).collect::<Vec<_>>(),
        ))],
        None,
    );
    crate::exec::chunk::typed_fixture::columns(
        &["file", "position", "spec_id", "partition"],
        &[first_slot, first_slot + 1, first_slot + 2, first_slot + 3].map(SlotId::new),
        &[
            FunctionValueType::new(DataType::Utf8, false),
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Int32, false),
            FunctionValueType::new(DataType::Struct(fields), true),
        ],
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(part),
        ],
        rows.len(),
    )
}

fn prepared(
    runtime: &RuntimeState,
    tracker: &Arc<MemTracker>,
    dop: i32,
    ordinal: i32,
) -> Box<dyn Operator> {
    let error = runtime.error_state();
    let construction = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        error.clone(),
    )
    .unwrap();
    let factory = PositionShardFactory::new(output_layout(), error, dop);
    factory.bind_pipeline_dop(dop).unwrap();
    let mut op = factory.create(dop, ordinal);
    op.set_mem_tracker(tracker.clone());
    op.set_construction_factory(construction);
    op.bind_runtime_state(runtime).unwrap();
    op.prepare().unwrap();
    op
}

fn value<T: Array + 'static>(chunk: &Chunk, index: usize) -> &T {
    chunk.columns()[index].as_any().downcast_ref().unwrap()
}

#[test]
fn eos_seals_complete_files_and_drain_keeps_actual_low32_and_author() {
    let runtime = RuntimeState::default();
    let tracker = MemTracker::new_root("position shard fixture");
    let mut operator = prepared(&runtime, &tracker, 4, 3);
    let mut driver = TestDriver::new(operator.as_mut(), &runtime);
    let high = (1_i64 << 32) + (17 << 16);
    driver
        .input_without_output(contribution(
            &[
                ("a", high + 1, 4, Some(8)),
                ("b", 1, 2, None),
                ("a", (4 << 16) + 7, 4, Some(8)),
            ],
            0,
        ))
        .unwrap();
    assert!(matches!(driver.next(), NextStep::NeedInput));
    driver
        .input_without_output(contribution(
            &[
                ("a", high + 1, 4, Some(8)),
                ("a", i64::MAX, 4, Some(8)),
                ("b", 2, 2, None),
                ("a", (4 << 16) + 9, 4, Some(8)),
            ],
            4,
        ))
        .unwrap();
    assert!(matches!(driver.next(), NextStep::NeedInput));
    driver.signal_eos().unwrap();
    let chunks = driver.collect_until_finished().unwrap();
    type DecodedWindow = (i32, i32, i64, i64, Vec<u32>);
    let mut by_file = BTreeMap::<String, Vec<DecodedWindow>>::new();
    let mut done = BTreeSet::new();
    let mut previous: Option<&str> = None;
    for chunk in &chunks {
        assert_eq!(chunk.len(), 1);
        let file = value::<StringArray>(chunk, 0).value(0);
        if previous != Some(file) {
            if let Some(previous) = previous {
                assert!(done.insert(previous));
            }
            assert!(!done.contains(file));
            previous = Some(file);
        }
        let spec = value::<Int32Array>(chunk, 1).value(0);
        let partition = value::<StructArray>(chunk, 2)
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(value::<Int64Array>(chunk, 3).value(0), 3);
        match file {
            "a" => assert_eq!((spec, partition.value(0)), (4, 8)),
            "b" => assert!(spec == 2 && partition.is_null(0)),
            _ => panic!("unexpected file"),
        }
        let bytes = value::<BinaryArray>(chunk, 8).value(0);
        let positions = RoaringBitmap::deserialize_from(bytes).unwrap();
        by_file.entry(file.to_owned()).or_default().push((
            value::<Int32Array>(chunk, 4).value(0),
            value::<Int32Array>(chunk, 5).value(0),
            value::<Int64Array>(chunk, 6).value(0),
            value::<Int64Array>(chunk, 7).value(0),
            positions.into_iter().collect(),
        ));
    }
    assert_eq!(
        by_file["a"],
        [
            (0, 4, 0, 3, vec![262_151, 262_153]),
            (1, 17, 1, 3, vec![1_114_113]),
            (i32::MAX, 65535, 2, 3, vec![u32::MAX]),
        ]
    );
    assert_eq!(by_file["b"], [(0, 0, 0, 1, vec![1, 2])]);
    let alias = chunks[0].batch_projection();
    drop(driver);
    operator.close().unwrap();
    drop(operator);
    drop(chunks);
    assert!(tracker.current() > 0);
    drop(alias);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn conflicting_file_metadata_fails_without_emitting_and_releases_state() {
    let runtime = RuntimeState::default();
    let tracker = MemTracker::new_root("position shard metadata conflict");
    let mut operator = prepared(&runtime, &tracker, 1, 0);
    let mut driver = TestDriver::new(operator.as_mut(), &runtime);
    driver
        .input_without_output(contribution(&[("a", 7, 0, Some(8))], 0))
        .unwrap();
    let error = driver
        .input_without_output(contribution(&[("a", 9, 0, Some(9))], 4))
        .unwrap_err();
    assert!(error.detail().contains("Conflicting position metadata"));
    drop(driver);
    operator.close().unwrap();
    drop(operator);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn cancelled_input_continuation_releases_actual_state_without_eos() {
    let runtime = RuntimeState::default();
    let tracker = MemTracker::new_root("position shard cancelled input");
    let mut operator = prepared(&runtime, &tracker, 1, 0);
    let mut driver = TestDriver::new(operator.as_mut(), &runtime);
    let input = (0..128)
        .map(|position| ("a", position, 0, None))
        .collect::<Vec<_>>();
    driver
        .accept_without_output(contribution(&input, 0))
        .unwrap();
    let mut committed = 0;
    for _ in 0..input.len() {
        let step = driver.drive().unwrap();
        committed += step.committed_rows;
        assert!(step.output.is_none());
        assert!(matches!(step.next, NextStep::Runnable | NextStep::Yield));
        if committed > 0 && matches!(step.next, NextStep::Yield) {
            break;
        }
    }
    assert!(matches!(driver.next(), NextStep::Yield));
    assert!((1..input.len() as u64).contains(&committed));
    assert!(tracker.current() >= WINDOW_BYTES as i64);
    drop(driver);
    operator.cancel();
    operator.close().unwrap();
    drop(operator);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn real_bitmap_allocator_refusal_exits_without_a_second_budget() {
    let tracker = MemTracker::new_root("position shard real bitmap budget");
    tracker.install_limit_once(WINDOW_BYTES as i64 - 1).unwrap();
    let host = Arc::new(
        crate::runtime::tracked_state_allocator::TrackedStateAllocator::new(tracker.clone()),
    );
    let mut accumulator = FileAccumulator::new(StateAllocator::new(host));
    let partition = StructArray::new_empty_fields(1, None);
    let result = accumulator.insert(
        "a",
        0,
        1,
        |_| unreachable!(),
        || {
            Ok(PartitionRow {
                array: Arc::new(partition.clone()),
                storage: ValueTypeStorageLifetime::pure(),
            })
        },
    );
    assert_eq!(
        result.unwrap_err().failure_fact().class(),
        FailureClass::Resource
    );
    drop(accumulator);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn shared_exchange_keeps_one_file_author_across_routes_and_waits_for_every_producer() {
    use crate::exec::hash_table::partition_keys::native_tuple_hashes_observed;
    use crate::exec::operators::local_exchanger::{LocalExchangePartitionSpec, LocalExchanger};
    use crate::exec::pipeline::driver_step::TurnWork;
    use novarocks_type_contract::PartitionHashDomain;
    use novarocks_value_arrays::{decode::ValueConversionContext, reachability::ReachableRows};
    use std::num::NonZeroU64;

    let runtime = RuntimeState::default();
    let tracker = MemTracker::new_root("position shard grouped shuffle");
    let control = runtime.error_state();
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        control.clone(),
    )
    .unwrap();
    let keys = (0..32).map(|i| format!("file-{i}")).collect::<Vec<_>>();
    let rows = keys
        .iter()
        .map(|file| (file.as_str(), 7, 0, Some(31)))
        .collect::<Vec<_>>();
    let source = contribution(&rows, 0);
    let owner = factory.begin_input_construction(&source).unwrap();
    let file_type = FunctionValueType::new(DataType::Utf8, false);
    let hashes = native_tuple_hashes_observed(
        &source.columns()[..1],
        std::slice::from_ref(&file_type),
        &ReachableRows::all(source.len()),
        &mut ValueConversionContext::default(),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();

    for dop in [1, 4] {
        let mut authors = BTreeMap::new();
        for instance in 0..2 {
            let routes = LocalExchanger::new_with_limits(
                dop,
                3,
                LocalExchangePartitionSpec::HierarchicalFileInput(file_type.clone()),
                64,
                i64::MAX,
            );
            let own_rows = rows
                .iter()
                .copied()
                .enumerate()
                .filter(|(i, _)| {
                    PartitionHashDomain::InstanceV1.mix(hashes.as_slice()[*i].unwrap()) % 2
                        == instance
                })
                .map(|(_, row)| row)
                .collect::<Vec<_>>();
            assert!(!own_rows.is_empty());
            let late = own_rows
                .iter()
                .flat_map(|&(file, _, spec, partition)| {
                    [(file, 7, spec, partition), (file, 9, spec, partition)]
                })
                .collect::<Vec<_>>();
            let push = |chunk| {
                let mut turn = TurnWork::new(NonZeroU64::MIN, 64);
                routes
                    .accept_created(
                        None,
                        chunk,
                        factory.as_ref(),
                        &mut DriverStep::new(
                            &control,
                            &mut turn,
                            &mut ValueConversionContext::default(),
                        ),
                        &mut 0,
                    )
                    .unwrap();
            };
            let mut operators = (0..dop)
                .map(|id| prepared(&runtime, &tracker, dop as i32, id as i32))
                .collect::<Vec<_>>();
            let mut drivers = operators
                .iter_mut()
                .map(|operator| TestDriver::new(operator.as_mut(), &runtime))
                .collect::<Vec<_>>();
            push(contribution(&own_rows, 0));
            assert!(!routes.finish_producer());
            for (driver_id, driver) in drivers.iter_mut().enumerate() {
                while let Some(chunk) = routes.pop_chunk(driver_id) {
                    driver.input_without_output(chunk).unwrap();
                }
                assert!(!routes.is_done(driver_id));
                assert!(matches!(driver.next(), NextStep::NeedInput));
            }

            push(contribution(&late, 4));
            assert!(!routes.finish_producer());
            push(contribution(&[], 8));
            for (driver_id, driver) in drivers.iter_mut().enumerate() {
                while let Some(chunk) = routes.pop_chunk(driver_id) {
                    driver.input_without_output(chunk).unwrap();
                }
                assert!(!routes.is_done(driver_id));
            }
            assert!(routes.finish_producer());
            let mut used = BTreeSet::new();
            for (driver_id, driver) in drivers.iter_mut().enumerate() {
                assert!(routes.is_done(driver_id));
                driver.signal_eos().unwrap();
                for chunk in driver.collect_until_finished().unwrap() {
                    let file = value::<StringArray>(&chunk, 0).value(0).to_owned();
                    let driver = value::<Int64Array>(&chunk, 3).value(0);
                    assert_eq!(driver, driver_id as i64);
                    assert!(authors.insert(file, (instance, driver)).is_none());
                    let positions =
                        RoaringBitmap::deserialize_from(value::<BinaryArray>(&chunk, 8).value(0))
                            .unwrap();
                    assert_eq!(positions.into_iter().collect::<Vec<_>>(), [7, 9]);
                    used.insert(driver);
                }
            }
            assert_eq!(used.len(), dop);
        }
        assert_eq!(authors.len(), keys.len());
    }
}
