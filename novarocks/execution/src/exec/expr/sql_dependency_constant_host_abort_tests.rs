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

//! One original record writer and schema/geometry chain with nominal capture-only host refusal.
use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, FixedSizeListArray, Int8Array, Int32Array,
    LargeListArray, ListArray, MapArray, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Int8Type};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use novarocks_functions::{ConstantPolicy, ConstantPool};
use novarocks_physical_plan::ConstantPoolId;
use novarocks_plan_codec::{
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    ipc_flat_pool_v2::{FlatPoolWriteFacts, FlatPoolWriteLimits},
    ipc_recursive_pool_v2::RecursivePoolWriteLimits,
    ipc_schema_v2::IpcSchemaProjectionLimits,
    physical_constant_v2::{
        ConstantWriteProjectionLimits, PhysicalConstantCodecError,
        prepare_constant_record_write_in, prepare_constant_record_write_with_host_in,
    },
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, NR_LOGICAL_TYPE_KEY,
    PureCompileControl, ValueLogicalType,
};
use novarocks_workload_control::{
    Reservation, ResourceClass, ResourceConfig, WorkClass, WorkError, WorkRequest, WorkloadConfig,
    WorkloadControl,
};
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn new(refusal: Option<(usize, CompileControlError)>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            refusal,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut calls = self.calls.lock().unwrap();
        calls.push(units);
        if let Some((at, cause)) = self.refusal.clone()
            && at == calls.len()
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn limits() -> ConstantWriteProjectionLimits {
    let flat = FlatPoolWriteLimits {
        max_rows: 4096,
        max_buffer_descriptors: 4096,
        max_body_bytes: 8 * 1024 * 1024,
        max_encoded_stream_bytes: 16 * 1024 * 1024,
        schema: IpcSchemaProjectionLimits {
            max_field_occurrences: 4096,
            max_type_occurrences: 4096,
            max_string_bytes: 65536,
            max_flatbuffer_bytes: 4 * 1024 * 1024,
        },
        max_new_allocation_request_bytes: 128 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
        max_cumulative_library_work: usize::MAX,
    };
    ConstantWriteProjectionLimits {
        flat,
        recursive: RecursivePoolWriteLimits {
            flat,
            max_field_nodes: 4096,
            max_total_rows: 65536,
        },
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 8 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 32 * 1024 * 1024,
        max_library_validation_bytes: 32 * 1024 * 1024,
    }
}
fn pool(array: ArrayRef, logical: ValueLogicalType) -> ConstantPool {
    let ty =
        FunctionValueType::try_with_logical_type(array.data_type().clone(), true, logical).unwrap();
    let mut metadata =
        std::collections::HashMap::from([("original".to_string(), "field_metadata".to_string())]);
    if logical == ValueLogicalType::LargeInt {
        metadata.insert(NR_LOGICAL_TYPE_KEY.into(), "largeint".into());
    }
    let field =
        Arc::new(Field::new("original", array.data_type().clone(), true).with_metadata(metadata));
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Decode,
        &Control::new(None),
    )
    .unwrap()
}
fn fixtures() -> Vec<ConstantPool> {
    let text = Arc::new(Field::new("text", DataType::Utf8, true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "nested_source".into(),
            "retained".into(),
        )]),
    ));
    let values: ArrayRef = Arc::new(StructArray::new(
        vec![text, Arc::new(Field::new("number", DataType::Int32, true))].into(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("unused"),
                Some("a"),
                None,
                Some("z"),
            ])),
            Arc::new(Int32Array::from(vec![Some(0), None, Some(2), Some(3)])),
        ],
        None,
    ));
    let list: ArrayRef = Arc::new(
        ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 4])),
            Arc::clone(&values),
            Some(NullBuffer::from(vec![true, true, false])),
        )
        .slice(1, 2),
    );
    let large: ArrayRef = Arc::new(
        LargeListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 1, 3, 4])),
            values,
            None,
        )
        .slice(1, 2),
    );
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec!["unused", "a", "b", "c"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
        None,
    );
    let map: ArrayRef = Arc::new(
        MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 4])),
            entries,
            None,
            false,
        )
        .slice(1, 2),
    );
    let structure: ArrayRef = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("list", large.data_type().clone(), true)),
            Arc::new(Field::new("flag", DataType::Boolean, true)),
        ]
        .into(),
        vec![
            Arc::clone(&large),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
        ],
        None,
    ));
    let mut pools = vec![
        pool(
            Arc::new(StringArray::from(vec![Some("a\0中"), None, Some("z")])),
            ValueLogicalType::Physical,
        ),
        pool(
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ValueLogicalType::Physical,
        ),
        pool(list, ValueLogicalType::Physical),
        pool(large, ValueLogicalType::Physical),
        pool(map, ValueLogicalType::Physical),
        pool(structure, ValueLogicalType::Physical),
    ];
    pools.push(pool(
        novarocks_functions::largeint::array_from_i128(&[Some(i128::MIN), None, Some(i128::MAX)])
            .unwrap(),
        ValueLogicalType::LargeInt,
    ));
    pools
}
fn source(pool: &ConstantPool) -> usize {
    // Explicit finite original-source fixture envelope, not actual retained heap.
    1024 * 1024 + usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes).unwrap()
}
fn owner() -> WorkloadControl {
    let owner = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 256 * 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 256 * 1024 * 1024 - 1024,
        },
    )
    .unwrap();
    owner.mark_ready().unwrap();
    owner
}
fn stock(
    held: &mut Option<Reservation>,
    authority: &novarocks_workload_control::LocalResourceAuthority,
    scope: &novarocks_workload_control::WorkScope,
    facts: &FlatPoolWriteFacts,
) -> Result<(), WorkError> {
    let bytes = u64::try_from(facts.new_allocation_request_bytes_upper_bound).unwrap();
    match held {
        Some(current) if bytes > current.remaining_bytes() => {
            current.grow(bytes - current.remaining_bytes())
        }
        Some(_) => Ok(()),
        None if bytes > 0 => {
            *held = Some(authority.reserve(scope, bytes, ResourceClass::Data)?);
            Ok(())
        }
        None => Ok(()),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Prepare,
    Emit,
}
type Receipt = (u32, Option<u32>, Option<u32>, i32, Vec<u8>);
fn workflow<H>(
    pool: &ConstantPool,
    control: &Control,
    host: &mut impl FnMut(Stage, &FlatPoolWriteFacts) -> Result<(), AdmissionRefusal<H>>,
) -> Result<Receipt, ProjectionFailure<PhysicalConstantCodecError, H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let token = prepare_constant_record_write_with_host_in(
        ConstantPoolId::new(u32::MAX),
        7,
        27,
        pool,
        source(pool),
        limits(),
        &mut |facts| host(Stage::Prepare, facts),
        &mut work,
    )?;
    let record = token.emit_with_host_in(&mut |facts| host(Stage::Emit, facts), &mut work)?;
    work.finish()?;
    Ok((
        record.id,
        record.value_type_id,
        record.source_field_id,
        record.compression,
        record.arrow_ipc,
    ))
}
fn original(pool: &ConstantPool) -> Receipt {
    let control = Control::new(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let token = prepare_constant_record_write_in(
        ConstantPoolId::new(u32::MAX),
        7,
        27,
        pool,
        source(pool),
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let record = token.emit_in(&mut |_| Ok(()), &mut work).unwrap();
    work.finish().unwrap();
    (
        record.id,
        record.value_type_id,
        record.source_field_id,
        record.compression,
        record.arrow_ipc,
    )
}
#[test]
fn sql_dependency_constant_host_abort_full_flat_recursive_bytes_metadata_and_ordinals() {
    for pool in fixtures() {
        let before = original(&pool);
        let actual = owner();
        let task = actual
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = task.owner.scope();
        let authority = actual
            .resources()
            .expect("test workload resource authority");
        let mut held = None;
        let mut stages = Vec::new();
        let after = workflow(&pool, &Control::new(None), &mut |stage, facts| {
            stages.push(stage);
            stock(&mut held, &authority, &scope, facts).map_err(AdmissionRefusal::Host)
        })
        .unwrap();
        assert_eq!(after, before);
        assert!(stages.contains(&Stage::Prepare));
        assert!(stages.contains(&Stage::Emit));
        assert!(authority.snapshot().data_reserved_bytes > 0);
        assert_eq!(authority.snapshot().data_used_bytes, 0);
        drop(after);
        drop(held);
        assert_eq!(authority.snapshot().held_bytes(), 0);
    }
}
#[test]
fn sql_dependency_constant_host_abort_every_prepare_callback_preserves_nominal_first_cause() {
    for pool in [fixtures().remove(0), fixtures().remove(2)] {
        let actual = owner();
        let foreign = owner();
        let task = actual
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = task.owner.scope();
        let authority = actual
            .resources()
            .expect("test workload resource authority");
        let cause = foreign
            .resources()
            .expect("test workload resource authority")
            .reserve(&scope, 1, ResourceClass::Data)
            .err()
            .unwrap();
        assert_eq!(cause, WorkError::ForeignAuthority);
        let mut count = 0;
        workflow(&pool, &Control::new(None), &mut |stage, _| {
            if stage == Stage::Prepare {
                count += 1;
            }
            Ok::<_, AdmissionRefusal<&WorkError>>(())
        })
        .unwrap();
        for at in 1..=count {
            let mut seen = 0;
            let mut held = None;
            let mut emitted = false;
            let error = workflow(&pool, &Control::new(None), &mut |stage, facts| {
                if stage == Stage::Emit {
                    emitted = true;
                    panic!("emit after preparation refusal");
                }
                seen += 1;
                assert!(seen <= at);
                if seen == at {
                    return Err(AdmissionRefusal::Host(&cause));
                }
                stock(&mut held, &authority, &scope, facts).unwrap();
                Ok(())
            })
            .err()
            .unwrap();
            assert!(matches!(error,ProjectionFailure::Host(error) if std::ptr::eq(error,&cause)));
            assert_eq!(seen, at);
            assert!(!emitted);
            drop(held);
            assert_eq!(authority.snapshot().held_bytes(), 0);
        }
    }
}
#[test]
fn sql_dependency_constant_host_abort_every_emit_callback_consumes_one_original_token() {
    for pool in [fixtures().remove(0), fixtures().remove(2)] {
        let actual = owner();
        let foreign = owner();
        let task = actual
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = task.owner.scope();
        let authority = actual
            .resources()
            .expect("test workload resource authority");
        let cause = foreign
            .resources()
            .expect("test workload resource authority")
            .reserve(&scope, 1, ResourceClass::Data)
            .err()
            .unwrap();
        let mut count = 0;
        workflow(&pool, &Control::new(None), &mut |stage, _| {
            if stage == Stage::Emit {
                count += 1;
            }
            Ok::<_, AdmissionRefusal<&WorkError>>(())
        })
        .unwrap();
        for at in 1..=count {
            let mut seen = 0;
            let mut held = None;
            let error = workflow(&pool, &Control::new(None), &mut |stage, facts| {
                if stage == Stage::Emit {
                    seen += 1;
                    assert!(seen <= at);
                    if seen == at {
                        return Err(AdmissionRefusal::Host(&cause));
                    }
                }
                stock(&mut held, &authority, &scope, facts).unwrap();
                Ok(())
            })
            .err()
            .unwrap();
            assert!(matches!(error,ProjectionFailure::Host(error) if std::ptr::eq(error,&cause)));
            assert_eq!(seen, at);
            drop(held);
            assert_eq!(authority.snapshot().held_bytes(), 0);
        }
    }
}
#[test]
fn sql_dependency_constant_host_abort_all_three_compile_causes_keep_no_failure_footer() {
    for pool in [fixtures().remove(0), fixtures().remove(2)] {
        let success = Control::new(None);
        workflow(&pool, &success, &mut |_, _| {
            Ok::<_, AdmissionRefusal<Infallible>>(())
        })
        .unwrap();
        let count = success.calls.lock().unwrap().len();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 1..=count {
                let control = Control::new(Some((at, cause.clone())));
                let error = workflow(&pool, &control, &mut |_, _| {
                    Ok::<_, AdmissionRefusal<Infallible>>(())
                })
                .err()
                .unwrap();
                assert!(
                    matches!(error,ProjectionFailure::Codec(PhysicalConstantCodecError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.calls.lock().unwrap().len(), at);
            }
        }
    }
}
#[test]
fn sql_dependency_constant_host_abort_different_original_control_refuses_before_host() {
    let pool = fixtures().remove(2);
    let original = Control::new(None);
    let other = Control::new(None);
    let mut work = CompileCheckpoints::try_new(&original, CompilePhase::Encode).unwrap();
    let token = prepare_constant_record_write_with_host_in(
        ConstantPoolId::new(0),
        0,
        0,
        &pool,
        source(&pool),
        limits(),
        &mut |_| Ok::<_, AdmissionRefusal<Infallible>>(()),
        &mut work,
    )
    .unwrap();
    let mut other_work = CompileCheckpoints::try_new(&other, CompilePhase::Encode).unwrap();
    let mut calls = 0;
    let error = token
        .emit_with_host_in(
            &mut |_| {
                calls += 1;
                Ok::<_, AdmissionRefusal<Infallible>>(())
            },
            &mut other_work,
        )
        .err()
        .unwrap();
    assert_eq!(
        error.to_string(),
        "constant record caller work has a different original control"
    );
    assert_eq!(calls, 0);
    assert_eq!(other.calls.lock().unwrap().len(), 1);
}
#[test]
fn sql_dependency_constant_host_abort_only_actual_capacity_uses_original_control_projection() {
    let pool = fixtures().remove(0);
    let actual = owner();
    let task = actual
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = task.owner.scope();
    let authority = actual
        .resources()
        .expect("test workload resource authority");
    let mut cause = None;
    let mut calls = 0;
    let error = workflow(&pool, &Control::new(None), &mut |_, _| {
        calls += 1;
        let raw = authority
            .reserve(&scope, u64::MAX, ResourceClass::Data)
            .err()
            .unwrap();
        assert_eq!(raw, WorkError::Capacity("scope allocation bytes"));
        cause = Some(raw);
        Err::<(), AdmissionRefusal<WorkError>>(AdmissionRefusal::Control(
            CompileControlError::ResourceExhausted,
        ))
    })
    .err()
    .unwrap();
    assert!(matches!(
        error,
        ProjectionFailure::Codec(PhysicalConstantCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(calls, 1);
    assert_eq!(cause, Some(WorkError::Capacity("scope allocation bytes")));
    assert_eq!(authority.snapshot().held_bytes(), 0);
}
#[test]
fn sql_dependency_constant_host_abort_explicit_unsupported_profiles_keep_old_codec_error() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0i8, 1]),
            Arc::new(StringArray::from(vec!["a", "b"])),
        )
        .unwrap(),
    );
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        1,
        Arc::new(Int32Array::from(vec![1, 2])),
        None,
    ));
    for array in [dictionary, fixed] {
        let pool = pool(array, ValueLogicalType::Physical);
        let control = Control::new(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let old = prepare_constant_record_write_in(
            ConstantPoolId::new(0),
            0,
            0,
            &pool,
            source(&pool),
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .err()
        .unwrap();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let new = prepare_constant_record_write_with_host_in(
            ConstantPoolId::new(0),
            0,
            0,
            &pool,
            source(&pool),
            limits(),
            &mut |_| Ok::<_, AdmissionRefusal<Infallible>>(()),
            &mut work,
        )
        .err()
        .unwrap();
        assert_eq!(new.to_string(), old.to_string());
        assert!(matches!(
            new,
            ProjectionFailure::Codec(PhysicalConstantCodecError::Type(_))
        ));
    }
}
