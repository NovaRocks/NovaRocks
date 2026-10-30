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

//! The sole original binding sender with exact original source loans and real host refusals.
use arrow::datatypes::{DataType, Field};
use novarocks_physical_plan::{BoundFunction, BoundTableFunction};
use novarocks_plan_codec::{
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    physical_binding_v2::{
        ArgumentTypeIds, BindingCodecError, BindingProjectionFacts, BindingProjectionLimits,
        BindingSource, FunctionBindingInput, ResultTypeIds, encode_function_bindings_in,
        encode_function_bindings_with_host_in,
    },
    physical_type_v2::{TypeProjectionLimits, encode_type_table_sources},
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionArgumentType, FunctionId,
    FunctionKind, FunctionOverloadId, FunctionValueType, PureCompileControl,
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
    refuse: Option<(usize, CompileControlError)>,
}
impl Control {
    fn new(refuse: Option<(usize, CompileControlError)>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            refuse,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut calls = self.calls.lock().unwrap();
        calls.push(n);
        if let Some((at, cause)) = self.refuse.clone()
            && calls.len() == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn owner() -> WorkloadControl {
    let owner = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 64 * 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 64 * 1024 * 1024 - 1024,
        },
    )
    .unwrap();
    owner.mark_ready().unwrap();
    owner
}
fn limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 64,
        max_type_references: 128,
        max_request_bytes: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    }
}
fn scalar(
    kind: FunctionKind,
    args: Vec<FunctionArgumentType>,
    result: FunctionValueType,
) -> BoundFunction {
    BoundFunction::from_exact_signature(
        FunctionId::try_new("fixture/exact").unwrap(),
        FunctionOverloadId::try_new("fixture/selected").unwrap(),
        kind,
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(args.into_boxed_slice()),
        result,
    )
}
fn stock(
    held: &mut Option<Reservation>,
    authority: &novarocks_workload_control::LocalResourceAuthority,
    scope: &novarocks_workload_control::WorkScope,
    bytes: usize,
) -> Result<(), WorkError> {
    let bytes = u64::try_from(bytes).unwrap();
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
/// Original source constructors and original type sender; no name resolution or source inference.
fn workflow<H>(
    control: &Control,
    original: bool,
    bad_order: bool,
    admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), AdmissionRefusal<H>>,
) -> Result<String, ProjectionFailure<BindingCodecError, H>> {
    let v = [
        (0, FunctionValueType::new(DataType::Int64, false)),
        (
            7,
            FunctionValueType::new(
                DataType::Struct(
                    vec![Arc::new(
                        Field::new("child", DataType::Utf8, true).with_metadata(
                            std::collections::HashMap::<String, String>::from([(
                                "original".into(),
                                "metadata".into(),
                            )]),
                        ),
                    )]
                    .into(),
                ),
                true,
            ),
        ),
        (
            u32::MAX,
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ),
    ];
    let type_control = Control::new(None);
    let types = encode_type_table_sources(
        &v,
        &[],
        TypeProjectionLimits {
            max_definitions: 64,
            max_expanded_nodes: 128,
            max_string_bytes: 4096,
        },
        &type_control,
    )
    .unwrap();
    let scalar_binding = scalar(
        FunctionKind::Scalar,
        vec![FunctionArgumentType::Value(v[2].1.clone())],
        v[1].1.clone(),
    );
    let aggregate = scalar(
        FunctionKind::Aggregate,
        vec![FunctionArgumentType::Lambda {
            parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                Box::from([v[0].1.clone(), v[2].1.clone()]),
            ),
            result_type: v[1].1.clone(),
        }],
        v[0].1.clone(),
    );
    let window = scalar(FunctionKind::Window, vec![], v[0].1.clone());
    let table = BoundTableFunction::from_exact_signature(
        FunctionId::try_new("fixture/table").unwrap(),
        FunctionOverloadId::try_new("fixture/table-selected").unwrap(),
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::from([
            FunctionArgumentType::Value(v[0].1.clone()),
        ])),
        novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(Box::from([
            v[0].1.clone(),
            v[1].1.clone(),
        ])),
    );
    let scalar_args = [ArgumentTypeIds::Value(u32::MAX)];
    let lambda_params = [0, u32::MAX];
    let lambda_args = [ArgumentTypeIds::Lambda {
        parameters: &lambda_params,
        result: 7,
    }];
    let table_args = [ArgumentTypeIds::Value(0)];
    let relation = [0, 7];
    let inputs = [
        FunctionBindingInput {
            id: 0,
            source: BindingSource::Scalar(&scalar_binding),
            arguments: &scalar_args,
            result: ResultTypeIds::Scalar(7),
        },
        FunctionBindingInput {
            id: if bad_order { 0 } else { 9 },
            source: BindingSource::Scalar(&aggregate),
            arguments: &lambda_args,
            result: ResultTypeIds::Scalar(0),
        },
        FunctionBindingInput {
            id: 27,
            source: BindingSource::Scalar(&window),
            arguments: &[],
            result: ResultTypeIds::Scalar(0),
        },
        FunctionBindingInput {
            id: u32::MAX,
            source: BindingSource::Table(&table),
            arguments: &table_args,
            result: ResultTypeIds::Relation(&relation),
        },
    ];
    // Explicit finite fixture invoice, including aliasing backing; not a native memory claim.
    let source = 512 * 1024;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let encoded = if original {
        encode_function_bindings_in(
            &types,
            &inputs,
            source,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .map_err(ProjectionFailure::Codec)?
    } else {
        encode_function_bindings_with_host_in(&types, &inputs, source, limits(), admit, &mut work)?
    };
    assert!(std::ptr::eq(
        encoded
            .scalar_binding_in(0, &mut |_| Ok(()), &mut work)?
            .unwrap(),
        &scalar_binding
    ));
    assert!(std::ptr::eq(
        encoded
            .table_binding_observed(u32::MAX, &mut work)?
            .unwrap(),
        &table
    ));
    work.finish()?;
    // Debug includes every protobuf field; this test has no independent serializer.
    Ok(format!("{:?}", encoded.as_wire()))
}
#[test]
fn sql_dependency_binding_host_abort_original_full_emission_and_real_data_grant() {
    let original = workflow(&Control::new(None), true, false, &mut |_| {
        Ok::<_, AdmissionRefusal<Infallible>>(())
    })
    .unwrap();
    let actual = owner();
    let task = actual
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = task.owner.scope();
    let authority = actual
        .resources()
        .expect("test workload resource authority");
    let mut held = None;
    let mut callbacks = 0;
    let selected = workflow(&Control::new(None), false, false, &mut |facts| {
        callbacks += 1;
        stock(
            &mut held,
            &authority,
            &scope,
            facts.request_bytes_upper_bound,
        )
        .map_err(AdmissionRefusal::Host)
    })
    .unwrap();
    assert_eq!(selected, original);
    assert!(callbacks > 10);
    assert!(authority.snapshot().data_reserved_bytes > 0);
    assert_eq!(authority.snapshot().data_used_bytes, 0);
    drop(held);
    assert_eq!(authority.snapshot().held_bytes(), 0);
}
#[test]
fn sql_dependency_binding_host_abort_preserves_original_invalid_shape_byte_text() {
    let before = workflow(&Control::new(None), true, true, &mut |_| {
        Ok::<_, AdmissionRefusal<Infallible>>(())
    })
    .err()
    .unwrap();
    let after = workflow(&Control::new(None), false, true, &mut |_| {
        Ok::<_, AdmissionRefusal<Infallible>>(())
    })
    .err()
    .unwrap();
    assert_eq!(after.to_string(), before.to_string());
    assert_eq!(
        after.to_string(),
        "binding input IDs must be unique and ascending"
    );
    assert!(matches!(
        after,
        ProjectionFailure::Codec(BindingCodecError::InvalidShape(_))
    ));
}
#[test]
fn sql_dependency_binding_host_abort_every_real_admission_keeps_nominal_cause_and_rolls_back() {
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
    let mut total = 0;
    workflow(&Control::new(None), false, false, &mut |_| {
        total += 1;
        Ok::<_, AdmissionRefusal<&WorkError>>(())
    })
    .unwrap();
    for refused in 1..=total {
        let mut held = None;
        let mut calls = 0;
        let control = Control::new(None);
        let error = workflow(&control, false, false, &mut |facts| {
            calls += 1;
            assert!(calls <= refused, "host callback after first refusal");
            if calls == refused {
                return Err(AdmissionRefusal::Host(&cause));
            }
            stock(
                &mut held,
                &authority,
                &scope,
                facts.request_bytes_upper_bound,
            )
            .unwrap();
            Ok(())
        })
        .err()
        .unwrap();
        assert!(matches!(error,ProjectionFailure::Host(error) if std::ptr::eq(error,&cause)));
        assert_eq!(calls, refused);
        drop(held);
        assert_eq!(authority.snapshot().held_bytes(), 0);
        assert_eq!(
            foreign
                .resources()
                .expect("test workload resource authority")
                .snapshot()
                .held_bytes(),
            0
        );
    }
}
#[test]
fn sql_dependency_binding_host_abort_original_three_control_causes_have_no_tail() {
    let count_control = Control::new(None);
    workflow(&count_control, false, false, &mut |_| {
        Ok::<_, AdmissionRefusal<Infallible>>(())
    })
    .unwrap();
    let count = count_control.calls.lock().unwrap().len();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=count {
            let control = Control::new(Some((at, cause.clone())));
            let error = workflow(&control, false, false, &mut |_| {
                Ok::<_, AdmissionRefusal<Infallible>>(())
            })
            .err()
            .unwrap();
            assert!(
                matches!(error,ProjectionFailure::Codec(BindingCodecError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.calls.lock().unwrap().len(), at);
        }
    }
}
#[test]
fn sql_dependency_binding_host_abort_only_actual_capacity_uses_original_control_projection() {
    let actual = owner();
    let task = actual
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = task.owner.scope();
    let authority = actual
        .resources()
        .expect("test workload resource authority");
    let mut original_cause = None;
    let mut calls = 0;
    let error = workflow(&Control::new(None), false, false, &mut |_| {
        calls += 1;
        let cause = authority
            .reserve(&scope, u64::MAX, ResourceClass::Data)
            .err()
            .unwrap();
        assert_eq!(cause, WorkError::Capacity("scope allocation bytes"));
        original_cause = Some(cause);
        Err::<(), AdmissionRefusal<WorkError>>(AdmissionRefusal::Control(
            CompileControlError::ResourceExhausted,
        ))
    })
    .err()
    .unwrap();
    assert!(matches!(
        error,
        ProjectionFailure::Codec(BindingCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(calls, 1);
    assert_eq!(
        original_cause,
        Some(WorkError::Capacity("scope allocation bytes"))
    );
    assert_eq!(authority.snapshot().held_bytes(), 0);
}
