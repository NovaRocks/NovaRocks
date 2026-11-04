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

use super::*;
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    late: Option<CompileControlError>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.trace.lock().unwrap().push(units);
        if units != 0
            && let Some(cause) = self.late.clone()
        {
            return Err(cause);
        }
        Ok(())
    }
}
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

#[test]
fn captured_constant_fields_are_actual_sparse_root_arcs_before_completed_lookup() {
    let roots = [
        (0, Arc::new(Field::new("zero", DataType::Int64, true))),
        (u32::MAX, Arc::new(Field::new("max", DataType::Utf8, false))),
    ];
    let table = encode_type_table_sources(
        &[],
        &roots,
        TypeProjectionLimits {
            max_definitions: 32,
            max_expanded_nodes: 32,
            max_string_bytes: 4096,
        },
        &Control::default(),
    )
    .unwrap();
    for (id, source) in &roots {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut captures = 0;
        let found = table
            .field_captured::<TypeCodecError>(
                *id,
                &mut |actual, _| {
                    assert!(std::ptr::eq(actual, source));
                    captures += 1;
                    Ok(())
                },
                &mut work,
            )
            .unwrap()
            .unwrap();
        assert!(std::ptr::eq(found, source));
        assert_eq!(captures, 1);
        work.finish().unwrap();
    }
    for cause in CAUSES {
        let control = Control {
            late: Some(cause),
            ..Control::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let result = table.field_captured::<TypeCodecError>(
            0,
            &mut |actual, _| {
                assert!(std::ptr::eq(actual, &roots[0].1));
                Err(CompileControlError::ResourceExhausted.into())
            },
            &mut work,
        );
        assert!(matches!(
            result,
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(*control.trace.lock().unwrap(), vec![0]);
    }
}

#[test]
fn original_carrier_scratch_admission_precedes_initialization_and_pending_control() {
    let ty = DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int64, true))].into());
    for cause in CAUSES {
        let control = Control {
            late: Some(cause),
            ..Control::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let result = validate_type_with_scratch_observed(
            &ty,
            &mut |layout| {
                assert_eq!(
                    layout,
                    std::alloc::Layout::new::<novarocks_type_contract::owned_resources::type_validation::TypeValidationScratch<'_>>()
                );
                Err(CompileControlError::ResourceExhausted)
            },
            &mut |_| panic!("source visit after scratch refusal"),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(*control.trace.lock().unwrap(), vec![0]);
    }
    let plain = Control::default();
    let mut old = CompileCheckpoints::try_new(&plain, CompilePhase::Decode).unwrap();
    validate_type(&ty, &mut old).unwrap();
    old.finish().unwrap();
    let caller = Control::default();
    let mut new = CompileCheckpoints::try_new(&caller, CompilePhase::Decode).unwrap();
    let mut visits = 0;
    validate_type_with_scratch_observed(
        &ty,
        &mut |_| Ok(()),
        &mut |_| {
            visits += 1;
            Ok(())
        },
        &mut new,
    )
    .unwrap();
    new.finish().unwrap();
    assert_eq!(visits, 4);
    assert_eq!(*plain.trace.lock().unwrap(), *caller.trace.lock().unwrap());
}
