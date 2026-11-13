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

//! Public compilation entry points on real supported and unsupported types.
//! No replacement resolver, invalid type graph, or runtime row invocation.
use arrow_schema::DataType;
use novarocks_failure_contract::{FailureFact, FailureProjection, OuterFailureProjection};
use novarocks_functions::*;
use novarocks_type_contract::CompilePhase;
use novarocks_type_contract::FunctionValueType;
use novarocks_type_contract::PureCompileControl;
use novarocks_type_contract::{CompileControlError, owned_resources::layout::LayoutResourceError};
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, Debug)]
enum Entry {
    Computed,
    Recipe,
}
const ENTRIES: [Entry; 2] = [Entry::Computed, Entry::Recipe];
impl Entry {
    fn run(
        self,
        source: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionValueType, ArithmeticPrepareError> {
        match self {
            Self::Computed => native_negate_computed_result_type(source, control),
            Self::Recipe => PreparedNativeNegateRecipe::try_new(source, source, control)
                .map(|prepared| prepared.result_type().clone()),
        }
    }
    fn refusal_prefix(self) -> &'static [u32] {
        match self {
            // Computed has its original entry, pre-clone flush, then recipe entry.
            Self::Computed => &[0, 0, 0],
            Self::Recipe => &[0],
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after original compile refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
}
fn causes() -> [CompileControlError; 4] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::from_domain(LayoutResourceError::Arithmetic),
    ]
}
fn assert_control(actual: ArithmeticPrepareError, original: &CompileControlError) {
    let actual = actual.control_error().expect("original control required");
    assert_eq!(&actual, original);
    if let (CompileControlError::Source(actual), CompileControlError::Source(original)) =
        (&actual, original)
    {
        assert!(std::ptr::eq(
            actual.downcast_ref::<LayoutResourceError>().unwrap(),
            original.downcast_ref::<LayoutResourceError>().unwrap(),
        ));
    }
}
fn assert_zero_refusal(actual: ArithmeticPrepareError, carrier: &DataType) {
    match actual {
        ArithmeticPrepareError::Kernel(KernelFailure::InvalidProgram(detail)) => assert_eq!(
            detail.message(),
            format!("NEGATE is not supported for data type {carrier:?}")
        ),
        other => panic!("original typed-zero refusal changed: {other:?}"),
    }
}
#[test]
fn native_negate_public_compile_original_zero_refusal_precedes_every_armed_late_control() {
    // These are valid immutable Arrow/type bindings. The real typed-zero author
    // refuses them; the test does not manufacture an impossible type graph.
    for carrier in [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Decimal32(7, 2),
        DataType::Decimal64(18, 2),
        DataType::Null,
    ] {
        let source = FunctionValueType::from_legacy_declared_projection(carrier.clone(), true);
        for entry in ENTRIES {
            let baseline = Control::default();
            assert_zero_refusal(entry.run(&source, &baseline).unwrap_err(), &carrier);
            assert_eq!(*baseline.trace.lock().unwrap(), entry.refusal_prefix());
            for late in causes() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((entry.refusal_prefix().len(), late)),
                };
                assert_zero_refusal(entry.run(&source, &control).unwrap_err(), &carrier);
                assert_eq!(*control.trace.lock().unwrap(), entry.refusal_prefix());
            }
            // Original checkpoints before the semantic author still take effect.
            for at in 0..entry.refusal_prefix().len() {
                for original in causes() {
                    let control = Control {
                        trace: Mutex::default(),
                        refusal: Some((at, original.clone())),
                    };
                    assert_control(entry.run(&source, &control).unwrap_err(), &original);
                    assert_eq!(
                        *control.trace.lock().unwrap(),
                        entry.refusal_prefix()[..=at]
                    );
                }
            }
        }
    }
}
#[test]
fn native_negate_public_compile_success_keeps_all_actual_checkpoints_and_original_four_causes() {
    for source in [
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Decimal256(76, 6), true),
    ] {
        for entry in ENTRIES {
            let baseline = Control::default();
            assert_eq!(entry.run(&source, &baseline).unwrap(), source);
            let trace = baseline.trace.into_inner().unwrap();
            assert_eq!(trace.first(), Some(&0));
            assert!(trace.last().is_some_and(|units| *units > 0));
            assert!(trace.len() > entry.refusal_prefix().len());
            for at in 0..trace.len() {
                for original in causes() {
                    let control = Control {
                        trace: Mutex::default(),
                        refusal: Some((at, original.clone())),
                    };
                    assert_control(entry.run(&source, &control).unwrap_err(), &original);
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[derive(Debug)]
struct CallerStorage {
    original: LayoutResourceError,
    bytes: Box<[u8]>,
    drops: Arc<AtomicUsize>,
}
impl fmt::Display for CallerStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.original, f)
    }
}
impl std::error::Error for CallerStorage {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}
impl FailureProjection for CallerStorage {
    fn failure_fact(&self) -> FailureFact {
        self.original.failure_fact()
    }
}
impl OuterFailureProjection for CallerStorage {}
impl Drop for CallerStorage {
    fn drop(&mut self) {
        assert_eq!(self.bytes.len(), 32 * 1024);
        assert!(self.bytes.iter().all(|byte| *byte == 73));
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn native_negate_public_compile_source_keeps_its_actual_storage_until_last_returned_owner_drop() {
    let source = FunctionValueType::new(DataType::Decimal256(76, 6), true);
    for entry in ENTRIES {
        let baseline = Control::default();
        entry.run(&source, &baseline).unwrap();
        let trace = baseline.trace.into_inner().unwrap();
        for at in 0..trace.len() {
            let drops = Arc::new(AtomicUsize::new(0));
            let original = CompileControlError::from_domain(CallerStorage {
                original: LayoutResourceError::Arithmetic,
                bytes: vec![73; 32 * 1024].into_boxed_slice(),
                drops: drops.clone(),
            });
            let CompileControlError::Source(cause) = &original else {
                unreachable!()
            };
            let pointer = cause.downcast_ref::<CallerStorage>().unwrap() as *const CallerStorage;
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, original)),
            };
            let returned = entry.run(&source, &control).unwrap_err();
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            let CompileControlError::Source(actual) = returned.control_error().unwrap() else {
                panic!("original caller source required")
            };
            assert_eq!(
                actual.downcast_ref::<CallerStorage>().unwrap() as *const _,
                pointer
            );
            let diagnostic_clone = returned.control_error().unwrap();
            drop(actual);
            drop(control);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(returned);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(diagnostic_clone);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }
}
