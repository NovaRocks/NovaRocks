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
use crate::PreparedArithmeticRecipe;
use arrow_array::{Array, ArrayRef, Float32Array, Float64Array};
use novarocks_type_contract::{CompileControlError, CompilePhase};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Compile {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        t.push(n);
        if let Some((target, cause)) = self.refusal.clone()
            && at == target
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        t.push(n);
        if let Some((target, cause)) = &self.refusal
            && at == *target
        {
            Err(cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Float arithmetic never waits")
    }
}
const OPS: [ArithmeticOperator; 5] = [
    ArithmeticOperator::Add,
    ArithmeticOperator::Subtract,
    ArithmeticOperator::Multiply,
    ArithmeticOperator::Divide,
    ArithmeticOperator::Modulo,
];
fn prepare(op: ArithmeticOperator, left: DataType, right: DataType) -> PreparedArithmeticRecipe {
    PreparedArithmeticRecipe::try_new(
        op,
        &FunctionValueType::from_legacy_declared_projection(left, true),
        &FunctionValueType::from_legacy_declared_projection(right, true),
        &FunctionValueType::new(DataType::Float64, true),
        DecimalOverflowPolicy::OutputNull,
        false,
        &Compile::default(),
    )
    .unwrap()
}
#[test]
fn float_arithmetic_recipe_consumes_original_arrow_math_and_width_zero_policy() {
    let left: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(1.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::from_bits(0x7ff8123456789abc)),
        None,
        Some(9.0),
    ]));
    let peers: Vec<ArrayRef> = vec![
        Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(1.0),
            Some(3.0),
            Some(7.0),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(1.0),
            Some(3.0),
            Some(7.0),
            None,
        ])),
    ];
    for right in peers {
        for op in OPS {
            let recipe = prepare(op, left.data_type().clone(), right.data_type().clone());
            let expected = match op {
                ArithmeticOperator::Add => crate::legacy_arithmetic::eval_add_arrays(
                    left.clone(),
                    right.clone(),
                    DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
                ArithmeticOperator::Subtract => crate::legacy_arithmetic::eval_sub_arrays(
                    left.clone(),
                    right.clone(),
                    DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
                ArithmeticOperator::Multiply => crate::legacy_arithmetic::eval_mul_arrays(
                    left.clone(),
                    right.clone(),
                    DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
                ArithmeticOperator::Divide => crate::legacy_arithmetic::eval_div_arrays(
                    left.clone(),
                    right.clone(),
                    &DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
                ArithmeticOperator::Modulo => crate::legacy_arithmetic::eval_mod_arrays(
                    left.clone(),
                    right.clone(),
                    DataType::Float64,
                    false,
                    DecimalOverflowPolicy::OutputNull,
                ),
            }
            .unwrap();
            let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
            for row in 0..left.len() {
                let actual = recipe
                    .evaluate_row(
                        EvaluatedArgument::Column(&left),
                        row,
                        row,
                        EvaluatedArgument::Column(&right),
                        row,
                        row,
                        &Control::default(),
                    )
                    .unwrap();
                if expected.is_null(row) {
                    assert_eq!(actual, ArithmeticRowResult::Null);
                } else {
                    let ArithmeticRowResult::Float(value) = actual else {
                        panic!("original F64 expected")
                    };
                    assert_eq!(value.to_bits(), expected.value(row).to_bits());
                }
            }
        }
    }
}
#[test]
fn float_arithmetic_recipe_full_fvt_result_and_nominal_boundary_is_exact() {
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(1, -128),
        DataType::Decimal128(38, 38),
    ] {
        for op in OPS {
            for (left, right) in [
                (DataType::Float32, ty.clone()),
                (ty.clone(), DataType::Float64),
            ] {
                let recipe = prepare(op, left.clone(), right.clone());
                assert_eq!(
                    recipe.left_type(),
                    &FunctionValueType::from_legacy_declared_projection(left, true)
                );
                assert_eq!(
                    recipe.right_type(),
                    &FunctionValueType::from_legacy_declared_projection(right, true)
                );
                assert_eq!(
                    recipe.result_type(),
                    &FunctionValueType::new(DataType::Float64, true)
                );
            }
        }
    }
    let f = FunctionValueType::new(DataType::Float64, true);
    for other in [
        DataType::UInt64,
        DataType::Float16,
        DataType::Decimal256(76, 1),
        DataType::Boolean,
        DataType::FixedSizeBinary(16),
    ] {
        assert_eq!(
            PreparedArithmeticRecipe::try_new(
                ArithmeticOperator::Add,
                &f,
                &FunctionValueType::from_legacy_declared_projection(other, true),
                &f,
                DecimalOverflowPolicy::OutputNull,
                false,
                &Compile::default()
            ),
            Err(ArithmeticPrepareError::Unsupported)
        );
    }
    assert_eq!(
        PreparedArithmeticRecipe::try_new(
            ArithmeticOperator::Add,
            &f,
            &f,
            &FunctionValueType::new(DataType::Int64, true),
            DecimalOverflowPolicy::OutputNull,
            false,
            &Compile::default()
        ),
        Err(ArithmeticPrepareError::TypeMismatch)
    );
    assert_eq!(
        PreparedArithmeticRecipe::try_new(
            ArithmeticOperator::Add,
            &f,
            &f,
            &FunctionValueType::new(DataType::Float64, false),
            DecimalOverflowPolicy::OutputNull,
            false,
            &Compile::default()
        ),
        Err(ArithmeticPrepareError::TypeMismatch)
    );
}
#[test]
fn float_arithmetic_recipe_three_compile_and_seven_runtime_causes_have_no_tail() {
    let f = FunctionValueType::new(DataType::Float64, true);
    let make = |c: &dyn PureCompileControl| {
        PreparedArithmeticRecipe::try_new(
            ArithmeticOperator::Add,
            &f,
            &f,
            &f,
            DecimalOverflowPolicy::OutputNull,
            false,
            c,
        )
    };
    let normal = Compile::default();
    let recipe = make(&normal).unwrap();
    let trace = normal.trace.lock().unwrap().clone();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let c = Compile {
                refusal: Some((at, cause.clone())),
                ..Compile::default()
            };
            assert_eq!(make(&c).unwrap_err().control_error(), Some(cause.clone()));
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
        }
    }
    let left: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0)]));
    let right: ArrayRef = Arc::new(Float64Array::from(vec![Some(2.0)]));
    let run = |c: &dyn KernelEvaluationControl| {
        recipe.evaluate_row(
            EvaluatedArgument::Column(&left),
            0,
            0,
            EvaluatedArgument::Column(&right),
            0,
            0,
            c,
        )
    };
    let normal = Control::default();
    assert_eq!(run(&normal).unwrap(), ArithmeticRowResult::Float(3.0));
    let trace = normal.trace.lock().unwrap().clone();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("actual Float control")),
        KernelFailure::InstanceFailed,
        KernelFailure::Internal(crate::KernelDiagnostic::new("actual Float control")),
        KernelFailure::Operational(crate::KernelDiagnostic::new("actual Float control")),
    ] {
        for at in 0..trace.len() {
            let c = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            assert_eq!(run(&c), Err(cause.clone()));
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn canonical_float_target_actual_factory_precedes_construction_and_survives_last_cast_loan() {
    use crate::{ArithmeticCreationFailure, ArithmeticTypeCreationFailure};
    use novarocks_type_contract::{
        ValueConstructionFailure, ValueSchemaCreationFactory, ValueSchemaCreationScope,
        ValueSchemaScopeBeginFailure, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::alloc::Layout;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Hold {
        charged: AtomicUsize,
        live: Arc<AtomicUsize>,
    }
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.live
                .fetch_sub(*self.charged.get_mut(), Ordering::SeqCst);
        }
    }
    struct Compile(
        Mutex<Vec<(CompilePhase, u32)>>,
        Option<(usize, CompileControlError)>,
    );
    impl PureCompileControl for Compile {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.0.lock().unwrap();
            let at = trace.len();
            if let Some((stop, _)) = self.1 {
                assert!(
                    at <= stop,
                    "callback after original arithmetic control refusal"
                );
            }
            trace.push((phase, units));
            match self.1.clone() {
                Some((stop, cause)) if at == stop => Err(cause),
                _ => Ok(()),
            }
        }
    }
    struct Factory<'a> {
        control: &'a Compile,
        attempts: AtomicUsize,
        fail: Option<usize>,
        refused_at_control: AtomicUsize,
        cause: Arc<()>,
        live: Arc<AtomicUsize>,
        holds: Mutex<Vec<std::sync::Weak<Hold>>>,
    }
    impl Factory<'_> {
        fn quote(&self, layout: Layout) -> Result<(), Arc<()>> {
            assert_ne!(layout.size(), 0);
            let at = self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail == Some(at) {
                self.refused_at_control
                    .store(self.control.0.lock().unwrap().len(), Ordering::SeqCst);
                return Err(self.cause.clone());
            }
            self.live.fetch_add(layout.size(), Ordering::SeqCst);
            Ok(())
        }
    }
    struct Scope<'a> {
        factory: &'a Factory<'a>,
        hold: Arc<Hold>,
    }
    impl ValueSchemaCreationScope<Arc<()>> for Scope<'_> {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            ValueTypeStorageLifetime::held(self.hold.clone())
        }
        fn request_layout(&mut self, layout: Layout) -> Result<(), Arc<()>> {
            self.factory.quote(layout)?;
            self.hold.charged.fetch_add(layout.size(), Ordering::SeqCst);
            Ok(())
        }
        fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), Arc<()>> {
            panic!("canonical Float64 creates no opaque collection or payload")
        }
    }
    impl ValueSchemaCreationFactory<Arc<()>> for Factory<'_> {
        type Scope<'a>
            = Scope<'a>
        where
            Self: 'a;
        fn begin_scope(&self) -> Result<Self::Scope<'_>, Arc<()>> {
            let layout =
                novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Hold>())
                    .unwrap();
            self.quote(layout)?;
            let hold = Arc::new(Hold {
                charged: AtomicUsize::new(layout.size()),
                live: self.live.clone(),
            });
            self.holds.lock().unwrap().push(Arc::downgrade(&hold));
            Ok(Scope {
                factory: self,
                hold,
            })
        }
        fn begin_scope_observed<O>(
            &self,
            observe: &mut dyn FnMut(Layout) -> Result<(), O>,
        ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, Arc<()>>> {
            let layout =
                novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Hold>())
                    .unwrap();
            observe(layout).map_err(ValueSchemaScopeBeginFailure::Observer)?;
            self.begin_scope()
                .map_err(ValueSchemaScopeBeginFailure::Creator)
        }
    }
    let source = FunctionValueType::new(DataType::Float32, false);
    let right = FunctionValueType::new(DataType::Int64, false);
    let output = FunctionValueType::new(DataType::Float64, true);
    let run = |fail,
               control_fail: Option<(usize, CompileControlError)>,
               expected_trace: &[(CompilePhase, u32)]| {
        let control = Compile(Mutex::new(Vec::new()), control_fail.clone());
        let live = Arc::new(AtomicUsize::new(0));
        let factory = Factory {
            control: &control,
            attempts: AtomicUsize::new(0),
            fail,
            refused_at_control: AtomicUsize::new(0),
            cause: Arc::new(()),
            live: live.clone(),
            holds: Mutex::new(Vec::new()),
        };
        let result = PreparedArithmeticRecipe::try_new_with_creator_in(
            ArithmeticOperator::Add,
            &source,
            &right,
            &output,
            DecimalOverflowPolicy::OutputNull,
            true,
            &factory,
            &control,
        );
        let total = factory.attempts.load(Ordering::SeqCst);
        if let Some(at) = fail {
            assert!(matches!(result,
                Err(ArithmeticCreationFailure::Construction(ValueConstructionFailure::Observer(
                    ArithmeticTypeCreationFailure::Creator(ref actual)))) if Arc::ptr_eq(actual, &factory.cause)));
            assert_eq!(total, at + 1);
            assert_eq!(
                control.0.lock().unwrap().len(),
                factory.refused_at_control.load(Ordering::SeqCst)
            );
            assert_eq!(live.load(Ordering::SeqCst), 0);
        } else if let Some((stop, cause)) = control_fail {
            match result.unwrap_err() {
                ArithmeticCreationFailure::Prepare(error) => {
                    assert_eq!(error.control_error(), Some(cause))
                }
                ArithmeticCreationFailure::Construction(ValueConstructionFailure::Observer(
                    ArithmeticTypeCreationFailure::Control(actual),
                )) => assert_eq!(actual, cause),
                other => panic!("wrong original arithmetic cause: {other:?}"),
            }
            assert_eq!(*control.0.lock().unwrap(), expected_trace[..=stop]);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        } else {
            let prepared = result.unwrap();
            let super::super::ArithmeticAlgorithm::Float(float) = &prepared.algorithm else {
                panic!("original Float profile")
            };
            let late_target = float.left.result_type().clone();
            assert!(
                float
                    .left
                    .result_type()
                    .value_type()
                    .shares_storage(float.right.result_type().value_type())
            );
            let weak = factory.holds.lock().unwrap()[0].clone();
            assert_eq!(factory.holds.lock().unwrap().len(), 1);
            drop(prepared);
            drop(factory);
            assert!(weak.upgrade().is_some());
            assert!(live.load(Ordering::SeqCst) > 0);
            drop(late_target);
            assert!(weak.upgrade().is_none());
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }
        let trace = control.0.lock().unwrap().clone();
        (total, trace)
    };
    let (total, trace) = run(None, None, &[]);
    assert!(
        total >= 3,
        "actual scope, layout and semantic holder operations"
    );
    for at in 0..total {
        run(Some(at), None, &[]);
    }
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for stop in 0..trace.len() {
            run(None, Some((stop, cause.clone())), &trace);
        }
    }
    let control = Compile(Mutex::new(Vec::new()), None);
    let factory = Factory {
        control: &control,
        attempts: AtomicUsize::new(0),
        fail: Some(0),
        refused_at_control: AtomicUsize::new(0),
        cause: Arc::new(()),
        live: Arc::new(AtomicUsize::new(0)),
        holds: Mutex::new(Vec::new()),
    };
    let unsupported = FunctionValueType::new(DataType::Utf8, false);
    assert!(matches!(
        PreparedArithmeticRecipe::try_new_with_creator_in(
            ArithmeticOperator::Add,
            &unsupported,
            &right,
            &output,
            DecimalOverflowPolicy::OutputNull,
            true,
            &factory,
            &control,
        ),
        Err(ArithmeticCreationFailure::Prepare(
            ArithmeticPrepareError::Unsupported
        ))
    ));
    assert_eq!(factory.attempts.load(Ordering::SeqCst), 0);
    let signed_output = FunctionValueType::new(DataType::Int64, true);
    PreparedArithmeticRecipe::try_new_with_creator_in(
        ArithmeticOperator::Add,
        &right,
        &right,
        &signed_output,
        DecimalOverflowPolicy::OutputNull,
        true,
        &factory,
        &control,
    )
    .unwrap();
    assert_eq!(factory.attempts.load(Ordering::SeqCst), 0);
}
