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
//! Exact Physical FSB16 cast profile; nominal LargeInt remains a hidden conversion.
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use novarocks_functions::*;
use novarocks_functions::{ConstantPolicy, ConstantPool, SelectedValues, Selection};
use novarocks_type_contract::CompileControlError;
use novarocks_type_contract::CompilePhase;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_type_contract::ExpressionEffectContext;
use novarocks_type_contract::FunctionValueType;
use novarocks_type_contract::PureCompileControl;
use novarocks_type_contract::ValueLogicalType;
use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refuse: Option<(usize, KernelFailure)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.refuse {
            assert!(at <= *stop);
        }
        t.push(n);
        if let Some((stop, cause)) = &self.refuse
            && *stop == at
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("LARGEINT text never waits")
    }
}
fn input() -> ArrayRef {
    novarocks_functions::largeint::array_from_i128(&[
        Some(i128::MIN),
        Some(42),
        None,
        Some(i128::MAX),
    ])
    .unwrap()
}
fn recipe(nullable: bool) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(DataType::FixedSizeBinary(16), nullable),
        &FunctionValueType::new(DataType::Utf8, nullable),
        DecimalOverflowPolicy::ReportError,
        true,
        &Control::default(),
    )
    .unwrap()
}
#[test]
fn largeint_text_physical_profile_preserves_nominal_gate_and_exact_types() {
    for nullable in [false, true] {
        let r = recipe(nullable);
        assert_eq!(
            r.source_type(),
            &FunctionValueType::new(DataType::FixedSizeBinary(16), nullable)
        );
        let c = ExpressionEffectContext {
            use_id: ExpressionUseId::new(1),
            domain: EvaluationDomainId::new(2),
            demand: EvaluationDemand::Value,
        };
        assert!(!r.own_effects(c).for_use(c).unwrap().may_raise_row_error);
    }
    for domain in [ValueLogicalType::LargeInt, ValueLogicalType::Uuid] {
        let source =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, domain)
                .unwrap();
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &FunctionValueType::new(DataType::Utf8, true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &Control::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &FunctionValueType::new(DataType::FixedSizeBinary(16), true),
            &FunctionValueType::new(DataType::Utf8, false),
            DecimalOverflowPolicy::ReportError,
            true,
            &Control::default()
        ),
        Err(CastPrepareError::TypeMismatch)
    );
    for width in [15, 17] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(DataType::FixedSizeBinary(width), true),
                &FunctionValueType::new(DataType::Utf8, true),
                DecimalOverflowPolicy::ReportError,
                true,
                &Control::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
}
#[test]
fn largeint_text_dense_compact_scalar_null_and_slice_keep_actual_addresses() {
    let values = input();
    let r = recipe(true);
    let rows = [1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let ty = r.source_type();
    let compact = SelectedValues::try_new(
        selection,
        ty.data_type(),
        values.slice(1, 3),
        Box::default(),
    )
    .unwrap();
    for (ordinal, row) in selection.iter().enumerate() {
        let expected = match row {
            1 => CastRowResult::Text("42".into()),
            2 => CastRowResult::Null,
            _ => CastRowResult::Text("170141183460469231731687303715884105727".into()),
        };
        for arg in [
            EvaluatedArgument::Column(&values),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            assert_eq!(
                r.evaluate_row(arg, ordinal, row, &Control::default())
                    .unwrap(),
                expected
            );
        }
    }
    let scalar = values.slice(0, 1);
    assert_eq!(
        r.evaluate_row(
            EvaluatedArgument::Scalar(&scalar),
            9,
            99,
            &Control::default()
        )
        .unwrap(),
        CastRowResult::Text("-170141183460469231731687303715884105728".into())
    );
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("actual-physical").unwrap()),
        ty.clone(),
        values.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 8,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 8,
            max_type_nodes: 64,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        },
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap();
    let value = pool.value(3).unwrap();
    assert_eq!(
        r.evaluate_row(
            EvaluatedArgument::Constant(&value),
            2,
            999,
            &Control::default()
        )
        .unwrap(),
        CastRowResult::Text("170141183460469231731687303715884105727".into())
    );
    let unresolved_rows = [1];
    let unresolved = SelectedValues::try_new(
        Selection::try_sparse(4, &unresolved_rows).unwrap(),
        ty.data_type(),
        values.slice(2, 1),
        vec![RowDataError::new(0, "original child")].into_boxed_slice(),
    )
    .unwrap();
    assert!(matches!(
        r.evaluate_row(
            EvaluatedArgument::SelectedColumn(&unresolved),
            0,
            1,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
#[test]
fn largeint_text_callbacks_keep_every_original_primary_cause() {
    let values = input();
    let r = recipe(true);
    let good = Control::default();
    r.evaluate_row(EvaluatedArgument::Column(&values), 0, 0, &good)
        .unwrap();
    let count = good.trace.lock().unwrap().len();
    assert!(count > 0);
    for stop in 0..count {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let c = Control {
                refuse: Some((stop, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                r.evaluate_row(EvaluatedArgument::Column(&values), 0, 0, &c),
                Err(cause)
            );
            assert_eq!(c.trace.lock().unwrap().len(), stop + 1);
        }
    }
}
