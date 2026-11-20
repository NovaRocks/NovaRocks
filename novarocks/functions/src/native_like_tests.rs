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
//! Sole original LIKE math, selected addresses and exact control refusal probes.
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, RowDataError, SelectedValues, Selection,
};
use arrow_array::{Int32Array, LargeStringArray, StringViewArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{sync::Mutex, time::Duration};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    fail: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        if let Some((at, _)) = &self.fail {
            assert!(t.len() < *at, "no callback after original refusal");
        }
        t.push(n);
        if let Some((at, c)) = &self.fail
            && t.len() == *at
        {
            return Err(c.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("LIKE never waits")
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
fn recipe(negated: bool) -> PreparedNativeLikeRecipe {
    let s = FunctionValueType::new(DataType::Utf8, true);
    PreparedNativeLikeRecipe::try_new(
        negated,
        &s,
        &s,
        &FunctionValueType::new(DataType::Boolean, true),
        &Control::default(),
    )
    .unwrap()
}
fn values(v: &crate::SelectedValues<'_>) -> Vec<Option<bool>> {
    v.values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("like-origin-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("like-origin-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("like-origin-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn like_core_unicode_nul_literal_backslash_and_full_errors() {
    for (text, pattern, expected) in [
        ("", "%", true),
        ("é中", "__", true),
        ("é中", "_", false),
        ("a\0b", "a_b", true),
        ("abc", "%b%", true),
        ("abc", "%d%", false),
        ("a%b", r"a\%b", false),
        ("a_b", r"a\_b", false),
        (r"a\xb", r"a\_b", true),
        (r"a\", r"a\", true),
        ("a!b", "a!%", true),
        ("apple", "a!%", false),
    ] {
        assert_eq!(like_match(text, pattern), expected);
    }
    let wrong: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let valid: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    assert_eq!(
        evaluate_required(&wrong, &valid).unwrap_err().to_string(),
        "like: first argument must be a string array"
    );
    assert_eq!(
        evaluate_required(&valid, &wrong).unwrap_err().to_string(),
        "like: second argument must be a string array"
    );
    for wide in [
        Arc::new(LargeStringArray::from(vec!["a"])) as ArrayRef,
        Arc::new(StringViewArray::from(vec!["a"])) as ArrayRef,
    ] {
        assert_eq!(
            evaluate_required(&wide, &valid).unwrap_err().to_string(),
            "like: first argument must be a string array"
        );
        assert_eq!(
            evaluate_required(&valid, &wide).unwrap_err().to_string(),
            "like: second argument must be a string array"
        );
    }
}

#[test]
fn like_explicit_escape_parses_every_token_and_requires_one_bmp_character() {
    let evaluate = |text, pattern, escape| {
        like_with_escape_observed(text, pattern, escape, &mut |_| Ok::<_, Infallible>(()))
    };
    assert!(evaluate("a_b", r"a\_b", r"\").unwrap());
    assert!(evaluate("a%b", "a界%b", "界").unwrap());
    assert!(evaluate("a!b", "a!!b", "!").unwrap());
    assert!(evaluate("a%", "a%%", "%").unwrap());
    assert!(matches!(
        evaluate("mismatch", "a!x", "!"),
        Err(LikeError::Pattern(LikePatternError::InvalidEscape))
    ));
    assert!(matches!(
        evaluate("mismatch", "a!", "!"),
        Err(LikeError::Pattern(LikePatternError::InvalidEscape))
    ));
    for escape in ["", "ab", "😀"] {
        assert!(matches!(
            evaluate("value", "%", escape),
            Err(LikeError::Pattern(LikePatternError::EscapeLength))
        ));
    }
}
#[test]
fn like_recipe_exact_original_utf8_nullability_and_wider_refusal() {
    for n in [false, true] {
        let s = FunctionValueType::new(DataType::Utf8, n);
        let b = FunctionValueType::new(DataType::Boolean, n);
        let r = PreparedNativeLikeRecipe::try_new(false, &s, &s, &b, &Control::default()).unwrap();
        assert_eq!(r.text_type(), &s);
        assert_eq!(r.result_type(), &b);
    }
    for dtype in [
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Null,
        DataType::Int32,
    ] {
        let s = FunctionValueType::from_legacy_declared_projection(dtype, true);
        assert_eq!(
            PreparedNativeLikeRecipe::try_new(
                false,
                &s,
                &s,
                &FunctionValueType::new(DataType::Boolean, true),
                &Control::default()
            )
            .unwrap_err(),
            crate::ComparisonPrepareError::Unsupported
        );
    }
    let s = FunctionValueType::new(DataType::Utf8, true);
    assert_eq!(
        PreparedNativeLikeRecipe::try_new(
            false,
            &s,
            &s,
            &FunctionValueType::new(DataType::Boolean, false),
            &Control::default()
        )
        .unwrap_err(),
        crate::ComparisonPrepareError::TypeMismatch
    );
}
#[test]
fn like_selected_original_columns_sparse_slice_dense_scalar_and_pool_ordinal() {
    let raw: ArrayRef = Arc::new(StringArray::from(vec![
        Some("inactive"),
        Some("apple"),
        None,
        Some("é中"),
        Some("a\0b"),
        Some("suffix"),
    ]));
    let text = raw.slice(1, 4);
    let pattern: ArrayRef = Arc::new(StringArray::from(vec!["a%", "%", "__", "a_b"]));
    let rows = [0, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let dense: ArrayRef = Arc::new(StringArray::from(vec![
        Some("apple"),
        Some("é中"),
        Some("a\0b"),
    ]));
    let compact =
        SelectedValues::try_new(selection, &DataType::Utf8, dense, Box::default()).unwrap();
    for negated in [false, true] {
        for truth in [false, true] {
            let r = recipe(negated);
            let old = evaluate_required(&text, &pattern).unwrap();
            let old = old.as_any().downcast_ref::<BooleanArray>().unwrap();
            let wanted = rows
                .iter()
                .map(|row| old.is_valid(*row).then(|| old.value(*row) ^ negated))
                .map(|v| if truth { Some(v.unwrap_or(false)) } else { v })
                .collect::<Vec<_>>();
            for arg in [
                EvaluatedArgument::Column(&text),
                EvaluatedArgument::SelectedColumn(&compact),
            ] {
                assert_eq!(
                    values(
                        &r.evaluate_selected(
                            arg,
                            EvaluatedArgument::Column(&pattern),
                            selection,
                            truth,
                            &Control::default()
                        )
                        .unwrap()
                    ),
                    wanted
                );
            }
        }
    }
    let scalar: ArrayRef = Arc::new(StringArray::from(vec!["a%"]));
    let r = recipe(false);
    assert_eq!(
        values(
            &r.evaluate_selected(
                EvaluatedArgument::Column(&text),
                EvaluatedArgument::Scalar(&scalar),
                selection,
                false,
                &Control::default()
            )
            .unwrap()
        ),
        vec![Some(true), Some(false), Some(true)]
    );
    let backing: ArrayRef = Arc::new(StringArray::from(vec!["wrong", "a%", "__"]));
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("actual-like-pattern").unwrap()),
        ty,
        backing.to_data(),
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
    let ordinal = pool.value(1).unwrap();
    assert_eq!(
        values(
            &r.evaluate_selected(
                EvaluatedArgument::Column(&text),
                EvaluatedArgument::Constant(&ordinal),
                selection,
                false,
                &Control::default()
            )
            .unwrap()
        ),
        vec![Some(true), Some(false), Some(true)]
    );
}
#[test]
fn like_selected_row_errors_are_not_nullable_success_and_empty_still_checks_carrier() {
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let text: ArrayRef = Arc::new(StringArray::from(vec![None, Some("apple")]));
    let pattern: ArrayRef = Arc::new(StringArray::from(vec![None, Some("a%")]));
    let left = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        text,
        Box::from([RowDataError::new(0, "source original error")]),
    )
    .unwrap();
    let right = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        pattern,
        Box::from([RowDataError::new(0, "pattern later error")]),
    )
    .unwrap();
    for negated in [false, true] {
        for truth in [false, true] {
            let out = recipe(negated)
                .evaluate_selected(
                    EvaluatedArgument::SelectedColumn(&left),
                    EvaluatedArgument::SelectedColumn(&right),
                    selection,
                    truth,
                    &Control::default(),
                )
                .unwrap();
            assert_eq!(values(&out), vec![None, Some(!negated)]);
            assert_eq!(out.errors()[0].message(), "source original error");
        }
    }
    let empty = Selection::try_sparse(0, &[]).unwrap();
    let wrong: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
    let valid: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    assert!(matches!(
        recipe(false).evaluate_selected(
            EvaluatedArgument::Column(&wrong),
            EvaluatedArgument::Column(&valid),
            empty,
            false,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
#[test]
fn like_short_pattern_is_a_typed_contract_failure_and_selected_sql_null_truth_is_preserved() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![Some("apple"), None]));
    let short: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let error = evaluate_required(&text, &short).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(error.failure_fact().reason().code(), 384);
    let patterns: ArrayRef = Arc::new(StringArray::from(vec!["a%", "a%"]));
    for negated in [false, true] {
        for truth in [false, true] {
            let out = recipe(negated)
                .evaluate_selected(
                    EvaluatedArgument::Column(&text),
                    EvaluatedArgument::Column(&patterns),
                    Selection::all(2),
                    truth,
                    &Control::default(),
                )
                .unwrap();
            assert_eq!(
                values(&out),
                vec![Some(!negated), if truth { Some(false) } else { None }]
            );
        }
    }
    let wrong: ArrayRef = Arc::new(Int32Array::from(vec![None]));
    let invalid = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Int32,
        wrong,
        Box::from([RowDataError::new(0, "original child error")]),
    )
    .unwrap();
    let pattern: ArrayRef = Arc::new(StringArray::from(vec!["%"]));
    assert!(matches!(
        recipe(false).evaluate_selected(
            EvaluatedArgument::SelectedColumn(&invalid),
            EvaluatedArgument::Column(&pattern),
            Selection::all(1),
            false,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
#[test]
fn like_recipe_actual_kernel_callbacks_all_seven_causes_no_footer() {
    let r = recipe(true);
    let text: ArrayRef = Arc::new(StringArray::from(vec!["aabcc"; 129]));
    let pattern: ArrayRef = Arc::new(StringArray::from(vec!["%b%"; 129]));
    for rows in [(0..129).collect::<Vec<_>>(), vec![0, 2, 128], Vec::new()] {
        let sel = Selection::try_sparse(129, &rows).unwrap();
        let success = Control::default();
        r.evaluate_selected(
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&pattern),
            sel,
            false,
            &success,
        )
        .unwrap();
        let trace = success.trace.into_inner().unwrap();
        for at in 1..=trace.len() {
            for cause in causes() {
                let c = Control {
                    trace: Mutex::new(vec![]),
                    fail: Some((at, cause.clone())),
                };
                assert!(
                    matches!(r.evaluate_selected(EvaluatedArgument::Column(&text),EvaluatedArgument::Column(&pattern),sel,false,&c),Err(found) if found==cause)
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..at]);
            }
        }
    }
}
#[test]
fn like_recipe_actual_compile_callbacks_three_causes_no_footer() {
    struct Compile {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        at: usize,
        cause: CompileControlError,
    }
    impl PureCompileControl for Compile {
        fn checkpoint(&self, p: CompilePhase, n: u32) -> Result<(), CompileControlError> {
            assert!(n <= 256);
            let mut t = self.trace.lock().unwrap();
            assert!(t.len() < self.at);
            t.push((p, n));
            if t.len() == self.at {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
    }
    let s = FunctionValueType::new(DataType::Utf8, true);
    let b = FunctionValueType::new(DataType::Boolean, true);
    let c = Compile {
        trace: Mutex::new(vec![]),
        at: usize::MAX,
        cause: CompileControlError::Cancelled,
    };
    PreparedNativeLikeRecipe::try_new(false, &s, &s, &b, &c).unwrap();
    let trace = c.trace.into_inner().unwrap();
    for at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = Compile {
                trace: Mutex::new(vec![]),
                at,
                cause: cause.clone(),
            };
            let e = PreparedNativeLikeRecipe::try_new(false, &s, &s, &b, &c).unwrap_err();
            assert_eq!(e.control_error(), Some(cause));
            assert_eq!(*c.trace.lock().unwrap(), trace[..at]);
        }
    }
}
