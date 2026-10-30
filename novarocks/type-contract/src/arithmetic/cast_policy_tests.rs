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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueTypeError,
};
use arrow_schema::{Field, UnionFields, UnionMode};
use std::sync::{Arc, Mutex};

// Independent copy of the pre-migration classifier, including Arrow's own
// equality and the original ordered child relation. Allocation here is only
// part of the historical test oracle, never the observed production author.
fn original_policy(source: &DataType, target: &DataType, policy: DecimalOverflowPolicy) -> bool {
    fn children(ty: &DataType) -> Vec<&DataType> {
        match ty {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => vec![field.data_type()],
            DataType::Struct(fields) => fields.iter().map(|field| field.data_type()).collect(),
            _ => Vec::new(),
        }
    }
    fn numeric_change(source: &DataType, target: &DataType) -> bool {
        if source == target {
            return false;
        }
        let s = children(source);
        let t = children(target);
        if s.is_empty() && t.is_empty() {
            return is_checked_decimal_numeric_cast(source, target);
        }
        let matching = matches!(
            (source, target),
            (DataType::List(_), DataType::List(_))
                | (DataType::LargeList(_), DataType::LargeList(_))
                | (DataType::FixedSizeList(..), DataType::FixedSizeList(..))
                | (DataType::Map(..), DataType::Map(..))
                | (DataType::Struct(_), DataType::Struct(_))
        ) && s.len() == t.len();
        if matching {
            return s.iter().zip(&t).any(|(s, t)| numeric_change(s, t));
        }
        if s.is_empty() {
            return t.iter().any(|t| numeric_change(source, t));
        }
        if t.is_empty() {
            return s.iter().any(|s| numeric_change(s, target));
        }
        s.iter().any(|s| t.iter().any(|t| numeric_change(s, t)))
    }
    policy == DecimalOverflowPolicy::OutputNull
        || source == target
        || (children(source).is_empty() && children(target).is_empty())
        || !numeric_change(source, target)
}

fn wrappers(leaf: DataType) -> Vec<DataType> {
    let field = Arc::new(Field::new("item", leaf.clone(), true));
    vec![
        leaf.clone(),
        DataType::List(field.clone()),
        DataType::LargeList(field.clone()),
        DataType::FixedSizeList(field.clone(), 2),
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", leaf.clone(), true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        DataType::Struct(
            vec![
                Field::new("value", leaf.clone(), true),
                Field::new("sibling", DataType::Utf8, true),
            ]
            .into(),
        ),
        // These are deliberately not traversed as recursive numeric containers
        // by the original classifier. Cast capability is a separate contract.
        DataType::ListView(field.clone()),
        DataType::LargeListView(field.clone()),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(leaf)),
        DataType::RunEndEncoded(
            Arc::new(Field::new("ends", DataType::Int32, false)),
            field.clone(),
        ),
        DataType::Union(
            UnionFields::try_new(vec![7], vec![field]).unwrap(),
            UnionMode::Dense,
        ),
    ]
}

#[test]
fn observed_cast_policy_matches_original_flat_nested_and_unsupported_container_relation() {
    let corpus: Vec<_> = [
        DataType::Int8,
        DataType::Float64,
        DataType::Utf8,
        DataType::Decimal128(8, 2),
        DataType::Decimal256(12, -2),
        DataType::FixedSizeBinary(16),
    ]
    .into_iter()
    .flat_map(wrappers)
    .collect();
    for source in &corpus {
        for target in &corpus {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let expected = original_policy(source, target, policy);
                assert_eq!(
                    decimal_error_policy_cast_supported_observed::<ValueTypeError>(
                        source,
                        target,
                        policy,
                        || Ok(())
                    ),
                    Ok(expected),
                    "{source:?} -> {target:?}, {policy:?}"
                );
                assert_eq!(
                    decimal_error_policy_cast_supported(source, target, policy),
                    expected
                );
            }
        }
    }
}

#[test]
fn observed_cast_policy_keeps_correspondence_and_conservative_unmatched_crossproduct() {
    let decimal = DataType::Decimal128(8, 2);
    let structure = |types: Vec<DataType>| {
        DataType::Struct(
            types
                .into_iter()
                .enumerate()
                .map(|(n, ty)| Field::new(n.to_string(), ty, true))
                .collect(),
        )
    };
    let source = structure(vec![decimal.clone(), DataType::Utf8]);
    // The unchanged decimal must not be compared against an unrelated sibling.
    let same_layout = structure(vec![decimal.clone(), DataType::Int8]);
    assert!(decimal_error_policy_cast_supported(
        &source,
        &same_layout,
        DecimalOverflowPolicy::ReportError
    ));
    // Different field counts preserve the original all-pairs conservative rule.
    let unmatched = structure(vec![decimal, DataType::Int8, DataType::Utf8]);
    assert!(!decimal_error_policy_cast_supported(
        &source,
        &unmatched,
        DecimalOverflowPolicy::ReportError
    ));
    for (s, t) in [
        (&source, &same_layout),
        (&source, &unmatched),
        (&same_layout, &DataType::Int8),
    ] {
        assert_eq!(
            decimal_error_policy_cast_supported_observed::<ValueTypeError>(
                s,
                t,
                DecimalOverflowPolicy::ReportError,
                || Ok(())
            ),
            Ok(original_policy(s, t, DecimalOverflowPolicy::ReportError))
        );
    }
}

#[test]
fn observed_cast_policy_preserves_arrow_dictionary_compatibility_and_metadata_retag() {
    #[allow(deprecated)]
    let dictionary = |id, ordered| {
        DataType::Struct(
            vec![Field::new_dict(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
                id,
                ordered,
            )]
            .into(),
        )
    };
    let a = dictionary(0, false);
    let b = dictionary(i64::MAX, true);
    assert_eq!(a, b, "Arrow compatibility ignores dictionary IPC identity");
    assert_eq!(
        crate::schema::arrow_data_types_equal_observed::<ValueTypeError>(&a, &b, || Ok(())),
        Ok(true)
    );
    assert_eq!(
        crate::schema::arrow_data_types_exact_observed::<ValueTypeError>(&a, &b, || Ok(())),
        Ok(false)
    );
    assert_eq!(
        decimal_error_policy_cast_supported_observed::<ValueTypeError>(
            &a,
            &b,
            DecimalOverflowPolicy::ReportError,
            || Ok(())
        ),
        Ok(true)
    );
    let retag = |tag: &str| {
        DataType::List(Arc::new(
            Field::new("item", DataType::Utf8, true).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                crate::NR_LOGICAL_TYPE_KEY.into(),
                tag.into(),
            )])),
        ))
    };
    let plain = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let json = retag("json");
    assert_ne!(plain, json);
    assert!(decimal_error_policy_cast_supported(
        &plain,
        &json,
        DecimalOverflowPolicy::ReportError
    ));
    assert!(
        !crate::preserves_nested_logical_identity(&plain, &json),
        "policy classifier grants no identity retag authority"
    );
}

#[test]
fn observed_cast_policy_keeps_output_null_early_return_and_reports_invalid_report_error_types() {
    let invalid = DataType::List(Arc::new(
        Field::new("v", DataType::Utf8, true).with_metadata(std::collections::HashMap::<
            String,
            String,
        >::from([(
            crate::NR_LOGICAL_TYPE_KEY.into(),
            "unknown".into(),
        )])),
    ));
    let mut observations = 0;
    assert_eq!(
        decimal_error_policy_cast_supported_observed::<ValueTypeError>(
            &invalid,
            &invalid,
            DecimalOverflowPolicy::OutputNull,
            || {
                observations += 1;
                Ok(())
            }
        ),
        Ok(true)
    );
    assert_eq!(observations, 1);
    assert_eq!(
        decimal_error_policy_cast_supported_observed::<ValueTypeError>(
            &invalid,
            &invalid,
            DecimalOverflowPolicy::ReportError,
            || Ok(())
        ),
        Err(ValueTypeError::UnknownLogicalMetadata)
    );
    assert!(!decimal_error_policy_cast_supported(
        &invalid,
        &invalid,
        DecimalOverflowPolicy::ReportError
    ));
    let mut deep = DataType::Int64;
    for _ in 0..crate::MAX_VALUE_TYPE_DEPTH {
        deep = DataType::List(Arc::new(Field::new("v", deep, true)));
    }
    assert!(decimal_error_policy_cast_supported(
        &deep,
        &deep,
        DecimalOverflowPolicy::OutputNull
    ));
    assert_eq!(
        decimal_error_policy_cast_supported_observed::<ValueTypeError>(
            &deep,
            &deep,
            DecimalOverflowPolicy::ReportError,
            || Ok(())
        ),
        Err(ValueTypeError::TooDeep)
    );
}

#[derive(Debug, PartialEq)]
enum Failure {
    Type(ValueTypeError),
    Control(CompileControlError),
}
impl From<ValueTypeError> for Failure {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}
impl From<CompileControlError> for Failure {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
struct Control {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        calls.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn run(source: &DataType, target: &DataType, control: &Control) -> Result<bool, Failure> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = decimal_error_policy_cast_supported_observed(
        source,
        target,
        DecimalOverflowPolicy::ReportError,
        || work.step().map_err(Failure::Control),
    );
    if matches!(result, Err(Failure::Control(_))) {
        return result;
    }
    // The caller owns mandatory completion, including ordinary policy refusal.
    work.finish()?;
    result
}
#[test]
fn observed_cast_policy_wide_first_and_late_changes_preserve_all_control_callback_prefixes() {
    let wide = |change: Option<usize>| {
        DataType::Struct(
            (0..320)
                .map(|n| {
                    let field = Field::new(
                        format!("f{n}"),
                        if change == Some(n) {
                            DataType::Decimal128(7, 2)
                        } else {
                            DataType::Decimal128(8, 2)
                        },
                        true,
                    );
                    if n % 37 == 0 {
                        field.with_metadata(std::collections::HashMap::<String, String>::from([(
                            "provider.id".into(),
                            n.to_string(),
                        )]))
                    } else {
                        field
                    }
                })
                .collect(),
        )
    };
    let source = wide(None);
    for change in [None, Some(0), Some(319)] {
        let target = wide(change);
        let success = Control {
            calls: Mutex::default(),
            refusal: None,
        };
        assert_eq!(run(&source, &target, &success), Ok(change.is_none()));
        let trace = success.calls.into_inner().unwrap();
        assert_eq!(trace[0], (CompilePhase::Validate, 0));
        assert_eq!(trace[1], (CompilePhase::Validate, 256));
        assert!(trace.len() > 3);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let control = Control {
                    calls: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert_eq!(
                    run(&source, &target, &control),
                    Err(Failure::Control(cause.clone()))
                );
                assert_eq!(control.calls.into_inner().unwrap(), trace[..=at]);
            }
        }
    }
}
