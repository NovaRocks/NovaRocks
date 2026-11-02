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

use super::*;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{collections::HashMap, sync::Mutex};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control(Mutex<State>);
#[derive(Default)]
struct State {
    trace: Vec<u32>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        *self.0.lock().unwrap() = State {
            trace: vec![],
            stop,
        };
    }
    fn trace(&self) -> Vec<u32> {
        self.0.lock().unwrap().trace.clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut s = self.0.lock().unwrap();
        let at = s.trace.len();
        if let Some((stop, _)) = s.stop {
            assert!(at <= stop, "callback after originating refusal");
        }
        s.trace.push(units);
        match s.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn run<T>(
    control: &Control,
    action: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, TypeCodecError>,
) -> Result<T, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = action(&mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn prefixes<T>(action: impl Fn(&mut CompileCheckpoints<'_>) -> Result<T, TypeCodecError>) {
    let c = Control::default();
    let _ = run(&c, &action);
    let trace = c.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            c.arm(Some((at, cause.clone())));
            assert!(
                matches!(run(&c, &action), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
fn metadata() -> arrow::datatypes::Metadata {
    arrow::datatypes::Metadata::from(HashMap::from([
        ("雪".into(), "snow 😀".into()),
        ("a\0".into(), "after a".into()),
        ("".into(), "empty key".into()),
        ("ä".into(), "unknown.extension".into()),
        ("a".into(), "".into()),
        ("\0".into(), "nul\0payload".into()),
    ]))
}

#[test]
fn metadata_matches_independent_unicode_nul_and_empty_canonical_wire() {
    let source = metadata();
    let before = source.clone();
    let c = Control::default();
    let actual = run(&c, |w| encode_metadata(&source, w)).unwrap();
    let expected = [
        ("", "empty key"),
        ("\0", "nul\0payload"),
        ("a", ""),
        ("a\0", "after a"),
        ("ä", "unknown.extension"),
        ("雪", "snow 😀"),
    ]
    .map(|(key, value)| plan::ArrowFieldMetadataEntry {
        key: key.into(),
        value: value.into(),
    });
    assert_eq!(actual, expected);
    assert_eq!(source, before);
    for entry in actual {
        if !entry.value.is_empty() {
            assert_ne!(entry.value.as_ptr(), source[&entry.key].as_ptr());
        }
    }
}

#[test]
fn spelling_copy_crosses_utf8_boundaries_without_retag_or_truncation() {
    for source in [
        String::new(),
        format!("{}😀雪e\u{301}\0", "a".repeat(255)),
        "😀雪e\u{301}\0".repeat(320),
    ] {
        let c = Control::default();
        let copy = run(&c, |w| copy_string(&source, w)).unwrap();
        assert_eq!(copy.as_bytes(), source.as_bytes());
        assert!(copy.capacity() >= source.len());
        if source.is_empty() {
            assert_eq!(copy.capacity(), 0);
        } else {
            assert_ne!(copy.as_ptr(), source.as_ptr());
        }
    }
}

#[test]
fn lexical_comparator_preserves_actual_byte_order_and_prefix_lengths() {
    for (left, right, expected) in [
        ("", "", Ordering::Equal),
        ("", "a", Ordering::Less),
        ("a", "a\0", Ordering::Less),
        ("a\0", "a", Ordering::Greater),
        ("aa", "ab", Ordering::Less),
        ("ab", "aa", Ordering::Greater),
        ("ä", "雪", Ordering::Less),
        ("😀", "雪", Ordering::Greater),
        ("雪\0", "雪\0", Ordering::Equal),
    ] {
        let c = Control::default();
        assert_eq!(run(&c, |w| compare_keys(left, right, w)).unwrap(), expected);
    }
}

#[test]
fn strict_order_keeps_duplicates_unsorted_and_ordinary_tail_with_original_control() {
    let c = Control::default();
    assert!(run(&c, |w| ordered_key(None, "", w)).unwrap());
    assert!(run(&c, |w| ordered_key(Some("a"), "a\0", w)).unwrap());
    assert!(!run(&c, |w| ordered_key(Some("same"), "same", w)).unwrap());
    assert!(!run(&c, |w| ordered_key(Some("z"), "a", w)).unwrap());
    let ordinary = |w: &mut CompileCheckpoints<'_>| {
        if !ordered_key(Some("a"), "a", w)? {
            return Err(TypeCodecError::InvalidShape(
                "field metadata must be sorted and unique",
            ));
        }
        Ok(())
    };
    c.arm(None);
    assert!(matches!(
        run(&c, ordinary),
        Err(TypeCodecError::InvalidShape(
            "field metadata must be sorted and unique"
        ))
    ));
    assert_eq!(c.trace(), [0, 2]);
    prefixes(ordinary);
}

#[test]
fn all_small_actual_metadata_copy_and_comparison_callbacks_keep_three_primary_causes() {
    let source = arrow::datatypes::Metadata::from(HashMap::from([
        ("b".into(), "雪".into()),
        ("a".into(), "a\0".into()),
    ]));
    prefixes(|w| encode_metadata(&source, w));
    prefixes(|w| copy_string("a😀雪", w));
    prefixes(|w| compare_keys("same-a", "same-b", w));
    prefixes(|w| ordered_key(None, "", w));
}

#[test]
fn empty_metadata_observes_actual_final_none_and_zero_capacity_reservations() {
    let c = Control::default();
    let source = arrow::datatypes::Metadata::new();
    let out = run(&c, |w| encode_metadata(&source, w)).unwrap();
    assert!(out.is_empty());
    assert_eq!(out.capacity(), 0);
    // Entry; scratch reserve; iterator construction; final None; output reserve;
    // caller footer. Each real successful opaque operation contributes one.
    assert_eq!(c.trace(), [0, 0, 1, 0, 1, 0, 1, 0, 1, 0]);
    prefixes(|w| encode_metadata(&source, w));
}

#[test]
fn captured_real_reserve_refusal_is_resource_before_any_later_control() {
    for late in CAUSES {
        let c = Control::default();
        c.arm(Some((2, late.clone())));
        assert!(matches!(
            run(&c, |w| reserve::<u8>(usize::MAX, w)),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(c.trace(), [0, 0]);
        for at in 0..2 {
            c.arm(Some((at, late.clone())));
            assert!(matches!(run(&c, |w| reserve::<u8>(usize::MAX, w)),
                Err(TypeCodecError::Control(actual)) if actual == late));
            assert_eq!(c.trace(), vec![0; at + 1]);
        }
    }
}

#[test]
fn wide_actual_ascii_append_and_byte_comparison_keep_real_quantum_prefixes() {
    let source = "a".repeat(320);
    let c = Control::default();
    for copy in [true, false] {
        let action = |w: &mut CompileCheckpoints<'_>| {
            if copy {
                assert_eq!(copy_string(&source, w)?, source);
            } else {
                assert_eq!(compare_keys(&source, &source, w)?, Ordering::Equal);
            }
            Ok(())
        };
        c.arm(None);
        run(&c, action).unwrap();
        let trace = c.trace();
        let quantum = trace.iter().position(|units| *units == 256).unwrap();
        for at in [0, quantum, trace.len() - 1] {
            for cause in CAUSES {
                c.arm(Some((at, cause.clone())));
                assert!(
                    matches!(run(&c, action), Err(TypeCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    }
}
