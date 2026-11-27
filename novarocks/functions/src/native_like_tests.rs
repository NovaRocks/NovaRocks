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
use crate::KernelFailure;

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
fn like_pattern_storage_is_demanded_and_preserves_actual_host_failure() {
    use crate::{AggregateStateAllocator, UnaccountedAggregateStateAllocator};
    use std::alloc::Layout;
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Host {
        requests: AtomicUsize,
        releases: AtomicUsize,
    }
    impl AggregateStateAllocator for Host {
        fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
            match self.requests.fetch_add(1, Ordering::Relaxed) {
                1 => Err(KernelFailure::ResourceExhausted),
                _ => UnaccountedAggregateStateAllocator.allocate(layout),
            }
        }
        unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
            self.releases.fetch_add(1, Ordering::Relaxed);
            // SAFETY: each successful request is allocated by this exact owner.
            unsafe { UnaccountedAggregateStateAllocator.release(pointer, layout) };
        }
    }

    let host = Arc::new(Host::default());
    let mut storage = LikePatternStorage::new(host.clone());
    let mut run = |text, pattern, escape| {
        like_in_observed(text, pattern, escape, Some(&mut storage), &mut |_| {
            Ok::<_, Infallible>(())
        })
    };
    assert!(run("", "", None).unwrap());
    assert!(matches!(
        run("a", "!", Some("!")),
        Err(LikeError::Pattern(LikePatternError::InvalidEscape))
    ));
    assert_eq!(host.requests.load(Ordering::Relaxed), 0);
    assert!(matches!(
        run("abc", "%c", None),
        Err(LikeError::Operation(KernelFailure::ResourceExhausted))
    ));
    assert!(run("abc", "%c", None).unwrap());
    assert_eq!(host.requests.load(Ordering::Relaxed), 3);
    assert_eq!(host.releases.load(Ordering::Relaxed), 1);
    drop(storage);
    assert_eq!(host.releases.load(Ordering::Relaxed), 2);
}
