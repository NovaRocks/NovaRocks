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

//! Fixed source-capture fanout for one already admitted lifetime guard.

use crate::SourceRetentionCapability;
use std::{
    fmt,
    sync::{Arc, Mutex},
};

struct SharedRetention {
    original: Mutex<Option<Box<dyn SourceRetentionCapability>>>,
}

struct RetentionCapsule {
    shared: Arc<SharedRetention>,
}
impl SourceRetentionCapability for RetentionCapsule {}
impl fmt::Debug for RetentionCapsule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A source's Debug may run while its own custody lock is held.
        f.debug_struct("SourceRetentionCapsule")
            .finish_non_exhaustive()
    }
}
impl Drop for RetentionCapsule {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let original = {
                let mut retained = self
                    .shared
                    .original
                    .lock()
                    .expect("source retention fanout custody");
                retained.take()
            };
            // The original issuer must observe this branch's unwind now,
            // outside our lock. A later normal alias cannot erase Unknown.
            drop(original);
        }
        // All-normal branches keep the one original guard until the final
        // shared node releases its capsule. There is no positive early exit.
    }
}

/// Split one funded guard over a source owner's fixed, known capture boundary.
/// Every capsule must be installed in a real canonical source node before the
/// owner returns. The owner first completes every fallible precheck, then
/// attaches every capsule without a normal early return. Unwinding an
/// unattached capsule still makes the original issuer observe Unknown.
///
/// This is not a cancellable builder: normally discarding an unattached
/// capsule does not report an unknown capture. Owners needing recoverable
/// attachment failure require a separate explicit ownership contract. The
/// capsules cannot be cloned, registered later, or used to create new Work.
/// Their finite storage must be covered by the original admitting owner.
/// Nothing here scans opaque errors or proves independently shared resources
/// inside their payloads have exited.
///
/// If any capsule unwinds, the original guard is destroyed once on that
/// unwinding stack, even when another node remains alive. Otherwise the last
/// capsule destroys it normally. The issuer supplies the guard's existing
/// positive/unknown exit semantics. An arity of one returns the original Box
/// unchanged, with no join allocation.
///
/// # Panics
///
/// Panics if `N` is zero.
#[must_use = "install every capsule in its declared canonical source node"]
pub fn split_source_retention<const N: usize>(
    original: Box<dyn SourceRetentionCapability>,
) -> [Box<dyn SourceRetentionCapability>; N] {
    assert!(N > 0, "source retention fanout requires a capture");
    if N == 1 {
        let mut original = Some(original);
        return std::array::from_fn(|_| original.take().expect("one unchanged source guard"));
    }
    let shared = Arc::new(SharedRetention {
        original: Mutex::new(Some(original)),
    });
    // Realize any platform mutex backing before a capsule is published,
    // rather than allocating it on the first source-destructor unwind.
    drop(
        shared
            .original
            .lock()
            .expect("source retention fanout custody"),
    );
    std::array::from_fn(|_| {
        Box::new(RetentionCapsule {
            shared: Arc::clone(&shared),
        }) as Box<dyn SourceRetentionCapability>
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FailureClass, FailureFact, FailureProjection, FailureReason, SharedFailureCause};
    use std::{
        error::Error,
        num::NonZeroU16,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Barrier, Weak,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct GuardExit {
        unknown: bool,
        sources: [usize; 2],
        join_unlocked: bool,
    }
    #[derive(Debug, Default)]
    struct Observations {
        source_drops: [AtomicUsize; 2],
        exits: Mutex<Vec<GuardExit>>,
    }
    struct OriginalGuard {
        observations: Arc<Observations>,
        join: Option<Weak<SharedRetention>>,
    }
    impl fmt::Debug for OriginalGuard {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("OriginalAdmittedSourceGuard")
        }
    }
    impl SourceRetentionCapability for OriginalGuard {}
    impl Drop for OriginalGuard {
        fn drop(&mut self) {
            let join_unlocked = self
                .join
                .as_ref()
                .and_then(Weak::upgrade)
                .is_none_or(|join| join.original.try_lock().is_ok());
            self.observations.exits.lock().unwrap().push(GuardExit {
                unknown: std::thread::panicking(),
                sources: std::array::from_fn(|index| {
                    self.observations.source_drops[index].load(Ordering::SeqCst)
                }),
                join_unlocked,
            });
        }
    }
    #[derive(Debug)]
    struct OriginalSource {
        index: usize,
        observations: Arc<Observations>,
        panic_on_drop: bool,
        together: Option<Arc<Barrier>>,
    }
    impl fmt::Display for OriginalSource {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "original source {}", self.index)
        }
    }
    impl Error for OriginalSource {}
    impl FailureProjection for OriginalSource {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(crate::domain::EXECUTION, NonZeroU16::new(403).unwrap()),
            )
        }
    }
    impl Drop for OriginalSource {
        fn drop(&mut self) {
            self.observations.source_drops[self.index].fetch_add(1, Ordering::SeqCst);
            if let Some(together) = &self.together {
                together.wait();
            }
            assert!(!self.panic_on_drop, "original source destructor panic");
        }
    }
    fn source(
        index: usize,
        observations: &Arc<Observations>,
        panic_on_drop: bool,
        together: Option<Arc<Barrier>>,
    ) -> SharedFailureCause {
        SharedFailureCause::new(OriginalSource {
            index,
            observations: observations.clone(),
            panic_on_drop,
            together,
        })
    }
    fn guard(observations: &Arc<Observations>) -> Box<dyn SourceRetentionCapability> {
        Box::new(OriginalGuard {
            observations: observations.clone(),
            join: None,
        })
    }

    #[test]
    fn normal_node_aliases_keep_one_original_guard_until_both_payloads_exit() {
        for early_aliases in [false, true] {
            let observations = Arc::new(Observations::default());
            let first = source(0, &observations, false, None);
            let second = source(1, &observations, false, None);
            let early = early_aliases.then(|| (first.clone(), second.clone()));
            let [first_guard, second_guard] = split_source_retention::<2>(guard(&observations));
            first.retain_source_lifetime(first_guard);
            second.retain_source_lifetime(second_guard);
            let (first_alias, second_alias) =
                early.unwrap_or_else(|| (first.clone(), second.clone()));
            drop(first);
            drop(second);
            assert!(observations.exits.lock().unwrap().is_empty());
            drop(first_alias);
            assert_eq!(observations.source_drops[0].load(Ordering::SeqCst), 1);
            assert_eq!(observations.source_drops[1].load(Ordering::SeqCst), 0);
            assert!(observations.exits.lock().unwrap().is_empty());
            drop(second_alias);
            assert_eq!(
                *observations.exits.lock().unwrap(),
                [GuardExit {
                    unknown: false,
                    sources: [1, 1],
                    join_unlocked: true,
                }]
            );
        }
    }

    #[test]
    fn a_panicking_node_reports_unknown_before_another_alias_exits_normally() {
        let observations = Arc::new(Observations::default());
        let first = source(0, &observations, true, None);
        let second = source(1, &observations, false, None);
        let second_alias = second.clone();
        let [first_guard, second_guard] = split_source_retention::<2>(guard(&observations));
        first.retain_source_lifetime(first_guard);
        second.retain_source_lifetime(second_guard);
        drop(second);
        let panic = catch_unwind(AssertUnwindSafe(|| drop(first)));
        let while_second_alive = observations.exits.lock().unwrap().clone();
        let second_drops_before_release = observations.source_drops[1].load(Ordering::SeqCst);
        // Finish the other real node before asserting. A red negative must not
        // accidentally turn its normal Drop into another test-induced unwind.
        drop(second_alias);
        drop(panic.unwrap_err());
        assert_eq!(second_drops_before_release, 0);
        let after_second_exit = observations.exits.lock().unwrap().clone();
        let expected = vec![GuardExit {
            unknown: true,
            sources: [1, 0],
            join_unlocked: true,
        }];
        assert_eq!(
            (&while_second_alive, &after_second_exit),
            (&expected, &expected),
            "a later normal alias cannot revive the original unknown exit"
        );
    }

    #[test]
    fn two_concurrent_source_panics_release_the_original_guard_only_once() {
        let observations = Arc::new(Observations::default());
        let together = Arc::new(Barrier::new(2));
        let first = source(0, &observations, true, Some(together.clone()));
        let second = source(1, &observations, true, Some(together));
        let [first_guard, second_guard] = split_source_retention::<2>(guard(&observations));
        first.retain_source_lifetime(first_guard);
        second.retain_source_lifetime(second_guard);
        std::thread::scope(|scope| {
            let first = scope.spawn(move || catch_unwind(AssertUnwindSafe(|| drop(first))));
            let second = scope.spawn(move || catch_unwind(AssertUnwindSafe(|| drop(second))));
            assert!(first.join().unwrap().is_err());
            assert!(second.join().unwrap().is_err());
        });
        assert_eq!(
            *observations.exits.lock().unwrap(),
            [GuardExit {
                unknown: true,
                sources: [1, 1],
                join_unlocked: true,
            }]
        );
    }

    #[test]
    fn capsule_debug_and_unwinding_guard_disposal_do_not_hold_the_join_lock() {
        let observations = Arc::new(Observations::default());
        let shared = Arc::new(SharedRetention {
            original: Mutex::new(None),
        });
        *shared.original.lock().unwrap() = Some(Box::new(OriginalGuard {
            observations: observations.clone(),
            join: Some(Arc::downgrade(&shared)),
        }));
        let capsule = RetentionCapsule {
            shared: shared.clone(),
        };
        {
            let _locked = shared.original.lock().unwrap();
            assert!(format!("{capsule:?}").contains("SourceRetentionCapsule"));
        }
        let first = source(0, &observations, true, None);
        first.retain_source_lifetime(Box::new(capsule));
        assert!(catch_unwind(AssertUnwindSafe(|| drop(first))).is_err());
        let exits = observations.exits.lock().unwrap().clone();
        assert_eq!(exits.len(), 1);
        assert!(exits[0].unknown && exits[0].join_unlocked);
        drop(shared);
        assert_eq!(*observations.exits.lock().unwrap(), exits);
    }

    #[test]
    fn one_capture_returns_the_original_box_without_a_join_capsule() {
        let observations = Arc::new(Observations::default());
        let original = guard(&observations);
        let original_pointer = &*original as *const dyn SourceRetentionCapability;
        let [unchanged] = split_source_retention::<1>(original);
        assert!(std::ptr::eq(&*unchanged, original_pointer));
        let source = source(0, &observations, false, None);
        source.retain_source_lifetime(unchanged);
        drop(source);
        assert_eq!(
            *observations.exits.lock().unwrap(),
            [GuardExit {
                unknown: false,
                sources: [1, 0],
                join_unlocked: true,
            }]
        );
    }

    #[test]
    fn zero_captures_are_rejected_before_a_positive_exit_can_be_invented() {
        let observations = Arc::new(Observations::default());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = split_source_retention::<0>(guard(&observations));
            }))
            .is_err()
        );
        let exits = observations.exits.lock().unwrap();
        assert_eq!(exits.len(), 1);
        assert!(exits[0].unknown);
    }
}
