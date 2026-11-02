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

//! Immutable sharing for a domain-owned cause without a dependency back to
//! Execution. Classification is projected once by that domain, never inferred
//! from its diagnostic or from the concrete Rust type in a central switch.

use crate::{BoundedDetail, FailureFact, FailureProjection, SourceRetentionCapability};
use std::{
    any::Any,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

trait ProjectedCause: Error + Send + Sync {
    fn fact(&self) -> FailureFact;
    fn local_detail(&self) -> &str;
    fn original(&self) -> &(dyn Error + 'static);
    fn retain_source_lifetime(&self, custody: Box<dyn SourceRetentionCapability>);
}
struct CauseRecord<E> {
    cause: E,
    fact: FailureFact,
    detail: BoundedDetail,
    // Keep last: destroy the original payload before its admitted guards,
    // including when that payload's destructor unwinds. This proves this
    // canonical node's exit, not every independently shared child resource.
    retention: Mutex<Vec<Box<dyn SourceRetentionCapability>>>,
}
impl<E> fmt::Debug for CauseRecord<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DomainCause")
            .field("fact", &self.fact)
            .field("detail", &self.detail)
            .finish()
    }
}
impl<E> fmt::Display for CauseRecord<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}
impl<E: Error + 'static> Error for CauseRecord<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}
impl<E: Error + Send + Sync + 'static> ProjectedCause for CauseRecord<E> {
    fn fact(&self) -> FailureFact {
        self.fact
    }
    fn local_detail(&self) -> &str {
        self.detail.as_str()
    }
    fn original(&self) -> &(dyn Error + 'static) {
        &self.cause
    }
    fn retain_source_lifetime(&self, custody: Box<dyn SourceRetentionCapability>) {
        self.retention
            .lock()
            .expect("shared source lifetime custody")
            .push(custody);
    }
}

/// Clones share one originating cause. Equality denotes that same origin,
/// rather than comparing arbitrary third-party graphs or diagnostic strings.
#[derive(Clone)]
pub struct SharedFailureCause(Arc<dyn ProjectedCause>);
impl SharedFailureCause {
    pub fn new<E: Error + FailureProjection + Send + Sync + 'static>(cause: E) -> Self {
        // Rewrapping a known shared handle must preserve its canonical owner,
        // including aliases made before a host attached custody. Returning a
        // clone drops only this known handle, never its still-pinned payload.
        if let Some(shared) = (&cause as &dyn Any).downcast_ref::<Self>() {
            return shared.clone();
        }
        let fact = cause.failure_fact();
        // A foreign formatter is diagnostic work, never the authority deciding
        // whether the real cause can be retained and accepted.
        let detail = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            BoundedDetail::truncating_display(&cause)
        }))
        .unwrap_or_else(|_| BoundedDetail::truncating("domain failure detail unavailable"));
        Self(Arc::new(CauseRecord {
            cause,
            fact,
            detail,
            retention: Mutex::new(Vec::new()),
        }))
    }
    /// A closed producer's static diagnostic avoids evaluating an arbitrary
    /// formatter. The actual cause and its independently authored fact remain
    /// the same original; this grants no allocation or execution capacity.
    /// Rewrapping a shared handle preserves its existing diagnostic and node.
    pub fn from_static_detail<E: Error + FailureProjection + Send + Sync + 'static>(
        cause: E,
        detail: &'static str,
    ) -> Self {
        // A known shared handle already owns its source and diagnostic. Keep
        // that canonical node so earlier aliases retain any admitted guard.
        if let Some(shared) = (&cause as &dyn Any).downcast_ref::<Self>() {
            return shared.clone();
        }
        let fact = cause.failure_fact();
        Self(Arc::new(CauseRecord {
            cause,
            fact,
            detail: BoundedDetail::truncating(detail),
            retention: Mutex::new(Vec::new()),
        }))
    }
    /// The actual private pointee, including fact and BoundedDetail header.
    /// The caller's locked Arc-layout author adds the original Arc counters;
    /// E's already-owned pointees and diagnostic bytes are separate custody.
    pub fn record_layout<E>() -> std::alloc::Layout {
        std::alloc::Layout::new::<CauseRecord<E>>()
    }
    pub fn detail(&self) -> &str {
        self.0.local_detail()
    }
    pub fn downcast_ref<E: Error + 'static>(&self) -> Option<&E> {
        self.0.original().downcast_ref()
    }
    /// Move one already funded guard onto this canonical source node. Every
    /// clone, including an earlier alias, keeps the same original payload and
    /// guard alive. Attaching creates no Work, callback or capacity. The issuer
    /// supplies a finite admitted set; independently shared resources inside
    /// the payload remain the responsibility of their own resource owners.
    pub fn retain_source_lifetime(&self, custody: Box<dyn SourceRetentionCapability>) {
        self.0.retain_source_lifetime(custody);
    }
}
impl PartialEq for SharedFailureCause {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for SharedFailureCause {}
impl fmt::Debug for SharedFailureCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedFailureCause")
            .field("fact", &self.0.fact())
            .field("detail", &self.detail())
            .finish()
    }
}
impl fmt::Display for SharedFailureCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
impl Error for SharedFailureCause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.original())
    }
}
impl FailureProjection for SharedFailureCause {
    fn failure_fact(&self) -> FailureFact {
        self.0.fact()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FailureClass, FailureReason, domain};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct ForeignCause {
        projections: Arc<AtomicUsize>,
        panic_on_display: bool,
    }
    impl fmt::Display for ForeignCause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            assert!(!self.panic_on_display, "foreign formatter panic");
            for _ in 0..4096 {
                f.write_str("界")?;
            }
            Ok(())
        }
    }
    impl Error for ForeignCause {}
    impl FailureProjection for ForeignCause {
        fn failure_fact(&self) -> FailureFact {
            self.projections.fetch_add(1, Ordering::SeqCst);
            FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::MEMORY, std::num::NonZeroU16::new(401).unwrap()),
            )
        }
    }

    #[test]
    fn foreign_formatter_panic_cannot_replace_the_original_fact_or_cause() {
        let projections = Arc::new(AtomicUsize::new(0));
        let source = SharedFailureCause::new(ForeignCause {
            projections: projections.clone(),
            panic_on_display: true,
        });
        let clone = source.clone();
        assert_eq!(source, clone);
        assert!(
            source
                .downcast_ref::<ForeignCause>()
                .unwrap()
                .panic_on_display
        );
        assert!(std::ptr::eq(
            source.downcast_ref::<ForeignCause>().unwrap(),
            clone
                .source()
                .unwrap()
                .downcast_ref::<ForeignCause>()
                .unwrap()
        ));
        assert_eq!(source.failure_fact().class(), FailureClass::Resource);
        assert_eq!(clone.failure_fact(), source.failure_fact());
        assert_eq!(source.detail(), "domain failure detail unavailable");
        assert_eq!(projections.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shared_diagnostic_is_bounded_utf8_and_independent_occurrences_stay_distinct() {
        let projections = Arc::new(AtomicUsize::new(0));
        let make = || {
            SharedFailureCause::new(ForeignCause {
                projections: projections.clone(),
                panic_on_display: false,
            })
        };
        let first = make();
        let second = make();
        assert_eq!(first.failure_fact(), second.failure_fact());
        assert_ne!(first, second);
        assert_eq!(first, first.clone());
        assert_eq!(first.detail().len(), crate::DIAGNOSTIC_MAX_BYTES / 3 * 3);
        assert!(first.detail().chars().all(|c| c == '界'));
        assert_eq!(projections.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn static_detail_preserves_original_fact_without_calling_foreign_formatter() {
        let projections = Arc::new(AtomicUsize::new(0));
        let detail: &'static str = "界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界界";
        let source = SharedFailureCause::from_static_detail(
            ForeignCause {
                projections: projections.clone(),
                panic_on_display: true,
            },
            detail,
        );
        assert_eq!(source.failure_fact().class(), FailureClass::Resource);
        assert!(
            source
                .downcast_ref::<ForeignCause>()
                .unwrap()
                .panic_on_display
        );
        assert_eq!(projections.load(Ordering::SeqCst), 1);
        assert_eq!(source.detail().len(), 510);
        assert!(source.detail().chars().all(|value| value == '界'));
        // The quote follows the actual erased Arc pointee, including its fact
        // and String header, rather than the smaller originating E alone.
        assert_eq!(
            SharedFailureCause::record_layout::<ForeignCause>(),
            std::alloc::Layout::for_value(source.0.as_ref())
        );
        assert!(
            SharedFailureCause::record_layout::<ForeignCause>().size()
                > std::mem::size_of::<ForeignCause>()
        );
    }

    #[derive(Debug, Default)]
    struct SourceLifetime {
        projections: AtomicUsize,
        renderings: AtomicUsize,
        source_drops: AtomicUsize,
        guard_drops: AtomicUsize,
        unwinding_guards: AtomicUsize,
        order: Mutex<Vec<&'static str>>,
    }
    #[derive(Debug)]
    struct OwnedSource {
        lifetime: Arc<SourceLifetime>,
        panic_on_drop: bool,
    }
    impl fmt::Display for OwnedSource {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.lifetime.renderings.fetch_add(1, Ordering::SeqCst);
            f.write_str("original canonical source")
        }
    }
    impl Error for OwnedSource {}
    impl FailureProjection for OwnedSource {
        fn failure_fact(&self) -> FailureFact {
            self.lifetime.projections.fetch_add(1, Ordering::SeqCst);
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, std::num::NonZeroU16::new(402).unwrap()),
            )
        }
    }
    impl Drop for OwnedSource {
        fn drop(&mut self) {
            self.lifetime.order.lock().unwrap().push("source");
            self.lifetime.source_drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "original source destructor panic");
        }
    }
    #[derive(Debug)]
    struct SourceGuard(Arc<SourceLifetime>);
    impl SourceRetentionCapability for SourceGuard {}
    impl Drop for SourceGuard {
        fn drop(&mut self) {
            self.0.order.lock().unwrap().push("guard");
            self.0.guard_drops.fetch_add(1, Ordering::SeqCst);
            if std::thread::panicking() {
                self.0.unwinding_guards.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    fn owned_source(panic_on_drop: bool) -> (SharedFailureCause, Arc<SourceLifetime>) {
        let lifetime = Arc::new(SourceLifetime::default());
        let source = SharedFailureCause::new(OwnedSource {
            lifetime: lifetime.clone(),
            panic_on_drop,
        });
        (source, lifetime)
    }

    #[test]
    fn aliases_from_before_or_after_attachment_keep_the_original_node_counted() {
        for alias_before_attachment in [false, true] {
            let (source, lifetime) = owned_source(false);
            let pointer = source.downcast_ref::<OwnedSource>().unwrap() as *const _;
            let fact = source.failure_fact();
            let early = alias_before_attachment.then(|| source.clone());
            source.retain_source_lifetime(Box::new(SourceGuard(lifetime.clone())));
            let alias = early.unwrap_or_else(|| source.clone());
            assert_eq!(source, alias);
            assert_eq!(alias.failure_fact(), fact);
            assert_eq!(
                alias
                    .source()
                    .unwrap()
                    .downcast_ref::<OwnedSource>()
                    .unwrap() as *const _,
                pointer
            );
            drop(source);
            assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 0);
            assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 0);
            drop(alias);
            assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 1);
            assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 1);
            assert_eq!(lifetime.unwinding_guards.load(Ordering::SeqCst), 0);
            assert_eq!(*lifetime.order.lock().unwrap(), ["source", "guard"]);
            assert_eq!(lifetime.projections.load(Ordering::SeqCst), 1);
            assert_eq!(lifetime.renderings.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn rewrapping_a_shared_cause_keeps_its_canonical_identity_and_original_projection() {
        let (source, lifetime) = owned_source(false);
        let alias = source.clone();
        let pointer = source.downcast_ref::<OwnedSource>().unwrap() as *const _;
        let fact = source.failure_fact();
        let detail = source.detail().to_owned();
        let wrapped = SharedFailureCause::new(source);
        let rewrapped = SharedFailureCause::new(wrapped.clone());
        assert_eq!(wrapped, alias);
        assert_eq!(rewrapped, alias);
        assert_eq!(rewrapped.failure_fact(), fact);
        assert_eq!(rewrapped.detail(), detail);
        assert_eq!(
            rewrapped.downcast_ref::<OwnedSource>().unwrap() as *const _,
            pointer
        );
        assert!(rewrapped.downcast_ref::<SharedFailureCause>().is_none());
        rewrapped.retain_source_lifetime(Box::new(SourceGuard(lifetime.clone())));
        drop(wrapped);
        drop(rewrapped);
        assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 0);
        assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 0);
        assert_eq!(lifetime.projections.load(Ordering::SeqCst), 1);
        assert_eq!(lifetime.renderings.load(Ordering::SeqCst), 1);
        drop(alias);
        assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 1);
        assert_eq!(*lifetime.order.lock().unwrap(), ["source", "guard"]);
    }

    #[test]
    fn original_source_destructor_panic_reaches_the_guard_during_unwind() {
        let (source, lifetime) = owned_source(true);
        source.retain_source_lifetime(Box::new(SourceGuard(lifetime.clone())));
        let alias = source.clone();
        drop(source);
        assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 0);
        assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(alias)));
        assert!(result.is_err());
        assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 1);
        assert_eq!(lifetime.unwinding_guards.load(Ordering::SeqCst), 1);
        assert_eq!(*lifetime.order.lock().unwrap(), ["source", "guard"]);
    }

    #[test]
    fn static_detail_and_both_rewrapping_entries_preserve_canonical_source_custody() {
        for early_alias in [false, true] {
            let lifetime = Arc::new(SourceLifetime::default());
            let source = SharedFailureCause::from_static_detail(
                OwnedSource {
                    lifetime: lifetime.clone(),
                    panic_on_drop: false,
                },
                "static original source",
            );
            assert_eq!(
                SharedFailureCause::record_layout::<OwnedSource>(),
                std::alloc::Layout::for_value(source.0.as_ref())
            );
            let before = early_alias.then(|| source.clone());
            source.retain_source_lifetime(Box::new(SourceGuard(lifetime.clone())));
            let alias = before.unwrap_or_else(|| source.clone());
            let wrapped = SharedFailureCause::from_static_detail(source, "outer diagnostic");
            let rewrapped = SharedFailureCause::new(wrapped.clone());
            assert_eq!(wrapped, alias);
            assert_eq!(rewrapped, alias);
            assert_eq!(rewrapped.detail(), "static original source");
            assert!(rewrapped.downcast_ref::<OwnedSource>().is_some());
            assert!(rewrapped.downcast_ref::<SharedFailureCause>().is_none());
            drop(wrapped);
            drop(rewrapped);
            assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 0);
            assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 0);
            assert_eq!(lifetime.projections.load(Ordering::SeqCst), 1);
            assert_eq!(lifetime.renderings.load(Ordering::SeqCst), 0);
            drop(alias);
            assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 1);
            assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 1);
            assert_eq!(*lifetime.order.lock().unwrap(), ["source", "guard"]);
        }
    }

    #[test]
    fn static_detail_source_unwind_cannot_turn_its_retained_guard_into_normal_exit() {
        let lifetime = Arc::new(SourceLifetime::default());
        let source = SharedFailureCause::from_static_detail(
            OwnedSource {
                lifetime: lifetime.clone(),
                panic_on_drop: true,
            },
            "static failure",
        );
        let alias = source.clone();
        source.retain_source_lifetime(Box::new(SourceGuard(lifetime.clone())));
        let wrapped = SharedFailureCause::from_static_detail(source, "outer failure");
        drop(wrapped);
        assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 0);
        assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 0);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(alias))).is_err());
        assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 1);
        assert_eq!(lifetime.unwinding_guards.load(Ordering::SeqCst), 1);
        assert_eq!(lifetime.projections.load(Ordering::SeqCst), 1);
        assert_eq!(lifetime.renderings.load(Ordering::SeqCst), 0);
        assert_eq!(*lifetime.order.lock().unwrap(), ["source", "guard"]);
    }

    #[test]
    fn independent_admitted_guards_exit_with_the_same_original_source_node() {
        let (source, lifetime) = owned_source(false);
        source.retain_source_lifetime(Box::new(SourceGuard(lifetime.clone())));
        source.retain_source_lifetime(Box::new(SourceGuard(lifetime.clone())));
        assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 0);
        drop(source);
        assert_eq!(lifetime.source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(lifetime.guard_drops.load(Ordering::SeqCst), 2);
        assert_eq!(
            *lifetime.order.lock().unwrap(),
            ["source", "guard", "guard"]
        );
    }
}
