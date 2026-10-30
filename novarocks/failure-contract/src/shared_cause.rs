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

use crate::{BoundedDetail, FailureFact, FailureProjection};
use std::{error::Error, fmt, sync::Arc};

trait ProjectedCause: Error + Send + Sync {
    fn fact(&self) -> FailureFact;
    fn local_detail(&self) -> &str;
    fn original(&self) -> &(dyn Error + 'static);
}
struct CauseRecord<E> {
    cause: E,
    fact: FailureFact,
    detail: BoundedDetail,
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
}

/// Clones share one originating cause. Equality denotes that same origin,
/// rather than comparing arbitrary third-party graphs or diagnostic strings.
#[derive(Clone)]
pub struct SharedFailureCause(Arc<dyn ProjectedCause>);
impl SharedFailureCause {
    pub fn new<E: Error + FailureProjection + Send + Sync + 'static>(cause: E) -> Self {
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
        }))
    }
    /// A closed producer's static diagnostic avoids evaluating an arbitrary
    /// formatter. The actual cause and its independently authored fact remain
    /// the same original; this grants no allocation or execution capacity.
    pub fn from_static_detail<E: Error + FailureProjection + Send + Sync + 'static>(
        cause: E,
        detail: &'static str,
    ) -> Self {
        let fact = cause.failure_fact();
        Self(Arc::new(CauseRecord {
            cause,
            fact,
            detail: BoundedDetail::truncating(detail),
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
}
