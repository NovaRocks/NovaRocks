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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Immutable runtime-filter sites keep their actual collection creator.

use crate::program::{AggregateTopNFilter, FilterConsumerAtExpr};
use novarocks_type_contract::{
    ValueTypeStorageLifetime,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};
use std::{alloc::Layout, fmt, ops::Deref, sync::Arc};

struct CreatedSites<T> {
    entries: Vec<T>,
}

#[derive(Clone)]
enum SiteStorage<T> {
    Legacy(Vec<T>),
    Created(Arc<CreatedSites<T>>),
}

#[derive(Clone)]
struct SiteCollection<T> {
    storage: SiteStorage<T>,
    // The full private Arc, including its backing, drops before this marker.
    _creator: ValueTypeStorageLifetime,
}

impl<T> SiteCollection<T> {
    fn legacy(entries: Vec<T>) -> Self {
        Self {
            storage: SiteStorage::Legacy(entries),
            _creator: ValueTypeStorageLifetime::pure(),
        }
    }

    fn try_from_vec_observed<E: From<LayoutResourceError>>(
        entries: Vec<T>,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(Layout) -> Result<(), E>,
    ) -> Result<Self, E> {
        if entries.capacity() == 0 {
            return Ok(Self::legacy(entries));
        }
        // Keep the moved, already-paid Vec ahead of its creator on rejection.
        let creator = storage;
        let entries = entries;
        request(arc_layout(Layout::new::<CreatedSites<T>>())?)?;
        Ok(Self {
            storage: SiteStorage::Created(Arc::new(CreatedSites { entries })),
            _creator: creator,
        })
    }

    fn as_slice(&self) -> &[T] {
        match &self.storage {
            SiteStorage::Legacy(entries) => entries,
            SiteStorage::Created(owner) => &owner.entries,
        }
    }
}

macro_rules! site_collection {
    ($name:ident, $element:ty) => {
        /// Whole immutable sites. Legacy clones retain the original Vec clone
        /// behavior; created collections share their complete original owner.
        #[derive(Clone)]
        pub struct $name(SiteCollection<$element>);

        impl $name {
            /// Move the original Vec without shrinking or reallocating it.
            /// The caller must have admitted its actual backing before Vec
            /// allocation on the scope represented by `storage`. This method
            /// requests only the new private Arc holder; zero-capacity input
            /// needs no holder. Empty input with spare capacity still retains
            /// its actual stock. No backing is charged twice or authorized here.
            pub fn try_from_vec_observed<E: From<LayoutResourceError>>(
                entries: Vec<$element>,
                storage: ValueTypeStorageLifetime,
                request: impl FnMut(Layout) -> Result<(), E>,
            ) -> Result<Self, E> {
                SiteCollection::try_from_vec_observed(entries, storage, request).map(Self)
            }

            pub fn as_slice(&self) -> &[$element] {
                self.0.as_slice()
            }
        }

        impl From<Vec<$element>> for $name {
            fn from(entries: Vec<$element>) -> Self {
                Self(SiteCollection::legacy(entries))
            }
        }
        impl Default for $name {
            fn default() -> Self {
                Vec::new().into()
            }
        }
        impl Deref for $name {
            type Target = [$element];
            fn deref(&self) -> &Self::Target {
                self.as_slice()
            }
        }
        impl AsRef<[$element]> for $name {
            fn as_ref(&self) -> &[$element] {
                self.as_slice()
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.as_slice().fmt(formatter)
            }
        }
        impl<'a> IntoIterator for &'a $name {
            type Item = &'a $element;
            type IntoIter = std::slice::Iter<'a, $element>;
            fn into_iter(self) -> Self::IntoIter {
                self.as_slice().iter()
            }
        }
    };
}

site_collection!(AggregateTopNFilters, AggregateTopNFilter);
site_collection!(ScanFilterConsumers, FilterConsumerAtExpr);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FilterConsumerActivation, FilterLateApplyGranularity, FilterNullOrder, FilterOrderKey,
        FilterOrderKeys, FilterProducerKind, FilterReduction, FilterSortDirection,
        StaticFilterConsumer, StaticFilterContract, StaticFilterProducer,
    };
    use novarocks_type_contract::{
        CompileControlError, FunctionValueType, ValueLogicalType, ValueSchemaCreationScope,
        ValueTypeStorageHold,
    };
    use std::{
        num::NonZeroU32,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    struct ElementLifetime(Arc<Mutex<Vec<&'static str>>>);
    impl ValueTypeStorageHold for ElementLifetime {}
    impl Drop for ElementLifetime {
        fn drop(&mut self) {
            self.0.lock().unwrap().push("element");
        }
    }

    struct CreationLifetime {
        held: Arc<AtomicUsize>,
        bytes: AtomicUsize,
        exits: Arc<Mutex<Vec<&'static str>>>,
    }
    impl ValueTypeStorageHold for CreationLifetime {}
    impl Drop for CreationLifetime {
        fn drop(&mut self) {
            self.held
                .fetch_sub(self.bytes.load(Ordering::SeqCst), Ordering::SeqCst);
            self.exits.lock().unwrap().push("creator");
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Refusal {
        Layout(LayoutResourceError),
        Host,
        Control(CompileControlError),
    }
    impl From<LayoutResourceError> for Refusal {
        fn from(error: LayoutResourceError) -> Self {
            Self::Layout(error)
        }
    }

    struct Scope {
        hold: Arc<CreationLifetime>,
        remaining: usize,
        calls: usize,
        stop: Option<CompileControlError>,
    }
    impl Scope {
        fn new(exits: Arc<Mutex<Vec<&'static str>>>, held: Arc<AtomicUsize>) -> Self {
            Self {
                hold: Arc::new(CreationLifetime {
                    held,
                    bytes: AtomicUsize::new(0),
                    exits,
                }),
                remaining: usize::MAX,
                calls: 0,
                stop: None,
            }
        }

        fn prepaid<T>(&mut self, entry: T) -> Vec<T> {
            // Spare capacity belongs to the actual caller's original Vec.
            self.request_layout(Layout::array::<T>(3).unwrap()).unwrap();
            let mut entries = Vec::with_capacity(3);
            entries.push(entry);
            entries
        }
    }
    impl ValueSchemaCreationScope<Refusal> for Scope {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            ValueTypeStorageLifetime::held(self.hold.clone())
        }
        fn request_layout(&mut self, layout: Layout) -> Result<(), Refusal> {
            self.calls += 1;
            if let Some(cause) = self.stop.clone() {
                return Err(Refusal::Control(cause));
            }
            if layout.size() > self.remaining {
                return Err(Refusal::Host);
            }
            self.remaining -= layout.size();
            self.hold.bytes.fetch_add(layout.size(), Ordering::SeqCst);
            self.hold.held.fetch_add(layout.size(), Ordering::SeqCst);
            Ok(())
        }
        fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), Refusal> {
            panic!("site collections request exact holder Layouts");
        }
    }

    fn contract(exits: Arc<Mutex<Vec<&'static str>>>) -> StaticFilterContract {
        let lifetime = ValueTypeStorageLifetime::held(Arc::new(ElementLifetime(exits)));
        let value_type =
            FunctionValueType::try_with_logical_type_with_construction::<std::convert::Infallible>(
                arrow_schema::DataType::Utf8,
                true,
                ValueLogicalType::Physical,
                lifetime,
                None,
            )
            .unwrap();
        StaticFilterContract::Ordered {
            keys: FilterOrderKeys::from_legacy(Arc::from([FilterOrderKey {
                value_type,
                direction: FilterSortDirection::Ascending,
                null_order: FilterNullOrder::Last,
            }])),
            comparator_digest: [1; 32],
            contract_digest: [2; 32],
        }
    }

    fn topn_site(exits: Arc<Mutex<Vec<&'static str>>>) -> AggregateTopNFilter {
        AggregateTopNFilter {
            group_key_expr: crate::test_support::root(0, 4),
            group_key_ordinal: 0,
            limit: NonZeroU32::new(7).unwrap(),
            producer: StaticFilterProducer::try_new(
                11,
                12,
                FilterProducerKind::OrderedBound,
                contract(exits),
                FilterReduction::TightenOrderedBound,
            )
            .unwrap(),
        }
    }

    fn scan_site(exits: Arc<Mutex<Vec<&'static str>>>) -> FilterConsumerAtExpr {
        FilterConsumerAtExpr {
            expr_id: crate::test_support::root(0, 4),
            consumer: StaticFilterConsumer::try_new(
                11,
                12,
                FilterConsumerActivation::NonBlockingLive {
                    late_apply: FilterLateApplyGranularity::Batch,
                },
                contract(exits),
                FilterReduction::TightenOrderedBound,
            )
            .unwrap(),
        }
    }

    #[test]
    fn runtime_filter_sites_empty_capacity_preserves_only_actual_stock() {
        macro_rules! check_empty {
            ($collection:ty, $element:ty) => {{
                let exits = Arc::new(Mutex::new(Vec::new()));
                let held = Arc::new(AtomicUsize::new(0));
                let mut scope = Scope::new(exits.clone(), held.clone());
                let empty = <$collection>::try_from_vec_observed(
                    Vec::<$element>::new(),
                    scope.storage_lifetime(),
                    |layout| scope.request_layout(layout),
                )
                .unwrap();
                assert_eq!(scope.calls, 0);
                assert!(empty.is_empty());
                drop(scope);
                assert_eq!(held.load(Ordering::SeqCst), 0);
                assert_eq!(*exits.lock().unwrap(), ["creator"]);
                drop(empty);

                let exits = Arc::new(Mutex::new(Vec::new()));
                let held = Arc::new(AtomicUsize::new(0));
                let mut scope = Scope::new(exits.clone(), held.clone());
                scope
                    .request_layout(Layout::array::<$element>(3).unwrap())
                    .unwrap();
                let entries = Vec::<$element>::with_capacity(3);
                let pointer = entries.as_ptr();
                let empty = <$collection>::try_from_vec_observed(
                    entries,
                    scope.storage_lifetime(),
                    |layout| scope.request_layout(layout),
                )
                .unwrap();
                assert_eq!(scope.calls, 2);
                assert!(empty.is_empty());
                assert_eq!(empty.as_ptr(), pointer);
                let paid = held.load(Ordering::SeqCst);
                assert!(paid > 0);
                let last = empty.clone();
                drop(scope);
                drop(empty);
                assert_eq!(held.load(Ordering::SeqCst), paid);
                assert!(exits.lock().unwrap().is_empty());
                std::thread::spawn(move || drop(last)).join().unwrap();
                assert_eq!(held.load(Ordering::SeqCst), 0);
                assert_eq!(*exits.lock().unwrap(), ["creator"]);
            }};
        }
        check_empty!(AggregateTopNFilters, AggregateTopNFilter);
        check_empty!(ScanFilterConsumers, FilterConsumerAtExpr);
    }

    // Exercise the original scope port and independent element/creator exits.
    // This checks public alias/prefix lifetimes, not private allocator traces.
    macro_rules! site_tests {
        ($collection:ident, $entry:ident, $last_alias:ident, $refusal:ident) => {
            #[test]
            fn $last_alias() {
                let exits = Arc::new(Mutex::new(Vec::new()));
                let held = Arc::new(AtomicUsize::new(0));
                let mut scope = Scope::new(exits.clone(), held.clone());
                let entries = scope.prepaid($entry(exits.clone()));
                let pointer = entries.as_ptr();
                let keys = $collection::try_from_vec_observed(
                    entries,
                    scope.storage_lifetime(),
                    |layout| scope.request_layout(layout),
                )
                .unwrap();
                assert_eq!(scope.calls, 2);
                assert_eq!(keys.as_ptr(), pointer);
                let paid = held.load(Ordering::SeqCst);
                let legacy: $collection = vec![$entry(Arc::new(Mutex::new(Vec::new())))].into();
                let legacy_copy = legacy.clone();
                assert_ne!(legacy.as_ptr(), legacy_copy.as_ptr());
                assert_eq!(format!("{legacy:?}"), format!("{legacy_copy:?}"));
                assert_eq!(format!("{keys:?}"), format!("{legacy:?}"));
                assert_eq!((&keys).into_iter().count(), 1);
                assert!($collection::default().is_empty());
                let last = keys.clone();
                assert_eq!(keys.as_ptr(), last.as_ptr());
                drop(scope);
                drop(keys);
                assert_eq!(held.load(Ordering::SeqCst), paid);
                assert!(exits.lock().unwrap().is_empty());
                std::thread::spawn(move || drop(last)).join().unwrap();
                assert_eq!(held.load(Ordering::SeqCst), 0);
                assert_eq!(*exits.lock().unwrap(), ["element", "creator"]);
            }

            #[test]
            fn $refusal() {
                for cause in [
                    None,
                    Some(CompileControlError::Cancelled),
                    Some(CompileControlError::DeadlineExceeded),
                    Some(CompileControlError::ResourceExhausted),
                ] {
                    let exits = Arc::new(Mutex::new(Vec::new()));
                    let held = Arc::new(AtomicUsize::new(0));
                    let mut scope = Scope::new(exits.clone(), held.clone());
                    let entries = scope.prepaid($entry(exits.clone()));
                    scope.remaining = 0;
                    scope.stop = cause.clone();
                    let result = $collection::try_from_vec_observed(
                        entries,
                        scope.storage_lifetime(),
                        |layout| scope.request_layout(layout),
                    );
                    assert_eq!(
                        result.unwrap_err(),
                        cause.map(Refusal::Control).unwrap_or(Refusal::Host)
                    );
                    assert_eq!(scope.calls, 2);
                    assert_eq!(*exits.lock().unwrap(), ["element"]);
                    drop(scope);
                    assert_eq!(held.load(Ordering::SeqCst), 0);
                    assert_eq!(*exits.lock().unwrap(), ["element", "creator"]);
                }
            }
        };
    }

    site_tests!(
        AggregateTopNFilters,
        topn_site,
        aggregate_topn_sites_keep_last_whole_alias_and_legacy_deep_clone,
        aggregate_topn_sites_release_prepaid_prefix_on_refusal
    );
    site_tests!(
        ScanFilterConsumers,
        scan_site,
        scan_filter_sites_keep_last_whole_alias_and_legacy_deep_clone,
        scan_filter_sites_release_prepaid_prefix_on_refusal
    );
}
