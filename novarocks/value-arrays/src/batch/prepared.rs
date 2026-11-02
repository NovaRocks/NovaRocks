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

use crate::{
    ValueError, ValueFailure, ValueViolation,
    arrow_raw::{BoundView, OwnedView},
    build::BuiltArray,
    decode::{DecodedBytes, RowMapping},
    reachability::SelectionSet,
};
use arrow_array::{Array, Int32Array};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{convert::Infallible, sync::Arc};

/// Borrows a decoded owner and its admitted logical selection. No conversion
/// cache borrow or temporary descriptor allocation escapes preparation.
#[derive(Clone, Copy)]
pub struct PreparedBytes<'a> {
    decoded: &'a DecodedBytes,
    rows: &'a SelectionSet,
    view: BoundView<'a>,
    count: usize,
}
impl<'a> PreparedBytes<'a> {
    pub fn new(decoded: &'a DecodedBytes, rows: &'a SelectionSet) -> Result<Self, ValueError> {
        Self::new_observed(decoded, rows, &mut |_| Ok::<_, Infallible>(()))
            .map_err(ValueFailure::into_error)
    }
    pub fn new_observed<E, W>(
        decoded: &'a DecodedBytes,
        rows: &'a SelectionSet,
        work: &mut W,
    ) -> Result<Self, ValueFailure<E>>
    where
        W: FnMut(usize) -> Result<(), E>,
    {
        if rows.len() != decoded.len() {
            return Err(ValueError::Invariant(ValueViolation::Bounds).into());
        }
        let view = BoundView::bind(decoded.bottom().as_ref())?;
        let count = check_prepared_selection(decoded, rows, view, work)?;
        Ok(Self {
            decoded,
            rows,
            view,
            count,
        })
    }
    /// Number of selected logical occurrences in ascending logical-row order.
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn is_utf8(&self) -> bool {
        self.decoded.is_utf8()
    }
    pub fn selection(&self) -> &SelectionSet {
        self.rows
    }
    pub(crate) fn bottom_len(&self) -> usize {
        self.decoded.bottom().len()
    }
    pub(crate) fn mapping(&self) -> &RowMapping {
        self.decoded.mapping()
    }
    pub(crate) fn view(&self) -> BoundView<'a> {
        self.view
    }
    pub(crate) fn check_row(&self, row: usize) -> Result<(), ValueError> {
        if self.rows.contains(row) {
            Ok(())
        } else {
            Err(ValueError::Invariant(ValueViolation::UndefinedRow))
        }
    }
}
// Both borrowed and owned preparation retain this single selected-domain
// validator. Its caller preserves the original length-before-bind ordering.
fn check_prepared_selection<E, W>(
    decoded: &DecodedBytes,
    rows: &SelectionSet,
    view: BoundView<'_>,
    work: &mut W,
) -> Result<usize, ValueFailure<E>>
where
    W: FnMut(usize) -> Result<(), E>,
{
    #[cfg(test)]
    PREPARATION_CHECKS.with(|checks| {
        let (calls, selected) = checks.get();
        checks.set((calls + 1, selected));
    });
    work(0).map_err(ValueFailure::Caller)?;
    let mut control = super::RowWork::new(work);
    let mut count = 0;
    let mut cursor = rows.cursor();
    while let Some(row) = cursor.next_observed(&mut |n| control.observe_metadata(n))? {
        control.row()?;
        #[cfg(test)]
        PREPARATION_CHECKS.with(|checks| {
            let (calls, selected) = checks.get();
            checks.set((calls, selected + 1));
        });
        if !decoded.readable_rows().contains(row) {
            return Err(ValueError::Invariant(ValueViolation::UndefinedRow).into());
        }
        if let Some(bottom) = decoded.mapping().mapped_row(row)? {
            if !decoded.bottom_coverage().contains(bottom) {
                return Err(ValueError::Invariant(ValueViolation::UndefinedRow).into());
            }
            // Check only selected values; placeholders outside coverage are
            // never dereferenced, even when their descriptor is present.
            view.value(bottom)?;
        }
        count += 1;
    }
    control.flush()?;
    Ok(count)
}

#[cfg(test)]
std::thread_local! {
    static PREPARATION_CHECKS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

/// An immutable checked byte batch which owns the real decoded source,
/// selection and view handles. Moving this value never creates a self borrow.
///
/// The selection Arc must come from its actual construction owner. `storage`
/// retains the creator for preparation metadata; decoded retains its own source
/// creator. Neither lifetime handle grants allocation capacity.
///
/// Fields destroy the extra view aliases before their decoded source creator,
/// then release the preparation creator after all owned preparation backing.
pub struct OwnedPreparedBytes {
    owner: OwnedView,
    rows: Arc<SelectionSet>,
    decoded: DecodedBytes,
    count: usize,
    _storage: ValueTypeStorageLifetime,
}
impl OwnedPreparedBytes {
    pub fn new_observed<E, G, W>(
        decoded: DecodedBytes,
        rows: Arc<SelectionSet>,
        storage: ValueTypeStorageLifetime,
        growth: &mut G,
        work: &mut W,
    ) -> Result<Self, ValueFailure<E>>
    where
        G: FnMut(std::alloc::Layout) -> Result<(), E>,
        W: FnMut(usize) -> Result<(), E>,
    {
        // On a failed construction the extra view handles (declared below)
        // must drop first, followed by rows/decoded and their preparation hold.
        // Keep these inputs together until no fallible step remains.
        let pending = (rows, decoded, storage);
        if pending.0.len() != pending.1.len() {
            return Err(ValueError::Invariant(ValueViolation::Bounds).into());
        }
        let owner = OwnedView::bind_observed(pending.1.bottom().as_ref(), growth, work)?;
        let count = check_prepared_selection(&pending.1, &pending.0, owner.view(), work)?;
        let (rows, decoded, storage) = pending;
        Ok(Self {
            owner,
            rows,
            decoded,
            count,
            _storage: storage,
        })
    }

    /// Recreate a short loan in constant time without binding, validation,
    /// selection traversal, allocation or payload copying. The private fields
    /// refer to this exact immutable checked owner; no new domain is admitted.
    pub fn borrow(&self) -> PreparedBytes<'_> {
        PreparedBytes {
            decoded: &self.decoded,
            rows: &self.rows,
            view: self.owner.view(),
            count: self.count,
        }
    }
}

/// A checked, immutable view page published by the public value builder.
/// The page owns its descriptor, validity, payload and defined-domain handles;
/// a new or compacted page never changes an existing page's readable values.
pub struct OwnedBytesPage {
    owner: OwnedView,
    defined: SelectionSet,
}
impl OwnedBytesPage {
    pub fn new<E, G, W>(
        built: BuiltArray,
        defined: SelectionSet,
        growth: &mut G,
        work: &mut W,
    ) -> Result<Self, ValueFailure<E>>
    where
        G: FnMut(std::alloc::Layout) -> Result<(), E>,
        W: FnMut(usize) -> Result<(), E>,
    {
        if defined.len() != built.array.len() {
            return Err(ValueError::Invariant(ValueViolation::Bounds).into());
        }
        let owner = OwnedView::bind_observed(built.array.as_ref(), growth, work)?;
        let view = owner.view();
        let mut control = super::RowWork::new(work);
        let mut cursor = defined.cursor();
        while let Some(row) = cursor.next_observed(&mut |n| control.observe_metadata(n))? {
            control.row()?;
            // Check the real descriptor extent, rather than trusting a caller's
            // coverage assertion or a pointer to separately retained storage.
            view.value(row)?;
        }
        control.flush()?;
        Ok(Self { owner, defined })
    }
    /// Physical page coordinates, including positions outside its defined set.
    pub fn len(&self) -> usize {
        self.owner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn is_utf8(&self) -> bool {
        self.owner.is_utf8()
    }
    pub fn defined_rows(&self) -> &SelectionSet {
        &self.defined
    }
    pub(crate) fn view(&self) -> BoundView<'_> {
        self.owner.view()
    }
    pub(crate) fn check_row(&self, row: usize) -> Result<(), ValueError> {
        if self.defined.contains(row) {
            Ok(())
        } else {
            Err(ValueError::Invariant(ValueViolation::UndefinedRow))
        }
    }
}

pub(crate) trait Mapping {
    fn index(&self, row: usize) -> Option<usize>;
}
pub(crate) struct Identity;
impl Mapping for Identity {
    fn index(&self, row: usize) -> Option<usize> {
        Some(row)
    }
}
pub(crate) struct Constant(pub(crate) usize);
impl Mapping for Constant {
    fn index(&self, _: usize) -> Option<usize> {
        Some(self.0)
    }
}
pub(crate) struct Dictionary<'a>(pub(crate) &'a Int32Array);
impl Mapping for Dictionary<'_> {
    fn index(&self, row: usize) -> Option<usize> {
        // All selected mapping values were checked during preparation; NULL
        // keys never expose their physical integer.
        (!self.0.is_null(row)).then(|| self.0.value(row) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ByteLayout, access::ByteSource, decode::ValueConversionContext, reachability::ReachableRows,
    };
    use arrow_array::{ArrayRef, StringArray};
    use std::sync::Arc;
    #[test]
    fn prepared_selection_keeps_coverage_and_owner_after_cache_clear() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None, Some("unread")]));
        let parsed = ByteSource::parse(&source, ByteLayout::Utf8, None).unwrap();
        let mut context = ValueConversionContext::default();
        let decoded = context
            .decode(&parsed, &ReachableRows::indices(3, &[0, 1]))
            .unwrap();
        context.clear();
        let mut rows = SelectionSet::try_new(3).unwrap();
        rows.insert(0).unwrap();
        rows.insert(1).unwrap();
        let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared.view().value(0).unwrap(), Some(b"x".as_slice()));
        assert_eq!(prepared.view().value(1).unwrap(), None);
        rows.insert(2).unwrap();
        assert!(matches!(
            PreparedBytes::new(&decoded, &rows),
            Err(ValueError::Invariant(ValueViolation::UndefinedRow))
        ));
    }
    #[test]
    fn prepared_reads_do_not_rebind_the_carrier_per_row() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            Some(
                "long external bytes to inspect"
            );
            4096
        ]));
        let parsed = ByteSource::parse(&source, ByteLayout::Utf8, None).unwrap();
        let mut context = ValueConversionContext::default();
        let decoded = context
            .decode(&parsed, &ReachableRows::all(source.len()))
            .unwrap();
        let rows = ReachableRows::all(source.len())
            .materialize(&mut |_| Ok::<_, Infallible>(()))
            .unwrap();
        let before = crate::arrow_raw::concrete_bindings();
        let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
        let bound = crate::arrow_raw::concrete_bindings();
        assert_eq!(bound - before, 1);
        for row in prepared.selection().iter() {
            assert!(prepared.view().value(row).unwrap().is_some());
        }
        assert_eq!(crate::arrow_raw::concrete_bindings(), bound);
    }

    #[test]
    fn preparation_reports_original_control_failure_even_for_null_rows() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 4096]));
        let parsed = ByteSource::parse(&source, ByteLayout::Utf8, None).unwrap();
        let decoded = ValueConversionContext::default()
            .decode(&parsed, &ReachableRows::all(source.len()))
            .unwrap();
        let rows = ReachableRows::all(source.len())
            .materialize(&mut |_| Ok::<_, Infallible>(()))
            .unwrap();
        let failure = PreparedBytes::new_observed(&decoded, &rows, &mut |units| {
            if units > 0 { Err("stop") } else { Ok(()) }
        });
        assert!(matches!(failure, Err(ValueFailure::Caller("stop"))));
    }
    #[test]
    fn empty_and_sparse_large_selection_observe_bitmap_work_before_rows() {
        let source: ArrayRef = Arc::new(arrow_array::StringViewArray::new_null(200_000));
        let parsed = ByteSource::parse(&source, ByteLayout::Utf8View, None).unwrap();
        let mut selected = SelectionSet::try_new(200_000).unwrap();
        let decoded = ValueConversionContext::default()
            .decode(&parsed, &ReachableRows::all(200_000))
            .unwrap();
        for sparse in [false, true] {
            if sparse {
                selected.insert(199_999).unwrap();
            }
            let result = PreparedBytes::new_observed(&decoded, &selected, &mut |units| {
                if units > 0 {
                    Err("bitmap stop")
                } else {
                    Ok(())
                }
            });
            assert!(matches!(result, Err(ValueFailure::Caller("bitmap stop"))));
        }
    }
}

#[cfg(test)]
mod owned_tests {
    use super::*;
    use crate::{
        ByteLayout,
        build::ValueBuilder,
        control::{global_growth, uncontrolled},
        reachability::ReachableRows,
    };

    #[test]
    fn checked_owned_page_keeps_real_source_and_sparse_domain() {
        let text = "retained external backing after builder and source exit";
        let mut builder = ValueBuilder::new(ByteLayout::Utf8View);
        builder
            .append_str(text, &mut global_growth, &mut uncontrolled)
            .unwrap();
        builder
            .append_null(&mut global_growth, &mut uncontrolled)
            .unwrap();
        builder
            .append_str("not admitted", &mut global_growth, &mut uncontrolled)
            .unwrap();
        let mut defined = SelectionSet::try_new(3).unwrap();
        defined.insert(0).unwrap();
        defined.insert(1).unwrap();
        let built = builder.finish().unwrap();
        let before = crate::arrow_raw::concrete_bindings();
        let page =
            OwnedBytesPage::new(built, defined, &mut global_growth, &mut uncontrolled).unwrap();
        let bound = crate::arrow_raw::concrete_bindings();
        assert_eq!(bound - before, 1);
        assert_eq!(page.len(), 3);
        assert!(page.is_utf8());
        for _ in 0..3 {
            assert_eq!(page.view().value(0).unwrap(), Some(text.as_bytes()));
            assert_eq!(page.view().value(1).unwrap(), None);
        }
        assert_eq!(crate::arrow_raw::concrete_bindings(), bound);
        assert!(matches!(
            page.check_row(2),
            Err(ValueError::Invariant(ValueViolation::UndefinedRow))
        ));
        assert!(matches!(
            page.check_row(3),
            Err(ValueError::Invariant(ValueViolation::UndefinedRow))
        ));
    }
    #[test]
    fn owned_binding_rejects_offsets_and_mismatched_domain() {
        for (layout, domain) in [(ByteLayout::Utf8, 1), (ByteLayout::Utf8View, 2)] {
            let mut builder = ValueBuilder::new(layout);
            builder
                .append_str("x", &mut global_growth, &mut uncontrolled)
                .unwrap();
            let defined = ReachableRows::all(domain)
                .materialize(&mut global_growth)
                .unwrap();
            let result = OwnedBytesPage::new(
                builder.finish().unwrap(),
                defined,
                &mut global_growth,
                &mut uncontrolled,
            );
            assert!(matches!(
                result,
                Err(ValueFailure::Value(ValueError::Invariant(_)))
            ));
        }
    }
    #[test]
    fn owned_page_preparation_preserves_control_failure_for_null_domain() {
        let mut builder = ValueBuilder::new(ByteLayout::BinaryView);
        for _ in 0..4096 {
            builder
                .append_null(&mut global_growth, &mut uncontrolled)
                .unwrap();
        }
        let rows = ReachableRows::all(4096)
            .materialize(&mut global_growth)
            .unwrap();
        let result =
            OwnedBytesPage::new(builder.finish().unwrap(), rows, &mut |_| Ok(()), &mut |n| {
                if n > 0 {
                    Err("owned-page-stop")
                } else {
                    Ok(())
                }
            });
        assert!(matches!(
            result,
            Err(ValueFailure::Caller("owned-page-stop"))
        ));
    }
}

#[cfg(test)]
mod owned_growth_test {
    use super::*;
    use crate::{
        ByteLayout,
        build::ValueBuilder,
        control::{global_growth, uncontrolled},
        reachability::ReachableRows,
    };
    #[test]
    fn owned_source_table_growth_refusal_keeps_original_caller() {
        let mut builder = ValueBuilder::new(ByteLayout::Utf8View);
        builder
            .append_str(
                "an external value owns a retained buffer",
                &mut global_growth,
                &mut uncontrolled,
            )
            .unwrap();
        let rows = ReachableRows::all(1)
            .materialize(&mut global_growth)
            .unwrap();
        let result = OwnedBytesPage::new(
            builder.finish().unwrap(),
            rows,
            &mut |_| Err("page-growth"),
            &mut |_| Ok(()),
        );
        assert!(matches!(result, Err(ValueFailure::Caller("page-growth"))));
    }
}

#[cfg(test)]
#[path = "owned_preparation_tests.rs"]
mod owned_preparation_tests;
