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

//! Original checked Package roots and authored occurrence IDs. These views
//! own only loan/ID buffers; stable writer inputs are built in a second stage.
//! All new backing requests and actual visitor steps are admitted before work.
//! Source union invoices and complete-package/host-memory admission remain
//! with the composer. Request bounds do not grant opaque allocator CPU.

use super::type_sources::{
    PackageTypeChannel, PackageTypeOccurrence, PackageTypeOwner, PackageTypeSource,
    PackageTypeSourceSink, visit_package_type_sources_admitted_in,
};
use crate::physical_type_v2::{BorrowedFieldTypeRoot, WriterTypeSource};
use arrow::datatypes::Field;
use novarocks_connector_contract::ConnectorWriteRecipeDraft;
use novarocks_physical_plan as p;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ControlResourceError, PureCompileControl,
    owned_resources::vec::{self as growth, VecPushGrowthFacts},
};
use std::{alloc::Layout, fmt, mem::size_of, ops::Range, sync::Arc};

#[derive(Debug)]
pub enum TypeViewError {
    Control(CompileControlError),
    SourceModel(&'static str),
    InvalidSource(&'static str),
}
impl From<CompileControlError> for TypeViewError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<ControlResourceError> for TypeViewError {
    fn from(error: ControlResourceError) -> Self {
        match error {
            ControlResourceError::Control(cause) => Self::Control(cause),
            ControlResourceError::SourceModel(message) => Self::SourceModel(message),
        }
    }
}
impl fmt::Display for TypeViewError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::SourceModel(message) | Self::InvalidSource(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for TypeViewError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::SourceModel(_) | Self::InvalidSource(_) => None,
        }
    }
}
fn add(a: usize, b: usize) -> Result<usize, TypeViewError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted.into())
}
fn mul(a: usize, b: usize) -> Result<usize, TypeViewError> {
    a.checked_mul(b)
        .ok_or(CompileControlError::ResourceExhausted.into())
}
fn id(position: usize) -> Result<u32, TypeViewError> {
    u32::try_from(position).map_err(|_| CompileControlError::ResourceExhausted.into())
}

/// Exact extension of an actual input Vec. Capturing this layout requests
/// nothing; all buffers for one source header are admitted together first.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceInputReserve<'source> {
    element: Layout,
    len: usize,
    capacity: usize,
    additional: usize,
    next_len: usize,
    old_backing: Option<Layout>,
    requested_backing: Option<Layout>,
    admitted_floor: Option<TypeViewFacts>,
    admitted_package: Option<&'source p::FragmentPackage>,
}
pub(crate) fn capture_source_input_reserve<'source, T>(
    source: &Vec<T>,
    additional: usize,
) -> Result<SourceInputReserve<'source>, TypeViewError> {
    if !novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN {
        return Err(TypeViewError::SourceModel(
            "source input Vec reserve model drift",
        ));
    }
    let next_len = add(source.len(), additional)?;
    let capacity = source.capacity();
    let old_backing = if size_of::<T>() == 0 || capacity == 0 {
        None
    } else {
        Some(Layout::array::<T>(capacity).map_err(|_| CompileControlError::ResourceExhausted)?)
    };
    let requested_backing = if size_of::<T>() == 0 || next_len <= capacity {
        None
    } else {
        Some(Layout::array::<T>(next_len).map_err(|_| CompileControlError::ResourceExhausted)?)
    };
    Ok(SourceInputReserve {
        element: Layout::new::<T>(),
        len: source.len(),
        capacity,
        additional,
        next_len,
        old_backing,
        requested_backing,
        admitted_floor: None,
        admitted_package: None,
    })
}

/// The same future exact reserves, extended as source headers become known.
/// Each extension replaces this batch's contribution by its checked delta;
/// final reserves consume the admitted records without billing them again.
pub(crate) struct SourceInputPrefix<'source, 'control, const N: usize> {
    requests: [SourceInputReserve<'source>; N],
    package: &'source p::FragmentPackage,
    control: &'control dyn PureCompileControl,
    floor: TypeViewFacts,
}
fn input_contribution(
    old_backing: Option<Layout>,
    requested_backing: Option<Layout>,
) -> Result<(usize, usize, usize), TypeViewError> {
    match requested_backing {
        None => Ok((0, 0, 0)),
        Some(layout) => {
            let bytes = layout.size();
            let old = old_backing.map_or(0, |layout| layout.size());
            Ok((1, bytes, add(128, mul(add(bytes, old)?, 4)?)?))
        }
    }
}
impl<'source, 'control, const N: usize> SourceInputPrefix<'source, 'control, N> {
    #[cfg(test)]
    #[cfg(test)]
    #[cfg(test)]
    pub(crate) fn new_in(
        requests: [SourceInputReserve<'source>; N],
        budget: &mut TypeViewBudget<'source, 'control, '_>,
        work: &CompileCheckpoints<'control>,
    ) -> Result<Self, TypeViewError> {
        Self::new_with_work_in(requests, 0, budget, work)
    }
    pub(crate) fn new_with_work_in(
        mut requests: [SourceInputReserve<'source>; N],
        completed_work_upper_bound: usize,
        budget: &mut TypeViewBudget<'source, 'control, '_>,
        work: &CompileCheckpoints<'control>,
    ) -> Result<Self, TypeViewError> {
        budget.check(budget.package, work)?;
        budget.admit_input_reserves_in(&mut requests, completed_work_upper_bound)?;
        Ok(Self {
            requests,
            package: budget.package,
            control: work.control(),
            floor: budget.facts(),
        })
    }
    fn check(
        &self,
        budget: &TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        budget.check(self.package, work)?;
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(TypeViewError::InvalidSource(
                "source input prefix uses a different caller control",
            ));
        }
        budget.require_collection_floor(self.floor)
    }
    #[cfg(test)]
    pub(crate) fn extend_in(
        &mut self,
        requests: [SourceInputReserve<'source>; N],
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        self.extend_with_work_in(requests, 0, budget, work)
    }
    pub(crate) fn extend_with_work_in(
        &mut self,
        mut requests: [SourceInputReserve<'source>; N],
        additional_completed_work_upper_bound: usize,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        self.check(budget, work)?;
        let mut delta = (0, 0, 0);
        for (old, new) in self.requests.iter().zip(&requests) {
            if new.admitted_floor.is_some()
                || old.element != new.element
                || old.len != new.len
                || old.capacity != new.capacity
                || old.old_backing != new.old_backing
                || new.additional < old.additional
            {
                return Err(TypeViewError::InvalidSource(
                    "source input prefix changed its original Vec geometry",
                ));
            }
            let before = input_contribution(old.old_backing, old.requested_backing)?;
            let after = input_contribution(new.old_backing, new.requested_backing)?;
            let difference = |a: usize, b: usize| {
                a.checked_sub(b).ok_or(TypeViewError::InvalidSource(
                    "source input prefix contribution decreased",
                ))
            };
            delta.0 = add(delta.0, difference(after.0, before.0)?)?;
            delta.1 = add(delta.1, difference(after.1, before.1)?)?;
            delta.2 = add(delta.2, difference(after.2, before.2)?)?;
        }
        budget.facts.allocation_requests_upper_bound =
            add(budget.facts.allocation_requests_upper_bound, delta.0)?;
        budget.facts.allocation_request_bytes_upper_bound =
            add(budget.facts.allocation_request_bytes_upper_bound, delta.1)?;
        budget.facts.cumulative_work_upper_bound = add(
            budget.facts.cumulative_work_upper_bound,
            add(delta.2, additional_completed_work_upper_bound)?,
        )?;
        budget.update_coexisting_request_bytes()?;
        budget.gate()?;
        for request in &mut requests {
            request.admitted_floor = Some(budget.facts());
            request.admitted_package = Some(budget.package);
        }
        self.requests = requests;
        self.floor = budget.facts();
        Ok(())
    }
    pub(crate) fn finish_in(
        self,
        budget: &TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<[SourceInputReserve<'source>; N], TypeViewError> {
        self.check(budget, work)?;
        Ok(self.requests)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TypeViewLimits {
    pub max_occurrences: usize,
    pub max_value_roots: usize,
    pub max_field_roots: usize,
    pub max_writer_recipes: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TypeViewFacts {
    pub occurrence_count: usize,
    pub value_root_count: usize,
    pub field_root_count: usize,
    pub writer_recipe_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}

/// One child contribution, retained across collection and input loan creation.
/// Prefix facts replace this child's previous snapshot at the real parent;
/// source B is the parent's exact original union, never an independent wallet.
pub(crate) struct TypeViewBudget<'source, 'control, 'parent> {
    package: &'source p::FragmentPackage,
    control: &'control dyn PureCompileControl,
    admission: Option<(usize, TypeViewLimits)>,
    facts: TypeViewFacts,
    admit: &'parent mut dyn FnMut(&TypeViewFacts) -> Result<(), CompileControlError>,
}
impl<'source, 'control, 'parent> TypeViewBudget<'source, 'control, 'parent> {
    #[cfg(test)]
    pub(crate) fn new_in(
        package: &'source p::FragmentPackage,
        source_retained_bytes: usize,
        limits: TypeViewLimits,
        admit: &'parent mut dyn FnMut(&TypeViewFacts) -> Result<(), CompileControlError>,
        work: &CompileCheckpoints<'control>,
    ) -> Result<Self, TypeViewError> {
        if source_retained_bytes < size_of::<p::FragmentPackage>() {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        Self::from_original_in(package, Some((source_retained_bytes, limits)), admit, work)
    }

    /// Collect concrete source loans and their local allocation facts without
    /// pricing the original package. Its existing owner retains that backing.
    pub(crate) fn new_input_derived_in(
        package: &'source p::FragmentPackage,
        admit: &'parent mut dyn FnMut(&TypeViewFacts) -> Result<(), CompileControlError>,
        work: &CompileCheckpoints<'control>,
    ) -> Result<Self, TypeViewError> {
        Self::from_original_in(package, None, admit, work)
    }

    fn from_original_in(
        package: &'source p::FragmentPackage,
        admission: Option<(usize, TypeViewLimits)>,
        admit: &'parent mut dyn FnMut(&TypeViewFacts) -> Result<(), CompileControlError>,
        work: &CompileCheckpoints<'control>,
    ) -> Result<Self, TypeViewError> {
        let mut model = Self {
            package,
            control: work.control(),
            admission,
            facts: TypeViewFacts {
                coexisting_source_and_request_bytes_upper_bound: admission
                    .map_or(0, |(source, _)| source),
                ..TypeViewFacts::default()
            },
            admit,
        };
        model.gate()?;
        Ok(model)
    }
    pub(crate) fn facts(&self) -> TypeViewFacts {
        self.facts
    }
    fn require_collection_floor(&self, floor: TypeViewFacts) -> Result<(), TypeViewError> {
        let current = self.facts;
        if current.occurrence_count < floor.occurrence_count
            || current.value_root_count < floor.value_root_count
            || current.field_root_count < floor.field_root_count
            || current.writer_recipe_count < floor.writer_recipe_count
            || current.allocation_requests_upper_bound < floor.allocation_requests_upper_bound
            || current.allocation_request_bytes_upper_bound
                < floor.allocation_request_bytes_upper_bound
            || current.coexisting_source_and_request_bytes_upper_bound
                < floor.coexisting_source_and_request_bytes_upper_bound
            || current.cumulative_work_upper_bound < floor.cumulative_work_upper_bound
        {
            return Err(TypeViewError::InvalidSource(
                "writer type loans omit the captured collection contribution",
            ));
        }
        Ok(())
    }
    fn gate(&mut self) -> Result<(), TypeViewError> {
        let f = self.facts;
        if let Some((_, limits)) = self.admission
            && (f.occurrence_count > limits.max_occurrences
                || f.value_root_count > limits.max_value_roots
                || f.field_root_count > limits.max_field_roots
                || f.writer_recipe_count > limits.max_writer_recipes
                || f.allocation_requests_upper_bound > limits.max_allocation_requests
                || f.allocation_request_bytes_upper_bound > limits.max_allocation_request_bytes
                || f.coexisting_source_and_request_bytes_upper_bound
                    > limits.max_coexisting_source_and_request_bytes
                || f.cumulative_work_upper_bound > limits.max_work)
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        (self.admit)(&f)?;
        Ok(())
    }
    pub(crate) fn check(
        &self,
        package: &p::FragmentPackage,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        if !std::ptr::eq(self.package, package) || !std::ptr::addr_eq(self.control, work.control())
        {
            return Err(TypeViewError::InvalidSource(
                "type views use a different package or caller control",
            ));
        }
        Ok(())
    }
    fn request(&mut self, facts: VecPushGrowthFacts) -> Result<(), TypeViewError> {
        // Actual grows and future exact batches share one contribution author.
        // These closed loan/ID elements have no arbitrary owned Drop body.
        let (requests, bytes, work) =
            input_contribution(facts.old_backing, facts.requested_backing)?;
        self.facts.allocation_requests_upper_bound =
            add(self.facts.allocation_requests_upper_bound, requests)?;
        self.facts.allocation_request_bytes_upper_bound =
            add(self.facts.allocation_request_bytes_upper_bound, bytes)?;
        self.facts.cumulative_work_upper_bound = add(self.facts.cumulative_work_upper_bound, work)?;
        self.update_coexisting_request_bytes()
    }
    fn update_coexisting_request_bytes(&mut self) -> Result<(), TypeViewError> {
        let source = self.admission.map_or(0, |(source, _)| source);
        self.facts.coexisting_source_and_request_bytes_upper_bound =
            add(source, self.facts.allocation_request_bytes_upper_bound)?;
        Ok(())
    }
    pub(crate) fn before_steps(&mut self, count: usize) -> Result<(), TypeViewError> {
        self.facts.cumulative_work_upper_bound =
            add(self.facts.cumulative_work_upper_bound, count)?;
        self.gate()
    }
    /// Charge all exact buffers captured for this original source header
    /// before the first reserve or completed lookup. No element is copied.
    pub(crate) fn admit_input_reserves_in(
        &mut self,
        requests: &mut [SourceInputReserve<'source>],
        completed_work_upper_bound: usize,
    ) -> Result<(), TypeViewError> {
        for request in requests.iter() {
            if request.admitted_floor.is_some() {
                return Err(TypeViewError::InvalidSource(
                    "source input request is already admitted",
                ));
            }
            self.request(VecPushGrowthFacts {
                next_len: request.next_len,
                requested_capacity: request.next_len.max(request.capacity),
                old_backing: request.old_backing,
                requested_backing: request.requested_backing,
            })?;
        }
        self.before_steps(completed_work_upper_bound)?;
        for request in requests {
            request.admitted_floor = Some(self.facts);
            request.admitted_package = Some(self.package);
        }
        Ok(())
    }
    /// Perform an already admitted exact reserve on the same actual Vec
    /// geometry. Closed borrowed/ID elements stay with their source author.
    pub(crate) fn reserve_input_in<T>(
        &mut self,
        source: &mut Vec<T>,
        captured: SourceInputReserve<'source>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        self.check(self.package, work)?;
        let floor = captured.admitted_floor.ok_or(TypeViewError::InvalidSource(
            "source input reserve was not admitted",
        ))?;
        if !captured
            .admitted_package
            .is_some_and(|package| std::ptr::eq(package, self.package))
        {
            return Err(TypeViewError::InvalidSource(
                "source input reserve belongs to another package",
            ));
        }
        self.require_collection_floor(floor)?;
        let actual = capture_source_input_reserve(source, captured.additional)?;
        if actual.len != captured.len
            || actual.element != captured.element
            || actual.capacity != captured.capacity
            || actual.next_len != captured.next_len
            || actual.old_backing != captured.old_backing
            || actual.requested_backing != captured.requested_backing
        {
            return Err(TypeViewError::InvalidSource(
                "source input Vec changed after its request capture",
            ));
        }
        self.gate()?;
        if captured.requested_backing.is_some() {
            work.flush()?;
            let reserved = source.try_reserve_exact(captured.additional);
            if reserved.is_ok() {
                work.step()?;
            }
            crate::allocation_exit_v2::reserve_exit::<TypeViewError>(reserved, work)?;
        }
        Ok(())
    }
    fn reserve<T>(
        &mut self,
        rows: &mut Vec<T>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        // The real growth geometry has already been added together with all
        // other requests for this capture before any completed callback.
        growth::reserve_for_push_in(rows, &mut |_| self.gate(), work)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TypeRootReference {
    Value(u32),
    Field(u32),
    SourceField(u32),
    /// A Cast borrows only its target carrier. Its ID is the previously
    /// captured expression Value root; the original Cast validator owns equality.
    CastCarrier {
        expression_value_type_id: u32,
    },
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct TypeOccurrenceRow<'source> {
    pub occurrence: PackageTypeOccurrence,
    pub root: TypeRootReference,
    pub source: PackageTypeSource<'source>,
}
struct WriterRoots<'source> {
    node: p::NodeId,
    recipe: &'source ConnectorWriteRecipeDraft,
    field_ids: Range<usize>,
}

/// Movable owned ID/loan buffers, with no references into their own storage.
/// The package objects remain original loans, including each repeated FVT.
pub(crate) struct PackageTypeViews<'source> {
    package: &'source p::FragmentPackage,
    control: &'source dyn PureCompileControl,
    collection_floor: TypeViewFacts,
    occurrences: Vec<TypeOccurrenceRow<'source>>,
    values: Vec<(u32, &'source p::ValueType)>,
    fields: Vec<BorrowedFieldTypeRoot<'source>>,
    source_fields: Vec<(u32, &'source Arc<Field>)>,
    writer_value_ids: Vec<u32>,
    writer_field_ids: Vec<u32>,
    writer_roots: Vec<WriterRoots<'source>>,
}
impl<'source> PackageTypeViews<'source> {
    #[cfg(test)]
    pub(crate) fn occurrences(&self) -> &[TypeOccurrenceRow<'source>] {
        &self.occurrences
    }
    pub(crate) fn values(&self) -> &[(u32, &'source p::ValueType)] {
        &self.values
    }
    pub(crate) fn fields(&self) -> &[BorrowedFieldTypeRoot<'source>] {
        &self.fields
    }
    pub(crate) fn source_fields(&self) -> &[(u32, &'source Arc<Field>)] {
        &self.source_fields
    }
    pub(crate) fn source_field_root_for_in(
        &self,
        occurrence: PackageTypeOccurrence,
        original: &Arc<Field>,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, TypeViewError> {
        self.root_for_in(occurrence, budget, work, |row| {
            match (row.root, row.source) {
                (TypeRootReference::SourceField(id), PackageTypeSource::SourceField(field))
                    if std::ptr::eq(field, original) =>
                {
                    Ok(id)
                }
                _ => Err(TypeViewError::InvalidSource(
                    "source field occurrence differs from its original source owner",
                )),
            }
        })
    }
    pub(crate) fn check_package_in(
        &self,
        package: &p::FragmentPackage,
        budget: &TypeViewBudget<'source, '_, '_>,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        budget.check(package, work)?;
        if !std::ptr::eq(self.package, package) || !std::ptr::addr_eq(self.control, work.control())
        {
            return Err(TypeViewError::InvalidSource(
                "type source views belong to another package or caller control",
            ));
        }
        budget.require_collection_floor(self.collection_floor)
    }
    /// Find a full root by the exact stored occurrence and original FVT
    /// address. Equal types and repeated pointers cannot choose another ID.
    /// Lookup comparisons acquire their whole numerical contribution before
    /// the first completed observation, without an index or dense-ID scratch.
    pub(crate) fn value_root_for_in(
        &self,
        occurrence: PackageTypeOccurrence,
        expected_original: &p::ValueType,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, TypeViewError> {
        self.root_for_in(occurrence, budget, work, |row| {
            let (TypeRootReference::Value(id), PackageTypeSource::Value(original)) =
                (row.root, row.source)
            else {
                return Err(TypeViewError::InvalidSource(
                    "type occurrence is not a complete Value root",
                ));
            };
            if !std::ptr::eq(original, expected_original) {
                return Err(TypeViewError::InvalidSource(
                    "type occurrence uses a different original Value type",
                ));
            }
            Ok(id)
        })
    }
    /// Schema inputs retain the exact original Arc loan and strict Field root.
    /// An equal Field or another occurrence cannot select this namespace ID.
    pub(crate) fn field_root_for_in(
        &self,
        occurrence: PackageTypeOccurrence,
        expected_original: &Arc<Field>,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, TypeViewError> {
        self.root_for_in(occurrence, budget, work, |row| {
            let original = match row.source {
                PackageTypeSource::StrictField(field)
                | PackageTypeSource::ExecutionField { field, .. } => field,
                _ => {
                    return Err(TypeViewError::InvalidSource(
                        "type occurrence is not a Field root",
                    ));
                }
            };
            let TypeRootReference::Field(id) = row.root else {
                return Err(TypeViewError::InvalidSource(
                    "type occurrence is not a strict Field root",
                ));
            };
            if !std::ptr::eq(original, expected_original) {
                return Err(TypeViewError::InvalidSource(
                    "type occurrence uses a different original Field loan",
                ));
            }
            Ok(id)
        })
    }
    fn root_for_in(
        &self,
        occurrence: PackageTypeOccurrence,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
        validate: impl Fn(&TypeOccurrenceRow<'source>) -> Result<u32, TypeViewError>,
    ) -> Result<u32, TypeViewError> {
        self.check_package_in(self.package, budget, work)?;
        budget.before_steps(mul(self.occurrences.len(), 2)?)?;
        let mut found = None;
        for row in &self.occurrences {
            let matches = row.occurrence == occurrence;
            work.step()?;
            if matches {
                let id = validate(row)?;
                if found.replace(id).is_some() {
                    return Err(TypeViewError::InvalidSource(
                        "type occurrence root is ambiguous",
                    ));
                }
                work.step()?;
            }
        }
        found.ok_or(TypeViewError::InvalidSource(
            "type occurrence root is absent",
        ))
    }
    /// A separate owned input Vec borrows stable ID slices from these views.
    /// It is a local compose input, never stored back in its borrowed owner.
    pub(crate) fn writer_sources_in<'views>(
        &'views self,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<WriterTypeSource<'views>>, TypeViewError> {
        budget.check(self.package, work)?;
        budget.require_collection_floor(self.collection_floor)?;
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(TypeViewError::InvalidSource(
                "writer type loans use a different caller control",
            ));
        }
        let mut sources = Vec::new();
        let mut position = 0;
        for (node, recipe) in self.package.writes() {
            // Header comparisons and the loan-buffer push are known before
            // the first completed observation for this input.
            let request = growth::push_growth(&sources)?;
            budget.request(request)?;
            budget.before_steps(4)?;
            let range = if let Some(group) = self.writer_roots.get(position)
                && group.node == *node
            {
                if !std::ptr::eq(group.recipe, recipe)
                    || group.field_ids.len() != recipe.input().field_count()
                {
                    return Err(TypeViewError::InvalidSource(
                        "writer type occurrence group differs from its original recipe",
                    ));
                }
                position += 1;
                group.field_ids.clone()
            } else {
                if recipe.input().field_count() != 0 {
                    return Err(TypeViewError::InvalidSource(
                        "writer type occurrence group is absent",
                    ));
                }
                0..0
            };
            work.step()?;
            budget.reserve(&mut sources, work)?;
            sources.push(WriterTypeSource::new_with_value_ids(
                recipe,
                &self.writer_field_ids[range.clone()],
                &self.writer_value_ids[range],
            ));
            work.step()?;
        }
        budget.before_steps(1)?;
        let complete = position == self.writer_roots.len();
        work.step()?;
        if !complete {
            return Err(TypeViewError::InvalidSource(
                "writer type occurrence group is unconsumed",
            ));
        }
        Ok(sources)
    }
}

struct Capture<'source, 'budget, 'control, 'parent> {
    views: PackageTypeViews<'source>,
    budget: &'budget mut TypeViewBudget<'source, 'control, 'parent>,
    next_field_id: usize,
}
impl<'source> PackageTypeSourceSink<'source, TypeViewError> for Capture<'source, '_, '_, '_> {
    fn capture(
        &mut self,
        occurrence: PackageTypeOccurrence,
        source: PackageTypeSource<'source>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        let row_growth = growth::push_growth(&self.views.occurrences)?;
        self.budget.request(row_growth)?;
        self.budget.facts.occurrence_count = row_growth.next_len;
        let root;
        match source {
            PackageTypeSource::MissingReadSchema => {
                return Err(TypeViewError::InvalidSource(
                    "frozen read must provide complete execution schema and independent source facts",
                ));
            }
            PackageTypeSource::Value(value) => {
                let request = growth::push_growth(&self.views.values)?;
                root = TypeRootReference::Value(id(self.views.values.len())?);
                self.budget.request(request)?;
                self.budget.facts.value_root_count = request.next_len;
                self.budget.before_steps(2)?;
                self.budget.reserve(&mut self.views.occurrences, work)?;
                self.budget.reserve(&mut self.views.values, work)?;
                self.views
                    .values
                    .push((id(self.views.values.len())?, value));
            }
            PackageTypeSource::SourceField(field) => {
                let request = growth::push_growth(&self.views.source_fields)?;
                self.budget.request(request)?;
                self.budget.facts.field_root_count = add(self.budget.facts.field_root_count, 1)?;
                let field_id = id(self.next_field_id)?;
                root = TypeRootReference::SourceField(field_id);
                self.budget.before_steps(2)?;
                self.budget.reserve(&mut self.views.occurrences, work)?;
                self.budget.reserve(&mut self.views.source_fields, work)?;
                self.views.source_fields.push((field_id, field));
                self.next_field_id = add(self.next_field_id, 1)?;
            }
            PackageTypeSource::StrictField(field)
            | PackageTypeSource::ExecutionField { field, .. } => {
                let value_type_id =
                    match source {
                        PackageTypeSource::ExecutionField { value, .. } => {
                            let previous = self.views.occurrences.last().ok_or(
                                TypeViewError::InvalidSource(
                                    "execution field has no preceding Value root",
                                ),
                            )?;
                            let (
                                TypeRootReference::Value(value_id),
                                PackageTypeSource::Value(original),
                            ) = (previous.root, previous.source)
                            else {
                                return Err(TypeViewError::InvalidSource(
                                    "execution field Value root is absent",
                                ));
                            };
                            let PackageTypeChannel::Field(ordinal) = occurrence.channel else {
                                return Err(TypeViewError::InvalidSource(
                                    "execution field has no occurrence ordinal",
                                ));
                            };
                            if previous.occurrence.owner != occurrence.owner
                                || previous.occurrence.channel
                                    != PackageTypeChannel::ScanExecutionValue(ordinal)
                                || !std::ptr::eq(original, value)
                            {
                                return Err(TypeViewError::InvalidSource(
                                    "execution field uses another Value occurrence",
                                ));
                            }
                            Some(value_id)
                        }
                        _ => None,
                    };
                let request = growth::push_growth(&self.views.fields)?;
                let field_id = id(self.next_field_id)?;
                root = TypeRootReference::Field(field_id);
                self.budget.request(request)?;
                self.budget.facts.field_root_count = add(self.budget.facts.field_root_count, 1)?;
                self.budget.before_steps(2)?;
                self.budget.reserve(&mut self.views.occurrences, work)?;
                self.budget.reserve(&mut self.views.fields, work)?;
                self.views.fields.push(BorrowedFieldTypeRoot {
                    id: field_id,
                    field,
                    value_type_id,
                });
                self.next_field_id = add(self.next_field_id, 1)?;
            }
            PackageTypeSource::Carrier => {
                let previous =
                    self.views
                        .occurrences
                        .last()
                        .ok_or(TypeViewError::InvalidSource(
                            "Cast expression Value root is absent",
                        ))?;
                let TypeRootReference::Value(expression_value_type_id) = previous.root else {
                    return Err(TypeViewError::InvalidSource(
                        "Cast expression root is not a complete Value type",
                    ));
                };
                if previous.occurrence.owner != occurrence.owner
                    || previous.occurrence.channel != PackageTypeChannel::Value
                    || !matches!(occurrence.owner, PackageTypeOwner::Expression { .. })
                    || occurrence.channel != PackageTypeChannel::CastTarget
                {
                    return Err(TypeViewError::InvalidSource(
                        "Cast target is not adjacent to its original expression Value root",
                    ));
                }
                root = TypeRootReference::CastCarrier {
                    expression_value_type_id,
                };
                self.budget.before_steps(2)?;
                self.budget.reserve(&mut self.views.occurrences, work)?;
            }
            PackageTypeSource::WriterField {
                recipe,
                role,
                ordinal,
                binding,
            } => {
                if occurrence.channel != (PackageTypeChannel::WriterField { role, ordinal }) {
                    return Err(TypeViewError::InvalidSource(
                        "writer field coordinates differ from its type occurrence",
                    ));
                }
                let PackageTypeOwner::Recipe(node) = occurrence.owner else {
                    return Err(TypeViewError::InvalidSource(
                        "writer field has no original recipe owner",
                    ));
                };
                let field_id = id(self.next_field_id)?;
                root = TypeRootReference::SourceField(field_id);
                let execution = binding
                    .execution_type()
                    .ok_or(TypeViewError::InvalidSource(
                        "writer source lacks its frozen complete execution binding",
                    ))?;
                let value_id = id(self.views.values.len())?;
                self.budget
                    .request(growth::push_growth(&self.views.values)?)?;
                self.budget
                    .request(growth::push_growth(&self.views.writer_value_ids)?)?;
                self.budget.facts.value_root_count = add(self.budget.facts.value_root_count, 1)?;
                let is_new = self
                    .views
                    .writer_roots
                    .last()
                    .is_none_or(|group| group.node != node);
                let ids_growth = growth::push_growth(&self.views.writer_field_ids)?;
                self.budget.request(ids_growth)?;
                if is_new {
                    let groups_growth = growth::push_growth(&self.views.writer_roots)?;
                    self.budget.request(groups_growth)?;
                }
                self.budget.facts.field_root_count = add(self.budget.facts.field_root_count, 1)?;
                // The full checked writes map includes empty shapes as well;
                // reserve its original namespace cardinality once, no guessed count.
                self.budget.facts.writer_recipe_count = self.views.package.writes().len();
                self.budget.before_steps(6)?;
                self.budget.reserve(&mut self.views.occurrences, work)?;
                self.budget
                    .reserve(&mut self.views.writer_field_ids, work)?;
                self.budget.reserve(&mut self.views.values, work)?;
                self.budget
                    .reserve(&mut self.views.writer_value_ids, work)?;
                self.views.values.push((value_id, execution));
                self.views.writer_value_ids.push(value_id);
                if is_new {
                    self.budget.reserve(&mut self.views.writer_roots, work)?;
                    let begin = self.views.writer_field_ids.len();
                    self.views.writer_roots.push(WriterRoots {
                        node,
                        recipe,
                        field_ids: begin..begin,
                    });
                }
                let group =
                    self.views
                        .writer_roots
                        .last_mut()
                        .ok_or(TypeViewError::InvalidSource(
                            "writer group was not captured",
                        ))?;
                if !std::ptr::eq(group.recipe, recipe) {
                    return Err(TypeViewError::InvalidSource(
                        "writer field changed its original recipe owner",
                    ));
                }
                self.views.writer_field_ids.push(field_id);
                group.field_ids.end = self.views.writer_field_ids.len();
                self.next_field_id = add(self.next_field_id, 1)?;
            }
        }
        self.views.occurrences.push(TypeOccurrenceRow {
            occurrence,
            root,
            source,
        });
        work.step()?;
        Ok(())
    }
    fn before_completed(&mut self) -> Result<(), TypeViewError> {
        self.budget.before_steps(1)
    }
}

pub(crate) fn collect_package_type_views_in<'source>(
    budget: &mut TypeViewBudget<'source, 'source, '_>,
    work: &mut CompileCheckpoints<'source>,
) -> Result<PackageTypeViews<'source>, TypeViewError> {
    budget.check(budget.package, work)?;
    // This full namespace count is known even for zero-field writer recipes,
    // before the first namespace or root is observed by the original visitor.
    budget.facts.writer_recipe_count = budget.package.writes().len();
    budget.gate()?;
    let views = PackageTypeViews {
        package: budget.package,
        control: work.control(),
        collection_floor: TypeViewFacts::default(),
        occurrences: Vec::new(),
        values: Vec::new(),
        fields: Vec::new(),
        source_fields: Vec::new(),
        writer_value_ids: Vec::new(),
        writer_field_ids: Vec::new(),
        writer_roots: Vec::new(),
    };
    let package = budget.package;
    let mut capture = Capture {
        views,
        budget,
        next_field_id: 0,
    };
    visit_package_type_sources_admitted_in(package, &mut capture, work)?;
    capture.views.collection_floor = capture.budget.facts();
    Ok(capture.views)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_type_v2::{
        PackageTypeProjectionLimits, WriterTypeRootRole, encode_borrowed_type_namespaces_sources_in,
    };
    use arrow::datatypes::DataType;
    use novarocks_connector_contract as c;
    use novarocks_type_contract::CompilePhase;
    use std::{collections::HashMap, sync::Mutex};

    // The bounded fresh original fixture and its new view loans fit this
    // conservative invoice. It is not private allocator introspection or MEM.
    const SOURCE: usize = 128 * 1024 * 1024;
    const CAUSES: [CompileControlError; 3] = [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ];
    #[derive(Default)]
    struct Control {
        events: Mutex<Vec<u32>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Encode);
            let mut events = self.events.lock().unwrap();
            let at = events.len();
            if let Some((stop, _)) = self.stop {
                assert!(at <= stop, "callback after refusal");
            }
            events.push(units);
            if let Some((stop, cause)) = self.stop.clone()
                && at == stop
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn limits() -> TypeViewLimits {
        TypeViewLimits {
            max_occurrences: 1000,
            max_value_roots: 1000,
            max_field_roots: 1000,
            max_writer_recipes: 100,
            max_allocation_requests: 1000,
            max_allocation_request_bytes: 1024 * 1024,
            max_coexisting_source_and_request_bytes: SOURCE + 1024 * 1024,
            max_work: 16 * 1024 * 1024,
        }
    }
    fn package() -> p::FragmentPackage {
        let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let catalog =
            c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
        let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
        let binding = c::ConnectorWriteBinding::new(
            c::ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        );
        let payload = c::ConnectorEncodedPayload::new(
            c::ConnectorEnvelopeHeader::new(
                provider,
                catalog,
                c::ConnectorCodecCategory::WriteHandle,
                c::ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![7u8].into(),
        );
        let recipe = c::ConnectorWriteRecipeDraft::try_new(
            binding,
            payload,
            c::ConnectorWriteInputShape::Data {
                fields: vec![
                    c::ConnectorWriteFieldBinding::new(
                        c::ConnectorWriteFieldToken::from_bytes([1; 32]),
                        Field::new("v", DataType::Int64, false).with_metadata(HashMap::from([
                            ("large".into(), "雪".repeat(6826) + "ab"),
                            ("embedded".into(), "a\0b".into()),
                        ])),
                    )
                    .with_execution_type(novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ))
                    .expect("explicit BIGINT writer declaration"),
                ],
            },
        )
        .unwrap();
        crate::physical_type_v2::sender_tests::checked_writer_package(recipe)
    }
    #[test]
    fn input_derived_collectors_preserve_the_original_package_and_local_requests() {
        use super::super::{
            binding_sources::collect_binding_sources_in,
            definition_sources::collect_definition_sources_in,
            provider_sources::collect_provider_sources_in,
        };

        let package = package();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut previous = TypeViewFacts::default();
        let mut admit = |next: &TypeViewFacts| {
            assert!(
                next.allocation_request_bytes_upper_bound
                    >= previous.allocation_request_bytes_upper_bound
            );
            assert_eq!(
                next.coexisting_source_and_request_bytes_upper_bound,
                next.allocation_request_bytes_upper_bound
            );
            previous = *next;
            Ok(())
        };
        let mut budget = TypeViewBudget::new_input_derived_in(&package, &mut admit, &work).unwrap();
        let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
        let writer_types = views.writer_sources_in(&mut budget, &mut work).unwrap();
        let bindings =
            collect_binding_sources_in(&package, &views, None, &mut budget, &mut work).unwrap();
        let providers = collect_provider_sources_in(
            &package,
            &views,
            &writer_types,
            None,
            &mut budget,
            &mut work,
        )
        .unwrap();
        let definitions = collect_definition_sources_in(
            &package,
            &views,
            &bindings,
            None,
            &mut budget,
            &mut work,
        )
        .unwrap();
        assert_eq!(
            definitions.values().len(),
            package.fragment().values().len()
        );
        assert_eq!(
            providers
                .prepare_inputs_in(&mut budget, &mut work)
                .unwrap()
                .writers()
                .len(),
            package.writes().len(),
        );
        assert!(
            budget.facts().allocation_request_bytes_upper_bound
                >= size_of::<TypeOccurrenceRow<'_>>() * views.occurrences().len()
        );
        work.finish().unwrap();
    }

    #[test]
    fn occurrence_lookup_retains_independent_ids_and_refuses_equal_foreign_types_and_field_roots() {
        let package = package();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut admit = |_: &TypeViewFacts| Ok(());
        let mut budget = TypeViewBudget::new_input_derived_in(&package, &mut admit, &work).unwrap();
        let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
        for row in views.occurrences() {
            if let (TypeRootReference::Value(id), PackageTypeSource::Value(original)) =
                (row.root, row.source)
            {
                assert_eq!(
                    views
                        .value_root_for_in(row.occurrence, original, &mut budget, &mut work)
                        .unwrap(),
                    id
                );
                let equal_copy = original.clone();
                assert!(matches!(
                    views.value_root_for_in(row.occurrence, &equal_copy, &mut budget, &mut work),
                    Err(TypeViewError::InvalidSource(_))
                ));
            }
        }
        let first = &views.occurrences()[0];
        let PackageTypeSource::Value(original) = first.source else {
            panic!("original Value");
        };
        let field = views.occurrences().last().unwrap();
        assert!(matches!(
            views.value_root_for_in(field.occurrence, original, &mut budget, &mut work),
            Err(TypeViewError::InvalidSource(_))
        ));
        let mut missing = first.occurrence;
        missing.fragment = p::FragmentId::new(u32::MAX);
        assert!(matches!(
            views.value_root_for_in(missing, original, &mut budget, &mut work),
            Err(TypeViewError::InvalidSource(_))
        ));
        let foreign = package.clone();
        assert!(matches!(
            views.check_package_in(&foreign, &budget, &work),
            Err(TypeViewError::InvalidSource(_))
        ));
        work.finish().unwrap();
    }
    #[test]
    fn occurrence_lookup_all_actual_callbacks_keep_primary_causes_and_known_work_precedes_pending_copy()
     {
        let package = package();
        let run = |stop| {
            let control = Control {
                stop,
                ..Default::default()
            };
            let result = (|| {
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
                let mut admit = |_: &TypeViewFacts| Ok(());
                let mut budget =
                    TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work)?;
                let views = collect_package_type_views_in(&mut budget, &mut work)?;
                for row in views.occurrences() {
                    if let PackageTypeSource::Value(original) = row.source {
                        views.value_root_for_in(
                            row.occurrence,
                            original,
                            &mut budget,
                            &mut work,
                        )?;
                    }
                }
                work.finish()?;
                Ok::<_, TypeViewError>(())
            })();
            (result, control.events.into_inner().unwrap())
        };
        let (result, trace) = run(None);
        assert!(result.is_ok());
        for at in 0..trace.len() {
            for cause in CAUSES {
                let (result, actual) = run(Some((at, cause.clone())));
                assert!(matches!(result, Err(TypeViewError::Control(actual)) if actual == cause));
                assert_eq!(actual, trace[..=at]);
            }
        }
        let prelude = |control: &Control, reject: bool| {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode).unwrap();
            let mut admit = |_: &TypeViewFacts| Ok(());
            let mut budget =
                TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work).unwrap();
            let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
            work.flush().unwrap();
            let spelling = "x".repeat(255);
            assert_eq!(
                novarocks_type_contract::owned_resources::copy::copy_string::<CompileControlError>(
                    &spelling, &mut work
                )
                .unwrap(),
                spelling
            );
            let prefix = control.events.lock().unwrap().clone();
            if reject {
                // A real lookup contributes a full known linear comparison
                // upper bound before its first completed observation.
                budget.admission.as_mut().unwrap().1.max_work =
                    budget.facts().cumulative_work_upper_bound + views.occurrences().len() * 2 - 1;
                let first = &views.occurrences()[0];
                let PackageTypeSource::Value(original) = first.source else {
                    panic!("original Value");
                };
                assert!(matches!(
                    views.value_root_for_in(first.occurrence, original, &mut budget, &mut work),
                    Err(TypeViewError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(control.events.lock().unwrap().as_slice(), prefix);
            }
            prefix
        };
        let prefix = prelude(&Control::default(), false);
        for cause in CAUSES {
            let actual = prelude(
                &Control {
                    stop: Some((prefix.len(), cause)),
                    ..Default::default()
                },
                true,
            );
            assert_eq!(actual, prefix);
        }
    }
    #[test]
    fn exact_input_batch_requires_admission_and_original_budget_control_and_geometry() {
        let package = package();
        let control = Control::default();
        let foreign_control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut admit = |_: &TypeViewFacts| Ok(());
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work).unwrap();
        let mut ids: Vec<u32> = Vec::new();
        let mut names = vec![1u8, 2];
        let mut requests = [
            capture_source_input_reserve(&ids, 3).unwrap(),
            capture_source_input_reserve(&names, 5).unwrap(),
        ];
        let initial = budget.facts();
        assert!(matches!(
            budget.reserve_input_in(&mut ids, requests[0], &mut work),
            Err(TypeViewError::InvalidSource(_))
        ));
        assert_eq!(budget.facts(), initial);
        assert_eq!(ids.capacity(), 0);
        assert_eq!(control.events.lock().unwrap().as_slice(), &[0]);
        let exact_bytes = std::alloc::Layout::array::<u32>(3).unwrap().size()
            + std::alloc::Layout::array::<u8>(7).unwrap().size();
        budget.admit_input_reserves_in(&mut requests, 12).unwrap();
        assert_eq!(budget.facts().allocation_requests_upper_bound, 2);
        assert_eq!(
            budget.facts().allocation_request_bytes_upper_bound,
            exact_bytes
        );
        assert!(matches!(
            budget.admit_input_reserves_in(&mut requests, 12),
            Err(TypeViewError::InvalidSource(_))
        ));
        let mut foreign_work =
            CompileCheckpoints::try_new(&foreign_control, CompilePhase::Encode).unwrap();
        assert!(matches!(
            budget.reserve_input_in(&mut ids, requests[0], &mut foreign_work),
            Err(TypeViewError::InvalidSource(_))
        ));
        assert_eq!(foreign_control.events.lock().unwrap().as_slice(), &[0]);
        let mut reset_admit = |_: &TypeViewFacts| Ok(());
        let mut reset =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut reset_admit, &work).unwrap();
        assert!(matches!(
            reset.reserve_input_in(&mut ids, requests[0], &mut work),
            Err(TypeViewError::InvalidSource(_))
        ));
        budget
            .reserve_input_in(&mut ids, requests[0], &mut work)
            .unwrap();
        assert!(ids.capacity() >= 3);
        // Actual next contents remain owned by the same buffer author.
        ids.extend([4, 5, 6]);
        names.push(3);
        let prefix = control.events.lock().unwrap().clone();
        assert!(matches!(
            budget.reserve_input_in(&mut names, requests[1], &mut work),
            Err(TypeViewError::InvalidSource(_))
        ));
        assert_eq!(control.events.lock().unwrap().as_slice(), prefix);
        assert_eq!(ids, [4, 5, 6]);
        let mut empty: Vec<u64> = Vec::new();
        let mut zero = [capture_source_input_reserve(&empty, 0).unwrap()];
        let before = budget.facts();
        budget.admit_input_reserves_in(&mut zero, 1).unwrap();
        budget
            .reserve_input_in(&mut empty, zero[0], &mut work)
            .unwrap();
        assert_eq!(
            budget.facts().allocation_requests_upper_bound,
            before.allocation_requests_upper_bound
        );
        assert_eq!(empty.capacity(), 0);
        work.finish().unwrap();
    }
    #[test]
    fn future_exact_inputs_keep_one_final_request_and_refuse_changed_source_geometry() {
        let package = package();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut admit = |_: &TypeViewFacts| Ok(());
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work).unwrap();
        let mut ids: Vec<u32> = Vec::new();
        let mut names = vec![1u8, 2];
        let old_bytes = Layout::array::<u8>(names.capacity()).unwrap().size();
        let mut prefix = SourceInputPrefix::new_in(
            [
                capture_source_input_reserve(&ids, 0).unwrap(),
                capture_source_input_reserve(&names, 0).unwrap(),
            ],
            &mut budget,
            &work,
        )
        .unwrap();
        assert_eq!(budget.facts().allocation_requests_upper_bound, 0);
        prefix
            .extend_in(
                [
                    capture_source_input_reserve(&ids, 1).unwrap(),
                    capture_source_input_reserve(&names, 1).unwrap(),
                ],
                &mut budget,
                &work,
            )
            .unwrap();
        assert_eq!(budget.facts().allocation_requests_upper_bound, 2);
        prefix
            .extend_in(
                [
                    capture_source_input_reserve(&ids, 320).unwrap(),
                    capture_source_input_reserve(&names, 5).unwrap(),
                ],
                &mut budget,
                &work,
            )
            .unwrap();
        let bytes =
            Layout::array::<u32>(320).unwrap().size() + Layout::array::<u8>(7).unwrap().size();
        let expected = TypeViewFacts {
            allocation_requests_upper_bound: 2,
            allocation_request_bytes_upper_bound: bytes,
            coexisting_source_and_request_bytes_upper_bound: SOURCE + bytes,
            cumulative_work_upper_bound: 2 * 128 + 4 * (bytes + old_bytes),
            ..Default::default()
        };
        assert_eq!(budget.facts(), expected);
        let original_trace = control.events.lock().unwrap().clone();
        assert!(matches!(
            prefix.extend_in(
                [
                    capture_source_input_reserve(&ids, 319).unwrap(),
                    capture_source_input_reserve(&names, 5).unwrap(),
                ],
                &mut budget,
                &work,
            ),
            Err(TypeViewError::InvalidSource(_))
        ));
        let mut reset_admit = |_: &TypeViewFacts| Ok(());
        let reset =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut reset_admit, &work).unwrap();
        assert!(matches!(
            prefix.check(&reset, &work),
            Err(TypeViewError::InvalidSource(_))
        ));
        let foreign_control = Control::default();
        let foreign_work =
            CompileCheckpoints::try_new(&foreign_control, CompilePhase::Encode).unwrap();
        assert!(matches!(
            prefix.check(&budget, &foreign_work),
            Err(TypeViewError::InvalidSource(_))
        ));
        assert_eq!(control.events.lock().unwrap().as_slice(), original_trace);
        let final_requests = prefix.finish_in(&budget, &work).unwrap();
        budget
            .reserve_input_in(&mut ids, final_requests[0], &mut work)
            .unwrap();
        budget
            .reserve_input_in(&mut names, final_requests[1], &mut work)
            .unwrap();
        assert!(ids.capacity() >= 320);
        assert!(names.capacity() >= 7);
        assert_eq!(
            budget.facts(),
            expected,
            "final reserve must not bill again"
        );
        let mut changed = SourceInputPrefix::new_in(
            [capture_source_input_reserve(&names, 0).unwrap()],
            &mut budget,
            &work,
        )
        .unwrap();
        names.push(3);
        let before = control.events.lock().unwrap().clone();
        assert!(matches!(
            changed.extend_in(
                [capture_source_input_reserve(&names, 1).unwrap()],
                &mut budget,
                &work,
            ),
            Err(TypeViewError::InvalidSource(_))
        ));
        assert_eq!(budget.facts(), expected);
        assert_eq!(control.events.lock().unwrap().as_slice(), before);
        work.finish().unwrap();
    }
    #[test]
    fn exact_batch_and_future_prefix_gate_known_growth_before_pending_copy_control() {
        let package = package();
        let run = |stop: Option<(usize, CompileControlError)>, axis: Option<usize>, future| {
            let control = Control {
                stop: stop.clone(),
                ..Default::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let request_hooks = std::cell::Cell::new(0);
            let mut admit = |facts: &TypeViewFacts| {
                if facts.allocation_requests_upper_bound > 0 {
                    request_hooks.set(request_hooks.get() + 1);
                    if let Some((_, cause)) = &stop {
                        return Err(cause.clone());
                    }
                }
                Ok(())
            };
            let mut budget =
                TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work).unwrap();
            let ids: Vec<u32> = Vec::new();
            let names: Vec<u8> = Vec::new();
            let mut prefix = SourceInputPrefix::new_in(
                [
                    capture_source_input_reserve(&ids, 0).unwrap(),
                    capture_source_input_reserve(&names, 0).unwrap(),
                ],
                &mut budget,
                &work,
            )
            .unwrap();
            let text = "x".repeat(255);
            assert_eq!(
                novarocks_type_contract::owned_resources::copy::copy_string::<CompileControlError>(
                    &text, &mut work,
                )
                .unwrap(),
                text
            );
            let trace = control.events.lock().unwrap().clone();
            if let Some(axis) = axis {
                let bytes = Layout::array::<u32>(3).unwrap().size()
                    + Layout::array::<u8>(5).unwrap().size();
                match axis {
                    0 => budget.admission.as_mut().unwrap().1.max_allocation_requests = 1,
                    1 => {
                        budget
                            .admission
                            .as_mut()
                            .unwrap()
                            .1
                            .max_allocation_request_bytes = bytes - 1
                    }
                    2 => {
                        budget
                            .admission
                            .as_mut()
                            .unwrap()
                            .1
                            .max_coexisting_source_and_request_bytes = SOURCE + bytes - 1
                    }
                    3 => {
                        budget.admission.as_mut().unwrap().1.max_work = 2 * 128 + 4 * bytes + 11 - 1
                    }
                    _ => unreachable!(),
                }
                let mut requests = [
                    capture_source_input_reserve(&ids, 3).unwrap(),
                    capture_source_input_reserve(&names, 5).unwrap(),
                ];
                let result = if future {
                    prefix.extend_with_work_in(requests, 11, &mut budget, &work)
                } else {
                    budget.admit_input_reserves_in(&mut requests, 11)
                };
                assert!(matches!(
                    result,
                    Err(TypeViewError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(ids.capacity(), 0);
                assert_eq!(names.capacity(), 0);
                assert_eq!(
                    request_hooks.get(),
                    0,
                    "known work must precede the parent hook too"
                );
                assert_eq!(control.events.lock().unwrap().as_slice(), trace);
            }
            trace
        };
        let trace = run(None, None, false);
        for future in [false, true] {
            for axis in 0..4 {
                for cause in CAUSES {
                    assert_eq!(run(Some((trace.len(), cause)), Some(axis), future), trace);
                }
            }
        }
    }
    #[test]
    fn checked_package_views_feed_original_borrowed_type_emitter_without_rebuilding_roots() {
        let package = package();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut prefix = TypeViewFacts::default();
        let mut admit = |facts: &TypeViewFacts| {
            prefix = *facts;
            Ok(())
        };
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work).unwrap();
        let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
        let writers = views.writer_sources_in(&mut budget, &mut work).unwrap();
        assert_eq!(
            (
                views.occurrences().len(),
                views.values().len(),
                views.fields().len(),
                writers.len()
            ),
            (27, 26, 0, 1)
        );
        assert_eq!(
            (
                budget.facts().occurrence_count,
                budget.facts().value_root_count,
                budget.facts().field_root_count
            ),
            (27, 26, 2)
        );
        let encoded = encode_borrowed_type_namespaces_sources_in(
            views.values(),
            views.fields(),
            views.source_fields(),
            &writers,
            SOURCE,
            PackageTypeProjectionLimits {
                max_definitions: 100_000,
                max_expanded_nodes: 1_000_000,
                max_string_bytes: 64 * 1024 * 1024,
                max_allocation_requests: 1_000_000,
                max_allocation_request_bytes: 512 * 1024 * 1024,
                max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
                max_work: usize::MAX / 4,
            },
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        assert_eq!(
            (
                encoded.as_wire().value_types.len(),
                encoded.as_wire().fields.len()
            ),
            (26, 0)
        );
        for row in views.occurrences() {
            match (row.root, row.source) {
                (TypeRootReference::Value(id), PackageTypeSource::Value(original)) => {
                    assert!(std::ptr::eq(
                        encoded.value_type_observed(id, &mut work).unwrap().unwrap(),
                        original
                    ));
                }
                (TypeRootReference::SourceField(id), PackageTypeSource::SourceField(field)) => {
                    let PackageTypeOwner::Constant(pool_id) = row.occurrence.owner else {
                        panic!("original constant Field owner");
                    };
                    assert_eq!(row.occurrence.channel, PackageTypeChannel::ConstantField);
                    assert!(std::ptr::eq(
                        field,
                        package.constants().entries()[&pool_id].field_ref()
                    ));
                    assert!(std::ptr::eq(
                        encoded
                            .source_field_source_observed(id, &mut work)
                            .unwrap()
                            .unwrap(),
                        field.as_ref()
                    ));
                }
                (
                    TypeRootReference::SourceField(id),
                    PackageTypeSource::WriterField { binding, .. },
                ) => {
                    assert!(std::ptr::eq(
                        encoded
                            .source_field_source_observed(id, &mut work)
                            .unwrap()
                            .unwrap(),
                        binding.field()
                    ));
                }
                _ => panic!("unexpected root in original Data fixture"),
            }
        }
        for (value_id, binding) in views.writer_value_ids.iter().zip(
            package
                .writes()
                .values()
                .flat_map(|recipe| recipe.input().fields_iter()),
        ) {
            assert!(std::ptr::eq(
                encoded
                    .value_type_observed(*value_id, &mut work)
                    .unwrap()
                    .unwrap(),
                binding
                    .execution_type()
                    .expect("original complete writer declaration"),
            ));
        }
        let source_wire = encoded
            .as_source_wire()
            .expect("independent writer Source facts");
        assert_eq!(source_wire.field_facts.len(), 2);
        assert_eq!(source_wire.field_facts[0].field_id, Some(0));
        assert!(source_wire.field_facts[0].metadata.is_empty());
        assert_eq!(source_wire.field_facts[1].field_id, Some(1));
        assert_eq!(
            source_wire.field_facts[1].metadata[1].value.len(),
            20 * 1024
        );
        work.finish().unwrap();
        assert!(budget.facts().allocation_requests_upper_bound > 0);
    }
    #[test]
    fn views_every_actual_callback_returns_primary_control_and_source_guard_precedes_parent() {
        let package = package();
        let run = |stop| {
            let control = Control {
                stop,
                ..Default::default()
            };
            let result = (|| {
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
                let mut admit = |_: &TypeViewFacts| Ok(());
                let mut budget =
                    TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work)?;
                let views = collect_package_type_views_in(&mut budget, &mut work)?;
                let _loans = views.writer_sources_in(&mut budget, &mut work)?;
                work.finish()?;
                Ok::<_, TypeViewError>(())
            })();
            (result, control.events.into_inner().unwrap())
        };
        let (result, trace) = run(None);
        assert!(result.is_ok());
        assert!(trace.len() > 2);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let (result, stopped) = run(Some((at, cause.clone())));
                assert!(matches!(result, Err(TypeViewError::Control(actual)) if actual == cause));
                assert_eq!(stopped, trace[..=at]);
            }
        }
        let owner = Control::default();
        let foreign = Control::default();
        let work = CompileCheckpoints::try_new(&owner, CompilePhase::Encode).unwrap();
        let mut seen = 0;
        let mut admit = |_: &TypeViewFacts| {
            seen += 1;
            Ok(())
        };
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work).unwrap();
        let mut foreign_work = CompileCheckpoints::try_new(&foreign, CompilePhase::Encode).unwrap();
        assert!(matches!(
            collect_package_type_views_in(&mut budget, &mut foreign_work),
            Err(TypeViewError::InvalidSource(_))
        ));
        assert_eq!(foreign.events.lock().unwrap().as_slice(), &[0]);
        assert_eq!(seen, 1);
    }
    #[test]
    fn writer_loans_refuse_a_reset_collection_contribution_before_parent_or_observation() {
        let package = package();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut original_admit = |_: &TypeViewFacts| Ok(());
        let mut original =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut original_admit, &work).unwrap();
        let views = collect_package_type_views_in(&mut original, &mut work).unwrap();
        let trace = control.events.lock().unwrap().clone();
        let mut called = 0;
        let mut reset_admit = |_: &TypeViewFacts| {
            called += 1;
            Ok(())
        };
        let mut reset =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut reset_admit, &work).unwrap();
        assert!(matches!(
            views.writer_sources_in(&mut reset, &mut work),
            Err(TypeViewError::InvalidSource(_))
        ));
        assert_eq!(control.events.lock().unwrap().as_slice(), trace);
        assert_eq!(reset.facts().allocation_requests_upper_bound, 0);
        assert_eq!(called, 1);
        let loans = views.writer_sources_in(&mut original, &mut work).unwrap();
        assert_eq!(loans.len(), 1);
        work.finish().unwrap();
    }
    #[test]
    fn all_first_capture_growth_is_admitted_before_pending_copy_callback() {
        let package = package();
        let row = growth::push_growth(&Vec::<TypeOccurrenceRow<'_>>::new()).unwrap();
        let value = growth::push_growth(&Vec::<(u32, &p::ValueType)>::new()).unwrap();
        let first_bytes =
            row.requested_backing.unwrap().size() + value.requested_backing.unwrap().size();
        for cause in CAUSES {
            let control = Control {
                stop: Some((3, cause)),
                ..Default::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let spelling = "x".repeat(255);
            let copied = novarocks_type_contract::owned_resources::copy::copy_string::<
                CompileControlError,
            >(&spelling, &mut work)
            .unwrap();
            assert_eq!(copied, spelling);
            let mut caps = limits();
            caps.max_allocation_request_bytes = first_bytes - 1;
            let mut admit = |_: &TypeViewFacts| Ok(());
            let mut budget =
                TypeViewBudget::new_in(&package, SOURCE, caps, &mut admit, &work).unwrap();
            assert!(matches!(
                collect_package_type_views_in(&mut budget, &mut work),
                Err(TypeViewError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(budget.facts().allocation_requests_upper_bound, 2);
            assert_eq!(
                budget.facts().allocation_request_bytes_upper_bound,
                first_bytes
            );
            assert_eq!(control.events.lock().unwrap().as_slice(), &[0, 0, 1]);
        }
    }
    #[test]
    fn exact_view_envelopes_pass_and_each_actual_axis_one_under_refuses() {
        let package = package();
        let run = |caps| {
            let control = Control::default();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let mut admit = |_: &TypeViewFacts| Ok(());
            let mut budget =
                TypeViewBudget::new_in(&package, SOURCE, caps, &mut admit, &work).unwrap();
            let result = collect_package_type_views_in(&mut budget, &mut work)
                .and_then(|views| views.writer_sources_in(&mut budget, &mut work).map(|_| ()));
            (result, budget.facts())
        };
        let (result, facts) = run(limits());
        assert!(result.is_ok());
        let exact = TypeViewLimits {
            max_occurrences: facts.occurrence_count,
            max_value_roots: facts.value_root_count,
            max_field_roots: facts.field_root_count,
            max_writer_recipes: facts.writer_recipe_count,
            max_allocation_requests: facts.allocation_requests_upper_bound,
            max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: facts
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: facts.cumulative_work_upper_bound,
        };
        let (result, repeated) = run(exact);
        assert!(result.is_ok());
        assert_eq!(repeated, facts);
        for axis in 0..8 {
            let mut caps = exact;
            match axis {
                0 => caps.max_occurrences -= 1,
                1 => caps.max_value_roots -= 1,
                2 => caps.max_field_roots -= 1,
                3 => caps.max_writer_recipes -= 1,
                4 => caps.max_allocation_requests -= 1,
                5 => caps.max_allocation_request_bytes -= 1,
                6 => caps.max_coexisting_source_and_request_bytes -= 1,
                7 => caps.max_work -= 1,
                _ => unreachable!(),
            }
            let (result, _) = run(caps);
            assert!(
                matches!(
                    result,
                    Err(TypeViewError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ),
                "axis {axis}"
            );
        }
    }
    #[test]
    fn storage_target_type_root_retains_its_original_author_and_independent_id() {
        let original = package();
        let recipe = original.writes().values().next().unwrap();
        let field = recipe.input().fields_iter().next().unwrap();
        let field = field
            .clone()
            .with_storage_type(field.execution_type().unwrap().clone())
            .unwrap();
        let recipe = c::ConnectorWriteRecipeDraft::try_new(
            recipe.binding().clone(),
            recipe.payload().clone(),
            c::ConnectorWriteInputShape::Data {
                fields: vec![field],
            },
        )
        .unwrap();
        let package = crate::physical_type_v2::sender_tests::checked_writer_package(recipe);
        let (node, recipe) = package.writes().iter().next().unwrap();
        let binding = recipe.input().fields_iter().next().unwrap();
        let storage = binding.storage_type().unwrap();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut admit = |_: &TypeViewFacts| Ok(());
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work).unwrap();
        let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
        let occurrence = PackageTypeOccurrence {
            fragment: package.fragment().id(),
            owner: PackageTypeOwner::Recipe(*node),
            channel: PackageTypeChannel::WriterStorage {
                role: WriterTypeRootRole::Data,
                ordinal: 0,
            },
        };
        let storage_id = views
            .value_root_for_in(occurrence, storage, &mut budget, &mut work)
            .unwrap();
        assert_ne!(storage_id, views.writer_value_ids[0]);
        let actual = views
            .values()
            .iter()
            .find(|(id, _)| *id == storage_id)
            .unwrap()
            .1;
        assert!(std::ptr::eq(actual, storage));
        let foreign = storage.clone();
        assert!(
            views
                .value_root_for_in(occurrence, &foreign, &mut budget, &mut work)
                .is_err()
        );
    }
}
