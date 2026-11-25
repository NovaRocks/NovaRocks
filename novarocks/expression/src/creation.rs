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

//! Actual before-construction ports shared by immutable program authors.
use novarocks_type_contract::owned_resources::{
    btree,
    layout::{LayoutResourceError, arc_layout},
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ValueSchemaCreationScope, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, collections::TryReserveError, fmt};

#[derive(Debug)]
pub enum ProgramCreationFailure<C, H> {
    Control(CompileControlError),
    Creator(H),
    Resource(LayoutResourceError),
    Capacity(TryReserveError),
    Owner(C),
}
impl<C: fmt::Display, H: fmt::Display> fmt::Display for ProgramCreationFailure<C, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Creator(e) => e.fmt(f),
            Self::Resource(e) => write!(f, "program construction resource: {e:?}"),
            Self::Capacity(e) => e.fmt(f),
            Self::Owner(e) => e.fmt(f),
        }
    }
}
impl<C: std::error::Error + 'static, H: std::error::Error + 'static> std::error::Error
    for ProgramCreationFailure<C, H>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Creator(e) => Some(e),
            Self::Resource(source) => Some(source),
            Self::Capacity(e) => Some(e),
            Self::Owner(e) => Some(e),
        }
    }
}
impl<C, H> ProgramCreationFailure<C, H> {
    pub fn map_owner<D>(self, map: impl FnOnce(C) -> D) -> ProgramCreationFailure<D, H> {
        match self {
            Self::Control(e) => ProgramCreationFailure::Control(e),
            Self::Creator(e) => ProgramCreationFailure::Creator(e),
            Self::Resource(e) => ProgramCreationFailure::Resource(e),
            Self::Capacity(e) => ProgramCreationFailure::Capacity(e),
            Self::Owner(e) => ProgramCreationFailure::Owner(map(e)),
        }
    }
}
impl<C, H> From<CompileControlError> for ProgramCreationFailure<C, H> {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl<C, H> From<LayoutResourceError> for ProgramCreationFailure<C, H> {
    fn from(e: LayoutResourceError) -> Self {
        Self::Resource(e)
    }
}

/// Stack-borrowed ports only. None is explicit pure preparation; no grant or
/// service is manufactured, stored in an entity or inferred from a profile.
pub(crate) struct ProgramCreation<'a, H> {
    pub(crate) creator: Option<&'a mut dyn ValueSchemaCreationScope<H>>,
}
impl<H> ProgramCreation<'_, H> {
    pub(crate) fn storage(&self) -> ValueTypeStorageLifetime {
        self.creator
            .as_ref()
            .map(|c| c.storage_lifetime())
            .unwrap_or_else(ValueTypeStorageLifetime::pure)
    }
    pub(crate) fn request<C>(
        &mut self,
        layout: Layout,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ProgramCreationFailure<C, H>> {
        if let Some(creator) = self.creator.as_deref_mut() {
            work.flush()?;
            creator
                .request_layout(layout)
                .map_err(ProgramCreationFailure::Creator)?;
        }
        Ok(())
    }
    pub(crate) fn opaque<C>(
        &mut self,
        bytes: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ProgramCreationFailure<C, H>> {
        if let Some(creator) = self.creator.as_deref_mut() {
            work.flush()?;
            creator
                .observe_opaque_upper_bound(bytes)
                .map_err(ProgramCreationFailure::Creator)?;
        }
        Ok(())
    }
    pub(crate) fn reserve<C, T>(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<T>, ProgramCreationFailure<C, H>> {
        let layout = Layout::array::<T>(count).map_err(|_| LayoutResourceError::Arithmetic)?;
        if count != 0 {
            self.request(layout, work)?;
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(ProgramCreationFailure::Capacity)?;
        Ok(values)
    }
    pub(crate) fn before_arc<C, T>(
        &mut self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ProgramCreationFailure<C, H>> {
        self.request(arc_layout(Layout::new::<T>())?, work)
    }
    /// Advance the sole insertion-only cumulative allowance before the next
    /// real insertion. Prior unused allowance remains in the same creator
    /// scope; this is not one-allocation-per-insert or exact retained capacity.
    pub(crate) fn before_tree_insert<C, K, V>(
        &mut self,
        prior_insertions: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ProgramCreationFailure<C, H>> {
        let map_error = |e| match e {
            btree::BTreeResourceError::SourceModel(_) => LayoutResourceError::SourceModel,
            btree::BTreeResourceError::Arithmetic(_) => LayoutResourceError::Arithmetic,
        };
        let next = prior_insertions
            .checked_add(1)
            .ok_or(LayoutResourceError::Arithmetic)?;
        let before = btree::insertion_only::<K, V>(prior_insertions).map_err(map_error)?;
        let after = btree::insertion_only::<K, V>(next).map_err(map_error)?;
        let delta = after
            .request_bytes_upper_bound
            .checked_sub(before.request_bytes_upper_bound)
            .ok_or(LayoutResourceError::Arithmetic)?;
        self.opaque(delta, work)
    }
}

/// Compile-time absence of a factory in the pure adapter. This uninhabited
/// type cannot create a scope, callback, authority or successful grant.
pub(crate) enum NoFactory {}
impl<H> novarocks_type_contract::ValueSchemaCreationScope<H> for NoFactory {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        match *self {}
    }
    fn request_layout(&mut self, _: Layout) -> Result<(), H> {
        match *self {}
    }
    fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), H> {
        match *self {}
    }
}
impl<H> novarocks_type_contract::ValueSchemaCreationFactory<H> for NoFactory {
    type Scope<'a> = NoFactory;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, H> {
        match *self {}
    }
    fn begin_scope_observed<O>(
        &self,
        _: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, novarocks_type_contract::ValueSchemaScopeBeginFailure<O, H>> {
        match *self {}
    }
}

impl<
    C: novarocks_failure_contract::FailureProjection,
    H: novarocks_failure_contract::FailureProjection,
> novarocks_failure_contract::FailureProjection for ProgramCreationFailure<C, H>
{
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Creator(source) => source.failure_fact(),
            Self::Resource(source) => source.failure_fact(),
            Self::Owner(source) => source.failure_fact(),
            // The publisher observed a real reserve refusal. The SDK does
            // not expose a capacity measurement; those values remain unknown.
            Self::Capacity(_) => FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::EXPRESSION, std::num::NonZeroU16::new(430).unwrap()),
            ),
        }
    }
}

pub(crate) fn publish_argument_types<H, T: novarocks_type_contract::TypedFact>(
    types: Vec<T>,
    mut creation: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    novarocks_type_contract::TypedFactSlice<T>,
    ProgramCreationFailure<std::convert::Infallible, H>,
> {
    use ProgramCreationFailure as Failure;
    use novarocks_type_contract::{TypedFactSlice, ValueTypeStorageLifetime};
    if let Some(c) = crate::creation::borrow_scope(&mut creation) {
        if types.capacity() != types.len() && !types.is_empty() {
            let layout = Layout::array::<T>(types.len())
                .map_err(|_| Failure::Resource(LayoutResourceError::Arithmetic))?;
            work.flush()?;
            c.request_layout(layout).map_err(Failure::Creator)?;
        }
        if !types.is_empty() {
            let layout =
                TypedFactSlice::<T>::storage_allocation_layout().map_err(Failure::Resource)?;
            work.flush()?;
            c.request_layout(layout).map_err(Failure::Creator)?;
        }
    }
    let storage = creation
        .as_ref()
        .map_or_else(ValueTypeStorageLifetime::pure, |c| c.storage_lifetime());
    Ok(TypedFactSlice::from_boxed_in(
        types.into_boxed_slice(),
        storage,
    ))
}
/// Borrow the optional real scope for one synchronous operation. The caller's
/// longer scope lifetime does not extend this mutable borrow across a loop.
pub(crate) fn borrow_scope<'borrow, H>(
    scope: &'borrow mut Option<&mut dyn ValueSchemaCreationScope<H>>,
) -> Option<&'borrow mut dyn ValueSchemaCreationScope<H>> {
    match scope {
        Some(scope) => Some(&mut **scope),
        None => None,
    }
}

#[cfg(test)]
#[path = "creation/argument_types_tests.rs"]
mod argument_types_tests;
