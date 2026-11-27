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

//! Original static requests, independent of runtime invocation occurrences.
//! Representation equality compares pool addresses, never selected values.
//! This table supplies no SQL provenance, effects or installed capability.

use novarocks_expression_contract::PhysicalCallRequest;
pub use novarocks_expression_contract::{
    CallRequestMaterializationError, CallRequestMaterializationFailure, MaterializedCallRequest,
    materialize_call_request_observed, materialize_call_request_with_creator_in,
    materialize_call_request_with_factory_in,
};

use crate::{Fragment, FragmentId, FrozenCallError, PhysicalCallBinding, PhysicalCallSite};
use novarocks_function_contract::FunctionBindingError;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionArgumentType, PureCompileControl,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

#[derive(Clone, Debug)]
pub struct FragmentCallRequests {
    fragment: FragmentId,
    entries: Arc<BTreeMap<PhysicalCallSite, PhysicalCallRequest>>,
    // The independent table may outlive its Fragment and decoded request Vec.
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl PartialEq for FragmentCallRequests {
    fn eq(&self, other: &Self) -> bool {
        self.fragment == other.fragment && self.entries == other.entries
    }
}
impl Eq for FragmentCallRequests {}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallRequestError {
    Control(CompileControlError),
    Binding(FunctionBindingError),
    Calls(FrozenCallError),
    Structure(crate::ValidationErrors),
    WrongFragment,
    DuplicateDefinition(PhysicalCallSite),
    MissingDefinition(PhysicalCallSite),
    ExtraDefinition,
    InvalidArgumentCount(PhysicalCallSite),
    ArgumentTypeMismatch(PhysicalCallSite),
    ConstructionResource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    ConstructionTree(novarocks_type_contract::owned_resources::btree::BTreeResourceError),
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for CallRequestError
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::ConstructionResource(error)
    }
}
#[derive(Debug)]
pub enum CallRequestCreationFailure<H> {
    Request(CallRequestError),
    Creator(H),
}
impl<H> From<CallRequestError> for CallRequestCreationFailure<H> {
    fn from(error: CallRequestError) -> Self {
        Self::Request(error)
    }
}
impl<H> From<CompileControlError> for CallRequestCreationFailure<H> {
    fn from(error: CompileControlError) -> Self {
        Self::Request(error.into())
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for CallRequestCreationFailure<H>
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Request(error.into())
    }
}
impl<H: fmt::Display> fmt::Display for CallRequestCreationFailure<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(error) => error.fmt(f),
            Self::Creator(error) => error.fmt(f),
        }
    }
}
impl<H: std::error::Error + 'static> std::error::Error for CallRequestCreationFailure<H> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Request(error) => Some(error),
            Self::Creator(error) => Some(error),
        }
    }
}
impl fmt::Display for CallRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid original call requests: {self:?}")
    }
}
impl std::error::Error for CallRequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::Calls(source) => Some(source),
            Self::Structure(source) => Some(source),
            Self::ConstructionResource(source) => Some(source),
            Self::ConstructionTree(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for CallRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<FunctionBindingError> for CallRequestError {
    fn from(error: FunctionBindingError) -> Self {
        match error {
            FunctionBindingError::Control(cause) => Self::Control(cause),
            other => Self::Binding(other),
        }
    }
}
impl From<FrozenCallError> for CallRequestError {
    fn from(error: FrozenCallError) -> Self {
        match error {
            FrozenCallError::Control(cause) => Self::Control(cause),
            other => Self::Calls(other),
        }
    }
}
fn finish<T>(
    result: Result<T, CallRequestError>,
    work: CompileCheckpoints<'_>,
) -> Result<T, CallRequestError> {
    if matches!(result, Err(CallRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
impl FragmentCallRequests {
    /// A construction input only. Publication rejects missing call records.
    pub(crate) fn unpublished_empty(fragment: FragmentId) -> Self {
        Self {
            fragment,
            entries: Arc::new(BTreeMap::new()),
            storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        }
    }
    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }
    pub fn entries(&self) -> &BTreeMap<PhysicalCallSite, PhysicalCallRequest> {
        &self.entries
    }
    pub fn get(&self, definition: PhysicalCallSite) -> Option<&PhysicalCallRequest> {
        self.entries.get(&definition)
    }
    pub fn try_new(
        fragment: &Fragment,
        entries: Vec<(PhysicalCallSite, PhysicalCallRequest)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, CallRequestError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = Self::try_new_core(
            fragment,
            entries,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
            &mut work,
            |table, fragment, _| table.validate_fragment(fragment, control),
        );
        finish(result, work)
    }
    /// Construct mandatory requests on the caller's original meter. The caller
    /// owns entry and ordinary/success finish and admits the original storage.
    /// This port supplies neither provenance nor an allocation/MEM grant.
    pub fn try_new_in(
        fragment: &Fragment,
        entries: Vec<(PhysicalCallSite, PhysicalCallRequest)>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, CallRequestError> {
        Self::try_new_core(
            fragment,
            entries,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
            work,
            |table, fragment, work| table.validate_fragment_in(fragment, work),
        )
    }
    /// Construct the same table through its actual caller's original ports.
    /// Known Arc layout and locked BTree operation bounds remain distinct.
    pub fn try_new_created_in<H>(
        fragment: &Fragment,
        entries: Vec<(PhysicalCallSite, PhysicalCallRequest)>,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, CallRequestCreationFailure<H>> {
        creation.with_error_mapping(CallRequestCreationFailure::Creator, |mapped| {
            Self::try_new_core(
                fragment,
                entries,
                storage,
                Some(mapped),
                work,
                |table, fragment, work| {
                    table
                        .validate_fragment_in(fragment, work)
                        .map_err(Into::into)
                },
            )
        })
    }
    pub fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.storage.clone()
    }
    fn try_new_core<E>(
        fragment: &Fragment,
        entries: Vec<(PhysicalCallSite, PhysicalCallRequest)>,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, E>>,
        work: &mut CompileCheckpoints<'_>,
        validate: impl FnOnce(&Self, &Fragment, &mut CompileCheckpoints<'_>) -> Result<(), E>,
    ) -> Result<Self, E>
    where
        E: From<CallRequestError>
            + From<CompileControlError>
            + From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
    {
        crate::resource::validate_call_request_source_observed(&entries, entries.capacity(), work)?;
        let storage = if let Some(creation) = creation {
            work.flush()?;
            let storage = creation
                .storage_lifetime()
                .retaining_observed(&storage, |layout| creation.request_layout(layout))?;
            let tree = novarocks_type_contract::owned_resources::btree::insertion_only::<
                PhysicalCallSite,
                PhysicalCallRequest,
            >(entries.len())
            .map_err(CallRequestError::ConstructionTree)?;
            if tree.request_bytes_upper_bound != 0 {
                creation.observe_opaque_upper_bound(tree.request_bytes_upper_bound)?;
            }
            let arc = novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<BTreeMap<PhysicalCallSite, PhysicalCallRequest>>(),
            )?;
            creation.request_layout(arc)?;
            storage
        } else {
            storage
        };

        let mut table = BTreeMap::new();
        for (definition, request) in entries {
            work.flush()?;
            let previous = table.insert(definition, request);
            work.step()?;
            work.flush()?;
            if previous.is_some() {
                return Err(CallRequestError::DuplicateDefinition(definition).into());
            }
        }
        let table = Self {
            fragment: fragment.id(),
            entries: Arc::new(table),
            storage,
        };
        work.flush()?;
        validate(&table, fragment, work)?;
        Ok(table)
    }
    pub fn validate_fragment(
        &self,
        fragment: &Fragment,
        control: &dyn PureCompileControl,
    ) -> Result<(), CallRequestError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = self.validate_fragment_in(fragment, &mut work);
        finish(result, work)
    }
    /// Recheck the complete original definition table, including dead and
    /// TypeOnly definitions, without creating a child scope or footer.
    pub fn validate_fragment_in(
        &self,
        fragment: &Fragment,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), CallRequestError> {
        let wrong_fragment = fragment.id() != self.fragment;
        work.step()?;
        if wrong_fragment {
            return Err(CallRequestError::WrongFragment);
        }
        crate::resource::validate_call_request_table_observed(self, work)?;
        let mut visited = 0usize;
        let mut check = |definition,
                         binding: PhysicalCallBinding<'_>,
                         window_logical_count: Option<usize>,
                         work: &mut CompileCheckpoints<'_>|
         -> Result<(), CallRequestError> {
            let request = self.entries.get(&definition);
            work.step()?;
            let request = request.ok_or(CallRequestError::MissingDefinition(definition))?;
            validate_request_binding(definition, request, binding, window_logical_count, work)?;
            visited = visited
                .checked_add(1)
                .ok_or(CompileControlError::ResourceExhausted)?;
            work.step()?;
            Ok(())
        };
        crate::visit_relational_calls_observed(fragment, work, |site, binding, work| {
            check(site, binding, None, work)
        })?;
        let extra = visited != self.entries.len();
        work.step()?;
        if extra {
            return Err(CallRequestError::ExtraDefinition);
        }
        Ok(())
    }
}
fn validate_request_binding(
    definition: PhysicalCallSite,
    request: &PhysicalCallRequest,
    binding: PhysicalCallBinding<'_>,
    window_logical_count: Option<usize>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CallRequestError> {
    let (types, logical) = match binding {
        PhysicalCallBinding::Window {
            function,
            aggregate,
        } => (
            &*function.argument_types,
            aggregate
                .map(|value| value.logical_argument_count as usize)
                .or(window_logical_count)
                .or(Some(function.argument_types.len())),
        ),
        PhysicalCallBinding::Aggregate(binding) => (
            &*binding.function.argument_types,
            Some(binding.logical_argument_count as usize),
        ),
        PhysicalCallBinding::Table(function) => (
            &*function.argument_types,
            Some(function.argument_types.len()),
        ),
    };
    let invalid_count = request.arguments.len() != types.len()
        || request.logical_argument_count > request.arguments.len()
        || logical.is_some_and(|logical| logical != request.logical_argument_count);
    work.step()?;
    if invalid_count {
        return Err(CallRequestError::InvalidArgumentCount(definition));
    }
    for (actual, expected) in request.arguments.iter().zip(types) {
        if matches!(expected, FunctionArgumentType::CandidateGroup { .. })
            || !actual.matches_type_observed(expected, work)?
        {
            return Err(CallRequestError::ArgumentTypeMismatch(definition));
        }
        work.step()?;
    }
    Ok(())
}
impl Fragment {
    /// Attach explicit original requests before executable publication. Nothing
    /// is recovered from constants, selected results, names or runtime uses.
    pub fn with_call_requests_observed(
        mut self,
        entries: Vec<(PhysicalCallSite, PhysicalCallRequest)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, CallRequestError> {
        let requests = FragmentCallRequests::try_new(&self, entries, control)?;
        self.call_requests = requests;
        Ok(self)
    }
    /// Attach the caller-authored mandatory table using the same publication
    /// meter. Complete source/resource/binding coverage is still rechecked.
    pub fn with_call_requests_in(
        mut self,
        entries: Vec<(PhysicalCallSite, PhysicalCallRequest)>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, CallRequestError> {
        let requests = FragmentCallRequests::try_new_in(&self, entries, work)?;
        self.call_requests = requests;
        Ok(self)
    }
    pub fn with_call_requests_created_in<H>(
        mut self,
        entries: Vec<(PhysicalCallSite, PhysicalCallRequest)>,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, CallRequestCreationFailure<H>> {
        self.call_requests =
            FragmentCallRequests::try_new_created_in(&self, entries, storage, creation, work)?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests;
