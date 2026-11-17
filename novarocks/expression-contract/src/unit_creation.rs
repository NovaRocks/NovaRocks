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

//! Borrow the enclosing caller's actual scratch and diagnostic storage ports.
//! No scope, quota, entry, footer, source copy or success hold is created here.
use crate::{ExprId, RootOrdinal, UnitError};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ExpressionUseId, ValueSchemaCreationScope,
    ValueTypeStorageLifetime,
    owned_resources::{btree, layout::LayoutResourceError},
};
use std::{
    alloc::Layout,
    collections::{BTreeMap, BTreeSet},
    fmt,
};

/// The diagnostic backing drops before the original retained lifetime.
#[derive(Debug)]
pub struct ConstructedUnitError {
    cause: UnitError,
    _storage: ValueTypeStorageLifetime,
}
impl ConstructedUnitError {
    pub fn cause(&self) -> &UnitError {
        &self.cause
    }
}
impl fmt::Display for ConstructedUnitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for ConstructedUnitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
/// Numerical facts from the actual construction author, before the matching
/// creator call. Observation neither grants storage nor traverses the unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnitConstructionStorage {
    Temporary,
    Diagnostic,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnitConstructionRequest {
    Exact {
        storage: UnitConstructionStorage,
        layout: Layout,
    },
    Opaque {
        storage: UnitConstructionStorage,
        facts: btree::InsertionOnlyFacts,
    },
}
#[derive(Debug)]
pub enum UnitConstructionFailure<H> {
    Control(CompileControlError),
    Creator(H),
    Observer(H),
    Resource(LayoutResourceError),
    Owner(ConstructedUnitError),
}
impl<H: fmt::Display> fmt::Display for UnitConstructionFailure<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Creator(e) => e.fmt(f),
            Self::Observer(e) => e.fmt(f),
            Self::Resource(e) => e.fmt(f),
            Self::Owner(e) => e.fmt(f),
        }
    }
}
impl<H: std::error::Error + 'static> std::error::Error for UnitConstructionFailure<H> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Creator(e) => Some(e),
            Self::Observer(e) => Some(e),
            Self::Resource(e) => Some(e),
            Self::Owner(e) => Some(e),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ConstructedUnitError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.cause.failure_fact()
    }
}
impl<H: novarocks_failure_contract::FailureProjection> novarocks_failure_contract::FailureProjection
    for UnitConstructionFailure<H>
{
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Control(e) => e.failure_fact(),
            Self::Creator(e) => e.failure_fact(),
            Self::Observer(e) => e.failure_fact(),
            Self::Resource(e) => e.failure_fact(),
            Self::Owner(e) => e.failure_fact(),
        }
    }
}
impl<H> From<CompileControlError> for UnitConstructionFailure<H> {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}

/// Private borrowed diagnoses exist only until the original parts are released.
/// Rendering happens once at the enclosing public error boundary.
pub(crate) enum UnitMessage<'a> {
    Static(&'static str),
    Type(crate::validation_types::TypeIssue<'a>),
    MissingClaim(ExpressionUseId),
}
impl From<&'static str> for UnitMessage<'_> {
    fn from(s: &'static str) -> Self {
        Self::Static(s)
    }
}
impl fmt::Display for UnitMessage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(s) => f.write_str(s),
            Self::Type(issue) => write!(
                f,
                "definitions[{}]: {}",
                issue.definition.get(),
                issue.message
            ),
            Self::MissingClaim(id) => write!(f, "function use {} lacks a call claim", id.get()),
        }
    }
}
pub(crate) enum ValidationFailure<'a, H> {
    Control(CompileControlError),
    Creator(H),
    Observer(H),
    Resource(LayoutResourceError),
    Owner(UnitError),
    Invalid {
        definition: Option<ExprId>,
        root: Option<RootOrdinal>,
        message: UnitMessage<'a>,
    },
}
impl<'a, H> ValidationFailure<'a, H> {
    pub(crate) fn invalid(
        definition: Option<ExprId>,
        root: Option<RootOrdinal>,
        message: impl Into<UnitMessage<'a>>,
    ) -> Self {
        Self::Invalid {
            definition,
            root,
            message: message.into(),
        }
    }
}
impl<H> From<CompileControlError> for ValidationFailure<'_, H> {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl<H> From<LayoutResourceError> for ValidationFailure<'_, H> {
    fn from(e: LayoutResourceError) -> Self {
        Self::Resource(e)
    }
}
impl<H> From<UnitError> for ValidationFailure<'_, H> {
    fn from(e: UnitError) -> Self {
        match e {
            UnitError::Control(e) => Self::Control(e),
            e => Self::Owner(e),
        }
    }
}
macro_rules! owner_error {
    ($($ty:ty),* $(,)?) => { $(impl<H> From<$ty> for ValidationFailure<'_, H> {
        fn from(e: $ty) -> Self { UnitError::from(e).into() }
    })* };
}
owner_error!(
    novarocks_type_contract::ExpressionControlFlowError,
    novarocks_type_contract::EffectContractError,
    novarocks_type_contract::ValueTypeError,
    novarocks_function_contract::FunctionBindingError,
    crate::TemporalSourceProjectionError
);

/// Allocation-only failures cannot contain a semantic diagnosis or an owned
/// source error. Other neutral authors may reborrow these exact request slots.
pub(crate) enum UnitAllocationFailure<H> {
    Control(CompileControlError),
    Creator(H),
    Observer(H),
    Resource(LayoutResourceError),
}
impl<H> From<CompileControlError> for UnitAllocationFailure<H> {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl<H> From<LayoutResourceError> for UnitAllocationFailure<H> {
    fn from(e: LayoutResourceError) -> Self {
        Self::Resource(e)
    }
}
impl<H> From<UnitAllocationFailure<H>> for ValidationFailure<'_, H> {
    fn from(e: UnitAllocationFailure<H>) -> Self {
        match e {
            UnitAllocationFailure::Control(e) => Self::Control(e),
            UnitAllocationFailure::Creator(e) => Self::Creator(e),
            UnitAllocationFailure::Observer(e) => Self::Observer(e),
            UnitAllocationFailure::Resource(e) => Self::Resource(e),
        }
    }
}

enum UnitValidationScopes<'a, H> {
    Pure,
    Separate {
        temporary: &'a mut dyn ValueSchemaCreationScope<H>,
        retained: &'a mut dyn ValueSchemaCreationScope<H>,
    },
    Single(&'a mut dyn ValueSchemaCreationScope<H>),
}
pub(crate) struct UnitValidationCreation<'a, H> {
    scopes: UnitValidationScopes<'a, H>,
    observer: Option<&'a mut dyn FnMut(UnitConstructionRequest) -> Result<(), H>>,
}
impl<'a, H> UnitValidationCreation<'a, H> {
    pub(crate) fn new(
        temporary: &'a mut dyn ValueSchemaCreationScope<H>,
        retained: &'a mut dyn ValueSchemaCreationScope<H>,
    ) -> Self {
        Self {
            scopes: UnitValidationScopes::Separate {
                temporary,
                retained,
            },
            observer: None,
        }
    }
    pub(crate) fn single(scope: &'a mut dyn ValueSchemaCreationScope<H>) -> Self {
        Self {
            scopes: UnitValidationScopes::Single(scope),
            observer: None,
        }
    }
    pub(crate) fn pure() -> Self {
        Self {
            scopes: UnitValidationScopes::Pure,
            observer: None,
        }
    }
    fn temporary_scope(&mut self) -> Option<&mut (dyn ValueSchemaCreationScope<H> + 'a)> {
        match &mut self.scopes {
            UnitValidationScopes::Pure => None,
            UnitValidationScopes::Separate { temporary, .. } => Some(&mut **temporary),
            UnitValidationScopes::Single(scope) => Some(&mut **scope),
        }
    }
    pub(crate) fn with_observer(
        mut self,
        observer: &'a mut dyn FnMut(UnitConstructionRequest) -> Result<(), H>,
    ) -> Self {
        self.observer = Some(observer);
        self
    }
    fn observe(
        &mut self,
        request: UnitConstructionRequest,
    ) -> Result<(), UnitAllocationFailure<H>> {
        if let Some(observer) = self.observer.as_deref_mut() {
            observer(request).map_err(UnitAllocationFailure::Observer)?;
        }
        Ok(())
    }
    pub(crate) fn request(
        &mut self,
        layout: Layout,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), UnitAllocationFailure<H>> {
        if layout.size() == 0 {
            return Ok(());
        }
        if self.observer.is_some() || !matches!(self.scopes, UnitValidationScopes::Pure) {
            work.flush()?;
        }
        self.observe(UnitConstructionRequest::Exact {
            storage: UnitConstructionStorage::Temporary,
            layout,
        })?;
        if let Some(scope) = self.temporary_scope() {
            scope
                .request_layout(layout)
                .map_err(UnitAllocationFailure::Creator)?;
        }
        Ok(())
    }
    pub(crate) fn reserve<T>(
        &mut self,
        out: &mut Vec<T>,
        additional: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), UnitAllocationFailure<H>> {
        work.flush()?;
        if additional > out.capacity() - out.len() && std::mem::size_of::<T>() != 0 {
            let target = out
                .len()
                .checked_add(additional)
                .ok_or(LayoutResourceError::Arithmetic)?;
            let layout = Layout::array::<T>(target).map_err(|_| LayoutResourceError::Arithmetic)?;
            self.request(layout, work)?;
        }
        out.try_reserve_exact(additional)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.step()?;
        work.flush()?;
        Ok(())
    }
    pub(crate) fn boxed<T>(
        &mut self,
        out: Vec<T>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[T]>, UnitAllocationFailure<H>> {
        work.flush()?;
        if out.capacity() != out.len() && !out.is_empty() && std::mem::size_of::<T>() != 0 {
            self.request(
                Layout::array::<T>(out.len()).map_err(|_| LayoutResourceError::Arithmetic)?,
                work,
            )?;
        }
        let out = out.into_boxed_slice();
        work.flush()?;
        Ok(out)
    }
    pub(crate) fn before_boxed_trim<T>(
        &mut self,
        out: &Vec<T>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), UnitAllocationFailure<H>> {
        if out.capacity() != out.len() && !out.is_empty() && std::mem::size_of::<T>() != 0 {
            self.request(
                Layout::array::<T>(out.len()).map_err(|_| LayoutResourceError::Arithmetic)?,
                work,
            )?;
        }
        Ok(())
    }
    fn before_tree<K, V>(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), UnitAllocationFailure<H>> {
        if self.observer.is_none() && matches!(self.scopes, UnitValidationScopes::Pure) {
            return Ok(());
        }
        let map_error = |e| match e {
            btree::BTreeResourceError::SourceModel(_) => LayoutResourceError::SourceModel,
            btree::BTreeResourceError::Arithmetic(_) => LayoutResourceError::Arithmetic,
        };
        let before = btree::insertion_only::<K, V>(count).map_err(map_error)?;
        let after = btree::insertion_only::<K, V>(
            count
                .checked_add(1)
                .ok_or(LayoutResourceError::Arithmetic)?,
        )
        .map_err(map_error)?;
        let facts = btree::InsertionOnlyFacts {
            allocation_requests_upper_bound: after
                .allocation_requests_upper_bound
                .checked_sub(before.allocation_requests_upper_bound)
                .ok_or(LayoutResourceError::Arithmetic)?,
            request_bytes_upper_bound: after
                .request_bytes_upper_bound
                .checked_sub(before.request_bytes_upper_bound)
                .ok_or(LayoutResourceError::Arithmetic)?,
            cumulative_work_upper_bound: after
                .cumulative_work_upper_bound
                .checked_sub(before.cumulative_work_upper_bound)
                .ok_or(LayoutResourceError::Arithmetic)?,
        };
        if facts.request_bytes_upper_bound != 0 {
            work.flush()?;
            self.observe(UnitConstructionRequest::Opaque {
                storage: UnitConstructionStorage::Temporary,
                facts,
            })?;
            if let Some(scope) = self.temporary_scope() {
                scope
                    .observe_opaque_upper_bound(facts.request_bytes_upper_bound)
                    .map_err(UnitAllocationFailure::Creator)?;
            }
        }
        Ok(())
    }
    pub(crate) fn set_insert<K: Ord + Copy>(
        &mut self,
        out: &mut BTreeSet<K>,
        key: K,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, UnitAllocationFailure<H>> {
        let existing = out.contains(&key);
        work.step()?;
        if !existing {
            self.before_tree::<K, ()>(out.len(), work)?;
        }
        Ok(out.insert(key))
    }
    pub(crate) fn map_insert<K: Ord + Copy, V>(
        &mut self,
        out: &mut BTreeMap<K, V>,
        key: K,
        value: V,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<V>, UnitAllocationFailure<H>> {
        let existing = out.contains_key(&key);
        work.step()?;
        if !existing {
            self.before_tree::<K, V>(out.len(), work)?;
        }
        Ok(out.insert(key, value))
    }
    pub(crate) fn finish_error(
        &mut self,
        failure: ValidationFailure<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> UnitConstructionFailure<H> {
        match failure {
            ValidationFailure::Control(e) => UnitConstructionFailure::Control(e),
            ValidationFailure::Creator(e) => UnitConstructionFailure::Creator(e),
            ValidationFailure::Observer(e) => UnitConstructionFailure::Observer(e),
            ValidationFailure::Resource(e) => UnitConstructionFailure::Resource(e),
            ValidationFailure::Owner(cause) => {
                UnitConstructionFailure::Owner(ConstructedUnitError {
                    cause,
                    _storage: ValueTypeStorageLifetime::pure(),
                })
            }
            ValidationFailure::Invalid {
                definition,
                root,
                message,
            } => {
                let mut writer = DiagnosticWriter {
                    text: String::new(),
                    failure: None,
                    retained: match &mut self.scopes {
                        UnitValidationScopes::Pure => None,
                        UnitValidationScopes::Separate { retained, .. } => Some(&mut **retained),
                        UnitValidationScopes::Single(scope) => Some(&mut **scope),
                    },
                    observer: self.observer.as_deref_mut(),
                    work,
                };
                let rendered = fmt::write(&mut writer, format_args!("{message}"));
                if let Some(e) = writer.failure {
                    return e;
                }
                if rendered.is_err() {
                    return UnitConstructionFailure::Control(
                        CompileControlError::ResourceExhausted,
                    );
                }
                let storage = writer
                    .retained
                    .as_ref()
                    .map_or_else(ValueTypeStorageLifetime::pure, |scope| {
                        scope.storage_lifetime()
                    });
                UnitConstructionFailure::Owner(ConstructedUnitError {
                    cause: UnitError::Invalid {
                        definition,
                        root,
                        message: writer.text,
                    },
                    _storage: storage,
                })
            }
        }
    }
}
struct DiagnosticWriter<'a, 'scope, 'observer, 'work, 'control, H> {
    text: String,
    failure: Option<UnitConstructionFailure<H>>,
    retained: Option<&'a mut (dyn ValueSchemaCreationScope<H> + 'scope)>,
    observer: Option<&'a mut (dyn FnMut(UnitConstructionRequest) -> Result<(), H> + 'observer)>,
    work: &'work mut CompileCheckpoints<'control>,
}
impl<H> fmt::Write for DiagnosticWriter<'_, '_, '_, '_, '_, H> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.failure.is_some() {
            return Err(fmt::Error);
        }
        let result = (|| {
            if text.len() > self.text.capacity() - self.text.len() {
                let target = self.text.len().checked_add(text.len()).ok_or(
                    UnitConstructionFailure::Resource(LayoutResourceError::Arithmetic),
                )?;
                let layout = Layout::array::<u8>(target).map_err(|_| {
                    UnitConstructionFailure::Resource(LayoutResourceError::Arithmetic)
                })?;
                if self.observer.is_some() || self.retained.is_some() {
                    self.work.flush()?;
                }
                if let Some(observer) = self.observer.as_deref_mut() {
                    observer(UnitConstructionRequest::Exact {
                        storage: UnitConstructionStorage::Diagnostic,
                        layout,
                    })
                    .map_err(UnitConstructionFailure::Observer)?;
                }
                if let Some(scope) = self.retained.as_deref_mut() {
                    scope
                        .request_layout(layout)
                        .map_err(UnitConstructionFailure::Creator)?;
                }
                self.text.try_reserve_exact(text.len()).map_err(|_| {
                    UnitConstructionFailure::Control(CompileControlError::ResourceExhausted)
                })?;
            }
            for character in text.chars() {
                self.text.push(character);
                self.work.step()?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                self.failure = Some(e);
                Err(fmt::Error)
            }
        }
    }
}
pub(crate) fn pure_error(error: UnitConstructionFailure<std::convert::Infallible>) -> UnitError {
    match error {
        UnitConstructionFailure::Control(e) => UnitError::Control(e),
        UnitConstructionFailure::Creator(never) | UnitConstructionFailure::Observer(never) => {
            match never {}
        }
        UnitConstructionFailure::Resource(_) => {
            UnitError::Control(CompileControlError::ResourceExhausted)
        }
        UnitConstructionFailure::Owner(e) => e.cause,
    }
}
