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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! Optional construction events inside the ONE original fixed scalar solver.
//! None is a pure author, not a scope or successful admission implementation.
use crate::FunctionValueType;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::owned_resources::{layout, vec};
use novarocks_type_contract::{
    ValueConstructionFailure, ValueConstructionRequest, ValueLogicalType, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, sync::Arc};
pub(super) type Failure<E> = ValueConstructionFailure<E>;
pub(super) struct ScalarConstruction<'a, E> {
    observer: Option<&'a mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>>,
    storage: ValueTypeStorageLifetime,
}
impl<'a, E> ScalarConstruction<'a, E> {
    pub(super) fn new(
        storage: ValueTypeStorageLifetime,
        observer: Option<&'a mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>>,
    ) -> Self {
        Self { storage, observer }
    }
    pub(super) fn storage(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
    pub(super) fn observer(
        &mut self,
    ) -> Option<&mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>> {
        match &mut self.observer {
            Some(observer) => Some(&mut **observer),
            None => None,
        }
    }
    pub(super) fn exact(
        &mut self,
        make: impl FnOnce() -> Result<Layout, layout::LayoutResourceError>,
    ) -> Result<(), Failure<E>> {
        if let Some(observer) = self.observer() {
            observer(ValueConstructionRequest::Exact(make()?)).map_err(Failure::Observer)?;
        }
        Ok(())
    }
    pub(super) fn array<T>(&mut self, len: usize) -> Result<(), Failure<E>> {
        if len != 0 {
            self.exact(|| {
                Layout::array::<T>(len).map_err(|_| layout::LayoutResourceError::Arithmetic)
            })?;
        }
        Ok(())
    }
    pub(super) fn arc_slice<T>(&mut self, len: usize) -> Result<(), Failure<E>> {
        self.exact(|| {
            layout::arc_layout(
                Layout::array::<T>(len).map_err(|_| layout::LayoutResourceError::Arithmetic)?,
            )
        })
    }
    pub(super) fn collected_vec<T>(&mut self, len: usize) -> Result<(), Failure<E>> {
        if let Some(observer) = self.observer() {
            let upper = vec::original_fresh_push_requests::<T, Failure<E>>(len, &mut || {
                observer(ValueConstructionRequest::Work(1)).map_err(Failure::Observer)
            })?;
            if upper != 0 {
                observer(ValueConstructionRequest::OpaqueUpper(upper))
                    .map_err(Failure::Observer)?;
            }
        }
        Ok(())
    }
    pub(super) fn collected_box<T>(&mut self, len: usize) -> Result<(), Failure<E>> {
        self.collected_vec::<T>(len)?;
        self.array::<T>(len)
    }
    pub(super) fn fields_from_values(&mut self, len: usize) -> Result<(), Failure<E>> {
        if let Some(observer) = self.observer() {
            for _ in 0..len {
                observer(ValueConstructionRequest::Work(1)).map_err(Failure::Observer)?;
                observer(ValueConstructionRequest::Exact(layout::arc_layout(
                    Layout::new::<Field>(),
                )?))
                .map_err(Failure::Observer)?;
            }
        }
        self.collected_vec::<Arc<Field>>(len)?;
        self.arc_slice::<Arc<Field>>(len)
    }
    pub(super) fn push<T>(&mut self, values: &Vec<T>) -> Result<(), Failure<E>> {
        if let Some(observer) = self.observer() {
            let facts = vec::push_growth(values)?;
            if let Some(layout) = facts.requested_backing {
                observer(ValueConstructionRequest::Exact(layout)).map_err(Failure::Observer)?;
            }
        }
        Ok(())
    }
    pub(super) fn field(
        &mut self,
        name: &str,
        ty: DataType,
        nullable: bool,
    ) -> Result<Arc<Field>, Failure<E>> {
        self.array::<u8>(name.len())?;
        let field = Field::new(name, ty, nullable);
        self.exact(|| layout::arc_layout(Layout::new::<Field>()))?;
        Ok(Arc::new(field))
    }
    pub(super) fn field_owned(
        &mut self,
        name: String,
        ty: DataType,
        nullable: bool,
    ) -> Result<Arc<Field>, Failure<E>> {
        // The original Field::new consumes this already-created String.
        self.exact(|| layout::arc_layout(Layout::new::<Field>()))?;
        Ok(Arc::new(Field::new(name, ty, nullable)))
    }
    pub(super) fn publish_facts<T: novarocks_type_contract::TypedFact>(
        &mut self,
        facts: Vec<T>,
    ) -> Result<novarocks_type_contract::TypedFactSlice<T>, Failure<E>> {
        if facts.capacity() != facts.len() {
            self.array::<T>(facts.len())?;
        }
        if !facts.is_empty() {
            self.exact(novarocks_type_contract::TypedFactSlice::<T>::storage_allocation_layout)?;
        }
        Ok(novarocks_type_contract::TypedFactSlice::from_boxed_in(
            facts.into_boxed_slice(),
            self.storage(),
        ))
    }
    pub(super) fn publish_arguments(
        &mut self,
        arguments: Vec<crate::FunctionArgumentType>,
    ) -> Result<novarocks_type_contract::FunctionArgumentTypes, Failure<E>> {
        self.publish_facts(arguments)
    }
    /// Exact existing `format!("col{}", ordinal+1)` role-label operation only.
    /// Rust 1.98.1 fmt::Arguments estimates twice the literal bytes (the first
    /// piece is nonempty), alloc::fmt constructs that String, and String's two
    /// writes use the original RawVec reserve geometry. This is a cumulative
    /// temporary operation upper, not a general diagnostic-format model.
    pub(super) fn col_name(&mut self, index: usize) -> Result<String, Failure<E>> {
        if let Some(observer) = self.observer() {
            let ordinal = index
                .checked_add(1)
                .ok_or(layout::LayoutResourceError::Arithmetic)?;
            let digits = ordinal.ilog10() as usize + 1;
            let initial = "col"
                .len()
                .checked_mul(2)
                .ok_or(layout::LayoutResourceError::Arithmetic)?;
            let target = "col"
                .len()
                .checked_add(digits)
                .ok_or(layout::LayoutResourceError::Arithmetic)?;
            observer(ValueConstructionRequest::Work(digits)).map_err(Failure::Observer)?;
            let growth = vec::original_resize_growth::<u8>("col".len(), initial, target)?;
            let upper = initial
                .checked_add(growth.requested_backing.map_or(0, |layout| layout.size()))
                .ok_or(layout::LayoutResourceError::Arithmetic)?;
            observer(ValueConstructionRequest::OpaqueUpper(upper)).map_err(Failure::Observer)?;
        }
        Ok(format!("col{}", index + 1))
    }
    pub(super) fn cloned_data_type(&mut self, ty: &DataType) -> Result<DataType, Failure<E>> {
        novarocks_type_contract::clone_data_type_with_construction(ty, self.observer())
    }
    pub(super) fn value(
        &mut self,
        ty: DataType,
        nullable: bool,
        domain: ValueLogicalType,
    ) -> Result<FunctionValueType, Failure<E>> {
        let storage = self.storage();
        FunctionValueType::try_with_logical_type_with_construction(
            ty,
            nullable,
            domain,
            storage,
            self.observer(),
        )
    }
    pub(super) fn nullable(
        &mut self,
        source: &FunctionValueType,
        nullable: bool,
    ) -> Result<FunctionValueType, Failure<E>> {
        let storage = self.storage();
        source.with_nullable_with_construction(nullable, storage, self.observer())
    }
    pub(super) fn child(
        &mut self,
        source: &FunctionValueType,
        index: usize,
    ) -> Result<Option<FunctionValueType>, Failure<E>> {
        let storage = self.storage();
        source.child_at_with_construction(index, storage, self.observer())
    }
    pub(super) fn wider(
        &mut self,
        left: &DataType,
        right: &DataType,
    ) -> Result<DataType, Failure<E>> {
        novarocks_type_contract::wider_type_with_construction(left, right, self.observer())
    }
    pub(super) fn common(
        &mut self,
        carrier: DataType,
        left: &FunctionValueType,
        right: &FunctionValueType,
    ) -> Result<FunctionValueType, Failure<E>> {
        let storage = self.storage();
        novarocks_type_contract::value_type_for_common_layout_with_construction(
            carrier,
            left,
            right,
            storage,
            self.observer(),
        )
    }
}
pub(super) fn semantic_option<T, E>(
    result: Result<T, Failure<E>>,
) -> Result<Option<T>, Failure<E>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(Failure::ValueType(_) | Failure::Rule(_)) => Ok(None),
        Err(error) => Err(error),
    }
}
pub(super) fn pure_string(error: Failure<std::convert::Infallible>) -> String {
    match error {
        Failure::Rule(error) => error,
        Failure::ValueType(error) => {
            novarocks_type_contract::ValueTypeError::from(error).to_string()
        }
        Failure::Observer(never) => match never {},
        Failure::Layout(_) | Failure::Vec(_) | Failure::HashMap(_) | Failure::BTree(_) => {
            unreachable!("pure scalar solver has no allocation observer")
        }
    }
}

pub(super) fn pure_value(
    error: Failure<std::convert::Infallible>,
) -> novarocks_type_contract::ValueTypeError {
    match error {
        Failure::ValueType(error) => error.into(),
        Failure::Observer(never) => match never {},
        Failure::Rule(_)
        | Failure::Layout(_)
        | Failure::Vec(_)
        | Failure::HashMap(_)
        | Failure::BTree(_) => {
            unreachable!("pure declaration has no allocation observer or string rule")
        }
    }
}

#[derive(Debug)]
pub(super) enum OwnerFailure<E> {
    Binding(crate::FunctionBindingError),
    Construction(Failure<E>),
}
impl<E> From<crate::FunctionBindingError> for OwnerFailure<E> {
    fn from(error: crate::FunctionBindingError) -> Self {
        Self::Binding(error)
    }
}
impl<E> From<Failure<E>> for OwnerFailure<E> {
    fn from(error: Failure<E>) -> Self {
        Self::Construction(error)
    }
}
impl OwnerFailure<std::convert::Infallible> {
    pub(super) fn into_pure(self) -> crate::FunctionBindingError {
        match self {
            Self::Binding(error) => error,
            Self::Construction(error) => crate::FunctionBindingError::from(pure_value(error)),
        }
    }
}

impl<E> From<novarocks_type_contract::ValueTypeError> for OwnerFailure<E> {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Binding(error.into())
    }
}

pub(super) use crate::FunctionBindingConstructionResource as ConstructionResource;
#[derive(Debug)]
pub(super) enum ResolutionFailure<H> {
    Resolution(crate::FunctionResolutionError),
    Creator(H),
    Resource(ConstructionResource),
}
impl<H> From<crate::FunctionResolutionError> for ResolutionFailure<H> {
    fn from(error: crate::FunctionResolutionError) -> Self {
        Self::Resolution(error)
    }
}
impl<H> From<novarocks_type_contract::CompileControlError> for ResolutionFailure<H> {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Resolution(crate::FunctionResolutionError::Control(error))
    }
}
impl ResolutionFailure<std::convert::Infallible> {
    pub(super) fn into_pure(self) -> crate::FunctionResolutionError {
        match self {
            Self::Resolution(error) => error,
            Self::Creator(never) => match never {},
            Self::Resource(_) => unreachable!("pure resolution has no construction observer"),
        }
    }
}
pub(super) fn borrow_creator<'borrow, H>(
    port: &'borrow mut Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Option<&'borrow mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>> {
    match port {
        Some(scope) => Some(&mut **scope),
        None => None,
    }
}
trait ScopedFailure<H>: From<novarocks_type_contract::CompileControlError> {
    fn creator(error: H) -> Self;
    fn source(error: novarocks_type_contract::ExecutionValueTypeError) -> Self;
    fn rule(error: String) -> Self;
    fn resource(error: ConstructionResource) -> Self;
}
impl<H> ScopedFailure<H> for ResolutionFailure<H> {
    fn creator(error: H) -> Self {
        Self::Creator(error)
    }
    fn source(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Resolution(crate::FunctionResolutionError::BadSignature(
            novarocks_type_contract::ValueTypeError::from(error).to_string(),
        ))
    }
    fn rule(error: String) -> Self {
        Self::Resolution(crate::FunctionResolutionError::BadSignature(error))
    }
    fn resource(error: ConstructionResource) -> Self {
        Self::Resource(error)
    }
}
impl<H> ScopedFailure<H> for crate::FunctionBindingCreationFailure<H> {
    fn creator(error: H) -> Self {
        Self::Creator(error)
    }
    fn source(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Binding(novarocks_type_contract::ValueTypeError::from(error).into())
    }
    fn rule(error: String) -> Self {
        Self::Binding(crate::FunctionBindingError::InvalidBinding(error.into()))
    }
    fn resource(error: ConstructionResource) -> Self {
        Self::Resource(error)
    }
}
fn with_scope_creation<T, H, F: ScopedFailure<H>>(
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    body: impl FnOnce(&mut ScalarConstruction<'_, F>) -> Result<T, Failure<F>>,
) -> Result<T, F> {
    let result = match creation {
        None => body(&mut ScalarConstruction::new(
            ValueTypeStorageLifetime::pure(),
            None,
        )),
        Some(creation) => {
            let storage = creation.storage_lifetime();
            let mut observe = |request| -> Result<(), F> {
                match request {
                    ValueConstructionRequest::Work(units) => {
                        for _ in 0..units {
                            work.step()?;
                        }
                        Ok(())
                    }
                    ValueConstructionRequest::Exact(layout) => {
                        work.flush()?;
                        creation.request_layout(layout).map_err(F::creator)
                    }
                    ValueConstructionRequest::OpaqueUpper(bytes) => {
                        work.flush()?;
                        creation
                            .observe_opaque_upper_bound(bytes)
                            .map_err(F::creator)
                    }
                }
            };
            body(&mut ScalarConstruction::new(storage, Some(&mut observe)))
        }
    };
    result.map_err(|error| match error {
        Failure::ValueType(error) => F::source(error),
        Failure::Rule(error) => F::rule(error),
        Failure::Observer(error) => error,
        Failure::Layout(error) => F::resource(ConstructionResource::Layout(error)),
        Failure::Vec(error) => F::resource(ConstructionResource::Vec(error)),
        Failure::HashMap(error) => F::resource(ConstructionResource::HashMap(error)),
        Failure::BTree(error) => F::resource(ConstructionResource::BTree(error)),
    })
}
pub(super) fn with_resolution_creation<T, H>(
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    body: impl FnOnce(
        &mut ScalarConstruction<'_, ResolutionFailure<H>>,
    ) -> Result<T, Failure<ResolutionFailure<H>>>,
) -> Result<T, ResolutionFailure<H>> {
    with_scope_creation(creation, work, body)
}
pub(super) fn with_binding_creation<T, H>(
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    body: impl FnOnce(
        &mut ScalarConstruction<'_, crate::FunctionBindingCreationFailure<H>>,
    ) -> Result<T, Failure<crate::FunctionBindingCreationFailure<H>>>,
) -> Result<T, crate::FunctionBindingCreationFailure<H>> {
    with_scope_creation(creation, work, body)
}

pub(super) fn into_binding_creation<H>(
    error: ResolutionFailure<H>,
) -> crate::FunctionBindingCreationFailure<H> {
    match error {
        ResolutionFailure::Resolution(error) => crate::FunctionBindingCreationFailure::Binding(
            super::catalogue::binding_resolution_error(error),
        ),
        ResolutionFailure::Creator(error) => crate::FunctionBindingCreationFailure::Creator(error),
        ResolutionFailure::Resource(error) => {
            crate::FunctionBindingCreationFailure::Resource(error)
        }
    }
}

pub(super) fn binding_owner_failure<H>(
    error: OwnerFailure<crate::FunctionBindingCreationFailure<H>>,
) -> Failure<crate::FunctionBindingCreationFailure<H>> {
    match error {
        OwnerFailure::Binding(error) => Failure::Observer(error.into()),
        OwnerFailure::Construction(error) => error,
    }
}
