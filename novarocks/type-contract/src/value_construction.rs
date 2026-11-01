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

//! Construction events from the original logical/physical type authors.
//! Pure callers carry no observer. Events are operation requests, not an
//! allocator ledger, source invoice, retained precision or allocation grant.
use crate::owned_resources::{
    layout::{self, LayoutResourceError},
    vec,
};
use crate::{ControlResourceError, ExecutionValueTypeError};
use std::alloc::Layout;

#[derive(Clone, Copy, Debug)]
pub enum ValueConstructionRequest {
    Exact(Layout),
    /// Work in the original numerical constructor projection, before a request.
    Work(usize),
    /// The original locked collection's cumulative scratch-operation upper.
    OpaqueUpper(usize),
}
#[derive(Debug)]
pub enum ValueConstructionFailure<E> {
    ValueType(ExecutionValueTypeError),
    Rule(String),
    Observer(E),
    Layout(LayoutResourceError),
    Vec(ControlResourceError),
    HashMap(crate::owned_resources::hashmap::HashMapResourceError),
    BTree(crate::owned_resources::btree::BTreeResourceError),
}
impl<E> From<ExecutionValueTypeError> for ValueConstructionFailure<E> {
    fn from(value: ExecutionValueTypeError) -> Self {
        Self::ValueType(value)
    }
}
impl<E> From<crate::ValueTypeError> for ValueConstructionFailure<E> {
    fn from(error: crate::ValueTypeError) -> Self {
        Self::ValueType(error.into())
    }
}
impl<E> From<LayoutResourceError> for ValueConstructionFailure<E> {
    fn from(value: LayoutResourceError) -> Self {
        Self::Layout(value)
    }
}
impl<E> From<ControlResourceError> for ValueConstructionFailure<E> {
    fn from(value: ControlResourceError) -> Self {
        Self::Vec(value)
    }
}
impl<E> From<crate::owned_resources::hashmap::HashMapResourceError>
    for ValueConstructionFailure<E>
{
    fn from(error: crate::owned_resources::hashmap::HashMapResourceError) -> Self {
        Self::HashMap(error)
    }
}
impl<E> From<crate::owned_resources::btree::BTreeResourceError> for ValueConstructionFailure<E> {
    fn from(error: crate::owned_resources::btree::BTreeResourceError) -> Self {
        Self::BTree(error)
    }
}
impl ValueConstructionFailure<std::convert::Infallible> {
    pub(crate) fn into_pure(self) -> ExecutionValueTypeError {
        match self {
            Self::ValueType(error) => error,
            Self::Observer(never) => match never {},
            Self::Rule(_) | Self::Layout(_) | Self::Vec(_) | Self::HashMap(_) | Self::BTree(_) => {
                unreachable!("pure construction has no allocation model observer")
            }
        }
    }
}

pub(crate) struct ValueConstruction<'a, E> {
    observer: Option<&'a mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>>,
}
impl<'a, E> ValueConstruction<'a, E> {
    pub(crate) fn new(
        observer: Option<&'a mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>>,
    ) -> Self {
        Self { observer }
    }
    pub(crate) fn observer(
        &mut self,
    ) -> Option<&mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>> {
        match &mut self.observer {
            Some(observer) => Some(&mut **observer),
            None => None,
        }
    }
    pub(crate) fn table<K, V>(&mut self, count: usize) -> Result<(), ValueConstructionFailure<E>> {
        if let Some(observer) = self.observer.as_deref_mut() {
            let facts = crate::owned_resources::hashmap::fresh_table_layout::<K, V>(count)?;
            if let Some(layout) = facts.layout {
                observer(ValueConstructionRequest::Exact(layout))
                    .map_err(ValueConstructionFailure::Observer)?;
            }
        }
        Ok(())
    }
    pub(crate) fn cloned_data_type(
        &mut self,
        ty: &arrow_schema::DataType,
    ) -> Result<arrow_schema::DataType, ValueConstructionFailure<E>> {
        self.cloned_type_backing(ty)?;
        Ok(ty.clone())
    }
    pub(crate) fn cloned_type_backing(
        &mut self,
        ty: &arrow_schema::DataType,
    ) -> Result<(), ValueConstructionFailure<E>> {
        if let arrow_schema::DataType::Dictionary(key, value) = ty {
            // Arrow's derived DataType::clone deep-copies only these Box roots;
            // Field/Fields/UnionFields/timezone handles clone their existing Arc.
            self.exact(|| Ok(Layout::new::<arrow_schema::DataType>()))?;
            self.cloned_type_backing(key)?;
            self.exact(|| Ok(Layout::new::<arrow_schema::DataType>()))?;
            self.cloned_type_backing(value)?;
        }
        Ok(())
    }
    pub(crate) fn metadata_conversion(
        &mut self,
        entries: usize,
        max_key_bytes: usize,
    ) -> Result<(), ValueConstructionFailure<E>> {
        let Some(observer) = self.observer.as_deref_mut() else {
            return Ok(());
        };
        let conversion =
            crate::owned_resources::arrow_metadata::hashmap_conversion_requests(entries)?;
        for layout in [
            conversion.entries_vector,
            conversion.sort_scratch,
            conversion.map_arc,
        ]
        .into_iter()
        .flatten()
        {
            observer(ValueConstructionRequest::Exact(layout))
                .map_err(ValueConstructionFailure::Observer)?;
        }
        for _ in 0..conversion.tree_nodes {
            observer(ValueConstructionRequest::OpaqueUpper(
                conversion.tree_node.size(),
            ))
            .map_err(ValueConstructionFailure::Observer)?;
        }
        let work = crate::owned_resources::arrow_metadata::hashmap_conversion_work_upper_bound(
            entries,
            max_key_bytes,
        )?;
        observer(ValueConstructionRequest::Work(work)).map_err(ValueConstructionFailure::Observer)
    }
    pub(crate) fn cloned_annotations(
        &mut self,
        capacity: usize,
    ) -> Result<(), ValueConstructionFailure<E>> {
        if self.observer.is_some() && capacity != 0 {
            // Strong fixed binding inputs are validated metadata-free ValueLayouts.
            // Sharing Metadata does not establish retention of arbitrary external annotations.
            return Err(ValueConstructionFailure::Layout(
                LayoutResourceError::SourceModel,
            ));
        }
        Ok(())
    }
    pub(crate) fn collected_box_slice<T>(
        &mut self,
        count: usize,
    ) -> Result<(), ValueConstructionFailure<E>> {
        if let Some(observer) = self.observer.as_deref_mut() {
            let upper = vec::original_fresh_push_requests::<T, ValueConstructionFailure<E>>(
                count,
                &mut || {
                    observer(ValueConstructionRequest::Work(1))
                        .map_err(ValueConstructionFailure::Observer)
                },
            )?;
            if upper != 0 {
                observer(ValueConstructionRequest::OpaqueUpper(upper))
                    .map_err(ValueConstructionFailure::Observer)?;
            }
        }
        // A successful Result collection can keep spare amortized capacity.
        // The original Vec->Box shrink, when needed, is separately bounded.
        self.array::<T>(count)
    }
    pub(crate) fn exact(
        &mut self,
        layout: impl FnOnce() -> Result<Layout, LayoutResourceError>,
    ) -> Result<(), ValueConstructionFailure<E>> {
        if let Some(observer) = self.observer.as_deref_mut() {
            observer(ValueConstructionRequest::Exact(layout()?))
                .map_err(ValueConstructionFailure::Observer)?;
        }
        Ok(())
    }
    pub(crate) fn name(&mut self, bytes: usize) -> Result<(), ValueConstructionFailure<E>> {
        if bytes != 0 {
            self.exact(|| Layout::array::<u8>(bytes).map_err(|_| LayoutResourceError::Arithmetic))?;
        }
        Ok(())
    }
    pub(crate) fn array<T>(&mut self, count: usize) -> Result<(), ValueConstructionFailure<E>> {
        if count != 0 {
            self.exact(|| Layout::array::<T>(count).map_err(|_| LayoutResourceError::Arithmetic))?;
        }
        Ok(())
    }
    pub(crate) fn arc_slice<T>(&mut self, count: usize) -> Result<(), ValueConstructionFailure<E>> {
        self.exact(|| {
            layout::arc_layout(
                Layout::array::<T>(count).map_err(|_| LayoutResourceError::Arithmetic)?,
            )
        })
    }
    /// Cover Arrow 60.1's unzip vectors, try_new pair vector and final Arc.
    pub(crate) fn collected_union_fields(
        &mut self,
        count: usize,
    ) -> Result<(), ValueConstructionFailure<E>> {
        if let Some(observer) = self.observer.as_deref_mut() {
            let work = count
                .checked_mul(3)
                .ok_or(LayoutResourceError::Arithmetic)?;
            observer(ValueConstructionRequest::Work(work))
                .map_err(ValueConstructionFailure::Observer)?;
            let facts = crate::owned_resources::arrow_union::from_iterator_requests(count)
                .map_err(|error| match error {
                    crate::owned_resources::arrow_union::UnionFieldsRequestError::Vec(source) => {
                        ValueConstructionFailure::Vec(source)
                    }
                    crate::owned_resources::arrow_union::UnionFieldsRequestError::Layout(
                        source,
                    ) => ValueConstructionFailure::Layout(source),
                })?;
            if facts.scratch_bytes_upper != 0 {
                observer(ValueConstructionRequest::OpaqueUpper(
                    facts.scratch_bytes_upper,
                ))
                .map_err(ValueConstructionFailure::Observer)?;
            }
            observer(ValueConstructionRequest::Exact(facts.final_arc))
                .map_err(ValueConstructionFailure::Observer)?;
        }
        Ok(())
    }
    /// Fields FromIterator consumes an Arc slice. The
    /// original Rust Arc collector's possible Vec growth is a conservative
    /// scratch-operation upper, separate from its final Arc slice request.
    pub(crate) fn collected_arc_slice<T>(
        &mut self,
        count: usize,
    ) -> Result<(), ValueConstructionFailure<E>> {
        if let Some(observer) = self.observer.as_deref_mut() {
            let upper = vec::original_fresh_push_requests::<T, ValueConstructionFailure<E>>(
                count,
                &mut || {
                    observer(ValueConstructionRequest::Work(1))
                        .map_err(ValueConstructionFailure::Observer)
                },
            )?;
            if upper != 0 {
                observer(ValueConstructionRequest::OpaqueUpper(upper))
                    .map_err(ValueConstructionFailure::Observer)?;
            }
        }
        self.arc_slice::<T>(count)
    }
}

/// Observe the existing Arrow clone's private Box roots before the clone.
/// None preserves the original pure clone without introducing an observer walk.
pub fn clone_data_type_with_construction<E>(
    ty: &arrow_schema::DataType,
    observer: Option<&mut dyn FnMut(ValueConstructionRequest) -> Result<(), E>>,
) -> Result<arrow_schema::DataType, ValueConstructionFailure<E>> {
    match observer {
        None => Ok(ty.clone()),
        Some(observer) => ValueConstruction::new(Some(observer)).cloned_data_type(ty),
    }
}

#[cfg(test)]
mod union_collection_tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn original_union_scratch_refusal_is_primary_before_field_construction() {
        let source = arrow_schema::DataType::Union(
            arrow_schema::UnionFields::try_new(
                [0],
                [arrow_schema::Field::new(
                    "original",
                    arrow_schema::DataType::Int64,
                    true,
                )],
            )
            .unwrap(),
            arrow_schema::UnionMode::Sparse,
        );
        let cause = Arc::new(73);
        let mut saw_scratch = false;
        let result = crate::ValueLayout::try_new_with_construction(
            &source,
            crate::ValueTypeStorageLifetime::pure(),
            Some(&mut |event| {
                assert!(
                    !saw_scratch,
                    "no callback after the original scratch refusal"
                );
                if matches!(event, ValueConstructionRequest::OpaqueUpper(_)) {
                    saw_scratch = true;
                    return Err(cause.clone());
                }
                Ok(())
            }),
        );
        let Err(ValueConstructionFailure::Observer(actual)) = result else {
            panic!("original observer refusal")
        };
        assert!(Arc::ptr_eq(&actual, &cause));
        assert!(saw_scratch);
    }
}
