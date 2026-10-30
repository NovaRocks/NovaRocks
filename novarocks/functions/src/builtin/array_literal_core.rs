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
//! ONE original ArrayExpr construction author; no arena, type inference or decoder.
use arrow_array::{ArrayRef, ListArray, make_array, new_empty_array, new_null_array};
use arrow_buffer::OffsetBuffer;
use arrow_data::transform::MutableArrayData;
use arrow_schema::{DataType, Field, Fields};
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub enum CollectionObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum ArrayLiteralError {
    Operation(crate::KernelFailure),
    MissingOutput,
    NotList(novarocks_failure_contract::BoundedDetail),
    ElementLength { expected: usize, actual: usize },
    OffsetOverflow { elements: usize, rows: usize },
    BackingExtent { required: u128 },
}
impl std::fmt::Display for ArrayLiteralError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Operation(source) => source.fmt(f),
            Self::MissingOutput => f.write_str("array_expr missing output type"),
            Self::NotList(ty) => write!(f, "array_expr output type must be List, got {ty}"),
            Self::ElementLength { expected, actual } => write!(
                f,
                "array_expr element length mismatch: expected {expected}, got {actual}"
            ),
            Self::OffsetOverflow { elements, rows } => write!(
                f,
                "array_expr offset overflow: elements={elements} rows={rows}"
            ),
            Self::BackingExtent { required } => write!(
                f,
                "array_expr backing requires {required} bytes, maximum {}",
                isize::MAX
            ),
        }
    }
}
impl std::error::Error for ArrayLiteralError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ArrayLiteralError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, ResourceDimension, ResourceFacts, ResourceScope, domain,
        };
        let (class, code) = match self {
            Self::Operation(source) => {
                return novarocks_failure_contract::FailureProjection::failure_fact(source);
            }
            Self::MissingOutput => (FailureClass::Invariant, 201),
            Self::NotList(_) => (FailureClass::Invariant, 202),
            Self::ElementLength { .. } => (FailureClass::Invariant, 203),
            Self::OffsetOverflow { .. } => (FailureClass::Representation, 204),
            Self::BackingExtent { .. } => (FailureClass::Resource, 205),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        match self {
            Self::OffsetOverflow { elements, rows } => {
                let single = *elements as u128 > i32::MAX as u128;
                fact.with_representation(RepresentationFacts {
                    boundary: if single {
                        RepresentationBoundary::MinimumUnit
                    } else {
                        RepresentationBoundary::SingleOutput
                    },
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: std::num::NonZeroU16::new(33).unwrap(),
                    },
                    required: u64::try_from(if single {
                        *elements as u128
                    } else {
                        *elements as u128 * *rows as u128
                    })
                    .ok(),
                    extent_limit: Some(i32::MAX as u64),
                    representable_prefix: None,
                })
            }
            Self::BackingExtent { required } => fact.with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Request),
                dimension: ResourceDimension::Bytes,
                requested: u64::try_from(*required).ok(),
                required_growth: None,
                limit: Some(isize::MAX as u64),
                live: None,
                committed: None,
            }),
            _ => fact,
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ArrayLiteralError {}
#[derive(Debug)]
pub enum ConstructionFailure<E> {
    Domain(ArrayLiteralError),
    Caller(E),
}
fn preflight_geometry(rows: usize, elements: usize) -> Result<(), ArrayLiteralError> {
    let slots = rows as u128 * elements as u128;
    if slots > i32::MAX as u128 {
        return Err(ArrayLiteralError::OffsetOverflow { elements, rows });
    }
    let offsets = (rows as u128 + 1) * std::mem::size_of::<i32>() as u128;
    // These are the actual original temporary and output vectors, not a grant.
    for required in [
        offsets,
        elements as u128 * std::mem::size_of::<ArrayRef>() as u128,
        elements as u128 * std::mem::size_of::<arrow_data::ArrayData>() as u128,
        elements as u128 * std::mem::size_of::<&arrow_data::ArrayData>() as u128,
    ] {
        if required > isize::MAX as u128 {
            return Err(ArrayLiteralError::BackingExtent { required });
        }
    }
    Ok(())
}
fn observe<E>(
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    event: CollectionObservation,
) -> Result<(), ConstructionFailure<E>> {
    observer(event).map_err(ConstructionFailure::Caller)
}
fn relax_map_entry_nullability<E>(
    target_type: &DataType,
    source_type: &DataType,
    observe: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<DataType, E> {
    let (DataType::Map(target_map_field, ordered), DataType::Map(source_map_field, _)) =
        (target_type, source_type)
    else {
        return Ok(target_type.clone());
    };
    let DataType::Struct(target_entries) = target_map_field.data_type() else {
        return Ok(target_type.clone());
    };
    let DataType::Struct(source_entries) = source_map_field.data_type() else {
        return Ok(target_type.clone());
    };
    if target_entries.len() != source_entries.len() {
        return Ok(target_type.clone());
    }

    let mut adjusted_entries = target_entries.iter().cloned().collect::<Vec<_>>();
    let mut changed = false;
    for idx in 0..adjusted_entries.len() {
        observe(CollectionObservation::Step)?;
        if !target_entries[idx].is_nullable() && source_entries[idx].is_nullable() {
            adjusted_entries[idx] = Arc::new(Field::new(
                target_entries[idx].name(),
                target_entries[idx].data_type().clone(),
                true,
            ));
            changed = true;
        }
    }
    if !changed {
        return Ok(target_type.clone());
    }

    Ok(DataType::Map(
        Arc::new(Field::new(
            target_map_field.name(),
            DataType::Struct(Fields::from(adjusted_entries)),
            target_map_field.is_nullable(),
        )),
        *ordered,
    ))
}

/// Incremental original admission keeps child-evaluation order in the shell.
/// Every child is checked immediately before a later child can be evaluated.
pub struct ArrayLiteralInputs {
    field: Arc<Field>,
    element_type: DataType,
    num_rows: usize,
    num_elements: usize,
    raw_arrays: Vec<ArrayRef>,
}
impl ArrayLiteralInputs {
    pub fn from_output<E>(
        output_type: Option<&DataType>,
        num_rows: usize,
        num_elements: usize,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<Self, ConstructionFailure<E>> {
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let output_type = output_type.cloned().ok_or(ConstructionFailure::Domain(
            ArrayLiteralError::MissingOutput,
        ))?;
        let (field, element_type) = match output_type {
            DataType::List(field) => {
                let element_type = field.data_type().clone();
                (field, element_type)
            }
            other => {
                return Err(ConstructionFailure::Domain(ArrayLiteralError::NotList(
                    novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
                        "{other:?}"
                    )),
                )));
            }
        };
        preflight_geometry(num_rows, num_elements).map_err(ConstructionFailure::Domain)?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let raw_arrays = Vec::with_capacity(num_elements);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        Ok(Self {
            field,
            element_type,
            num_rows,
            num_elements,
            raw_arrays,
        })
    }
    pub fn push_evaluated<E>(
        &mut self,
        array: ArrayRef,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<(), ConstructionFailure<E>> {
        if array.len() != self.num_rows {
            return Err(ConstructionFailure::Domain(
                ArrayLiteralError::ElementLength {
                    expected: self.num_rows,
                    actual: array.len(),
                },
            ));
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        self.element_type =
            relax_map_entry_nullability(&self.element_type, array.data_type(), observer)
                .map_err(ConstructionFailure::Caller)?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        self.raw_arrays.push(array);
        Ok(())
    }
    /// Legacy supplies its ORIGINAL special-cast port. Exact selected inputs
    /// already have the frozen item type, so that port is never invoked there.
    pub fn finish<E>(
        mut self,
        mut special_cast: impl FnMut(&ArrayRef, &DataType) -> Result<ArrayRef, E>,
        observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    ) -> Result<ArrayRef, ConstructionFailure<E>> {
        if self.num_elements == 0 {
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            let mut offsets = Vec::with_capacity(self.num_rows + 1);
            offsets.push(0_i32);
            for _ in 0..self.num_rows {
                observe(observer, CollectionObservation::Step)?;
                offsets.push(0_i32);
            }
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            let values = new_empty_array(&self.element_type);
            let list = ListArray::new(self.field, OffsetBuffer::new(offsets.into()), values, None);
            observe(observer, CollectionObservation::OpaqueBoundary)?;
            return Ok(Arc::new(list));
        }
        if self.field.data_type() != &self.element_type {
            self.field = Arc::new(Field::new(
                self.field.name(),
                self.element_type.clone(),
                self.field.is_nullable(),
            ));
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let mut element_arrays = Vec::with_capacity(self.num_elements);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        for array in self.raw_arrays {
            observe(observer, CollectionObservation::Step)?;
            let array = if array.data_type() == &self.element_type {
                array
            } else if array.data_type() == &DataType::Null && self.element_type != DataType::Null {
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                let out = new_null_array(&self.element_type, self.num_rows);
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                out
            } else {
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                let out = special_cast(&array, &self.element_type)
                    .map_err(ConstructionFailure::Caller)?;
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                out
            };
            element_arrays.push(array);
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let mut offsets = Vec::with_capacity(self.num_rows + 1);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        offsets.push(0_i32);
        let mut current: i64 = 0;
        for _ in 0..self.num_rows {
            observe(observer, CollectionObservation::Step)?;
            current += self.num_elements as i64;
            offsets.push(current as i32);
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let mut data_storage = Vec::with_capacity(element_arrays.len());
        for arr in &element_arrays {
            observe(observer, CollectionObservation::Step)?;
            data_storage.push(arr.to_data());
        }
        let mut data_refs = Vec::with_capacity(data_storage.len());
        for data in &data_storage {
            observe(observer, CollectionObservation::Step)?;
            data_refs.push(data);
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let mut mutable = MutableArrayData::new(data_refs, true, self.num_rows * self.num_elements);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        for row in 0..self.num_rows {
            for idx in 0..element_arrays.len() {
                observe(observer, CollectionObservation::Step)?;
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                mutable.try_extend(idx, row, row + 1).map_err(|source| {
                    ConstructionFailure::Domain(ArrayLiteralError::Operation(
                        super::collection_arrow::arrow_failure(
                            "array_expr",
                            super::collection_arrow::ArrowSite::Extend,
                            source,
                        ),
                    ))
                })?;
                observe(observer, CollectionObservation::OpaqueBoundary)?;
            }
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let values = make_array(mutable.freeze());
        let list = ListArray::new(self.field, OffsetBuffer::new(offsets.into()), values, None);
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        Ok(Arc::new(list))
    }
}

#[cfg(test)]
mod domain_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn array_literal_checks_exact_output_and_backing_extent_without_allocation() {
        assert!(preflight_geometry(1, i32::MAX as usize).is_ok());
        let error = preflight_geometry(1, i32::MAX as usize + 1).unwrap_err();
        assert_eq!(
            error.failure_fact().representation().unwrap().boundary,
            RepresentationBoundary::MinimumUnit
        );
        let error = preflight_geometry(2, i32::MAX as usize).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(
            fact.representation().unwrap().boundary,
            RepresentationBoundary::SingleOutput
        );
        assert_eq!(
            fact.representation().unwrap().required,
            Some(2 * i32::MAX as u64)
        );
        let error = preflight_geometry(usize::MAX, 0).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert_eq!(error.failure_fact().resource().unwrap().committed, None);
    }
    #[test]
    fn refused_cast_keeps_original_cause_without_later_observation() {
        use std::cell::Cell;
        let observed = Cell::new(0);
        let mut observer = |_| {
            observed.set(observed.get() + 1);
            Ok::<(), &'static str>(())
        };
        let output = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let mut inputs =
            ArrayLiteralInputs::from_output(Some(&output), 1, 1, &mut observer).unwrap();
        inputs
            .push_evaluated(
                Arc::new(arrow_array::Int32Array::from(vec![7])),
                &mut observer,
            )
            .unwrap();
        let before = observed.get();
        let error = inputs
            .finish(|_, _| Err("original cast refusal"), &mut observer)
            .unwrap_err();
        assert!(matches!(
            error,
            ConstructionFailure::Caller("original cast refusal")
        ));
        // Entry, vector construction, element step, then pre-cast boundary.
        assert_eq!(observed.get() - before, 4);
    }
    #[test]
    fn failed_extension_retains_actual_arrow_source_without_later_observation() {
        use std::cell::Cell;
        use std::error::Error;
        let observed = Cell::new(0);
        let mut observer = |_| {
            observed.set(observed.get() + 1);
            Ok::<(), &'static str>(())
        };
        let output = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let mut inputs =
            ArrayLiteralInputs::from_output(Some(&output), 1, 1, &mut observer).unwrap();
        inputs
            .push_evaluated(
                Arc::new(arrow_array::Int32Array::from(vec![7])),
                &mut observer,
            )
            .unwrap();
        let before = observed.get();
        // The supplied cast returns a malformed length. The actual extension
        // must reject its source range before any completion observation.
        let error = inputs
            .finish(
                |_, _| Ok(Arc::new(arrow_array::Int64Array::from(Vec::<i64>::new())) as ArrayRef),
                &mut observer,
            )
            .unwrap_err();
        let ConstructionFailure::Domain(error @ ArrayLiteralError::Operation(_)) = error else {
            panic!("expected the retained extension operation failure");
        };
        assert_eq!(observed.get() - before, 15);
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Invariant);
        assert_eq!(fact.reason().code(), 375);
        assert_eq!(fact.representation(), None);
        let mut source = error.source();
        let actual = loop {
            let current = source.expect("the original Arrow source must remain in the chain");
            if let Some(actual) = current.downcast_ref::<arrow_schema::ArrowError>() {
                break actual;
            }
            source = current.source();
        };
        assert!(matches!(
            actual,
            arrow_schema::ArrowError::InvalidArgumentError(_)
        ));
        assert!(actual.to_string().contains("Invalid range 0..1"));
    }

    #[test]
    fn invalid_output_cause_does_not_retain_arrow_field() {
        let field = Arc::new(Field::new("child", DataType::Int32, true));
        let weak = Arc::downgrade(&field);
        let output = DataType::Struct(vec![field].into());
        let error =
            match ArrayLiteralInputs::from_output(Some(&output), 1, 1, &mut |_| Ok::<(), ()>(())) {
                Err(ConstructionFailure::Domain(error)) => error,
                _ => panic!("expected invalid frozen output"),
            };
        drop(output);
        assert!(weak.upgrade().is_none());
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
}
