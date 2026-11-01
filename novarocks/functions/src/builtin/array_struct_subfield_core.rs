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
use arrow_array::{Array, ArrayRef, ListArray, StringArray, StructArray, UInt32Array};
use arrow_select::take::take;
use std::sync::Arc;

use super::collection_arrow::{self, ArrowSite};
use crate::KernelFailure;
use std::fmt;

/// Exact original operation inputs. These observations do not grant memory.
pub(crate) enum ProjectionOperation<'a> {
    FieldNameCopy {
        text: &'a str,
    },
    Indices,
    IndicesArray,
    Take {
        values: &'a ArrayRef,
        indices: &'a UInt32Array,
    },
    OutputField,
    ListResult,
}
/// The caller owns operation authorization and diagnostics. A port must keep
/// actual grants through the backing's real Drop; this trait is not a wallet.
pub(crate) trait ProjectionPort {
    type Failure: From<KernelFailure>;
    fn value_error(&mut self, original: fmt::Arguments<'_>) -> Self::Failure;
    fn step(&mut self) -> Result<(), Self::Failure>;
    fn before_operation(&mut self, original: ProjectionOperation<'_>) -> Result<(), Self::Failure>;
    /// Only a successful operation observes its completion boundary.
    fn after_operation(&mut self) -> Result<(), Self::Failure>;
}
struct RequiredPort;
impl ProjectionPort for RequiredPort {
    type Failure = KernelFailure;
    fn value_error(&mut self, original: fmt::Arguments<'_>) -> KernelFailure {
        KernelFailure::from_domain(RequiredFieldNameValue(
            novarocks_failure_contract::BoundedDetail::truncating_display(original),
        ))
    }
    fn step(&mut self) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn before_operation(
        &mut self,
        operation: ProjectionOperation<'_>,
    ) -> Result<(), KernelFailure> {
        if let ProjectionOperation::Take { values, indices } = operation {
            crate::selected_copy::preflight_original_take(
                values.as_ref(),
                crate::selected_copy::CopyIndexView::UInt32(indices),
                |_| Ok(()),
            )
            .map_err(crate::selected_copy::CopyError::into_kernel)?;
        }
        Ok(())
    }
    fn after_operation(&mut self) -> Result<(), KernelFailure> {
        Ok(())
    }
}
/// One required invocation over the original physical operation author.
pub fn project(
    input: &ArrayRef,
    field_name_arr: &ArrayRef,
    output_type: Option<&arrow_schema::DataType>,
) -> Result<ArrayRef, KernelFailure> {
    project_with_port(input, field_name_arr, output_type, &mut RequiredPort)
}
/// ONE original computation; the port changes ownership/control, never math.
pub(crate) fn project_with_port<P: ProjectionPort>(
    input: &ArrayRef,
    field_name_arr: &ArrayRef,
    output_type: Option<&arrow_schema::DataType>,
    port: &mut P,
) -> Result<ArrayRef, P::Failure> {
    project_at_with_port(input, field_name_arr, output_type, None, port)
}

pub(crate) fn project_prepared_with_port<P: ProjectionPort>(
    input: &ArrayRef,
    field_name_arr: &ArrayRef,
    output_type: &arrow_schema::DataType,
    field_ordinal: usize,
    port: &mut P,
) -> Result<ArrayRef, P::Failure> {
    project_at_with_port(
        input,
        field_name_arr,
        Some(output_type),
        Some(field_ordinal),
        port,
    )
}

enum InspectionFailure<E> {
    Binding(crate::FunctionBindingError),
    Type(novarocks_type_contract::ValueTypeError),
    Observer(E),
}
impl<E> From<crate::FunctionBindingError> for InspectionFailure<E> {
    fn from(source: crate::FunctionBindingError) -> Self {
        Self::Binding(source)
    }
}
impl<E> From<novarocks_type_contract::ValueTypeError> for InspectionFailure<E> {
    fn from(source: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Type(source)
    }
}
fn inspection_failure<P: ProjectionPort>(source: InspectionFailure<P::Failure>) -> P::Failure {
    match source {
        InspectionFailure::Observer(source) => source,
        InspectionFailure::Binding(source) => {
            super::struct_field_resolution::into_kernel(source).into()
        }
        InspectionFailure::Type(source) => KernelFailure::from(source).into(),
    }
}

fn project_at_with_port<P: ProjectionPort>(
    input: &ArrayRef,
    field_name_arr: &ArrayRef,
    output_type: Option<&arrow_schema::DataType>,
    prepared_ordinal: Option<usize>,
    port: &mut P,
) -> Result<ArrayRef, P::Failure> {
    let list = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        P::Failure::from(collection_arrow::contract(format_args!(
            "__array_struct_subfield expects ListArray, got {:?}",
            input.data_type()
        )))
    })?;
    let struct_values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            P::Failure::from(collection_arrow::contract(format_args!(
                "__array_struct_subfield expects list values to be StructArray, got {:?}",
                list.values().data_type()
            )))
        })?;
    let field_name = parse_constant_field_name(field_name_arr.as_ref(), port)?;
    let field_idx = if let Some(ordinal) = prepared_ordinal {
        let Some(field) = struct_values.fields().get(ordinal) else {
            return Err(crate::kernel_control::invalid(
                "ARRAY projection prepared field ordinal is outside its source",
            )
            .into());
        };
        port.step()?;
        if !field.name().eq_ignore_ascii_case(&field_name) {
            return Err(port.value_error(format_args!(
                "__array_struct_subfield field '{}' does not exist",
                field_name,
            )));
        }
        ordinal
    } else {
        super::struct_field_resolution::resolve_struct_field_ordinal_observed::<
            InspectionFailure<P::Failure>,
        >(struct_values.fields(), &field_name, || {
            port.step().map_err(InspectionFailure::Observer)
        })
        .map_err(inspection_failure::<P>)?
        .ok_or_else(|| {
            port.value_error(format_args!(
                "__array_struct_subfield field '{}' does not exist",
                field_name,
            ))
        })?
    };
    let field_col = struct_values.column(field_idx);

    port.before_operation(ProjectionOperation::Indices)?;
    let mut indices = Vec::new();
    indices
        .try_reserve_exact(struct_values.len())
        .map_err(|source| {
            P::Failure::from(KernelFailure::from_domain(
                crate::allocation_failure::AllocationRefusal::new(
                    "array struct projection indices",
                    source,
                ),
            ))
        })?;
    port.after_operation()?;
    for row in 0..struct_values.len() {
        port.step()?;
        if struct_values.is_null(row) {
            indices.push(None);
        } else {
            let idx = u32::try_from(row).map_err(|source| {
                P::Failure::from(KernelFailure::from_domain(ProjectionIndexFailure {
                    row,
                    source,
                }))
            })?;
            indices.push(Some(idx));
        }
    }

    port.before_operation(ProjectionOperation::IndicesArray)?;
    let indices = UInt32Array::from(indices);
    port.after_operation()?;
    port.before_operation(ProjectionOperation::Take {
        values: field_col,
        indices: &indices,
    })?;
    let out_values = take(field_col.as_ref(), &indices, None).map_err(|source| {
        P::Failure::from(collection_arrow::arrow_failure(
            "__array_struct_subfield",
            ArrowSite::Take,
            source,
        ))
    })?;
    port.after_operation()?;
    let output_field = match output_type {
        Some(arrow_schema::DataType::List(field)) => field.clone(),
        Some(other) => {
            return Err(P::Failure::from(collection_arrow::contract(format_args!(
                "__array_struct_subfield output type must be List, got {other:?}"
            ))));
        }
        None => {
            port.before_operation(ProjectionOperation::OutputField)?;
            let field = Arc::new(
                struct_values.fields()[field_idx]
                    .as_ref()
                    .clone()
                    .with_name("item")
                    .with_nullable(true),
            );
            port.after_operation()?;
            field
        }
    };
    if !novarocks_type_contract::arrow_data_types_exact_observed::<InspectionFailure<P::Failure>>(
        out_values.data_type(),
        output_field.data_type(),
        || port.step().map_err(InspectionFailure::Observer),
    )
    .map_err(inspection_failure::<P>)?
    {
        return Err(crate::kernel_control::invalid(
            "ARRAY projection field differs from its prepared result declaration",
        )
        .into());
    }

    port.before_operation(ProjectionOperation::ListResult)?;
    let out = ListArray::try_new(
        output_field,
        list.offsets().clone(),
        out_values,
        list.nulls().cloned(),
    )
    .map_err(|source| {
        P::Failure::from(collection_arrow::arrow_failure(
            "__array_struct_subfield",
            ArrowSite::Construct,
            source,
        ))
    })?;
    let out = Arc::new(out) as ArrayRef;
    port.after_operation()?;
    Ok(out)
}
fn parse_constant_field_name<P: ProjectionPort>(
    field_name_arr: &dyn Array,
    port: &mut P,
) -> Result<String, P::Failure> {
    let arr = field_name_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            P::Failure::from(collection_arrow::contract(format_args!(
                "__array_struct_subfield field-name argument must be VARCHAR"
            )))
        })?;
    if arr.is_empty() {
        return Err(port.value_error(format_args!(
            "__array_struct_subfield field-name argument is empty"
        )));
    }
    let first = if arr.is_null(0) {
        return Err(port.value_error(format_args!(
            "__array_struct_subfield field-name argument must be non-null"
        )));
    } else {
        arr.value(0)
    };
    for i in 1..arr.len() {
        port.step()?;
        if arr.is_null(i) || arr.value(i) != first {
            return Err(port.value_error(format_args!(
                "__array_struct_subfield field-name argument must be constant"
            )));
        }
    }
    port.before_operation(ProjectionOperation::FieldNameCopy { text: first })?;
    let mut result = String::new();
    result.try_reserve_exact(first.len()).map_err(|source| {
        P::Failure::from(KernelFailure::from_domain(
            crate::allocation_failure::AllocationRefusal::new("array field-name copy", source),
        ))
    })?;
    result.push_str(first);
    port.after_operation()?;
    Ok(result)
}

#[cfg(test)]
#[path = "array_projection_operation_before_tests.rs"]
mod array_projection_operation_before_tests;

#[derive(Debug)]
struct RequiredFieldNameValue(novarocks_failure_contract::BoundedDetail);
impl fmt::Display for RequiredFieldNameValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RequiredFieldNameValue {}
impl novarocks_failure_contract::FailureProjection for RequiredFieldNameValue {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(
            &crate::DataFailureReason::InvalidValue,
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for RequiredFieldNameValue {}
#[derive(Debug)]
struct ProjectionIndexFailure {
    row: usize,
    source: std::num::TryFromIntError,
}
impl fmt::Display for ProjectionIndexFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "__array_struct_subfield index {} exceeds UInt32 range",
            self.row
        )
    }
}
impl std::error::Error for ProjectionIndexFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for ProjectionIndexFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let code = std::num::NonZeroU16::new(387).unwrap();
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, code),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::KERNEL,
                code,
            },
            required: u64::try_from(self.row).ok(),
            extent_limit: Some(u64::from(u32::MAX)),
            representable_prefix: None,
        })
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ProjectionIndexFailure {}

#[cfg(test)]
mod required_projection_properties {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field};
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    fn source() -> ArrayRef {
        let values = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("id", DataType::Int32, false))].into(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
            Some(NullBuffer::from(vec![false, true])),
        ));
        Arc::new(ListArray::new(
            Arc::new(Field::new("entry", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 2].into()),
            values,
            None,
        ))
    }
    fn sdk(failure: &KernelFailure) -> &arrow_schema::ArrowError {
        let mut source: &dyn Error = failure;
        loop {
            if let Some(sdk) = source.downcast_ref::<arrow_schema::ArrowError>() {
                return sdk;
            }
            source = source
                .source()
                .expect("actual SDK source must remain available");
        }
    }
    #[test]
    fn real_constructor_refusal_is_outer_and_never_a_field_name_value_failure() {
        let input = source();
        let weak = Arc::downgrade(&input);
        let names: ArrayRef = Arc::new(StringArray::from(vec!["id"]));
        let target = DataType::List(Arc::new(Field::new("selected", DataType::Int32, false)));
        let failure = project(&input, &names, Some(&target)).unwrap_err();
        drop(input);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 375);
        assert!(matches!(
            sdk(&failure),
            arrow_schema::ArrowError::InvalidArgumentError(_)
        ));
    }
    #[test]
    fn incompatible_output_declaration_is_rejected_without_implicit_cast_or_input_owner() {
        let input = source();
        let weak = Arc::downgrade(&input);
        let names: ArrayRef = Arc::new(StringArray::from(vec!["id"]));
        let target = DataType::List(Arc::new(Field::new(
            "selected",
            DataType::Struct(vec![Arc::new(Field::new("x", DataType::Int32, true))].into()),
            true,
        )));
        let failure = project(&input, &names, Some(&target)).unwrap_err();
        drop(input);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert!(failure.to_string().contains("prepared result declaration"));
    }
}
