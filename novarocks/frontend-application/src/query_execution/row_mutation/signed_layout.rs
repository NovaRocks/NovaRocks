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

//! Shared value planning under the signed complete declaration. SQL already
//! converted values; this boundary only expands admitted encodings and restores
//! the signed field metadata after the value layer normalized representation.
use arrow::{
    array::{ArrayRef, make_array},
    datatypes::{DataType, Field, SchemaRef},
    record_batch::RecordBatch,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ValueError, ValueFailure,
    decode::ValueConversionContext,
    hydrate::{HydrateOptions, HydratePlan, HydrateProjection},
};
#[cfg(test)]
use std::sync::Arc;
fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn allocation(source: std::collections::TryReserveError) -> ConnectorError {
    ConnectorError::from_domain(
        ConnectorErrorKind::ResourceExhausted,
        novarocks_functions::allocation_failure::AllocationRefusal::new(
            "signed COW layout metadata",
            source,
        ),
    )
}
#[derive(Debug)]
struct ArrowAssemblyFailure(arrow::error::ArrowError);
impl std::fmt::Display for ArrowAssemblyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for ArrowAssemblyFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for ArrowAssemblyFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow::error::ArrowError;
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match &self.0 {
            ArrowError::MemoryError(_) => (FailureClass::Resource, 6001),
            ArrowError::OffsetOverflowError(_) => (FailureClass::Representation, 6002),
            ArrowError::IoError(..) => (FailureClass::Unexpected, 6003),
            _ => (FailureClass::Invariant, 6004),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
fn arrow_assembly(source: arrow::error::ArrowError) -> ConnectorError {
    ConnectorError::from_domain(ConnectorErrorKind::Internal, ArrowAssemblyFailure(source))
}

pub(super) fn value_error(source: ValueError) -> ConnectorError {
    ConnectorError::from_domain(ConnectorErrorKind::InvalidRequest, source)
}
fn failure(error: ValueFailure<ConnectorError>) -> ConnectorError {
    match error {
        ValueFailure::Caller(error) => error,
        ValueFailure::Value(source) => value_error(source),
    }
}

enum MatchFailure {
    Caller(ConnectorError),
    Type(novarocks_type_contract::ValueTypeError),
}
impl From<novarocks_type_contract::ValueTypeError> for MatchFailure {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Type(error)
    }
}
fn matches_signed(
    declaration: &FunctionValueType,
    field: &Field,
    work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
) -> Result<bool, ConnectorError> {
    novarocks_type_contract::value_layout_matches_carrier_observed(
        declaration.typed_binding().layout(),
        field.data_type(),
        |units| work(units).map_err(MatchFailure::Caller),
    )
    .map_err(|error| match error {
        MatchFailure::Caller(error) => error,
        MatchFailure::Type(error) => {
            ConnectorError::from_domain(ConnectorErrorKind::InvalidRequest, error)
        }
    })
}

pub(super) fn validate_declarations(
    schema: &SchemaRef,
    declarations: &[FunctionValueType],
    work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
) -> Result<(), ConnectorError> {
    if declarations.len() != schema.fields().len() {
        return Err(invalid(
            "COW output width differs from its complete signed declaration",
        ));
    }
    for (field, declaration) in schema.fields().iter().zip(declarations) {
        if declaration.nullable() != field.is_nullable()
            || !matches_signed(declaration, field, work)?
        {
            return Err(invalid(
                "COW signed declaration differs from its output field",
            ));
        }
    }
    Ok(())
}

/// The actual completed SQL root is distinct from the Provider's target.
/// All column associations originate in its frozen generated output aliases.
pub(super) struct CowSqlOutput {
    declarations: Vec<FunctionValueType>,
    target_to_source: Vec<usize>,
    after: Vec<bool>,
}
impl CowSqlOutput {
    pub(super) fn try_new(
        output: &[novarocks_query_application::api::ResultField],
        target: &SchemaRef,
        validator: &super::RowMutationMatchValidator,
        work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        validator.validate_schema(target)?;
        if output.len() != target.fields().len() {
            return Err(value_error(ValueError::Invariant(
                novarocks_value_arrays::ValueViolation::Layout,
            )));
        }
        let mut target_to_source = Vec::new();
        target_to_source
            .try_reserve_exact(output.len())
            .map_err(allocation)?;
        target_to_source.resize(output.len(), usize::MAX);
        for (source, field) in output.iter().enumerate() {
            work(1)?;
            let ordinal = field
                .name()
                .strip_prefix("__nr_sel_")
                .and_then(|ordinal| ordinal.parse::<usize>().ok())
                .filter(|&ordinal| ordinal < output.len())
                .ok_or_else(|| {
                    value_error(ValueError::Invariant(
                        novarocks_value_arrays::ValueViolation::Layout,
                    ))
                })?;
            if field.name() != format!("__nr_sel_{ordinal}")
                || target_to_source[ordinal] != usize::MAX
            {
                return Err(value_error(ValueError::Invariant(
                    novarocks_value_arrays::ValueViolation::Layout,
                )));
            }
            target_to_source[ordinal] = source;
        }
        let mut after = Vec::new();
        after.try_reserve_exact(output.len()).map_err(allocation)?;
        after.resize(output.len(), false);
        for field in validator.contract.after_fields() {
            work(1)?;
            let ordinal = usize::try_from(field.target_ordinal()).map_err(|_| {
                value_error(ValueError::Invariant(
                    novarocks_value_arrays::ValueViolation::Layout,
                ))
            })?;
            *after.get_mut(ordinal).ok_or_else(|| {
                value_error(ValueError::Invariant(
                    novarocks_value_arrays::ValueViolation::Layout,
                ))
            })? = true;
        }
        let mut declarations = Vec::new();
        declarations
            .try_reserve_exact(output.len())
            .map_err(allocation)?;
        for field in output {
            work(1)?;
            declarations.push(field.value_type().clone());
        }
        Ok(Self {
            declarations,
            target_to_source,
            after,
        })
    }

    pub(super) fn validate_and_project<'a>(
        &self,
        batch: &'a RecordBatch,
        targets: &[FunctionValueType],
        work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<std::borrow::Cow<'a, RecordBatch>, ConnectorError> {
        use novarocks_value_arrays::hydrate::{
            required_values_satisfied_observed, validate_required_with_options_observed,
        };
        if batch.num_columns() != self.declarations.len() || targets.len() != self.after.len() {
            return Err(value_error(ValueError::Invariant(
                novarocks_value_arrays::ValueViolation::Layout,
            )));
        }
        let options = HydrateOptions {
            dictionary_value_layout: None,
            projection: HydrateProjection::FrozenNamesAndNullability,
        };
        // Validate every actual SQL promise before inspecting any business
        // target. A required SQL value containing NULL is still an invariant.
        for (array, declaration) in batch.columns().iter().zip(&self.declarations) {
            validate_required_with_options_observed(
                array,
                declaration.typed_binding(),
                options,
                work,
                &mut |_| Ok(()),
            )
            .map_err(failure)?;
        }
        // The ordinary order stays borrowed. A real permutation only reorders
        // existing Arc columns; no payload is copied or cast.
        let projected = if self
            .target_to_source
            .iter()
            .enumerate()
            .all(|(target, &source)| target == source)
        {
            std::borrow::Cow::Borrowed(batch)
        } else {
            std::borrow::Cow::Owned(
                batch
                    .project(&self.target_to_source)
                    .map_err(arrow_assembly)?,
            )
        };
        // SQL may be wider than a target. Identity/Before/effect promises must
        // all hold before an After business refusal can be selected. Only an
        // exact complete binding can reuse the SQL validation for this middle
        // check; the original Hydrate validation below remains independent.
        for (ordinal, ((array, target), after)) in projected
            .columns()
            .iter()
            .zip(targets)
            .zip(&self.after)
            .enumerate()
        {
            if !*after
                && !self.declarations[self.target_to_source[ordinal]]
                    .exactly_equals_checked_observed(target, || work(1))?
            {
                validate_required_with_options_observed(
                    array,
                    target.typed_binding(),
                    options,
                    work,
                    &mut |_| Ok(()),
                )
                .map_err(failure)?;
            }
        }
        for (ordinal, ((array, target), after)) in projected
            .columns()
            .iter()
            .zip(targets)
            .zip(&self.after)
            .enumerate()
        {
            if *after
                && !self.declarations[self.target_to_source[ordinal]]
                    .exactly_equals_checked_observed(target, || work(1))?
                && !required_values_satisfied_observed(
                    array,
                    target.typed_binding(),
                    options,
                    work,
                    &mut |_| Ok(()),
                )
                .map_err(failure)?
            {
                return Err(super::invalid_match(
                    "COW assignment value does not satisfy its required target",
                ));
            }
        }
        Ok(projected)
    }
}

pub(super) struct CowSignedLayoutPlan {
    pub columns: Vec<HydratePlan>,
    schema: SchemaRef,
    pub planning_requested_bytes: usize,
}
impl CowSignedLayoutPlan {
    pub fn plan(
        schema: &SchemaRef,
        batch: &RecordBatch,
        declarations: &[FunctionValueType],
        context: &mut ValueConversionContext,
        planning_limit: usize,
        work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        if batch.num_columns() != schema.fields().len()
            || declarations.len() != schema.fields().len()
        {
            return Err(invalid(
                "COW output width differs from its complete signed declaration",
            ));
        }
        validate_declarations(schema, declarations, work)?;
        let mut requested = 0usize;
        let mut growth = |layout: std::alloc::Layout| {
            requested = requested
                .checked_add(layout.size())
                .ok_or_else(super::cast_footprint::exhausted)?;
            if requested > planning_limit {
                return Err(super::cast_footprint::exhausted());
            }
            Ok(())
        };
        growth(
            std::alloc::Layout::array::<HydratePlan>(declarations.len())
                .map_err(|_| super::cast_footprint::exhausted())?,
        )?;
        let mut columns = Vec::new();
        columns
            .try_reserve_exact(declarations.len())
            .map_err(allocation)?;
        for ((array, _field), declaration) in batch
            .columns()
            .iter()
            .zip(schema.fields())
            .zip(declarations)
        {
            let plan = HydratePlan::plan_with_options_and_storage_observed(
                array,
                declaration.typed_binding(),
                HydrateOptions {
                    dictionary_value_layout: None,
                    projection: HydrateProjection::FrozenNamesAndNullability,
                },
                context,
                &ValueTypeStorageLifetime::pure(),
                &mut |_, _| false,
                &mut growth,
                work,
                &mut |_| Ok(()),
            )
            .map_err(failure)?;
            // Representation failure takes precedence over the whole-output
            // capacity quote. A legal small dictionary may repeat beyond 2GiB.
            plan.require_complete().map_err(value_error)?;
            columns.push(plan);
        }
        Ok(Self {
            columns,
            schema: schema.clone(),
            planning_requested_bytes: requested,
        })
    }
    #[cfg(test)]
    pub fn execute(
        &self,
        context: &mut ValueConversionContext,
        work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<RecordBatch, ConnectorError> {
        self.execute_with_growth(context, &mut |_| Ok(()), work)
    }
    pub(super) fn execute_with_growth(
        &self,
        context: &mut ValueConversionContext,
        growth: &mut dyn FnMut(std::alloc::Layout) -> Result<(), ConnectorError>,
        work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<RecordBatch, ConnectorError> {
        let mut columns = Vec::new();
        columns
            .try_reserve_exact(self.columns.len())
            .map_err(allocation)?;
        for (plan, field) in self.columns.iter().zip(self.schema.fields()) {
            let output = plan
                .execute_observed(context, growth, work, &mut |_| Ok(()))
                .map_err(failure)?;
            columns.push(attach_signed_type(&output, field.data_type(), work)?);
        }
        RecordBatch::try_new(self.schema.clone(), columns).map_err(arrow_assembly)
    }
}

/// Metadata projection only, after shared validation/materialization proved
/// every NULL and value parameter. No value or encoding decision occurs here.
fn attach_signed_type(
    array: &ArrayRef,
    target: &DataType,
    work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
) -> Result<ArrayRef, ConnectorError> {
    fn project(
        data: arrow::array::ArrayData,
        target: &DataType,
        work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
    ) -> Result<arrow::array::ArrayData, ConnectorError> {
        work(1)?;
        let child_types: Vec<&DataType> = match target {
            DataType::Struct(fields) => fields.iter().map(|f| f.data_type()).collect(),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => vec![field.data_type()],
            _ => Vec::new(),
        };
        if child_types.len() != data.child_data().len() {
            return Err(invalid(
                "COW normalized child geometry differs from its signed type",
            ));
        }
        let mut children = Vec::new();
        children
            .try_reserve_exact(child_types.len())
            .map_err(allocation)?;
        for (child, ty) in data.child_data().iter().zip(child_types) {
            children.push(project(child.clone(), ty, work)?);
        }
        data.into_builder()
            .data_type(target.clone())
            .child_data(children)
            .build()
            .map_err(arrow_assembly)
    }
    Ok(make_array(project(array.to_data(), target, work)?))
}

#[cfg(test)]
pub(super) fn fixture_declarations(schema: &arrow::datatypes::Schema) -> Vec<FunctionValueType> {
    schema
        .fields()
        .iter()
        .map(|field| FunctionValueType::new(field.data_type().clone(), field.is_nullable()))
        .collect()
}
#[cfg(test)]
pub(super) fn normalize(
    schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch, ConnectorError> {
    let mut context = ValueConversionContext::new(0);
    let plan = CowSignedLayoutPlan::plan(
        schema,
        batch,
        &fixture_declarations(schema),
        &mut context,
        super::cast_footprint::TRANSFORM_BYTES,
        &mut |_| Ok(()),
    )?;
    plan.execute(&mut context, &mut |_| Ok(()))
}
#[cfg(test)]
mod tests;
