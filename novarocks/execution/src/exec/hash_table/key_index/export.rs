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

//! Typed output construction for an immutable requested key sequence.
//! This consumes original constructors and retains their actual caller creator;
//! it never exposes encoded complex bytes under an Arrow logical binding.

use super::read::{KeyReadColumn, KeyReadValues};
use super::{KeySpec, OwnedKeyReadBatch, checkpoint_at, invariant, original, vector};
use crate::{
    exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::array::{Array, ArrayRef, BinaryViewArray, StringViewArray};
use novarocks_functions::{
    AggregateStateAllocator, KernelEvaluationControl,
    selected_copy::{
        CopyConstructionError, copy_nested_values_construction, preflight_empty_construction,
    },
};
use novarocks_type_contract::{FunctionValueType, ValueSchemaCreation, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ByteLayout, ValueViolation,
    build::{ValueBuilder, WriteFailure},
};
use std::sync::Arc;

pub(crate) struct ExportedKeys {
    arrays: Vec<ArrayRef>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains exact signed output bindings and their metadata lifetimes beside exported arrays."
        )
    )]
    bindings: Vec<FunctionValueType>,
    _storage: ValueTypeStorageLifetime,
}
impl ExportedKeys {
    pub(crate) fn arrays(&self) -> &[ArrayRef] {
        &self.arrays
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen typed export exposes the exact signed output declaration."
        )
    )]
    pub(crate) fn bindings(&self) -> &[FunctionValueType] {
        &self.bindings
    }
}

pub(super) fn validate_targets(
    spec: &KeySpec,
    targets: &[FunctionValueType],
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<()> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    checkpoint()?;
    if targets.len() != spec.types().len() {
        return Err(invariant(ValueViolation::Layout));
    }
    for (source, target) in spec.types().iter().zip(targets) {
        // The Arrow carrier is not the logical domain. This retains nested
        // NULL promises, field names and all nominal parameters even for [].
        if !source
            .same_value_domain_observed::<super::prepared::DomainFailure>(target, || {
                checkpoint().map_err(super::prepared::DomainFailure::Caller)
            })
            .map_err(ExecutionFailure::from)?
        {
            return Err(invariant(ValueViolation::Layout));
        }
    }
    checkpoint()
}

pub(super) fn construct(
    read: &OwnedKeyReadBatch<'_>,
    spec: &KeySpec,
    targets: &[FunctionValueType],
    owner: &dyn InputConstructionOwner,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ExportedKeys> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    checkpoint()?;
    let mut arrays = vector(owner, targets.len())?;
    let mut bindings = vector(owner, targets.len())?;
    for ((column, source), target) in read.columns().iter().zip(spec.types()).zip(targets) {
        checkpoint()?;
        if !target.nullable() {
            reject_selected_nulls(column, control)?;
        }
        let array = if read.len() == 0 {
            empty(target, owner, control)?
        } else if super::prepared::is_fixed(source.data_type()) {
            fixed(column, read.len(), owner, control)?
        } else if ByteLayout::from_data_type(source.data_type()).is_some() {
            bytes(
                column,
                read.len(),
                ByteLayout::from_data_type(target.data_type())
                    .ok_or_else(|| invariant(ValueViolation::Layout))?,
                owner,
                control,
            )?
        } else {
            canonical(column, read.len(), source, target, owner, control)?
        };
        // Original value-layer validation enforces the exact final carrier and
        // active-child NULL promises; no signed metadata is inferred from it.
        novarocks_value_arrays::hydrate::validate_required_observed(
            &array,
            target.typed_binding(),
            &mut |_| checkpoint(),
            &mut |event| {
                novarocks_functions::array_projection::observe_value_hydration(event, &mut |step| {
                    owner.observe(0, &[], step).map_err(ExecutionFailure::from)
                })
            },
        )
        .map_err(original)?;
        let array = super::store::retain_created_array(
            array,
            target,
            &owner.storage_lifetime(),
            Arc::clone(&host),
            control,
        )?;
        arrays.push(array);
        bindings.push(target.clone_observed(checkpoint)?);
    }
    checkpoint()?;
    Ok(ExportedKeys {
        arrays,
        bindings,
        _storage: owner.storage_lifetime(),
    })
}

fn reject_selected_nulls(
    column: &KeyReadColumn<'_>,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<()> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    for page in column.pages() {
        checkpoint()?;
        match page.values() {
            KeyReadValues::Fixed { array, .. }
            | KeyReadValues::Bytes { array, .. }
            | KeyReadValues::NominalBytes { raw: array, .. } => {
                for (i, selected) in page.rows().iter().enumerate() {
                    checkpoint_at(i, &checkpoint)?;
                    if crate::exec::hash_table::partition_keys::normalized_root_is_null(
                        array.as_ref(),
                        selected.row,
                    ) {
                        return Err(invariant(ValueViolation::RequiredNull));
                    }
                }
            }
            KeyReadValues::Canonical { encoded, .. } => {
                for (i, selected) in page.rows().iter().enumerate() {
                    checkpoint_at(i, &checkpoint)?;
                    if crate::exec::hash_table::key_builder::canonical_component_payload(
                        encoded.value(selected.row),
                    )?
                    .is_none()
                    {
                        return Err(invariant(ValueViolation::RequiredNull));
                    }
                }
            }
        }
    }
    checkpoint()
}
fn copy_error(cause: CopyConstructionError<ConstructionFailure>) -> ExecutionFailure {
    match cause {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    }
}
fn empty(
    target: &FunctionValueType,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    control.checkpoint(0)?;
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation =
        ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
    preflight_empty_construction(target.typed_binding().layout(), &mut creation, control)
        .map_err(copy_error)?;
    control.checkpoint(0)?;
    let output = arrow::array::new_empty_array(target.data_type());
    control.checkpoint(0)?;
    Ok(output)
}
fn fixed(
    column: &KeyReadColumn<'_>,
    rows: usize,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    let mut sources = vector(owner, column.pages().len())?;
    let mut choices = vector(owner, rows)?;
    for i in 0..rows {
        checkpoint_at(i, &checkpoint)?;
        choices.push((0usize, 0usize));
    }
    for (source, page) in column.pages().iter().enumerate() {
        checkpoint()?;
        let KeyReadValues::Fixed { array, .. } = page.values() else {
            return Err(invariant(ValueViolation::Carrier));
        };
        sources.push(Arc::clone(array));
        for (i, selected) in page.rows().iter().enumerate() {
            checkpoint_at(i, &checkpoint)?;
            choices[selected.ordinal] = (source, selected.row);
        }
    }
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation =
        ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
    copy_nested_values_construction(&sources, &choices, &mut creation, control).map_err(copy_error)
}
fn bytes(
    column: &KeyReadColumn<'_>,
    rows: usize,
    target: ByteLayout,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    let mut builder = ValueBuilder::new(target);
    if target.is_utf8() {
        let mut values = vector::<Option<&str>>(owner, rows)?;
        for i in 0..rows {
            checkpoint_at(i, &checkpoint)?;
            values.push(None);
        }
        for page in column.pages() {
            checkpoint()?;
            let array = match page.values() {
                KeyReadValues::Bytes { array, .. }
                | KeyReadValues::NominalBytes { raw: array, .. } => *array,
                _ => return Err(invariant(ValueViolation::Carrier)),
            };
            let source = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            for (i, selected) in page.rows().iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                values[selected.ordinal] =
                    (!source.is_null(selected.row)).then(|| source.value(selected.row));
            }
        }
        for value in values {
            let mut growth = |layout| owner.request(layout).map_err(ExecutionFailure::from);
            let mut work = |_| checkpoint();
            let result = match value {
                Some(value) => builder.append_str(value, &mut growth, &mut work),
                None => builder.append_null(&mut growth, &mut work),
            };
            write(result, target, builder.len())?;
        }
    } else {
        let mut values = vector::<Option<&[u8]>>(owner, rows)?;
        for i in 0..rows {
            checkpoint_at(i, &checkpoint)?;
            values.push(None);
        }
        for page in column.pages() {
            checkpoint()?;
            let array = match page.values() {
                KeyReadValues::Bytes { array, .. }
                | KeyReadValues::NominalBytes { raw: array, .. } => *array,
                _ => return Err(invariant(ValueViolation::Carrier)),
            };
            let source = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            for (i, selected) in page.rows().iter().enumerate() {
                checkpoint_at(i, &checkpoint)?;
                values[selected.ordinal] =
                    (!source.is_null(selected.row)).then(|| source.value(selected.row));
            }
        }
        for value in values {
            let mut growth = |layout| owner.request(layout).map_err(ExecutionFailure::from);
            let mut work = |_| checkpoint();
            let result = match value {
                Some(value) => builder.append_bytes(value, &mut growth, &mut work),
                None => builder.append_null(&mut growth, &mut work),
            };
            write(result, target, builder.len())?;
        }
    }
    let built = builder
        .finish_structured_observed(
            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| checkpoint(),
            &mut |event| {
                owner
                    .observe(
                        0,
                        &[],
                        crate::exec::chunk::ArrayNormalizationStep::ValueStructure {
                            path: &[],
                            event,
                        },
                    )
                    .map_err(ExecutionFailure::from)
            },
        )
        .map_err(original)?;
    Ok(built.array)
}
fn write(
    value: Result<(), WriteFailure<ExecutionFailure>>,
    layout: ByteLayout,
    committed_rows: usize,
) -> ExecutionResult<()> {
    match value {
        Ok(()) => Ok(()),
        Err(WriteFailure::Failure(cause)) => Err(original(cause)),
        // Full is the original constructor's observed prefix boundary. This
        // API promises a complete single output, so it cannot publish a prefix
        // as success. Do not invent a byte maximum or relabel it malformed.
        // The actual rejected size/limit are not supplied by Full: preserve
        // them as unknown and report only the real committed prefix.
        Err(WriteFailure::Full) => Err(ExecutionFailure::from_domain(
            novarocks_value_arrays::ValueError::Representation(layout.representation(
                novarocks_failure_contract::RepresentationBoundary::SingleOutput,
                None,
                None,
                u64::try_from(committed_rows).ok(),
            )),
        )),
    }
}

fn canonical(
    column: &KeyReadColumn<'_>,
    rows: usize,
    source_projection: &FunctionValueType,
    target: &FunctionValueType,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    use crate::exec::hash_table::key_builder::{
        canonical_component_payload, decode_group_key_rows_construction,
    };
    use novarocks_value_arrays::{
        decode::ValueConversionContext,
        hydrate::{HydrateOptions, HydratePlan, HydrateProjection},
    };
    let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
    let mut values = vector::<Option<&[u8]>>(owner, rows)?;
    for row in 0..rows {
        checkpoint_at(row, &checkpoint)?;
        values.push(None);
    }
    for page in column.pages() {
        checkpoint()?;
        let KeyReadValues::Canonical {
            encoded,
            logical_binding,
        } = page.values()
        else {
            return Err(invariant(ValueViolation::Carrier));
        };
        // Page construction must retain the exact index projection used by its
        // original codec. Target byte layout never chooses a framing marker.
        if !source_projection
            .typed_binding()
            .layout()
            .equals_observed(logical_binding.typed_binding().layout(), checkpoint)?
        {
            return Err(invariant(ValueViolation::Layout));
        }
        for (i, selected) in page.rows().iter().enumerate() {
            checkpoint_at(i, &checkpoint)?;
            values[selected.ordinal] = canonical_component_payload(encoded.value(selected.row))?;
        }
    }
    let source =
        decode_group_key_rows_construction(source_projection.data_type(), &values, owner, control)?;
    let mut conversions = ValueConversionContext::default();
    let storage = owner.storage_lifetime();
    let mut growth = |layout| owner.request(layout).map_err(ExecutionFailure::from);
    let mut work = |_| checkpoint();
    let mut observe = |event: novarocks_value_arrays::hydrate::HydrateEvent<'_>| {
        novarocks_functions::array_projection::observe_value_hydration(event, &mut |step| {
            owner.observe(0, &[], step).map_err(ExecutionFailure::from)
        })
    };
    let plan = HydratePlan::plan_with_options_and_storage_observed(
        &source,
        target.typed_binding(),
        HydrateOptions {
            projection: HydrateProjection::ExactByteLayouts,
            ..Default::default()
        },
        &mut conversions,
        &storage,
        &mut |_, _| false,
        &mut growth,
        &mut work,
        &mut observe,
    )
    .map_err(original)?;
    plan.require_complete()
        .map_err(ExecutionFailure::from_domain)?;
    plan.execute_observed(&mut conversions, &mut growth, &mut work, &mut observe)
        .map_err(original)
}
