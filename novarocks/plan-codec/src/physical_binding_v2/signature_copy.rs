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

use super::{BindingCodecError, BindingProjectionLimits, MaterializationModel, add};
use crate::physical_type_v2::clone_value_type_observed;
use novarocks_expression_contract::BoundFunction;
use novarocks_physical_plan::BoundTableFunction;
use novarocks_type_contract::{
    CompileCheckpoints, FunctionArgumentType, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionValueType,
};

pub(crate) fn header(
    source: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    let exact = source.legacy_metadata.is_none();
    work.step()?;
    if !exact {
        return Err(BindingCodecError::InvalidShape(
            "signature copy requires an exact binding without legacy metadata",
        ));
    }
    let scalar_result = matches!(
        source.kind,
        FunctionKind::Scalar | FunctionKind::Aggregate | FunctionKind::Window
    );
    work.step()?;
    if !scalar_result {
        return Err(BindingCodecError::InvalidShape(
            "signature copy requires a scalar-result binding kind",
        ));
    }
    Ok(())
}

/// Charge every owned signature occurrence into the caller's original model.
/// The caller lends the binding from the actual materialized namespace and
/// admits the cumulative model before copying; this port does not certify that
/// association, an installed owner, or an allocation grant.
pub(crate) fn preflight_scalar_signature_copy(
    source: &BoundFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_scalar_signature_copy_counts(source, model, limits, work)?;
    preflight_scalar_signature_copy_types(source, model, limits, work)
}

/// Admit all owned collections and root occurrences before visiting types.
/// A containing node can gate its complete cumulative model between these
/// two stages; the complete helper preserves this original operation order.
pub(crate) fn preflight_scalar_signature_copy_counts(
    source: &BoundFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_scalar_signature_copy_counts_core(source, model, limits, None, work)
}
/// Same collection/type authors, borrowing a containing binding admission.
pub(crate) fn preflight_scalar_signature_copy_in(
    source: &BoundFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    admit: &mut super::owner_admission::Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_scalar_signature_copy_counts_in(source, model, limits, admit, work)?;
    preflight_scalar_signature_copy_types_in(source, model, limits, admit, work)
}
pub(crate) fn preflight_scalar_signature_copy_counts_in(
    source: &BoundFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    admit: &mut super::owner_admission::Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_scalar_signature_copy_counts_core(source, model, limits, Some(admit), work)
}
pub(crate) fn preflight_scalar_signature_copy_types_in(
    source: &BoundFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    admit: &mut super::owner_admission::Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_scalar_signature_copy_types_core(source, model, limits, Some(admit), work)
}
fn preflight_scalar_signature_copy_counts_core(
    source: &BoundFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    mut admit: Option<&mut super::owner_admission::Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    if admit.is_some() {
        scalar_collection_counts(source, model)?;
    }
    model.check(limits)?;
    if let Some(parent) = admit.as_mut() {
        parent(&model.facts)?;
    }
    header(source, work)?;
    if admit.is_none() {
        scalar_collection_counts(source, model)?;
    }
    model.check(limits)?;
    if let Some(parent) = admit.as_mut() {
        parent(&model.facts)?;
    }
    preflight_argument_counts(&source.argument_types, model, limits, admit, work)
}

fn scalar_collection_counts(
    source: &BoundFunction,
    model: &mut MaterializationModel,
) -> Result<(), BindingCodecError> {
    model.items = add(model.items, source.argument_types.len())?;
    model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
    Ok(())
}

pub(crate) fn preflight_scalar_signature_copy_types(
    source: &BoundFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_scalar_signature_copy_types_core(source, model, limits, None, work)
}
fn preflight_scalar_signature_copy_types_core(
    source: &BoundFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    mut admit: Option<&mut super::owner_admission::Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_argument_types(
        &source.argument_types,
        model,
        limits,
        admit.as_deref_mut(),
        work,
    )?;
    if let Some(parent) = admit.as_mut() {
        model.count_owned_type_clone_in(&source.result_type, limits, *parent, work)?;
    } else {
        model.count_owned_type_clone(&source.result_type, limits, work)?;
    }
    work.step()?;
    Ok(())
}
fn preflight_argument_counts(
    arguments: &[FunctionArgumentType],
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    mut admit: Option<&mut super::owner_admission::Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    // Admit all references and collection requests before the sole type walk.
    for argument in arguments {
        match argument {
            FunctionArgumentType::Value(_) => {
                model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
            }
            FunctionArgumentType::Lambda {
                parameter_types, ..
            } => {
                model.items = add(model.items, parameter_types.len())?;
                model.facts.type_reference_count = add(
                    model.facts.type_reference_count,
                    add(parameter_types.len(), 1)?,
                )?;
            }
        }
        model.check(limits)?;
        if let Some(parent) = admit.as_mut() {
            parent(&model.facts)?;
        }
        work.step()?;
    }
    Ok(())
}
fn preflight_argument_types(
    arguments: &[FunctionArgumentType],
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    mut admit: Option<&mut super::owner_admission::Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    for argument in arguments {
        match argument {
            FunctionArgumentType::Value(value) => {
                if let Some(parent) = admit.as_mut() {
                    model.count_owned_type_clone_in(value, limits, *parent, work)?;
                } else {
                    model.count_owned_type_clone(value, limits, work)?;
                }
            }
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => {
                for parameter in parameter_types {
                    if let Some(parent) = admit.as_mut() {
                        model.count_owned_type_clone_in(parameter, limits, *parent, work)?;
                    } else {
                        model.count_owned_type_clone(parameter, limits, work)?;
                    }
                    work.step()?;
                }
                if let Some(parent) = admit.as_mut() {
                    model.count_owned_type_clone_in(result_type, limits, *parent, work)?;
                } else {
                    model.count_owned_type_clone(result_type, limits, work)?;
                }
            }
        }
        work.step()?;
    }
    Ok(())
}

fn copy_type(
    source: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, BindingCodecError> {
    work.flush()?;
    let output = clone_value_type_observed(source, work)?;
    work.step()?;
    work.flush()?;
    Ok(output)
}

/// Copy an already admitted exact signature using the original identity and
/// type authors. The caller must preflight this occurrence in the same model;
/// it owns the ordinary/success footer on the original checkpoint scope.
pub(crate) fn copy_scalar_signature_observed(
    source: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundFunction, BindingCodecError> {
    copy_scalar_signature_with_creator_observed::<std::convert::Infallible>(source, None, work)
        .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}
pub(crate) fn copy_scalar_signature_with_creator_observed<H>(
    source: &BoundFunction,
    creator: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundFunction, super::creation::Failure<H>> {
    header(source, work)?;
    let _ = creator; // Sharing an immutable collection constructs no new holder.

    let (function_id, overload) = copy_identity(&source.function_id, &source.overload, work)?;
    let argument_types = source.argument_types.clone_observed(|| work.step())?;
    let result_type = copy_type(&source.result_type, work)?;
    let output = BoundFunction::from_exact_signature(
        function_id,
        overload,
        source.kind,
        argument_types,
        result_type,
    );
    work.step()?;
    Ok(output)
}

fn copy_identity(
    function: &FunctionId,
    overload: &FunctionOverloadId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(FunctionId, FunctionOverloadId), BindingCodecError> {
    work.flush()?;
    let function_id = function.clone_observed(|| work.step())?;
    let overload = overload.clone_observed(|| work.step())?;
    work.flush()?;
    Ok((function_id, overload))
}

fn table_header(
    source: &BoundTableFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    let exact = source.legacy_metadata.is_none();
    work.step()?;
    if !exact {
        return Err(BindingCodecError::InvalidShape(
            "table signature copy requires an exact binding without legacy metadata",
        ));
    }
    Ok(())
}

/// Admit an actual relation signature occurrence into the same caller model.
/// Its original namespace and output membership remain caller-owned facts.
#[cfg(test)]
fn preflight_table_signature_copy(
    source: &BoundTableFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_table_signature_copy_counts(source, model, limits, work)?;
    preflight_table_signature_copy_types(source, model, limits, work)
}
/// Count-only original pass lets an encompassing owner admit cumulative work
/// and requests before this signature's nested clone preflight starts.
pub(crate) fn preflight_table_signature_copy_counts(
    source: &BoundTableFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_table_signature_copy_counts_core(source, model, limits, None, work)
}
pub(crate) fn preflight_table_signature_copy_counts_in(
    source: &BoundTableFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    admit: &mut super::owner_admission::Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_table_signature_copy_counts_core(source, model, limits, Some(admit), work)
}
fn preflight_table_signature_copy_counts_core(
    source: &BoundTableFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    mut admit: Option<&mut super::owner_admission::Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    if admit.is_some() {
        table_collection_counts(source, model)?;
    }
    model.check(limits)?;
    if let Some(parent) = admit.as_mut() {
        parent(&model.facts)?;
    }
    table_header(source, work)?;
    if admit.is_none() {
        table_collection_counts(source, model)?;
    }
    model.check(limits)?;
    if let Some(parent) = admit.as_mut() {
        parent(&model.facts)?;
    }
    preflight_argument_counts(&source.argument_types, model, limits, admit, work)?;
    Ok(())
}
fn table_collection_counts(
    source: &BoundTableFunction,
    model: &mut MaterializationModel,
) -> Result<(), BindingCodecError> {
    model.items = add(
        model.items,
        add(source.argument_types.len(), source.result_types.len())?,
    )?;
    model.facts.type_reference_count =
        add(model.facts.type_reference_count, source.result_types.len())?;
    Ok(())
}

/// Continue the already counted occurrence in the same model and scope.
/// The caller must not add a second collection count or reset the model.
pub(crate) fn preflight_table_signature_copy_types(
    source: &BoundTableFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_table_signature_copy_types_core(source, model, limits, None, work)
}
pub(crate) fn preflight_table_signature_copy_types_in(
    source: &BoundTableFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    admit: &mut super::owner_admission::Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_table_signature_copy_types_core(source, model, limits, Some(admit), work)
}
fn preflight_table_signature_copy_types_core(
    source: &BoundTableFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    mut admit: Option<&mut super::owner_admission::Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_argument_types(
        &source.argument_types,
        model,
        limits,
        admit.as_deref_mut(),
        work,
    )?;
    for value in &source.result_types {
        if let Some(parent) = admit.as_mut() {
            model.count_owned_type_clone_in(value, limits, *parent, work)?;
        } else {
            model.count_owned_type_clone(value, limits, work)?;
        }
        work.step()?;
    }
    Ok(())
}

/// Copy an admitted table occurrence through the sole argument/FVT authors.
/// No entry/footer, implicit legacy metadata or installed capability is added.
pub(crate) fn copy_table_signature_observed(
    source: &BoundTableFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundTableFunction, BindingCodecError> {
    table_header(source, work)?;
    let (function_id, overload) = copy_identity(&source.function_id, &source.overload, work)?;
    let argument_types = source.argument_types.clone_observed(|| work.step())?;
    let result_types = source.result_types.clone_observed(|| work.step())?;
    let output = BoundTableFunction::from_exact_signature(
        function_id,
        overload,
        argument_types,
        result_types,
    );
    work.step()?;
    Ok(output)
}

#[cfg(test)]
#[path = "signature_copy_tests.rs"]
mod tests;
