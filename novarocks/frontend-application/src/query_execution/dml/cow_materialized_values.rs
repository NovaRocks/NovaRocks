// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Compact selected rows, never an original unselected column sent to IPC.
use super::cow_closed_ast::{BuildError, Result, exact_vec};
use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::datatypes::Field;
use novarocks_functions::{ConstantCreationFailure, ConstantPolicy, ConstantPool, ConstantValue};
use novarocks_spi::connector::{
    ConnectorOriginalResultScope, ConnectorRowMutationSelection,
    ConnectorRowMutationSelectionOrdinal,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
    ValueTypeStorageLifetime,
};
use std::sync::Arc;
pub(super) type Rows = Arc<[Box<[ConstantValue]>]>;

struct ConstantHold {
    _original: ConnectorOriginalResultScope,
}
impl novarocks_type_contract::ValueTypeStorageHold for ConstantHold {}
fn constant_failure(error: ConstantCreationFailure<BuildError>) -> BuildError {
    match error {
        ConstantCreationFailure::Creator(error) => error,
        ConstantCreationFailure::Constant(error) => BuildError::Constant(error),
        ConstantCreationFailure::Resource(error) => BuildError::Layout(error),
        ConstantCreationFailure::SourceMetadata(_) => {
            BuildError::InvalidSource("new COW constant has external source annotations")
        }
    }
}
fn pool(
    array: &ArrayRef,
    value_type: &FunctionValueType,
    storage: &ValueTypeStorageLifetime,
    policy: ConstantPolicy,
    check: &impl Fn() -> Result<()>,
    control: &dyn PureCompileControl,
) -> Result<ConstantPool> {
    check()?;
    // Canonical value layout shares child Field/parameter holders. The empty
    // source name requests no String bytes and no provider facts are copied.
    let field = Arc::new(Field::new(
        "",
        value_type.data_type().clone(),
        value_type.nullable(),
    ));
    check()?;
    let data = array.to_data();
    check()?;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    ConstantPool::try_new_owned_observed_in(
        field,
        value_type.clone(),
        data,
        policy,
        storage.clone(),
        &mut |_facts| check(),
        &mut work,
    )
    .map_err(constant_failure)
}

fn protocol_type(
    ty: arrow::datatypes::DataType,
    storage: &ValueTypeStorageLifetime,
    check: &impl Fn() -> Result<()>,
) -> Result<FunctionValueType> {
    check()?;
    let value = novarocks_type_contract::ValueTypeRef::try_new_observed(
        novarocks_type_contract::ValueKind::Scalar(ty.clone()),
        false,
        storage.clone(),
        |_| check(),
    )?;
    let layout = novarocks_type_contract::ValueLayout::try_from_normalized_arrow_observed(
        ty,
        storage.clone(),
        |_| check(),
    )?;
    let binding = novarocks_type_contract::TypedValueBinding::try_new(value, layout)
        .map_err(BuildError::ValueType)?;
    FunctionValueType::try_from_typed_binding(binding)
        .map_err(|e| BuildError::Constant(novarocks_functions::ConstantError::Type(e)))
}

/// Build compact constants on the original compiler owner and ConstantPolicy.
/// Original control remains checked through each actual conversion.
/// `columns` borrows the route's actual signed ordinal and complete domain.
pub(super) fn build(
    selection: &ConnectorRowMutationSelection,
    selected: &[ConnectorRowMutationSelectionOrdinal],
    columns: &[(u32, &FunctionValueType)],
    protocol_tail: bool,
    effect: Option<u32>,
    original: &ConnectorOriginalResultScope,
    policy: ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<Rows> {
    if selected.is_empty() || columns.is_empty() || protocol_tail != effect.is_some() {
        return Err(BuildError::InvalidSource(
            "COW compact source shape is invalid",
        ));
    }
    let check = || original.check_active().map_err(BuildError::Original);
    check()?;
    let width = columns
        .len()
        .checked_add(if protocol_tail { 2 } else { 0 })
        .ok_or(BuildError::ResourceExhausted)?;
    let mut output = exact_vec::<Vec<ConstantValue>>(selected.len())?;
    for _ in selected {
        output.push(exact_vec(width)?);
    }
    let storage = ValueTypeStorageLifetime::held(Arc::new(ConstantHold {
        _original: original.clone(),
    }));
    // This original compiler operation owns temporary conversions. Reuse
    // remains off; ConstantPolicy and CompileCheckpoints keep their semantics.
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::new(0);
    let effect_type = if effect.is_some() {
        Some(protocol_type(
            arrow::datatypes::DataType::Int8,
            &storage,
            &check,
        )?)
    } else {
        None
    };
    let matched_type = if protocol_tail {
        Some(protocol_type(
            arrow::datatypes::DataType::Boolean,
            &storage,
            &check,
        )?)
    } else {
        None
    };
    let matched = if let Some(ty) = matched_type.as_ref() {
        check()?;
        let array: ArrayRef = Arc::new(arrow::array::BooleanArray::new(
            arrow::buffer::BooleanBuffer::new(arrow::buffer::Buffer::from(vec![1u8]), 0, 1),
            None,
        ));
        Some(pool(&array, ty, &storage, policy, &check, control)?)
    } else {
        None
    };
    let effect_column = effect.zip(effect_type.as_ref());
    for (ordinal, value_type) in columns.iter().copied().chain(effect_column) {
        // Keep the original scalar constant-policy unit. Larger contiguous
        // pooling would apply max_rows/retained-buffer limits to a new set and
        // change availability. This compact array contains only this selected
        // occurrence; its complete checked value is addressed by local zero.
        for (position, row) in selected.iter().enumerate() {
            check()?;
            let view = selection.locate(*row).ok_or(BuildError::InvalidSource(
                "COW selected row is outside its original owner",
            ))?;
            let batch = view.batch();
            let mut indices = exact_vec(1)?;
            indices
                .push(u64::try_from(view.row_index()).map_err(|_| BuildError::ResourceExhausted)?);
            let mut output_positions = exact_vec(1)?;
            output_positions.push(position);
            check()?;
            let indices = UInt64Array::from(indices);
            check()?;
            let array = batch
                .columns()
                .get(ordinal as usize)
                .ok_or(BuildError::InvalidSource(
                    "COW signed column is outside its actual batch",
                ))?;
            check()?;
            let compact = arrow::compute::take(
                array.as_ref(),
                &indices,
                Some(arrow::compute::TakeOptions { check_bounds: true }),
            )
            .map_err(|error| BuildError::OriginalSemantic(error.to_string()))?;
            check()?;
            // The existing shared representation author owns dictionary/plain
            // adaptation; semantic meaning remains the signed full declaration.
            let compact =
                novarocks_functions::array_projection::normalize_typed_array_plain_observed(
                    &compact,
                    value_type.typed_binding(),
                    &mut value_conversions,
                    &storage,
                    &mut |_layout| check(),
                    &mut |_| check(),
                    &mut |_step| check(),
                )?;
            let owner = pool(&compact, value_type, &storage, policy, &check, control)?;
            for (local, &position) in output_positions.iter().enumerate() {
                check()?;
                // Rewrite's matched protocol column precedes its effect.
                if protocol_tail && output[position].len() == columns.len() {
                    output[position].push(
                        matched
                            .as_ref()
                            .expect("protocol constant authored before data")
                            .value(0)
                            .map_err(BuildError::Constant)?,
                    );
                }
                output[position].push(
                    owner
                        .value(u32::try_from(local).map_err(|_| BuildError::ResourceExhausted)?)
                        .map_err(BuildError::Constant)?,
                );
            }
        }
    }
    let mut rows = exact_vec(output.len())?;
    for row in output {
        if row.len() != width {
            return Err(BuildError::InvalidSource(
                "COW compact source lost an output occurrence",
            ));
        }
        rows.push(row.into_boxed_slice());
    }
    check()?;
    let rows = Arc::from(rows);
    Ok(rows)
}
