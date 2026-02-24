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
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

use super::functions;
use super::functions::AggKind;

pub(super) fn build_spec_from_type(
    func: &AggFunction,
    input_type: Option<&DataType>,
    input_is_intermediate: bool,
) -> Result<AggSpec, String> {
    let mut spec = functions::build_spec_from_type(func, input_type, input_is_intermediate)?;

    if !input_is_intermediate {
        if let Some(data_type) = input_type {
            spec.input_arg_type = Some(data_type.clone());
        }
    }

    apply_type_signature(spec, func, input_is_intermediate)
}

pub(super) fn agg_type_signature(func: &AggFunction) -> Option<&AggTypeSignature> {
    func.types.as_ref()
}

fn is_compatible_signature_type(expected: &DataType, sig_type: &DataType) -> bool {
    match (expected, sig_type) {
        (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => true,
        (DataType::Decimal256(_, _), DataType::Decimal256(_, _)) => true,
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => true,
        (DataType::Utf8, DataType::Binary) | (DataType::Binary, DataType::Utf8) => true,
        (DataType::List(_), DataType::Struct(sig_fields)) if sig_fields.len() == 1 => {
            is_compatible_signature_type(expected, sig_fields[0].data_type())
        }
        (DataType::Struct(expected_fields), DataType::List(_)) if expected_fields.len() == 1 => {
            is_compatible_signature_type(expected_fields[0].data_type(), sig_type)
        }
        (DataType::Struct(expected_fields), DataType::Struct(sig_fields)) => {
            if expected_fields.len() != sig_fields.len() {
                return false;
            }
            expected_fields
                .iter()
                .zip(sig_fields.iter())
                .all(|(expected, sig)| {
                    is_compatible_signature_type(expected.data_type(), sig.data_type())
                })
        }
        _ => expected == sig_type,
    }
}

fn apply_type_signature(
    spec: AggSpec,
    func: &AggFunction,
    input_is_intermediate: bool,
) -> Result<AggSpec, String> {
    let sig = agg_type_signature(func)
        .ok_or_else(|| "aggregate type signature is required".to_string())?;
    let output_type = sig
        .output_type
        .as_ref()
        .ok_or_else(|| "aggregate output_type signature is required".to_string())?;

    let mut out = spec;
    if !is_compatible_signature_type(&out.output_type, output_type) {
        return Err(format!(
            "aggregate output type signature mismatch for {}: expected {:?}, got {:?}",
            func.name, out.output_type, output_type
        ));
    }
    out.output_type = output_type.clone();

    let _ = input_is_intermediate;
    if let Some(intermediate_type) = sig.intermediate_type.as_ref() {
        if !is_compatible_signature_type(&out.intermediate_type, intermediate_type) {
            return Err(format!(
                "aggregate intermediate type signature mismatch for {}: expected {:?}, got {:?}",
                func.name, out.intermediate_type, intermediate_type
            ));
        }
        out.intermediate_type = intermediate_type.clone();
    }

    if let Some(t) = sig.input_arg_type.as_ref() {
        out.input_arg_type = Some(t.clone());
    }
    Ok(out)
}

#[derive(Clone, Debug)]
pub(super) struct AggSpec {
    pub(super) kind: AggKind,
    pub(super) output_type: DataType,
    pub(super) intermediate_type: DataType,
    /// The FE-declared type of the first input argument (TFunction.arg_types[0]).
    /// StarRocks BE uses this scale for avg(decimal) (see ctx->get_arg_type(0)->scale).
    pub(super) input_arg_type: Option<DataType>,
    pub(super) count_all: bool,
}
