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

use crate::exec::chunk::type_compatibility::check_exact;
use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

use super::functions;
use super::functions::AggKind;

pub(super) fn build_spec_from_type(
    func: &AggFunction,
    input_type: Option<&DataType>,
    input_is_intermediate: bool,
) -> Result<AggSpec, String> {
    let mut spec = functions::build_spec_from_type(func, input_type, input_is_intermediate)?;

    if !input_is_intermediate && let Some(data_type) = input_type {
        spec.input_arg_type = Some(data_type.clone());
    }

    apply_type_signature(spec, func, input_is_intermediate)
}

pub(super) fn agg_type_signature(func: &AggFunction) -> Option<&AggTypeSignature> {
    func.types.as_ref()
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
    validate_state_combinator_binary_signature(
        &out.kind,
        output_type,
        sig.intermediate_type.as_ref(),
    )?;
    let output_matches_final = check_exact(&out.output_type, output_type).is_ok();
    let output_matches_intermediate = !input_is_intermediate
        && sig
            .intermediate_type
            .as_ref()
            .map(|intermediate_type| check_exact(intermediate_type, output_type).is_ok())
            .unwrap_or(false);
    if !output_matches_final && !output_matches_intermediate {
        return Err(format!(
            "aggregate output type signature mismatch for {}: expected {:?}, got {:?}",
            func.name, out.output_type, output_type
        ));
    }
    if output_matches_final {
        out.output_type = output_type.clone();
    }

    if let Some(intermediate_type) = sig.intermediate_type.as_ref() {
        if check_exact(&out.intermediate_type, intermediate_type).is_err() {
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

fn validate_state_combinator_binary_signature(
    kind: &AggKind,
    output_type: &DataType,
    intermediate_type: Option<&DataType>,
) -> Result<(), String> {
    if !is_opaque_state_combinator_kind(kind) {
        return Ok(());
    }
    if output_type != &DataType::Binary {
        return Err(format!(
            "state combinator output_type must be Binary, got {:?}",
            output_type
        ));
    }
    if let Some(intermediate_type) = intermediate_type
        && intermediate_type != &DataType::Binary
    {
        return Err(format!(
            "state combinator intermediate_type must be Binary, got {:?}",
            intermediate_type
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_signature_accepts_local_intermediate_output() {
        let func = AggFunction {
            name: "avg".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Utf8),
                output_type: Some(DataType::Utf8),
                input_arg_type: Some(DataType::Int64),
            }),
            order: Default::default(),
        };

        let spec = build_spec_from_type(&func, Some(&DataType::Int64), false)
            .expect("local aggregate signature may declare intermediate output");

        assert_eq!(spec.output_type, DataType::Float64);
        assert_eq!(spec.intermediate_type, DataType::Utf8);
    }

    #[test]
    fn aggregate_signature_rejects_decimal_precision_drift() {
        let func = AggFunction {
            name: "sum".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Decimal128(20, 2)),
                output_type: Some(DataType::Decimal128(20, 2)),
                input_arg_type: Some(DataType::Decimal128(10, 2)),
            }),
            order: Default::default(),
        };

        let err = build_spec_from_type(&func, Some(&DataType::Decimal128(10, 2)), false)
            .expect_err("aggregate signature must reject decimal precision drift");

        assert!(
            err.contains("aggregate intermediate type signature mismatch"),
            "{err}"
        );
        assert!(err.contains("Decimal128(38, 2)"), "{err}");
        assert!(err.contains("Decimal128(20, 2)"), "{err}");
    }
}

fn is_opaque_state_combinator_kind(kind: &AggKind) -> bool {
    matches!(
        kind,
        AggKind::CountState
            | AggKind::CountStateSigned
            | AggKind::BoolState
            | AggKind::BoolStateSigned
            | AggKind::MinState
            | AggKind::MaxState
            | AggKind::MinStateSigned
            | AggKind::MaxStateSigned
            | AggKind::SumStateInt64
            | AggKind::SumStateDecimal128
            | AggKind::CountStateMerge
            | AggKind::AvgStateMerge
            | AggKind::MinStateMerge
            | AggKind::MaxStateMerge
            | AggKind::BoolAndStateMerge
            | AggKind::BoolOrStateMerge
            | AggKind::CountDistinctStateMerge
            | AggKind::ApproxCountDistinctStateMerge
            | AggKind::SumStateSignedInt64
            | AggKind::SumStateSignedDecimal128
            | AggKind::SumStateMerge
    )
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
