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

use super::*;
use crate::{AggregatePhase, ExprKind, Fragment, NodeId, RootOrdinal};

pub(crate) fn validate_window_frame(
    fragment: &Fragment,
    frame: &crate::WindowFrame,
    path: &str,
    errors: &mut ValidationContext,
) {
    if matches!(frame.start, crate::WindowBound::UnboundedFollowing)
        || matches!(frame.end, crate::WindowBound::UnboundedPreceding)
        || window_bound_position(fragment, &frame.start)
            > window_bound_position(fragment, &frame.end)
    {
        errors.push(ValidationError::new(
            path,
            "window frame start follows its end",
        ));
    }
    // Offset source/type/value checks belong to the exact constant-pool owner
    // at package publication; offsets are checked references, not roots.
    if frame.units == crate::WindowFrameUnits::Range && frame_has_offset(frame) {
        errors.push(ValidationError::new(
            path,
            "RANGE window offsets require typed order-key arithmetic not represented by contract revision 1",
        ));
    }
}

pub(crate) fn frame_has_offset(frame: &crate::WindowFrame) -> bool {
    [&frame.start, &frame.end].iter().any(|bound| {
        matches!(
            bound,
            crate::WindowBound::Preceding(_) | crate::WindowBound::Following(_)
        )
    })
}

pub(crate) fn window_bound_position(_fragment: &Fragment, bound: &crate::WindowBound) -> u8 {
    match bound {
        crate::WindowBound::UnboundedPreceding => 0,
        crate::WindowBound::Preceding(_) => 1,
        crate::WindowBound::CurrentRow => 2,
        crate::WindowBound::Following(_) => 3,
        crate::WindowBound::UnboundedFollowing => 4,
    }
}

pub(crate) fn validate_function_arguments(
    fragment: &Fragment,
    node: NodeId,
    function: &crate::FunctionId,
    expected: &[crate::FunctionArgumentType],
    args: &[RootOrdinal],
    path: &str,
    errors: &mut ValidationContext,
) {
    if expected.len() != args.len() {
        errors.push(ValidationError::new(
            path,
            format!(
                "bound function expects {} arguments, got {}",
                expected.len(),
                args.len()
            ),
        ));
        return;
    }
    for (ordinal, (expected, argument)) in expected.iter().zip(args).enumerate() {
        let valid = fragment
            .root_expression(node, *argument)
            .is_some_and(|actual| {
                match (expected, &actual.kind) {
                    (crate::FunctionArgumentType::Value(_), ExprKind::Lambda { .. }) => false,
                    // A parameter that accepts null accepts a value that never
                    // writes one; the mismatch is the other way round.
                    (crate::FunctionArgumentType::Value(expected), _) => {
                        actual.ty.fits_value_type(expected)
                    }
                    (
                        crate::FunctionArgumentType::Lambda {
                            parameter_types,
                            result_type,
                        },
                        ExprKind::Lambda {
                            parameter_types: actual_parameters,
                            ..
                        },
                    ) => {
                        actual_parameters.as_ref() == parameter_types.as_slice()
                            && &actual.ty == result_type
                    }
                    (
                        crate::FunctionArgumentType::Lambda { .. }
                        | crate::FunctionArgumentType::CandidateGroup { .. },
                        _,
                    ) => false,
                }
            });
        if !valid {
            let actual = fragment
                .root_expression(node, *argument)
                .map_or_else(|| "absent".to_string(), |actual| format!("{:?}", actual.ty));
            errors.push(ValidationError::new(
                path,
                format!(
                    "function `{}` argument {ordinal} shape differs from its bound signature: bound {expected:?}, got {actual}",
                    function.as_str()
                ),
            ));
        }
    }
}

pub(crate) fn validate_aggregate_arguments(
    fragment: &Fragment,
    node: NodeId,
    binding: &crate::AggregateBinding,
    args: &[RootOrdinal],
    order_by: &[crate::SortExpr],
    path: &str,
    errors: &mut ValidationContext,
) {
    match binding.phase {
        AggregatePhase::Single | AggregatePhase::Partial { .. } => {
            let logical_count =
                usize::try_from(binding.logical_argument_count).unwrap_or(usize::MAX);
            if logical_count != args.len()
                || binding.function.argument_types.len() != args.len() + order_by.len()
            {
                errors.push(ValidationError::new(
                    path,
                    "aggregate logical/ORDER BY channel counts differ from the bound signature",
                ));
            } else {
                if binding.function.argument_types[logical_count..]
                    .iter()
                    .any(|argument| !matches!(argument, crate::FunctionArgumentType::Value(_)))
                {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate ORDER BY update channels must be scalar values",
                    ));
                }
                let inputs = args
                    .iter()
                    .copied()
                    .chain(order_by.iter().map(|item| item.expr))
                    .collect::<Vec<_>>();
                validate_function_arguments(
                    fragment,
                    node,
                    &binding.function.function_id,
                    &binding.function.argument_types,
                    &inputs,
                    path,
                    errors,
                );
            }
        }
        AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. } => {
            if args.len() != 1 || !order_by.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "state-consuming aggregate phase requires exactly one state input",
                ));
            } else if let Some(argument) = fragment.root_expression(node, args[0])
                && (!super::value_bindings_match_ignoring_root_nullability(
                    &argument.ty,
                    &binding.intermediate_type,
                ) || (binding.intermediate_type.nullable() && !argument.ty.nullable()))
            {
                // The carrier the state travelled in may admit null the
                // phase before it never wrote -- a state crossing an
                // exchange is declared by the column layout, not by the
                // binding. It may not claim the reverse.
                errors.push(ValidationError::new(
                    path,
                    "aggregate state input differs from its bound intermediate type",
                ));
            }
        }
    }
}
