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

//! Closed receiving projections using the original Physical expression vocabulary.
//! Observation, source references and complete frame semantics belong to the caller.

use super::ExpressionCodecError;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;

fn shape(message: &'static str) -> ExpressionCodecError {
    ExpressionCodecError::InvalidShape(message)
}

/// Constant-size closed projections. The caller owns observation and scope.
pub(super) fn decode_unary(
    op: i32,
) -> Result<novarocks_expression_contract::UnaryOperator, ExpressionCodecError> {
    use wire::UnaryOperator as W;
    Ok(match W::try_from(op) {
        Ok(W::Not) => novarocks_expression_contract::UnaryOperator::Not,
        Ok(W::Unspecified) | Err(_) => {
            return Err(shape("unary operator is unknown or unspecified"));
        }
    })
}

pub(super) fn decode_binary(
    op: i32,
) -> Result<novarocks_expression_contract::BinaryOperator, ExpressionCodecError> {
    use wire::BinaryOperator as W;
    Ok(
        match W::try_from(op).map_err(|_| shape("binary operator is unknown"))? {
            W::Unspecified => return Err(shape("binary operator is unspecified")),
            W::Add => novarocks_expression_contract::BinaryOperator::Add,
            W::Subtract => novarocks_expression_contract::BinaryOperator::Subtract,
            W::Multiply => novarocks_expression_contract::BinaryOperator::Multiply,
            W::Divide => novarocks_expression_contract::BinaryOperator::Divide,
            W::Modulo => novarocks_expression_contract::BinaryOperator::Modulo,
            W::Eq => novarocks_expression_contract::BinaryOperator::Eq,
            W::EqForNull => novarocks_expression_contract::BinaryOperator::EqForNull,
            W::NotEq => novarocks_expression_contract::BinaryOperator::NotEq,
            W::Lt => novarocks_expression_contract::BinaryOperator::Lt,
            W::LtEq => novarocks_expression_contract::BinaryOperator::LtEq,
            W::Gt => novarocks_expression_contract::BinaryOperator::Gt,
            W::GtEq => novarocks_expression_contract::BinaryOperator::GtEq,
        },
    )
}

pub(crate) fn decode_window_units(units: i32) -> Result<p::WindowFrameUnits, ExpressionCodecError> {
    use wire::WindowFrameUnits as W;
    Ok(match W::try_from(units) {
        Ok(W::Rows) => p::WindowFrameUnits::Rows,
        Ok(W::Range) => p::WindowFrameUnits::Range,
        Ok(W::Groups) => p::WindowFrameUnits::Groups,
        Ok(W::Unspecified) | Err(_) => {
            return Err(shape(
                "window frame unit or exclusion is unknown or unspecified",
            ));
        }
    })
}

pub(crate) fn decode_window_exclusion(
    exclusion: i32,
) -> Result<p::WindowFrameExclusion, ExpressionCodecError> {
    use wire::WindowFrameExclusion as W;
    Ok(match W::try_from(exclusion) {
        Ok(W::NoOthers) => p::WindowFrameExclusion::NoOthers,
        Ok(W::CurrentRow) => p::WindowFrameExclusion::CurrentRow,
        Ok(W::Group) => p::WindowFrameExclusion::Group,
        Ok(W::Ties) => p::WindowFrameExclusion::Ties,
        Ok(W::Unspecified) | Err(_) => {
            return Err(shape(
                "window frame unit or exclusion is unknown or unspecified",
            ));
        }
    })
}

fn decode_constant(
    value: &wire::ConstantReference,
) -> Result<p::ConstantReference, ExpressionCodecError> {
    Ok(p::ConstantReference {
        pool: p::ConstantPoolId::new(
            value
                .pool_id
                .ok_or_else(|| shape("window constant pool is absent"))?,
        ),
        ordinal: value.row_ordinal,
    })
}
pub(crate) fn decode_window_bound(
    bound: &wire::WindowBound,
) -> Result<p::WindowBound, ExpressionCodecError> {
    use wire::window_bound::Kind as K;
    Ok(
        match bound
            .kind
            .as_ref()
            .ok_or_else(|| shape("window bound kind is absent"))?
        {
            K::UnboundedPreceding(_) => p::WindowBound::UnboundedPreceding,
            K::Preceding(value) => p::WindowBound::Preceding(decode_constant(value)?),
            K::CurrentRow(_) => p::WindowBound::CurrentRow,
            K::Following(value) => p::WindowBound::Following(decode_constant(value)?),
            K::UnboundedFollowing(_) => p::WindowBound::UnboundedFollowing,
        },
    )
}

pub(crate) fn decode_window_frame(
    frame: &wire::WindowFrame,
) -> Result<p::WindowFrame, ExpressionCodecError> {
    // The original receiving reference pass checks ordered start/end presence
    // and kinds before its later units/exclusion validation. Preserve that
    // ordinary-error order for this complete projection as well.
    let start = decode_window_bound(
        frame
            .start
            .as_ref()
            .ok_or_else(|| shape("window frame bound is absent"))?,
    )?;
    let end = decode_window_bound(
        frame
            .end
            .as_ref()
            .ok_or_else(|| shape("window frame bound is absent"))?,
    )?;
    Ok(p::WindowFrame {
        start,
        end,
        units: decode_window_units(frame.units)?,
        exclusion: decode_window_exclusion(frame.exclusion)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_proto_models::physical_control_v2::Empty;

    fn error<T>(result: Result<T, ExpressionCodecError>, expected: &'static str) {
        assert!(
            matches!(result, Err(ExpressionCodecError::InvalidShape(actual)) if actual == expected)
        );
    }
    fn bound(kind: wire::window_bound::Kind) -> wire::WindowBound {
        wire::WindowBound { kind: Some(kind) }
    }

    #[test]
    fn receiving_operators_cover_every_closed_variant_and_original_error_categories() {
        use novarocks_expression_contract::{BinaryOperator as B, UnaryOperator as P};
        use wire::{BinaryOperator as V, UnaryOperator as W};
        assert_eq!(decode_unary(W::Not as i32).unwrap(), P::Not);
        for (wire, expected) in [
            (V::Add, B::Add),
            (V::Subtract, B::Subtract),
            (V::Multiply, B::Multiply),
            (V::Divide, B::Divide),
            (V::Modulo, B::Modulo),
            (V::Eq, B::Eq),
            (V::EqForNull, B::EqForNull),
            (V::NotEq, B::NotEq),
            (V::Lt, B::Lt),
            (V::LtEq, B::LtEq),
            (V::Gt, B::Gt),
            (V::GtEq, B::GtEq),
        ] {
            assert_eq!(decode_binary(wire as i32).unwrap(), expected);
        }
        // Former operation numbers are reserved and follow the unknown-value path.
        for raw in [0, 1, 2, 4, -1, i32::MAX] {
            error(
                decode_unary(raw),
                "unary operator is unknown or unspecified",
            );
        }
        error(decode_binary(0), "binary operator is unspecified");
        for raw in [13, 14, 15, -1, i32::MAX] {
            error(decode_binary(raw), "binary operator is unknown");
        }
    }

    #[test]
    fn receiving_frames_cover_units_exclusions_and_all_sparse_raw_bounds() {
        use wire::window_bound::Kind as K;
        for (wire, expected) in [
            (wire::WindowFrameUnits::Rows, p::WindowFrameUnits::Rows),
            (wire::WindowFrameUnits::Range, p::WindowFrameUnits::Range),
            (wire::WindowFrameUnits::Groups, p::WindowFrameUnits::Groups),
        ] {
            assert_eq!(decode_window_units(wire as i32).unwrap(), expected);
        }
        for (wire, expected) in [
            (
                wire::WindowFrameExclusion::NoOthers,
                p::WindowFrameExclusion::NoOthers,
            ),
            (
                wire::WindowFrameExclusion::CurrentRow,
                p::WindowFrameExclusion::CurrentRow,
            ),
            (
                wire::WindowFrameExclusion::Group,
                p::WindowFrameExclusion::Group,
            ),
            (
                wire::WindowFrameExclusion::Ties,
                p::WindowFrameExclusion::Ties,
            ),
        ] {
            assert_eq!(decode_window_exclusion(wire as i32).unwrap(), expected);
        }
        for (wire, expected) in [
            (
                K::UnboundedPreceding(Empty {}),
                p::WindowBound::UnboundedPreceding,
            ),
            (
                K::Preceding(wire::ConstantReference {
                    pool_id: Some(0),
                    row_ordinal: 0,
                }),
                p::WindowBound::Preceding(p::ConstantReference {
                    pool: p::ConstantPoolId::new(0),
                    ordinal: 0,
                }),
            ),
            (
                K::Preceding(wire::ConstantReference {
                    pool_id: Some(u32::MAX),
                    row_ordinal: u32::MAX,
                }),
                p::WindowBound::Preceding(p::ConstantReference {
                    pool: p::ConstantPoolId::new(u32::MAX),
                    ordinal: u32::MAX,
                }),
            ),
            (K::CurrentRow(Empty {}), p::WindowBound::CurrentRow),
            (
                K::Following(wire::ConstantReference {
                    pool_id: Some(0),
                    row_ordinal: 0,
                }),
                p::WindowBound::Following(p::ConstantReference {
                    pool: p::ConstantPoolId::new(0),
                    ordinal: 0,
                }),
            ),
            (
                K::Following(wire::ConstantReference {
                    pool_id: Some(u32::MAX),
                    row_ordinal: u32::MAX,
                }),
                p::WindowBound::Following(p::ConstantReference {
                    pool: p::ConstantPoolId::new(u32::MAX),
                    ordinal: u32::MAX,
                }),
            ),
            (
                K::UnboundedFollowing(Empty {}),
                p::WindowBound::UnboundedFollowing,
            ),
        ] {
            assert_eq!(decode_window_bound(&bound(wire)).unwrap(), expected);
        }
        let raw = wire::WindowFrame {
            units: wire::WindowFrameUnits::Groups as i32,
            exclusion: wire::WindowFrameExclusion::Ties as i32,
            start: Some(bound(K::Following(wire::ConstantReference {
                pool_id: Some(u32::MAX),
                row_ordinal: u32::MAX,
            }))),
            end: Some(bound(K::Preceding(wire::ConstantReference {
                pool_id: Some(0),
                row_ordinal: 0,
            }))),
        };
        assert_eq!(
            decode_window_frame(&raw).unwrap(),
            p::WindowFrame {
                units: p::WindowFrameUnits::Groups,
                exclusion: p::WindowFrameExclusion::Ties,
                start: p::WindowBound::Following(p::ConstantReference {
                    pool: p::ConstantPoolId::new(u32::MAX),
                    ordinal: u32::MAX
                }),
                end: p::WindowBound::Preceding(p::ConstantReference {
                    pool: p::ConstantPoolId::new(0),
                    ordinal: 0
                })
            }
        );
        // This projection deliberately preserves bounds the semantic owner may
        // reject; it does not infer offsets or reorder start/end.
    }

    #[test]
    fn receiving_frame_presence_and_closed_errors_keep_original_order() {
        use wire::window_bound::Kind as K;
        for raw in [0, -1, i32::MAX] {
            error(
                decode_window_units(raw),
                "window frame unit or exclusion is unknown or unspecified",
            );
            error(
                decode_window_exclusion(raw),
                "window frame unit or exclusion is unknown or unspecified",
            );
        }
        error(
            decode_window_bound(&wire::WindowBound { kind: None }),
            "window bound kind is absent",
        );
        error(
            decode_window_bound(&bound(K::Preceding(wire::ConstantReference {
                pool_id: None,
                row_ordinal: 0,
            }))),
            "window constant pool is absent",
        );
        let mut frame = wire::WindowFrame {
            units: 0,
            exclusion: 0,
            start: None,
            end: None,
        };
        error(decode_window_frame(&frame), "window frame bound is absent");
        frame.start = Some(wire::WindowBound { kind: None });
        error(decode_window_frame(&frame), "window bound kind is absent");
        frame.start = Some(bound(K::CurrentRow(Empty {})));
        error(decode_window_frame(&frame), "window frame bound is absent");
        frame.end = Some(wire::WindowBound { kind: None });
        error(decode_window_frame(&frame), "window bound kind is absent");
        frame.end = Some(bound(K::UnboundedFollowing(Empty {})));
        error(
            decode_window_frame(&frame),
            "window frame unit or exclusion is unknown or unspecified",
        );
        frame.units = wire::WindowFrameUnits::Rows as i32;
        error(
            decode_window_frame(&frame),
            "window frame unit or exclusion is unknown or unspecified",
        );
    }
}
