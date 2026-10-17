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
//! Author exact source facts from the expression the current v1 codec emits.
//! SQL syntax that was already canonicalized away is not reconstructed.
//! Callers admit temporary cast/source containers before entry; observation
//! does not mint funding or authenticate an installed owner.
use crate::{ExprArena, ExprId, ExprKind};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, MAX_CONTROL_DEPTH, TemporalCastKind,
    TemporalSourceDefinitions, TemporalSourceFacts, TemporalSourceKind,
};
#[derive(Debug)]
pub enum TemporalSourceProjectionError {
    Control(CompileControlError),
    Invalid(&'static str),
}
impl From<CompileControlError> for TemporalSourceProjectionError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
pub use novarocks_type_contract::{NonCanonicalNativeV1FunctionName, native_v1_function_name};
fn dtype<'a>(
    arena: &'a ExprArena,
    id: ExprId,
) -> Result<&'a DataType, TemporalSourceProjectionError> {
    Ok(&arena
        .get(id)
        .ok_or(TemporalSourceProjectionError::Invalid(
            "temporal source definition is absent",
        ))?
        .ty
        .data_type())
}
fn cast_kind(
    arena: &ExprArena,
    target: &DataType,
    child: ExprId,
) -> Result<TemporalCastKind, TemporalSourceProjectionError> {
    Ok(if matches!(target, DataType::Time64(_)) {
        if matches!(dtype(arena, child)?, DataType::Timestamp(_, _)) {
            TemporalCastKind::TimeFromDatetime
        } else {
            TemporalCastKind::Time
        }
    } else {
        TemporalCastKind::Ordinary
    })
}
fn own_definitions(
    source: &[ExprId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[ExprId]>, TemporalSourceProjectionError> {
    work.flush()?;
    let mut out = Vec::new();
    out.try_reserve_exact(source.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for definition in source {
        out.push(*definition);
        work.step()?;
    }
    work.flush()?;
    let out = out.into_boxed_slice();
    work.flush()?;
    Ok(out)
}
fn own_cast_chain(
    source: &[TemporalCastKind],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[TemporalCastKind]>, TemporalSourceProjectionError> {
    work.flush()?;
    let mut out = Vec::new();
    out.try_reserve_exact(source.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for kind in source {
        out.push(*kind);
        work.step()?;
    }
    work.flush()?;
    let out = out.into_boxed_slice();
    work.flush()?;
    Ok(out)
}
pub fn temporal_source_definitions_observed(
    kind: TemporalSourceKind,
    arena: &ExprArena,
    args: &[ExprId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<TemporalSourceDefinitions<ExprId>, TemporalSourceProjectionError> {
    let arity = if kind == TemporalSourceKind::TimeFormat {
        2
    } else {
        1
    };
    if args.len() != arity {
        return Err(TemporalSourceProjectionError::Invalid(
            "temporal source signature has wrong arity",
        ));
    }
    let normal = args[0];
    let node = arena
        .get(normal)
        .ok_or(TemporalSourceProjectionError::Invalid(
            "temporal normal definition is absent",
        ))?;
    work.step()?;
    if kind == TemporalSourceKind::TimeFormat {
        let (facts, defs) = if let ExprKind::Cast { expr, target, .. } = &node.kind {
            let ordinary = cast_kind(arena, target, *expr)? == TemporalCastKind::Ordinary;
            work.step()?;
            if ordinary && matches!(dtype(arena, *expr)?, DataType::Utf8) {
                (
                    TemporalSourceFacts::FormatUtf8Override,
                    Some([*expr, normal, args[1]]),
                )
            } else {
                (TemporalSourceFacts::FormatOrdinary, None)
            }
        } else {
            (TemporalSourceFacts::FormatOrdinary, None)
        };
        let definitions = if let Some(defs) = defs {
            own_definitions(&defs, work)?
        } else {
            own_definitions(&[normal, args[1]], work)?
        };
        return Ok(TemporalSourceDefinitions { facts, definitions });
    }
    let mut deepest = normal;
    let mut immediate = None;
    // Fixed stack scratch bounds graph inspection without an allocator growth
    // strategy. Only the exact immutable trace is copied once at publication.
    let mut casts = [TemporalCastKind::Ordinary; MAX_CONTROL_DEPTH];
    let mut cast_count = 0usize;
    while let ExprKind::Cast { expr, target, .. } = &arena
        .get(deepest)
        .ok_or(TemporalSourceProjectionError::Invalid(
            "temporal cast definition is absent",
        ))?
        .kind
    {
        if cast_count == MAX_CONTROL_DEPTH {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let kind = cast_kind(arena, target, *expr)?;
        work.step()?;
        if immediate.is_none() {
            immediate = Some(*expr)
        }
        casts[cast_count] = kind;
        cast_count += 1;
        deepest = *expr;
    }
    if let ExprKind::FunctionCall { function, args } = &arena
        .get(deepest)
        .ok_or(TemporalSourceProjectionError::Invalid(
            "temporal deepest definition is absent",
        ))?
        .kind
    {
        work.flush()?;
        for _ in function.function_id.as_str().bytes() {
            work.step()?;
        }
        work.flush()?;
        let is_sec = native_v1_function_name(&function.function_id).map_err(|_| {
            TemporalSourceProjectionError::Invalid(
                "temporal source has no actual native v1 function projection",
            )
        })? == "sec_to_time";
        work.flush()?;
        if is_sec && args.len() == 1 {
            work.flush()?;
            let facts = TemporalSourceFacts::SecondsRoundtrip {
                cast_chain: own_cast_chain(&casts[..cast_count], work)?,
            };
            let definitions = own_definitions(&[args[0]], work)?;
            work.flush()?;
            return Ok(TemporalSourceDefinitions { facts, definitions });
        }
    }
    let (facts, defs) = if matches!(dtype(arena, normal)?, DataType::Utf8) || immediate.is_none() {
        (
            TemporalSourceFacts::SecondsDirect {
                cast_chain: own_cast_chain(&casts[..cast_count], work)?,
            },
            ([normal, normal, normal], 1),
        )
    } else {
        let immediate = immediate.ok_or(TemporalSourceProjectionError::Invalid(
            "temporal cast source is absent",
        ))?;
        if matches!(dtype(arena, immediate)?, DataType::Utf8) {
            (
                TemporalSourceFacts::SecondsCastString {
                    cast_chain: own_cast_chain(&casts[..cast_count], work)?,
                },
                ([normal, immediate, immediate], 2),
            )
        } else {
            (
                TemporalSourceFacts::SecondsCastOther {
                    cast_chain: own_cast_chain(&casts[..cast_count], work)?,
                },
                ([normal, immediate, deepest], 3),
            )
        }
    };
    work.flush()?;
    let definitions = own_definitions(&defs.0[..defs.1], work)?;
    work.flush()?;
    Ok(TemporalSourceDefinitions { facts, definitions })
}

/// ONE author of the actual native-v1 checked Constant -> Literal projection.
/// This classifies the emitted expression kind, never lexical syntax or
/// FunctionArgument.constant. Native codec payload admission remains exact.
pub fn native_v1_emitted_constant_reference(kind: &ExprKind) -> Option<crate::ConstantReference> {
    match kind {
        ExprKind::Constant(reference) => Some(*reference),
        _ => None,
    }
}
/// Author the existing two-child scalar's immutable pattern error policy.
/// A Utf8 NULL Constant emits LiteralNull, and is masked before this policy;
/// the nominal fact promises only the non-NULL emitted LiteralUtf8 shape.
pub fn regexp_count_pattern_source_observed(
    arena: &ExprArena,
    args: &[ExprId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::RegexpCountPatternSource, TemporalSourceProjectionError> {
    if args.len() != 2 {
        return Err(TemporalSourceProjectionError::Invalid(
            "regexp_count source requires two arguments",
        ));
    }
    let pattern = arena
        .get(args[1])
        .ok_or(TemporalSourceProjectionError::Invalid(
            "regexp_count pattern definition is absent",
        ))?;
    work.step()?;
    if matches!(pattern.kind, ExprKind::Literal(_)) {
        return Err(TemporalSourceProjectionError::Invalid(
            "regexp_count requires checked native-v1 source; unchecked Literal is not emitted",
        ));
    }
    novarocks_type_contract::RegexpCountPatternSource::from_native_v1_emitted_shape(
        &pattern.ty,
        native_v1_emitted_constant_reference(&pattern.kind).is_some(),
    )
    .map_err(TemporalSourceProjectionError::Invalid)
}

/// Exact TO_BASE64 immediate source author using the same actual native-v1 identity projection.
pub fn to_base64_byte_source_observed(
    arena: &ExprArena,
    args: &[ExprId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::ToBase64ByteSource, TemporalSourceProjectionError> {
    if args.len() != 1 {
        return Err(TemporalSourceProjectionError::Invalid(
            "to_base64 source requires one argument",
        ));
    }
    let child = arena
        .get(args[0])
        .ok_or(TemporalSourceProjectionError::Invalid(
            "to_base64 source definition is absent",
        ))?;
    work.step()?;
    if matches!(child.kind, ExprKind::Literal(_)) {
        return Err(TemporalSourceProjectionError::Invalid(
            "to_base64 requires checked native-v1 source; unchecked Literal is not emitted",
        ));
    }
    if child.ty.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        || *child.ty.data_type() != DataType::Utf8
    {
        return Err(TemporalSourceProjectionError::Invalid(
            "to_base64 emitted source is not exact Physical Utf8",
        ));
    }
    let name = if let ExprKind::FunctionCall { function, .. } = &child.kind {
        Some(native_v1_function_name(&function.function_id).map_err(|_| {
            TemporalSourceProjectionError::Invalid(
                "to_base64 immediate function has no native-v1 identity",
            )
        })?)
    } else {
        None
    };
    work.flush()?;
    let source =
        novarocks_type_contract::ToBase64ByteSource::from_immediate_native_function_name(name);
    work.flush()?;
    Ok(source)
}
