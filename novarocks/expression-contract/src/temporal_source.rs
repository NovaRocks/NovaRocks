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
//! Pure callers have no funding port. Unit validation borrows its actual
//! temporary creation scope at each owned cast/source container operation.
use crate::unit_creation::{UnitAllocationFailure, UnitValidationCreation, ValidationFailure};
use crate::{ExprId, ExprKind, ExprNode};
use arrow_schema::DataType;
use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, MAX_CONTROL_DEPTH, TemporalCastKind,
    TemporalSourceDefinitions, TemporalSourceFacts, TemporalSourceKind,
};
use std::alloc::Layout;
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
enum TemporalSourceConstructionFailure<H> {
    Control(CompileControlError),
    Creator(H),
    Observer(H),
    Resource(LayoutResourceError),
    Owner(TemporalSourceProjectionError),
}
impl<H> From<CompileControlError> for TemporalSourceConstructionFailure<H> {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl<H> From<TemporalSourceProjectionError> for TemporalSourceConstructionFailure<H> {
    fn from(e: TemporalSourceProjectionError) -> Self {
        match e {
            TemporalSourceProjectionError::Control(e) => Self::Control(e),
            e => Self::Owner(e),
        }
    }
}
impl<H> From<UnitAllocationFailure<H>> for TemporalSourceConstructionFailure<H> {
    fn from(e: UnitAllocationFailure<H>) -> Self {
        match e {
            UnitAllocationFailure::Control(e) => Self::Control(e),
            UnitAllocationFailure::Creator(e) => Self::Creator(e),
            UnitAllocationFailure::Observer(e) => Self::Observer(e),
            UnitAllocationFailure::Resource(e) => Self::Resource(e),
        }
    }
}
impl<H> From<LayoutResourceError> for TemporalSourceConstructionFailure<H> {
    fn from(e: LayoutResourceError) -> Self {
        Self::Resource(e)
    }
}
pub use novarocks_type_contract::{NonCanonicalNativeV1FunctionName, native_v1_function_name};
fn dtype<'source>(
    definition: &impl Fn(ExprId) -> Option<&'source ExprNode>,
    id: ExprId,
) -> Result<&'source DataType, TemporalSourceProjectionError> {
    Ok(definition(id)
        .ok_or(TemporalSourceProjectionError::Invalid(
            "temporal source definition is absent",
        ))?
        .ty
        .data_type())
}
fn cast_kind<'source>(
    definition: &impl Fn(ExprId) -> Option<&'source ExprNode>,
    target: &DataType,
    child: ExprId,
) -> Result<TemporalCastKind, TemporalSourceProjectionError> {
    Ok(if matches!(target, DataType::Time64(_)) {
        if matches!(dtype(definition, child)?, DataType::Timestamp(_, _)) {
            TemporalCastKind::TimeFromDatetime
        } else {
            TemporalCastKind::Time
        }
    } else {
        TemporalCastKind::Ordinary
    })
}
fn own_definitions<H>(
    source: &[ExprId],
    creation: &mut UnitValidationCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[ExprId]>, TemporalSourceConstructionFailure<H>> {
    work.flush()?;
    let mut out = Vec::new();
    creation.request(
        Layout::array::<ExprId>(source.len()).map_err(|_| LayoutResourceError::Arithmetic)?,
        work,
    )?;
    out.try_reserve_exact(source.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for definition in source {
        out.push(*definition);
        work.step()?;
    }
    work.flush()?;
    creation.before_boxed_trim(&out, work)?;
    let out = out.into_boxed_slice();
    work.flush()?;
    Ok(out)
}
fn own_cast_chain<H>(
    source: &[TemporalCastKind],
    creation: &mut UnitValidationCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[TemporalCastKind]>, TemporalSourceConstructionFailure<H>> {
    work.flush()?;
    let mut out = Vec::new();
    creation.request(
        Layout::array::<TemporalCastKind>(source.len())
            .map_err(|_| LayoutResourceError::Arithmetic)?,
        work,
    )?;
    out.try_reserve_exact(source.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for kind in source {
        out.push(*kind);
        work.step()?;
    }
    work.flush()?;
    creation.before_boxed_trim(&out, work)?;
    let out = out.into_boxed_slice();
    work.flush()?;
    Ok(out)
}
pub fn temporal_source_definitions_observed(
    kind: TemporalSourceKind,
    arena: &[ExprNode],
    args: &[ExprId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<TemporalSourceDefinitions<ExprId>, TemporalSourceProjectionError> {
    temporal_source_definitions_from_lookup_observed(
        kind,
        |id| arena.get(id.get() as usize),
        args,
        work,
    )
}

/// Borrow original definitions while their unit is still being constructed.
/// This is the same source author as the immutable-slice entry: it does not
/// copy definitions, open a scope, or finish the caller's work.
pub fn temporal_source_definitions_from_lookup_observed<'source>(
    kind: TemporalSourceKind,
    definition: impl Fn(ExprId) -> Option<&'source ExprNode>,
    args: &[ExprId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<TemporalSourceDefinitions<ExprId>, TemporalSourceProjectionError> {
    let mut creation = UnitValidationCreation::<std::convert::Infallible>::pure();
    temporal_source_core(kind, definition, args, &mut creation, work).map_err(|error| match error {
        TemporalSourceConstructionFailure::Control(e) => TemporalSourceProjectionError::Control(e),
        TemporalSourceConstructionFailure::Creator(never)
        | TemporalSourceConstructionFailure::Observer(never) => match never {},
        TemporalSourceConstructionFailure::Resource(_) => {
            TemporalSourceProjectionError::Control(CompileControlError::ResourceExhausted)
        }
        TemporalSourceConstructionFailure::Owner(e) => e,
    })
}

/// Only the unit validator borrows funded scratch. The produced Boxes are
/// consumed and dropped inside that same validation call, before its original
/// temporary scope is released; they are never published as source authority.
pub(crate) fn temporal_source_definitions_for_unit<'a, H>(
    kind: TemporalSourceKind,
    definitions: &[ExprNode],
    args: &[ExprId],
    creation: &mut UnitValidationCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<TemporalSourceDefinitions<ExprId>, ValidationFailure<'a, H>> {
    temporal_source_core(
        kind,
        |id| definitions.get(id.get() as usize),
        args,
        creation,
        work,
    )
    .map_err(|error| match error {
        TemporalSourceConstructionFailure::Control(e) => ValidationFailure::Control(e),
        TemporalSourceConstructionFailure::Creator(e) => ValidationFailure::Creator(e),
        TemporalSourceConstructionFailure::Observer(e) => ValidationFailure::Observer(e),
        TemporalSourceConstructionFailure::Resource(e) => ValidationFailure::Resource(e),
        TemporalSourceConstructionFailure::Owner(e) => e.into(),
    })
}

fn temporal_source_core<'source, H>(
    kind: TemporalSourceKind,
    definition: impl Fn(ExprId) -> Option<&'source ExprNode>,
    args: &[ExprId],
    creation: &mut UnitValidationCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<TemporalSourceDefinitions<ExprId>, TemporalSourceConstructionFailure<H>> {
    let arity = if kind == TemporalSourceKind::TimeFormat {
        2
    } else {
        1
    };
    if args.len() != arity {
        return Err(TemporalSourceProjectionError::Invalid(
            "temporal source signature has wrong arity",
        )
        .into());
    }
    let normal = args[0];
    let node = definition(normal).ok_or(TemporalSourceProjectionError::Invalid(
        "temporal normal definition is absent",
    ))?;
    work.step()?;
    if kind == TemporalSourceKind::TimeFormat {
        let (facts, defs) = if let ExprKind::Cast { expr, target, .. } = &node.kind {
            let ordinary = cast_kind(&definition, target, *expr)? == TemporalCastKind::Ordinary;
            work.step()?;
            if ordinary && matches!(dtype(&definition, *expr)?, DataType::Utf8) {
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
            own_definitions(&defs, creation, work)?
        } else {
            own_definitions(&[normal, args[1]], creation, work)?
        };
        return Ok(TemporalSourceDefinitions { facts, definitions });
    }
    let mut deepest = normal;
    let mut immediate = None;
    // Fixed stack scratch bounds graph inspection without an allocator growth
    // strategy. Only the exact immutable trace is copied once at publication.
    let mut casts = [TemporalCastKind::Ordinary; MAX_CONTROL_DEPTH];
    let mut cast_count = 0usize;
    while let ExprKind::Cast { expr, target, .. } = &definition(deepest)
        .ok_or(TemporalSourceProjectionError::Invalid(
            "temporal cast definition is absent",
        ))?
        .kind
    {
        if cast_count == MAX_CONTROL_DEPTH {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let kind = cast_kind(&definition, target, *expr)?;
        work.step()?;
        if immediate.is_none() {
            immediate = Some(*expr)
        }
        casts[cast_count] = kind;
        cast_count += 1;
        deepest = *expr;
    }
    if let ExprKind::FunctionCall { function, args } = &definition(deepest)
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
                cast_chain: own_cast_chain(&casts[..cast_count], creation, work)?,
            };
            let definitions = own_definitions(&[args[0]], creation, work)?;
            work.flush()?;
            return Ok(TemporalSourceDefinitions { facts, definitions });
        }
    }
    let (facts, defs) =
        if matches!(dtype(&definition, normal)?, DataType::Utf8) || immediate.is_none() {
            (
                TemporalSourceFacts::SecondsDirect {
                    cast_chain: own_cast_chain(&casts[..cast_count], creation, work)?,
                },
                ([normal, normal, normal], 1),
            )
        } else {
            let immediate = immediate.ok_or(TemporalSourceProjectionError::Invalid(
                "temporal cast source is absent",
            ))?;
            if matches!(dtype(&definition, immediate)?, DataType::Utf8) {
                (
                    TemporalSourceFacts::SecondsCastString {
                        cast_chain: own_cast_chain(&casts[..cast_count], creation, work)?,
                    },
                    ([normal, immediate, immediate], 2),
                )
            } else {
                (
                    TemporalSourceFacts::SecondsCastOther {
                        cast_chain: own_cast_chain(&casts[..cast_count], creation, work)?,
                    },
                    ([normal, immediate, deepest], 3),
                )
            }
        };
    work.flush()?;
    let definitions = own_definitions(&defs.0[..defs.1], creation, work)?;
    work.flush()?;
    Ok(TemporalSourceDefinitions { facts, definitions })
}

impl std::fmt::Display for TemporalSourceProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for TemporalSourceProjectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}
