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

//! Borrowed complete-type comparison admission. The mandatory source invoice
//! proves the original inline roots separately from comparison work. Both
//! immutable normalized layouts and full semantics use their actual borrowed
//! comparison observers. Frozen work facts grant no memory or capacity.

use crate::physical_type_v2::TypeCodecError;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, FunctionValueType};
use std::mem;

#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundTypeComparisonFacts {
    work_upper_bound: usize,
    flags_match: bool,
}
impl BoundTypeComparisonFacts {
    pub(crate) const fn work_upper_bound(&self) -> usize {
        self.work_upper_bound
    }
    pub(crate) const fn flags_match(&self) -> bool {
        self.flags_match
    }
}

/// One admitted numerical pass and its ensuing exact owner comparison. The
/// caller can accumulate this bound without repeating either operation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct VerifiedTypeBinding {
    facts: BoundTypeComparisonFacts,
    matches: bool,
}
impl VerifiedTypeBinding {
    pub(crate) const fn matches(&self) -> bool {
        self.matches
    }
    pub(crate) const fn work_upper_bound(&self) -> usize {
        self.facts.work_upper_bound()
    }
}

fn shape(message: &'static str) -> TypeCodecError {
    TypeCodecError::InvalidShape(message)
}
fn add(left: usize, right: usize) -> Result<usize, TypeCodecError> {
    left.checked_add(right)
        .ok_or_else(|| shape("borrowed type comparison work sum overflow"))
}
fn mul(left: usize, right: usize) -> Result<usize, TypeCodecError> {
    left.checked_mul(right)
        .ok_or_else(|| shape("borrowed type comparison work product overflow"))
}
fn cap(bound: usize, maximum: usize) -> Result<(), TypeCodecError> {
    if bound > maximum {
        return Err(TypeCodecError::Control(
            CompileControlError::ResourceExhausted,
        ));
    }
    Ok(())
}

fn cap_optional(bound: usize, maximum: Option<usize>) -> Result<(), TypeCodecError> {
    if let Some(maximum) = maximum {
        cap(bound, maximum)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
enum Arithmetic {
    #[default]
    Plain,
    Parent,
}
impl Arithmetic {
    fn add(self, left: usize, right: usize) -> Result<usize, TypeCodecError> {
        match self {
            Self::Plain => add(left, right),
            Self::Parent => left.checked_add(right).ok_or(TypeCodecError::Control(
                CompileControlError::ResourceExhausted,
            )),
        }
    }
}

/// The sole closed-root prefix bound, without observations or scratch setup.
/// This does not prove datatype equality or authorize a logical/carrier domain.
#[cfg(test)]
pub(crate) fn type_binding_prefix_work_upper_bound(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    type_binding_prefix_core(left, right, source_retained_bytes, Arithmetic::Plain)
}

/// The same root arithmetic under caller-owned numerical admission.
pub(crate) fn type_binding_prefix_work_upper_bound_in(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    type_binding_prefix_core(left, right, source_retained_bytes, Arithmetic::Parent)
}

fn type_binding_prefix_core(
    left: &FunctionValueType,
    right: &FunctionValueType,
    _source_retained_bytes: usize,
    arithmetic: Arithmetic,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    // Only closed root flags are inspected. No datatype walk, metadata probe
    // or scratch initialization precedes this numerical admission.
    let flags_match =
        left.nullable() == right.nullable() && left.logical_type() == right.logical_type();
    Ok(BoundTypeComparisonFacts {
        work_upper_bound: arithmetic.add(0, 2)?,
        flags_match,
    })
}

pub(crate) fn preflight_type_binding(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: impl Into<Option<usize>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    preflight_type_binding_admitted(
        left,
        right,
        source_retained_bytes,
        max_work,
        &mut |_| Ok(()),
        work,
    )
}

/// Expose the sole comparison model before its next observation or opaque
/// metadata probe. The caller accumulates actual prefixes, never a guessed B multiplier.
pub(crate) fn preflight_type_binding_admitted<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: impl Into<Option<usize>>,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundTypeComparisonFacts, E> {
    let max_work = max_work.into();
    preflight_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        max_work,
        admit,
        work,
        if max_work.is_some() {
            Arithmetic::Plain
        } else {
            Arithmetic::Parent
        },
    )
}

pub(crate) fn preflight_type_binding_admitted_in<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundTypeComparisonFacts, E> {
    preflight_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        Some(max_work),
        admit,
        work,
        Arithmetic::Parent,
    )
}

fn preflight_type_binding_admitted_core<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: Option<usize>,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
    arithmetic: Arithmetic,
) -> Result<BoundTypeComparisonFacts, E> {
    // Distinct immutable source roots occupy distinct inline storage, even
    // when their nested Field/type allocations alias. This is only a known
    // floor; it cannot prove the trusted invoice's complete retained backing.
    let roots = if std::ptr::eq(left, right) { 1 } else { 2 };
    let minimum = mul(roots, mem::size_of::<FunctionValueType>())?;
    let source_covers_roots = source_retained_bytes >= minimum;
    let prefix = type_binding_prefix_core(left, right, source_retained_bytes, arithmetic)?;
    let facts = if prefix.flags_match() {
        let left_work = left
            .comparison_work_upper_bound()
            .map_err(|_| shape("borrowed type comparison work sum overflow"))?;
        let right_work = right
            .comparison_work_upper_bound()
            .map_err(|_| shape("borrowed type comparison work sum overflow"))?;
        let pair_work = arithmetic.add(left_work, right_work)?;
        BoundTypeComparisonFacts {
            work_upper_bound: arithmetic
                .add(prefix.work_upper_bound(), arithmetic.add(8, pair_work)?)?,
            flags_match: true,
        }
    } else {
        prefix
    };
    // Immutable cache loads and checked arithmetic expose the complete known
    // work before any pending opaque boundary. Missing source remains the
    // original ordinary refusal, with its original controller observations.
    if source_covers_roots {
        cap_optional(facts.work_upper_bound(), max_work)?;
        admit(prefix)?;
        admit(facts)?;
    }
    work.flush().map_err(TypeCodecError::from)?;
    work.step().map_err(TypeCodecError::from)?;
    if !source_covers_roots {
        return Err(shape("borrowed type source invoice omits original inline roots").into());
    }
    work.step().map_err(TypeCodecError::from)?;
    work.step().map_err(TypeCodecError::from)?;
    work.flush().map_err(TypeCodecError::from)?;
    // Real fixed cache/header operations, never a replay of predicted bytes.
    if facts.flags_match() {
        work.step().map_err(TypeCodecError::from)?;
    }
    work.flush().map_err(TypeCodecError::from)?;
    Ok(facts)
}

/// Gate once, then call the original borrowed exact datatype author. Even the
/// same immutable FVT pointer is compared; only the original root-flag mismatch
/// rule permits an early false. No source clone, index or heap scratch exists.
pub(crate) fn verify_type_binding(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: impl Into<Option<usize>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedTypeBinding, TypeCodecError> {
    verify_type_binding_admitted(
        left,
        right,
        source_retained_bytes,
        max_work,
        &mut |_| Ok(()),
        work,
    )
}

/// One actual numerical pass and its exact comparison, using the original
/// body. Every growing prefix is synchronously admitted by the parent.
pub(crate) fn verify_type_binding_admitted<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: impl Into<Option<usize>>,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedTypeBinding, E> {
    let max_work = max_work.into();
    verify_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        max_work,
        admit,
        work,
        if max_work.is_some() {
            Arithmetic::Plain
        } else {
            Arithmetic::Parent
        },
    )
}

pub(crate) fn verify_type_binding_admitted_in<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: usize,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedTypeBinding, E> {
    verify_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        Some(max_work),
        admit,
        work,
        Arithmetic::Parent,
    )
}

/// Same comparison author with an input-derived numerical envelope. Checked
/// source/model bounds still precede every real comparison; None supplies no
/// synthetic work quota and never authorizes a memory allocation.
pub(crate) fn verify_type_binding_optional_work_in<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: Option<usize>,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedTypeBinding, E> {
    verify_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        max_work,
        admit,
        work,
        Arithmetic::Parent,
    )
}

fn verify_type_binding_admitted_core<E: From<TypeCodecError>>(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source_retained_bytes: usize,
    max_work: Option<usize>,
    admit: &mut impl FnMut(BoundTypeComparisonFacts) -> Result<(), E>,
    work: &mut CompileCheckpoints<'_>,
    arithmetic: Arithmetic,
) -> Result<VerifiedTypeBinding, E> {
    let facts = preflight_type_binding_admitted_core(
        left,
        right,
        source_retained_bytes,
        max_work,
        admit,
        work,
        arithmetic,
    )?;
    if !facts.flags_match() {
        return Ok(VerifiedTypeBinding {
            facts,
            matches: false,
        });
    }
    work.flush().map_err(TypeCodecError::from)?;
    let compared = left.exactly_equals_weighted_observed::<TypeCodecError>(right, |_| {
        work.step().map_err(TypeCodecError::from)
    });
    let compared = match compared {
        Err(TypeCodecError::Control(error)) => return Err(TypeCodecError::Control(error).into()),
        result => result,
    };
    // Observe the actual comparison's ordinary refusal exit. Publication and
    // the enclosing caller's ordinary/success finish still occur later.
    work.flush().map_err(TypeCodecError::from)?;
    Ok(VerifiedTypeBinding {
        facts,
        matches: compared?,
    })
}

#[cfg(test)]
#[path = "borrowed_type_resources/tests.rs"]
mod tests;
