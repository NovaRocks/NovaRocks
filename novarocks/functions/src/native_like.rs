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
//! Unicode LIKE matching shared by native predicates and the explicit ESCAPE function.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use allocator_api2::{
    alloc::{Allocator, Global},
    vec::Vec as AllocatedVec,
};
use std::{borrow::Cow, convert::Infallible, fmt, str::Chars, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub enum LikeObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum LikeError<E> {
    Operation(crate::KernelFailure),
    Pattern(LikePatternError),
    Host(E),
}

#[derive(Debug)]
pub enum LikePatternError {
    EscapeLength,
    InvalidEscape,
}

pub(crate) struct LikePatternStorage {
    host: Arc<dyn crate::AggregateStateAllocator>,
    allocator: Option<HostAggregateAllocator>,
}
impl LikePatternStorage {
    pub(crate) fn new(host: Arc<dyn crate::AggregateStateAllocator>) -> Self {
        Self {
            host,
            allocator: None,
        }
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        self.allocator
            .as_ref()
            .map_or(0, HostAggregateAllocator::metadata_bytes)
    }
    fn allocator(&mut self) -> Result<&HostAggregateAllocator, crate::KernelFailure> {
        if self.allocator.is_none() {
            self.allocator = Some(HostAggregateAllocator::try_new(Arc::clone(&self.host))?);
        }
        Ok(self
            .allocator
            .as_ref()
            .expect("LIKE pattern allocator is bound"))
    }
}

impl fmt::Display for LikePatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EscapeLength => "Escape string must be a single character",
            Self::InvalidEscape => {
                "Escape character must be followed by '%', '_' or the escape character itself"
            }
        })
    }
}
impl std::error::Error for LikePatternError {}
impl novarocks_failure_contract::FailureProjection for LikePatternError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Data,
            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for LikePatternError {}

fn event<E>(
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
    e: LikeObservation,
) -> Result<(), LikeError<E>> {
    observe(e).map_err(LikeError::Host)
}
fn required<T>(
    run: impl FnOnce(
        &mut dyn FnMut(LikeObservation) -> Result<(), Infallible>,
    ) -> Result<T, LikeError<Infallible>>,
) -> Result<T, crate::KernelFailure> {
    match run(&mut |_| Ok(())) {
        Ok(v) => Ok(v),
        Err(LikeError::Operation(cause)) => Err(cause),
        Err(LikeError::Pattern(cause)) => Err(crate::KernelFailure::from_domain(cause)),
        Err(LikeError::Host(never)) => match never {},
    }
}
pub fn like_match(text: &str, pattern: &str) -> bool {
    required(|observe| like_match_observed(text, pattern, observe)).unwrap()
}
pub fn like_match_observed<E>(
    text: &str,
    pattern: &str,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    like_in_observed(text, pattern, None, None, observe)
}

#[cfg(test)]
pub(crate) fn like_with_escape_observed<E>(
    text: &str,
    pattern: &str,
    escape: &str,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    like_in_observed(text, pattern, Some(escape), None, observe)
}

pub(crate) fn like_in_observed<E>(
    text: &str,
    pattern: &str,
    escape: Option<&str>,
    storage: Option<&mut LikePatternStorage>,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    let escape = escape
        .map(escape_character)
        .transpose()
        .map_err(LikeError::Pattern)?;
    let mut chars = pattern.chars();
    let Some(first) = next_pattern_token(&mut chars, escape, observe)? else {
        event(observe, LikeObservation::Step)?;
        return Ok(text.is_empty());
    };
    match storage {
        Some(storage) => {
            event(observe, LikeObservation::OpaqueBoundary)?;
            let allocator = storage.allocator().map_err(LikeError::Operation)?;
            match_observed(
                text,
                chars,
                first,
                escape,
                allocator.clone(),
                || allocator.take_failure(),
                observe,
            )
        }
        None => match_observed(
            text,
            chars,
            first,
            escape,
            Global,
            || crate::KernelFailure::ResourceExhausted,
            observe,
        ),
    }
}

fn escape_character(escape: &str) -> Result<char, LikePatternError> {
    let mut chars = escape.chars();
    chars
        .next()
        .filter(|c| c.len_utf16() == 1 && chars.next().is_none())
        .ok_or(LikePatternError::EscapeLength)
}

#[derive(Clone, Copy)]
enum PatternToken {
    Literal(char),
    AnyOne,
    AnySequence,
}

fn next_pattern_token<E>(
    chars: &mut Chars<'_>,
    escape: Option<char>,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<Option<PatternToken>, LikeError<E>> {
    let Some(character) = chars.next() else {
        return Ok(None);
    };
    event(observe, LikeObservation::Step)?;
    let token = if Some(character) == escape {
        let escaped = chars
            .next()
            .filter(|escaped| matches!(*escaped, '%' | '_') || *escaped == character)
            .ok_or(LikeError::Pattern(LikePatternError::InvalidEscape))?;
        event(observe, LikeObservation::Step)?;
        PatternToken::Literal(escaped)
    } else {
        match character {
            '%' => PatternToken::AnySequence,
            '_' => PatternToken::AnyOne,
            _ => PatternToken::Literal(character),
        }
    };
    Ok(Some(token))
}

/// Prove that a LIKE pattern matches exactly one string. A wildcard removes
/// the proof. The executor remains responsible for evaluating the predicate
/// and reporting malformed patterns.
pub fn exact_literal_observed<'a, E>(
    pattern: &'a str,
    escape: Option<&str>,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<Option<Cow<'a, str>>, LikeError<E>> {
    let escape = escape
        .map(escape_character)
        .transpose()
        .map_err(LikeError::Pattern)?;
    let mut chars = pattern.chars();
    let mut literal = String::new();
    while let Some(token) = next_pattern_token(&mut chars, escape, observe)? {
        let PatternToken::Literal(value) = token else {
            return Ok(None);
        };
        if escape.is_some() {
            if literal.capacity() - literal.len() < value.len_utf8() {
                event(observe, LikeObservation::OpaqueBoundary)?;
                literal
                    .try_reserve(value.len_utf8())
                    .map_err(|_| LikeError::Operation(crate::KernelFailure::ResourceExhausted))?;
                event(observe, LikeObservation::OpaqueBoundary)?;
            }
            literal.push(value);
        }
    }
    Ok(Some(if escape.is_some() {
        Cow::Owned(literal)
    } else {
        Cow::Borrowed(pattern)
    }))
}

fn parse_pattern<E, A: Allocator>(
    mut chars: Chars<'_>,
    first: PatternToken,
    escape: Option<char>,
    allocator: A,
    allocation_failure: impl Fn() -> crate::KernelFailure,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<AllocatedVec<PatternToken, A>, LikeError<E>> {
    let mut tokens = AllocatedVec::new_in(allocator);
    let mut next = Some(first);
    while let Some(token) = next {
        if tokens.len() == tokens.capacity() {
            event(observe, LikeObservation::OpaqueBoundary)?;
            tokens
                .try_reserve(1)
                .map_err(|_| LikeError::Operation(allocation_failure()))?;
            event(observe, LikeObservation::OpaqueBoundary)?;
        }
        tokens.push(token);
        next = next_pattern_token(&mut chars, escape, observe)?;
    }
    Ok(tokens)
}

fn match_observed<E, A: Allocator>(
    text: &str,
    pattern: Chars<'_>,
    first: PatternToken,
    escape: Option<char>,
    allocator: A,
    allocation_failure: impl Fn() -> crate::KernelFailure,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    // Validate the whole pattern even when a mismatch at the start of the
    // input would be sufficient to decide the match.
    let tokens = parse_pattern(
        pattern,
        first,
        escape,
        allocator,
        allocation_failure,
        observe,
    )?;
    let mut text = text.chars();
    let mut pattern_index = 0;
    let mut wildcard = None;
    loop {
        event(observe, LikeObservation::Step)?;
        match tokens.get(pattern_index).copied() {
            Some(PatternToken::AnySequence) => {
                pattern_index += 1;
                wildcard = Some((pattern_index, text.clone()));
            }
            Some(PatternToken::AnyOne) if text.next().is_some() => pattern_index += 1,
            Some(PatternToken::Literal(literal)) if text.next() == Some(literal) => {
                pattern_index += 1;
            }
            None if text.as_str().is_empty() => return Ok(true),
            _ => {
                // Retry the suffix after the most recent '%' with one more
                // input code point consumed by that wildcard.
                let Some((resume_pattern, resume_text)) = &mut wildcard else {
                    return Ok(false);
                };
                if resume_text.next().is_none() {
                    return Ok(false);
                }
                pattern_index = *resume_pattern;
                text = resume_text.clone();
            }
        }
    }
}
#[cfg(test)]
#[path = "native_like_tests.rs"]
mod tests;
