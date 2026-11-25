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

//! Versioned SQL JSON values. All stored offsets and lengths are relative to
//! their enclosing item; borrowing an item does not materialize a value tree.

pub(crate) mod build;
mod codec;
pub(crate) use codec::validate_utf8;
mod number;
mod render;

pub use build::{Input, encode, parse};
pub use codec::{Container, Entries, Entry, Kind, Lookup, Node, Number, Scalar};
pub(crate) use number::{FixedText, write_scalar_text};
pub use number::{java_double_decimal, write_scalar_cast};
pub use render::{TextCursor, TextEvent, format};

use crate::KernelFailure;
use std::fmt;

pub const VERSION: u8 = 1;
pub const MAX_DEPTH: usize = 1024;

/// Errors at the native value boundary. External text syntax errors and
/// failures reported by the actual query owner remain separate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonValueError {
    Version,
    Extent,
    Tag,
    Text,
    Scalar,
    Directory,
    Depth,
}

impl fmt::Display for JsonValueError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::Version => "Invalid native JSON version",
            Self::Extent => "Invalid native JSON extent",
            Self::Tag => "Invalid native JSON tag",
            Self::Text => "Invalid native JSON UTF-8",
            Self::Scalar => "Invalid native JSON scalar",
            Self::Directory => "Invalid native JSON directory",
            Self::Depth => "JSON nesting depth exceeds 1024",
        })
    }
}

impl std::error::Error for JsonValueError {}

impl novarocks_failure_contract::FailureProjection for JsonValueError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(421).unwrap()),
        )
    }
}

impl novarocks_failure_contract::OuterFailureProjection for JsonValueError {}

#[derive(Debug)]
pub enum ObservedError<E> {
    Value(JsonValueError),
    Control(E),
}

impl<E> From<JsonValueError> for ObservedError<E> {
    fn from(value: JsonValueError) -> Self {
        Self::Value(value)
    }
}

impl<E: fmt::Display> fmt::Display for ObservedError<E> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(error) => error.fmt(out),
            Self::Control(error) => error.fmt(out),
        }
    }
}

#[derive(Debug)]
pub enum JsonError {
    InvalidText(usize),
    Value(JsonValueError),
    Depth,
    Host(KernelFailure),
}

impl fmt::Display for JsonError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidText(offset) => write!(out, "Invalid JSON text at byte {offset}"),
            Self::Value(error) => error.fmt(out),
            Self::Depth => JsonValueError::Depth.fmt(out),
            Self::Host(error) => error.fmt(out),
        }
    }
}

impl std::error::Error for JsonError {}

impl From<JsonValueError> for JsonError {
    fn from(error: JsonValueError) -> Self {
        Self::Value(error)
    }
}

impl From<KernelFailure> for JsonError {
    fn from(error: KernelFailure) -> Self {
        Self::Host(error)
    }
}

/// The caller supplies the original work control and memory custody. Scratch
/// reservations follow the actual allocations; output reservations belong to
/// the caller through adoption or the last alias of the returned allocation.
pub trait JsonWork {
    type Reservation;

    fn step(&mut self) -> Result<(), KernelFailure>;

    fn reserve(&mut self, bytes: usize) -> Result<Self::Reservation, KernelFailure>;

    fn reserve_output(&mut self, bytes: usize) -> Result<(), KernelFailure>;
}

struct Scratch<T, R> {
    values: Vec<T>,
    grant: Option<R>,
}

impl<T, R> Scratch<T, R> {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            grant: None,
        }
    }

    fn push(
        &mut self,
        value: T,
        work: &mut impl JsonWork<Reservation = R>,
    ) -> Result<(), JsonError> {
        if self.values.len() == self.values.capacity() {
            let capacity = self.values.capacity().saturating_mul(2).max(4);
            let bytes = capacity
                .checked_mul(size_of::<T>())
                .ok_or(KernelFailure::ResourceExhausted)?;
            let grant = work.reserve(bytes)?;
            let mut replacement = Vec::new();
            replacement
                .try_reserve_exact(capacity)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            {
                let mut previously_allocated = self.values.drain(..);
                while !previously_allocated.as_slice().is_empty() {
                    work.step()?;
                    replacement.extend(previously_allocated.by_ref().take(256));
                }
            }
            self.values = replacement;
            self.grant = Some(grant);
        }
        self.values.push(value);
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use tests::test_array;
