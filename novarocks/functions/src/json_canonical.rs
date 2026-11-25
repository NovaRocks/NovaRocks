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
//! Canonical Binary equality keys for native SQL JSON.

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::json_value::{Entries, JsonValueError, Kind, Node, Number, Scalar};
use crate::{AggregateStateAllocator, KernelEvaluationControl, KernelFailure};
use allocator_api2::vec::Vec as HostVec;
use std::{cmp::Ordering, fmt, num::NonZeroU16, ops::Range, sync::Arc};

pub const FUNCTION_NAME: &str = "__json_canonical_key";
pub const FUNCTION_ID: &str = "builtin.scalar/__json_canonical_key/v1";

#[derive(Debug)]
pub struct JsonCanonicalKey {
    bytes: KeyBytes,
}
impl JsonCanonicalKey {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn equals_observed(
        &self,
        other: &Self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, KernelFailure> {
        if self.bytes.len() != other.bytes.len() {
            return Ok(false);
        }
        Ok(compare_bytes(&self.bytes, &other.bytes, control)?.is_eq())
    }
}

#[derive(Debug)]
pub struct InvalidJsonKey(JsonValueError);
impl fmt::Display for InvalidJsonKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cannot compare malformed native JSON: {}", self.0)
    }
}
impl std::error::Error for InvalidJsonKey {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for InvalidJsonKey {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(419).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for InvalidJsonKey {}

fn invalid(error: JsonValueError) -> KernelFailure {
    KernelFailure::from_domain(InvalidJsonKey(error))
}

#[derive(Debug)]
pub struct JsonKeyOutputExtent {
    pub bytes: usize,
}
impl fmt::Display for JsonKeyOutputExtent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "canonical JSON output requires {} bytes beyond Binary's i32 offset representation",
            self.bytes
        )
    }
}
impl std::error::Error for JsonKeyOutputExtent {}
impl novarocks_failure_contract::FailureProjection for JsonKeyOutputExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(420).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for JsonKeyOutputExtent {}
fn push<T>(v: &mut HostVec<T, HostAggregateAllocator>, value: T) -> Result<(), KernelFailure> {
    v.try_reserve(1).map_err(|_| v.allocator().take_failure())?;
    v.push(value);
    Ok(())
}
fn extend(v: &mut HostVec<u8, HostAggregateAllocator>, bytes: &[u8]) -> Result<(), KernelFailure> {
    v.try_reserve(bytes.len())
        .map_err(|_| v.allocator().take_failure())?;
    v.extend_from_slice(bytes);
    Ok(())
}
// The representation is private to the Binary projection. Counts delimit
// container children; a byte length precedes each variable-width payload.
const JSON_NULL: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const STRING: u8 = 3;
const NUMBER: u8 = 4;
const DATE: u8 = 5;
const TIME: u8 = 6;
const ZONED_TIME: u8 = 7;
const TIMESTAMP: u8 = 8;
const ZONED_TIMESTAMP: u8 = 9;
const ARRAY: u8 = 10;
const OBJECT: u8 = 11;

type KeyBytes = HostVec<u8, HostAggregateAllocator>;
type KeyPairs = HostVec<Range<usize>, HostAggregateAllocator>;

fn append_length(out: &mut KeyBytes, mut value: usize) -> Result<(), KernelFailure> {
    while value >= 128 {
        push(out, (value as u8) | 0x80)?;
        value >>= 7;
    }
    push(out, value as u8)
}

fn extend_observed(
    out: &mut KeyBytes,
    bytes: &[u8],
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    out.try_reserve(bytes.len())
        .map_err(|_| out.allocator().take_failure())?;
    for section in bytes.chunks(256) {
        control.checkpoint(1)?;
        out.extend_from_slice(section);
    }
    Ok(())
}

fn append_blob(
    out: &mut KeyBytes,
    bytes: &[u8],
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    append_length(out, bytes.len())?;
    extend_observed(out, bytes, control)
}

fn finite_key(out: &mut KeyBytes, scale: i32, coefficient: &[u8]) -> Result<(), KernelFailure> {
    push(out, 0)?;
    extend(out, &scale.to_le_bytes())?;
    append_length(out, coefficient.len())?;
    extend(out, coefficient)
}

fn finite_integer_key(
    out: &mut KeyBytes,
    mut coefficient: i128,
    mut scale: i32,
) -> Result<(), KernelFailure> {
    if coefficient == 0 {
        scale = 0;
    } else {
        while coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
    }
    let bytes = coefficient.to_le_bytes();
    let mut minimal = bytes.as_slice();
    while let [.., next, last] = minimal {
        if (*last == 0 && next & 128 == 0) || (*last == 255 && next & 128 != 0) {
            minimal = &minimal[..minimal.len() - 1];
        } else {
            break;
        }
    }
    finite_key(out, scale, minimal)
}

fn float_key(out: &mut KeyBytes, value: f64) -> Result<(), KernelFailure> {
    match crate::json_value::java_double_decimal(value) {
        Some((coefficient, scale)) => finite_integer_key(out, i128::from(coefficient), scale),
        None => push(
            out,
            if value.is_nan() {
                1
            } else if value.is_sign_positive() {
                2
            } else {
                3
            },
        ),
    }
}

fn scalar_key(
    out: &mut KeyBytes,
    value: Scalar<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    match value {
        Scalar::Boolean(false) => return push(out, FALSE),
        Scalar::Boolean(true) => return push(out, TRUE),
        Scalar::String(text) => {
            push(out, STRING)?;
            return append_blob(out, text.as_bytes(), control);
        }
        Scalar::Date(days) => {
            push(out, DATE)?;
            return extend(out, &days.to_le_bytes());
        }
        Scalar::Temporal { ty, value } => {
            use novarocks_type_contract::TemporalType;
            push(
                out,
                match ty {
                    TemporalType::Time(_) => TIME,
                    TemporalType::TimeWithTimeZone(_) => ZONED_TIME,
                    TemporalType::Timestamp(_) => TIMESTAMP,
                    TemporalType::TimestampWithTimeZone(_) => ZONED_TIMESTAMP,
                    TemporalType::Interval(_) => return Err(invalid(JsonValueError::Scalar)),
                },
            )?;
            let key = value
                .relation_key(ty)
                .map_err(|_| invalid(JsonValueError::Scalar))?;
            return extend(out, &key.order_bytes());
        }
        _ => {}
    }
    push(out, NUMBER)?;
    match value {
        Scalar::TinyInt(n) => finite_integer_key(out, i128::from(n), 0),
        Scalar::SmallInt(n) => finite_integer_key(out, i128::from(n), 0),
        Scalar::Integer(n) => finite_integer_key(out, i128::from(n), 0),
        Scalar::BigInt(n) => finite_integer_key(out, i128::from(n), 0),
        Scalar::Real(n) => float_key(out, f64::from(n)),
        Scalar::Double(n) => float_key(out, n),
        Scalar::Decimal {
            coefficient, scale, ..
        } => finite_integer_key(out, coefficient, i32::from(scale)),
        Scalar::Number(Number::Finite { coefficient, scale }) => {
            finite_key(out, scale, coefficient)
        }
        Scalar::Number(Number::NaN) => push(out, 1),
        Scalar::Number(Number::PositiveInfinity) => push(out, 2),
        Scalar::Number(Number::NegativeInfinity) => push(out, 3),
        _ => unreachable!("non-numeric JSON values were handled above"),
    }
}

struct ObjectFrame {
    pairs: usize,
    output: usize,
    last: Option<usize>,
}

// One shared allocation for all completed pairs and a reusable reorder buffer.
// Nested object pairs are retired before their enclosing pair is completed.
struct Frame<'a> {
    entries: Entries<'a>,
    object: Option<ObjectFrame>,
}

fn consume_key(
    root: Node<'_>,
    allocator: HostAggregateAllocator,
    control: &dyn KernelEvaluationControl,
) -> Result<KeyBytes, KernelFailure> {
    let mut frames = HostVec::new_in(allocator.clone());
    let mut pairs = HostVec::new_in(allocator.clone());
    let mut scratch = HostVec::new_in(allocator.clone());
    let mut out = HostVec::new_in(allocator);
    let mut pending = Some(root);
    loop {
        control.checkpoint(1)?;
        if let Some(node) = pending.take() {
            let kind =
                node.kind_observed(|| control.checkpoint(1))
                    .map_err(|error| match error {
                        crate::json_value::ObservedError::Value(error) => invalid(error),
                        crate::json_value::ObservedError::Control(error) => error,
                    })?;
            match kind {
                Kind::Null => push(&mut out, JSON_NULL)?,
                Kind::Scalar(scalar) => scalar_key(&mut out, scalar, control)?,
                Kind::Array(container) | Kind::Object(container) => {
                    if frames.len() == crate::json_value::MAX_DEPTH {
                        return Err(invalid(JsonValueError::Depth));
                    }
                    let object = matches!(kind, Kind::Object(_));
                    push(&mut out, if object { OBJECT } else { ARRAY })?;
                    append_length(&mut out, container.len())?;
                    push(
                        &mut frames,
                        Frame {
                            entries: container.entries(),
                            object: object.then_some(ObjectFrame {
                                pairs: pairs.len(),
                                output: out.len(),
                                last: None,
                            }),
                        },
                    )?;
                }
            }
        }
        let Some(frame) = frames.last_mut() else {
            return Ok(out);
        };
        if let Some(object) = frame.object.as_mut()
            && let Some(begin) = object.last.take()
        {
            push(&mut pairs, begin..out.len())?;
        }
        match frame.entries.next().transpose().map_err(invalid)? {
            Some(entry) => {
                if let Some(object) = frame.object.as_mut() {
                    let key = entry.key.ok_or_else(|| invalid(JsonValueError::Tag))?;
                    object.last = Some(out.len());
                    append_blob(&mut out, key.as_bytes(), control)?;
                }
                pending = Some(entry.value);
            }
            None => {
                if let Some(object) = frame.object.take() {
                    order_pairs(object, &mut pairs, &mut out, &mut scratch, control)?;
                }
                frames.pop();
            }
        }
    }
}

fn order_pairs(
    frame: ObjectFrame,
    pairs: &mut KeyPairs,
    out: &mut KeyBytes,
    scratch: &mut KeyBytes,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let children = &mut pairs[frame.pairs..];
    let compare = |a: &Range<usize>, b: &Range<usize>| {
        compare_bytes(&out[a.clone()], &out[b.clone()], control)
    };
    let mut sorted = true;
    for pair in children.windows(2) {
        if compare(&pair[0], &pair[1])?.is_gt() {
            sorted = false;
            break;
        }
    }
    if !sorted {
        sort_pairs(children, compare)?;
        scratch.clear();
        scratch
            .try_reserve(out.len() - frame.output)
            .map_err(|_| scratch.allocator().take_failure())?;
        for child in children {
            extend_observed(scratch, &out[child.clone()], control)?;
        }
        out.truncate(frame.output);
        extend_observed(out, scratch, control)?;
    }
    pairs.truncate(frame.pairs);
    Ok(())
}

fn compare_bytes(
    left: &[u8],
    right: &[u8],
    control: &dyn KernelEvaluationControl,
) -> Result<Ordering, KernelFailure> {
    control.checkpoint(1)?;
    for (a, b) in left.chunks(256).zip(right.chunks(256)) {
        control.checkpoint(1)?;
        let order = a.cmp(b);
        if !order.is_eq() {
            return Ok(order);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

fn sort_pairs(
    pairs: &mut [Range<usize>],
    compare: impl Fn(&Range<usize>, &Range<usize>) -> Result<Ordering, KernelFailure>,
) -> Result<(), KernelFailure> {
    fn sift(
        pairs: &mut [Range<usize>],
        mut index: usize,
        compare: &impl Fn(&Range<usize>, &Range<usize>) -> Result<Ordering, KernelFailure>,
    ) -> Result<(), KernelFailure> {
        while index < pairs.len() / 2 {
            let left = index * 2 + 1;
            let right = left + 1;
            let larger = if right < pairs.len() && compare(&pairs[left], &pairs[right])?.is_lt() {
                right
            } else {
                left
            };
            if !compare(&pairs[index], &pairs[larger])?.is_lt() {
                break;
            }
            pairs.swap(index, larger);
            index = larger;
        }
        Ok(())
    }
    for index in (0..pairs.len() / 2).rev() {
        sift(pairs, index, &compare)?;
    }
    for end in (1..pairs.len()).rev() {
        pairs.swap(0, end);
        sift(&mut pairs[..end], 0, &compare)?;
    }
    Ok(())
}

/// Project a value on the original allocation and control host.
pub fn normalize_json_key(
    input: &[u8],
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<JsonCanonicalKey, KernelFailure> {
    control.checkpoint(0)?;
    normalize_json_key_in(input, key_allocator(host)?, control)
}

pub(crate) fn key_allocator(
    host: Arc<dyn AggregateStateAllocator>,
) -> Result<HostAggregateAllocator, KernelFailure> {
    if host.opaque_allocation_host().is_some() {
        HostAggregateAllocator::try_new_with_journal(host)
    } else {
        HostAggregateAllocator::try_new(host)
    }
}

pub(crate) fn normalize_json_key_in(
    input: &[u8],
    allocator: HostAggregateAllocator,
    control: &dyn KernelEvaluationControl,
) -> Result<JsonCanonicalKey, KernelFailure> {
    control.checkpoint(0)?;
    let value = Node::from_root(input).map_err(invalid)?;
    let bytes = consume_key(value, allocator, control)?;
    control.checkpoint(0)?;
    Ok(JsonCanonicalKey { bytes })
}

#[cfg(test)]
#[path = "json_canonical_tests.rs"]
mod tests;
