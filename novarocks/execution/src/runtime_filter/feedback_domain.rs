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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Canonical, transport-neutral terminal domains sent from a Backend to the
//! Frontend for whole-file pruning.  This is intentionally not a physical
//! runtime-filter artifact codec.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use arrow::datatypes::DataType;
use sha2::{Digest, Sha256};

use super::contribution::{
    ContributionCodecError, MembershipValues, ValueDomainDelta, decode_value_domain,
    encode_value_domain,
};

pub const MAX_FEEDBACK_DOMAIN_BYTES: usize = 64 * 1024;
const MAGIC: &[u8; 4] = b"NRFF";
const VERSION: u8 = 1;
const EXACT_TAG: u8 = 1;
const RANGE_TAG: u8 = 2;
const ALL_TAG: u8 = 3;
const HEADER_LEN: usize = MAGIC.len() + 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterFeedbackDomainError {
    InvalidEncoding,
    UnsupportedVersion,
    ResourceLimit,
    NonCanonical,
    InvalidRange,
    Contribution(ContributionCodecError),
}

impl fmt::Display for RuntimeFilterFeedbackDomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid runtime-filter feedback domain: {self:?}")
    }
}

impl Error for RuntimeFilterFeedbackDomainError {}

impl From<ContributionCodecError> for RuntimeFilterFeedbackDomainError {
    fn from(value: ContributionCodecError) -> Self {
        Self::Contribution(value)
    }
}

/// A conservative logical-domain projection. `Range` is only used when its
/// type has an identical, reliable ordering at both execution boundaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterFeedbackDomain {
    Exact(ValueDomainDelta),
    EnclosingRange {
        lower: ValueDomainDelta,
        upper: ValueDomainDelta,
        contains_null: bool,
    },
    All,
}

impl RuntimeFilterFeedbackDomain {
    /// Projects a membership result under the FE feedback payload budget.
    /// It deliberately widens, never truncates: exact set, then enclosing
    /// range for supported types, then unconstrained `All`.
    pub fn project(
        membership: &ValueDomainDelta,
        max_encoded_bytes: usize,
    ) -> Result<Self, RuntimeFilterFeedbackDomainError> {
        let max_encoded_bytes = capped_limit(max_encoded_bytes)?;
        let exact = Self::Exact(membership.clone());
        if exact.encode(max_encoded_bytes).is_ok() {
            return Ok(exact);
        }
        if let Some((lower, upper)) = enclosing_bounds(membership) {
            let range = Self::EnclosingRange {
                lower,
                upper,
                contains_null: membership.contains_null(),
            };
            if range.encode(max_encoded_bytes).is_ok() {
                return Ok(range);
            }
        }
        Ok(Self::All)
    }

    pub fn encode(
        &self,
        max_encoded_bytes: usize,
    ) -> Result<Vec<u8>, RuntimeFilterFeedbackDomainError> {
        let max_encoded_bytes = capped_limit(max_encoded_bytes)?;
        let mut out = Vec::with_capacity(HEADER_LEN);
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        match self {
            Self::Exact(domain) => {
                out.push(EXACT_TAG);
                out.extend_from_slice(&encode_value_domain(domain, max_encoded_bytes)?);
            }
            Self::EnclosingRange {
                lower,
                upper,
                contains_null,
            } => {
                validate_range(lower, upper)?;
                out.push(RANGE_TAG);
                let lower = encode_value_domain(lower, max_encoded_bytes)?;
                let upper = encode_value_domain(upper, max_encoded_bytes)?;
                push_len_prefixed(&mut out, &lower)?;
                push_len_prefixed(&mut out, &upper)?;
                out.push(u8::from(*contains_null));
            }
            Self::All => out.push(ALL_TAG),
        }
        if out.len() > max_encoded_bytes {
            return Err(RuntimeFilterFeedbackDomainError::ResourceLimit);
        }
        Ok(out)
    }

    pub fn decode(
        encoded: &[u8],
        expected_type: &DataType,
        max_encoded_bytes: usize,
    ) -> Result<Self, RuntimeFilterFeedbackDomainError> {
        let max_encoded_bytes = capped_limit(max_encoded_bytes)?;
        if encoded.len() > max_encoded_bytes {
            return Err(RuntimeFilterFeedbackDomainError::ResourceLimit);
        }
        if encoded.len() < HEADER_LEN || &encoded[..MAGIC.len()] != MAGIC {
            return Err(RuntimeFilterFeedbackDomainError::InvalidEncoding);
        }
        if encoded[MAGIC.len()] != VERSION {
            return Err(RuntimeFilterFeedbackDomainError::UnsupportedVersion);
        }
        let body = &encoded[HEADER_LEN..];
        let domain = match encoded[MAGIC.len() + 1] {
            EXACT_TAG => Self::Exact(decode_value_domain(body, expected_type, max_encoded_bytes)?),
            ALL_TAG if body.is_empty() => Self::All,
            RANGE_TAG => {
                let (lower, remainder) = take_len_prefixed(body)?;
                let (upper, remainder) = take_len_prefixed(remainder)?;
                let [contains_null] = remainder else {
                    return Err(RuntimeFilterFeedbackDomainError::InvalidEncoding);
                };
                if *contains_null > 1 {
                    return Err(RuntimeFilterFeedbackDomainError::InvalidEncoding);
                }
                let lower = decode_value_domain(lower, expected_type, max_encoded_bytes)?;
                let upper = decode_value_domain(upper, expected_type, max_encoded_bytes)?;
                validate_range(&lower, &upper)?;
                Self::EnclosingRange {
                    lower,
                    upper,
                    contains_null: *contains_null == 1,
                }
            }
            _ => return Err(RuntimeFilterFeedbackDomainError::InvalidEncoding),
        };
        if domain.encode(max_encoded_bytes)? != encoded {
            return Err(RuntimeFilterFeedbackDomainError::NonCanonical);
        }
        Ok(domain)
    }

    pub fn fingerprint(
        &self,
        max_encoded_bytes: usize,
    ) -> Result<[u8; 32], RuntimeFilterFeedbackDomainError> {
        Ok(Sha256::digest(self.encode(max_encoded_bytes)?).into())
    }
}

fn capped_limit(limit: usize) -> Result<usize, RuntimeFilterFeedbackDomainError> {
    if limit == 0 {
        return Err(RuntimeFilterFeedbackDomainError::ResourceLimit);
    }
    Ok(limit.min(MAX_FEEDBACK_DOMAIN_BYTES))
}

fn push_len_prefixed(
    out: &mut Vec<u8>,
    value: &[u8],
) -> Result<(), RuntimeFilterFeedbackDomainError> {
    let len =
        u32::try_from(value.len()).map_err(|_| RuntimeFilterFeedbackDomainError::ResourceLimit)?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn take_len_prefixed(input: &[u8]) -> Result<(&[u8], &[u8]), RuntimeFilterFeedbackDomainError> {
    let Some(prefix) = input.get(..4) else {
        return Err(RuntimeFilterFeedbackDomainError::InvalidEncoding);
    };
    let len = u32::from_be_bytes(prefix.try_into().expect("four-byte slice")) as usize;
    let Some(value) = input.get(4..4 + len) else {
        return Err(RuntimeFilterFeedbackDomainError::InvalidEncoding);
    };
    Ok((value, &input[4 + len..]))
}

fn validate_range(
    lower: &ValueDomainDelta,
    upper: &ValueDomainDelta,
) -> Result<(), RuntimeFilterFeedbackDomainError> {
    if lower.contains_null()
        || upper.contains_null()
        || lower.data_type() != upper.data_type()
        || !is_singleton(lower.values())
        || !is_singleton(upper.values())
        || !range_supported(lower.values())
    {
        return Err(RuntimeFilterFeedbackDomainError::InvalidRange);
    }
    Ok(())
}

fn is_singleton(values: &MembershipValues) -> bool {
    match values {
        MembershipValues::Boolean(v) => v.len() == 1,
        MembershipValues::Int8(v) => v.len() == 1,
        MembershipValues::Int16(v) => v.len() == 1,
        MembershipValues::Int32(v) => v.len() == 1,
        MembershipValues::Int64(v) => v.len() == 1,
        MembershipValues::LargeInt(v) => v.len() == 1,
        MembershipValues::Float32(v) => v.len() == 1,
        MembershipValues::Float64(v) => v.len() == 1,
        MembershipValues::Utf8(v) => v.len() == 1,
        MembershipValues::Date32(v) => v.len() == 1,
        MembershipValues::Timestamp { values, .. } => values.len() == 1,
        MembershipValues::Decimal128 { values, .. } => values.len() == 1,
        MembershipValues::Temporal { values, .. } => values.len() == 1,
    }
}

fn range_supported(values: &MembershipValues) -> bool {
    !matches!(
        values,
        MembershipValues::Float32(_) | MembershipValues::Float64(_)
    )
}

fn enclosing_bounds(domain: &ValueDomainDelta) -> Option<(ValueDomainDelta, ValueDomainDelta)> {
    let values = domain.values();
    if values.is_empty() || !range_supported(values) {
        return None;
    }
    let (lower, upper) = match values {
        MembershipValues::Boolean(v) => singleton_bounds(v, MembershipValues::boolean),
        MembershipValues::Int8(v) => singleton_bounds(v, MembershipValues::int8),
        MembershipValues::Int16(v) => singleton_bounds(v, MembershipValues::int16),
        MembershipValues::Int32(v) => singleton_bounds(v, MembershipValues::int32),
        MembershipValues::Int64(v) => singleton_bounds(v, MembershipValues::int64),
        MembershipValues::LargeInt(v) => singleton_bounds(v, MembershipValues::large_int),
        MembershipValues::Utf8(v) => singleton_bounds(v, MembershipValues::utf8),
        MembershipValues::Date32(v) => singleton_bounds(v, MembershipValues::date32),
        MembershipValues::Timestamp {
            unit,
            timezone,
            values,
        } => {
            let lower = *values.first()?;
            let upper = *values.last()?;
            (
                MembershipValues::timestamp(*unit, timezone.clone(), [lower]),
                MembershipValues::timestamp(*unit, timezone.clone(), [upper]),
            )
        }
        MembershipValues::Decimal128 {
            precision,
            scale,
            values,
        } => {
            let lower = *values.first()?;
            let upper = *values.last()?;
            (
                MembershipValues::decimal128(*precision, *scale, [lower]).ok()?,
                MembershipValues::decimal128(*precision, *scale, [upper]).ok()?,
            )
        }
        MembershipValues::Float32(_)
        | MembershipValues::Float64(_)
        | MembershipValues::Temporal { .. } => return None,
    };
    Some((
        ValueDomainDelta::new(lower, false),
        ValueDomainDelta::new(upper, false),
    ))
}

fn singleton_bounds<T: Clone + Ord, F>(
    values: &BTreeSet<T>,
    construct: F,
) -> (MembershipValues, MembershipValues)
where
    F: FnOnce([T; 1]) -> MembershipValues + Copy,
{
    let lower = values.first().expect("nonempty checked by caller").clone();
    let upper = values.last().expect("nonempty checked by caller").clone();
    (construct([lower]), construct([upper]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_integer_set_widens_to_a_canonical_range() {
        let domain = ValueDomainDelta::new(MembershipValues::int64(0..10_000), true);
        let feedback = RuntimeFilterFeedbackDomain::project(&domain, 256).expect("projection");
        let RuntimeFilterFeedbackDomain::EnclosingRange { contains_null, .. } = feedback else {
            panic!("must widen to a range");
        };
        assert!(contains_null);
    }

    #[test]
    fn oversized_float_set_widens_to_all() {
        let domain = ValueDomainDelta::new(
            MembershipValues::float64((0..10_000).map(|value| value as f64)),
            false,
        );
        assert_eq!(
            RuntimeFilterFeedbackDomain::project(&domain, 128).expect("projection"),
            RuntimeFilterFeedbackDomain::All
        );
    }

    #[test]
    fn codec_rejects_noncanonical_or_over_budget_frames() {
        let domain = RuntimeFilterFeedbackDomain::Exact(ValueDomainDelta::new(
            MembershipValues::int32([1, 2]),
            false,
        ));
        let mut encoded = domain.encode(128).expect("encode");
        encoded.push(0);
        assert_eq!(
            RuntimeFilterFeedbackDomain::decode(&encoded, &DataType::Int32, 128),
            Err(RuntimeFilterFeedbackDomainError::Contribution(
                ContributionCodecError::NonCanonicalPayload
            ))
        );
    }
}
