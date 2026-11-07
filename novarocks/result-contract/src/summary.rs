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

//! Closed Task-summary vocabulary, independent of Arrow carriers and execution.
//!
//! Field order, non-null wire types and merging are semantic contracts. They do
//! not prove source coverage or authorize publication; those remain owner facts.

use std::fmt;

use crate::{LogicalChangeCountError, LogicalChangeCounts};

/// Exact summary kinds. The numeric identities are frozen wire values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u32)]
pub enum SummaryKind {
    WriteCounts = 1,
    DeleteVerification = 2,
}

impl SummaryKind {
    pub const ALL: [Self; 2] = [Self::WriteCounts, Self::DeleteVerification];

    pub const fn wire_value(self) -> u32 {
        self as u32
    }

    pub const fn try_from_wire(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::WriteCounts),
            2 => Some(Self::DeleteVerification),
            _ => None,
        }
    }

    pub const fn fields(self) -> &'static [SummaryField] {
        match self {
            Self::WriteCounts => &[
                SummaryField::Inserted,
                SummaryField::Replaced,
                SummaryField::Deleted,
            ],
            Self::DeleteVerification => &[
                SummaryField::Requested,
                SummaryField::Matched,
                SummaryField::InsufficientKeys,
                SummaryField::InsufficientFingerprint,
            ],
        }
    }

    pub fn wire_types(self) -> impl ExactSizeIterator<Item = SummaryWireType> {
        self.fields().iter().map(|field| field.wire_type())
    }
}

impl fmt::Display for SummaryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::WriteCounts => "write-counts",
            Self::DeleteVerification => "delete-verification",
        })
    }
}

/// Non-null wire leaf types. This vocabulary does not own Arrow data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SummaryWireType {
    Int64,
    FixedSizeBinary(u16),
}

/// Each field belongs to exactly one kind, so an inconsistent pair is absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SummaryField {
    Inserted,
    Replaced,
    Deleted,
    Requested,
    Matched,
    InsufficientKeys,
    InsufficientFingerprint,
}

impl SummaryField {
    pub const fn kind(self) -> SummaryKind {
        match self {
            Self::Inserted | Self::Replaced | Self::Deleted => SummaryKind::WriteCounts,
            Self::Requested
            | Self::Matched
            | Self::InsufficientKeys
            | Self::InsufficientFingerprint => SummaryKind::DeleteVerification,
        }
    }

    /// Zero-based position in the kind's fixed field contract.
    pub const fn ordinal(self) -> usize {
        match self {
            Self::Inserted | Self::Requested => 0,
            Self::Replaced | Self::Matched => 1,
            Self::Deleted | Self::InsufficientKeys => 2,
            Self::InsufficientFingerprint => 3,
        }
    }

    pub fn from_ordinal(kind: SummaryKind, ordinal: usize) -> Option<Self> {
        kind.fields().get(ordinal).copied()
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Inserted => "inserted",
            Self::Replaced => "replaced",
            Self::Deleted => "deleted",
            Self::Requested => "requested",
            Self::Matched => "matched",
            Self::InsufficientKeys => "insufficient_keys",
            Self::InsufficientFingerprint => "insufficient_fingerprint",
        }
    }

    pub const fn wire_type(self) -> SummaryWireType {
        match self {
            Self::InsufficientFingerprint => SummaryWireType::FixedSizeBinary(16),
            _ => SummaryWireType::Int64,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteVerificationCountError {
    Negative,
    Overflow,
}

impl fmt::Display for DeleteVerificationCountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Negative => "delete verification count must be nonnegative",
            Self::Overflow => "delete verification count exceeds the Int64 result domain",
        })
    }
}

impl std::error::Error for DeleteVerificationCountError {}

/// Counters have separate Int64 domains; their sum has no shared bound.
/// Fingerprints are diagnostic evidence and merge modulo 2^128.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteVerificationCounts {
    requested: u64,
    matched: u64,
    insufficient_keys: u64,
    fingerprint: u128,
}

impl DeleteVerificationCounts {
    pub const ZERO: Self = Self {
        requested: 0,
        matched: 0,
        insufficient_keys: 0,
        fingerprint: 0,
    };

    pub fn try_new(
        requested: u64,
        matched: u64,
        insufficient_keys: u64,
        fingerprint: u128,
    ) -> Result<Self, DeleteVerificationCountError> {
        if requested > i64::MAX as u64
            || matched > i64::MAX as u64
            || insufficient_keys > i64::MAX as u64
        {
            return Err(DeleteVerificationCountError::Overflow);
        }
        Ok(Self {
            requested,
            matched,
            insufficient_keys,
            fingerprint,
        })
    }

    pub fn try_from_wire(
        requested: i64,
        matched: i64,
        insufficient_keys: i64,
        fingerprint: [u8; 16],
    ) -> Result<Self, DeleteVerificationCountError> {
        if requested < 0 || matched < 0 || insufficient_keys < 0 {
            return Err(DeleteVerificationCountError::Negative);
        }
        Self::try_new(
            requested as u64,
            matched as u64,
            insufficient_keys as u64,
            u128::from_le_bytes(fingerprint),
        )
    }

    pub const fn requested(self) -> u64 {
        self.requested
    }

    pub const fn matched(self) -> u64 {
        self.matched
    }

    pub const fn insufficient_keys(self) -> u64 {
        self.insufficient_keys
    }

    pub const fn fingerprint(self) -> u128 {
        self.fingerprint
    }

    pub const fn into_wire(self) -> ([i64; 3], [u8; 16]) {
        (
            [
                self.requested as i64,
                self.matched as i64,
                self.insufficient_keys as i64,
            ],
            self.fingerprint.to_le_bytes(),
        )
    }

    pub fn checked_add(self, rhs: Self) -> Result<Self, DeleteVerificationCountError> {
        Self::try_new(
            self.requested
                .checked_add(rhs.requested)
                .ok_or(DeleteVerificationCountError::Overflow)?,
            self.matched
                .checked_add(rhs.matched)
                .ok_or(DeleteVerificationCountError::Overflow)?,
            self.insufficient_keys
                .checked_add(rhs.insufficient_keys)
                .ok_or(DeleteVerificationCountError::Overflow)?,
            self.fingerprint.wrapping_add(rhs.fingerprint),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SummaryMergeError {
    KindMismatch {
        expected: SummaryKind,
        actual: SummaryKind,
    },
    WriteCounts(LogicalChangeCountError),
    DeleteVerification(DeleteVerificationCountError),
}

impl fmt::Display for SummaryMergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KindMismatch { expected, actual } => {
                write!(f, "cannot merge {actual} into {expected} summary")
            }
            Self::WriteCounts(error) => write!(f, "write-counts summary: {error}"),
            Self::DeleteVerification(error) => write!(f, "delete-verification summary: {error}"),
        }
    }
}

impl std::error::Error for SummaryMergeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::KindMismatch { .. } => None,
            Self::WriteCounts(error) => Some(error),
            Self::DeleteVerification(error) => Some(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SummaryValue {
    WriteCounts(LogicalChangeCounts),
    DeleteVerification(DeleteVerificationCounts),
}

impl SummaryValue {
    pub const fn zero(kind: SummaryKind) -> Self {
        match kind {
            SummaryKind::WriteCounts => Self::WriteCounts(LogicalChangeCounts::ZERO),
            SummaryKind::DeleteVerification => {
                Self::DeleteVerification(DeleteVerificationCounts::ZERO)
            }
        }
    }

    pub const fn kind(self) -> SummaryKind {
        match self {
            Self::WriteCounts(_) => SummaryKind::WriteCounts,
            Self::DeleteVerification(_) => SummaryKind::DeleteVerification,
        }
    }

    pub fn checked_merge(self, rhs: Self) -> Result<Self, SummaryMergeError> {
        match (self, rhs) {
            (Self::WriteCounts(a), Self::WriteCounts(b)) => a
                .checked_add(b)
                .map(Self::WriteCounts)
                .map_err(SummaryMergeError::WriteCounts),
            (Self::DeleteVerification(a), Self::DeleteVerification(b)) => a
                .checked_add(b)
                .map(Self::DeleteVerification)
                .map_err(SummaryMergeError::DeleteVerification),
            _ => Err(SummaryMergeError::KindMismatch {
                expected: self.kind(),
                actual: rhs.kind(),
            }),
        }
    }
}

/// Closed per-kind slots. Each kind can contain at most one value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PerKind<T> {
    pub write_counts: Option<T>,
    pub delete_verification: Option<T>,
}

impl<T> Default for PerKind<T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T> PerKind<T> {
    pub const fn empty() -> Self {
        Self {
            write_counts: None,
            delete_verification: None,
        }
    }

    pub const fn get(&self, kind: SummaryKind) -> Option<&T> {
        match kind {
            SummaryKind::WriteCounts => self.write_counts.as_ref(),
            SummaryKind::DeleteVerification => self.delete_verification.as_ref(),
        }
    }

    pub const fn get_mut(&mut self, kind: SummaryKind) -> Option<&mut T> {
        match kind {
            SummaryKind::WriteCounts => self.write_counts.as_mut(),
            SummaryKind::DeleteVerification => self.delete_verification.as_mut(),
        }
    }

    pub fn insert(&mut self, kind: SummaryKind, value: T) -> Option<T> {
        match kind {
            SummaryKind::WriteCounts => self.write_counts.replace(value),
            SummaryKind::DeleteVerification => self.delete_verification.replace(value),
        }
    }

    pub const fn is_empty(&self) -> bool {
        self.write_counts.is_none() && self.delete_verification.is_none()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (SummaryKind, &T)> {
        [
            (SummaryKind::WriteCounts, self.write_counts.as_ref()),
            (
                SummaryKind::DeleteVerification,
                self.delete_verification.as_ref(),
            ),
        ]
        .into_iter()
        .filter_map(|(kind, value)| value.map(|value| (kind, value)))
    }

    pub fn iter_mut(&mut self) -> impl DoubleEndedIterator<Item = (SummaryKind, &mut T)> {
        [
            (SummaryKind::WriteCounts, self.write_counts.as_mut()),
            (
                SummaryKind::DeleteVerification,
                self.delete_verification.as_mut(),
            ),
        ]
        .into_iter()
        .filter_map(|(kind, value)| value.map(|value| (kind, value)))
    }

    pub fn map<U>(self, mut f: impl FnMut(SummaryKind, T) -> U) -> PerKind<U> {
        PerKind {
            write_counts: self
                .write_counts
                .map(|value| f(SummaryKind::WriteCounts, value)),
            delete_verification: self
                .delete_verification
                .map(|value| f(SummaryKind::DeleteVerification, value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pure owner tests protect the fixed semantic contract. They need no fixture
    // or process cleanup and run with the result-contract package's Rust tests.
    #[test]
    fn kind_fields_freeze_nonnull_wire_order() {
        assert_eq!(SummaryKind::WriteCounts.wire_value(), 1);
        assert_eq!(SummaryKind::DeleteVerification.wire_value(), 2);
        assert_eq!(
            SummaryKind::try_from_wire(1),
            Some(SummaryKind::WriteCounts)
        );
        assert_eq!(
            SummaryKind::try_from_wire(2),
            Some(SummaryKind::DeleteVerification)
        );
        assert_eq!(
            SummaryKind::WriteCounts.fields(),
            &[
                SummaryField::Inserted,
                SummaryField::Replaced,
                SummaryField::Deleted,
            ]
        );
        assert_eq!(
            SummaryKind::DeleteVerification.fields(),
            &[
                SummaryField::Requested,
                SummaryField::Matched,
                SummaryField::InsufficientKeys,
                SummaryField::InsufficientFingerprint,
            ]
        );
        assert_eq!(
            SummaryKind::WriteCounts.wire_types().collect::<Vec<_>>(),
            vec![SummaryWireType::Int64; 3]
        );
        assert_eq!(
            SummaryKind::DeleteVerification
                .wire_types()
                .collect::<Vec<_>>(),
            vec![
                SummaryWireType::Int64,
                SummaryWireType::Int64,
                SummaryWireType::Int64,
                SummaryWireType::FixedSizeBinary(16),
            ]
        );
        for kind in SummaryKind::ALL {
            assert_eq!(SummaryKind::try_from_wire(kind.wire_value()), Some(kind));
            for (ordinal, field) in kind.fields().iter().copied().enumerate() {
                assert_eq!(field.kind(), kind);
                assert_eq!(field.ordinal(), ordinal);
                assert_eq!(SummaryField::from_ordinal(kind, ordinal), Some(field));
            }
            assert_eq!(SummaryField::from_ordinal(kind, kind.fields().len()), None);
        }
        assert_eq!(SummaryKind::try_from_wire(0), None);
        assert_eq!(SummaryKind::try_from_wire(3), None);
    }

    #[test]
    fn both_summary_kinds_have_distinct_zero_values() {
        assert_eq!(
            SummaryValue::zero(SummaryKind::WriteCounts),
            SummaryValue::WriteCounts(LogicalChangeCounts::ZERO)
        );
        assert_eq!(
            SummaryValue::zero(SummaryKind::DeleteVerification),
            SummaryValue::DeleteVerification(DeleteVerificationCounts::ZERO)
        );
        assert_eq!(
            DeleteVerificationCounts::ZERO.into_wire(),
            ([0; 3], [0; 16])
        );
        let write = SummaryValue::WriteCounts(LogicalChangeCounts::REPLACE);
        assert_eq!(
            write.checked_merge(SummaryValue::zero(SummaryKind::WriteCounts)),
            Ok(write)
        );
        assert_eq!(
            write.checked_merge(SummaryValue::zero(SummaryKind::DeleteVerification)),
            Err(SummaryMergeError::KindMismatch {
                expected: SummaryKind::WriteCounts,
                actual: SummaryKind::DeleteVerification,
            })
        );
    }

    #[test]
    fn delete_verification_checks_each_count_domain_independently() {
        for values in [[-1, 0, 0], [0, -1, 0], [0, 0, -1]] {
            assert_eq!(
                DeleteVerificationCounts::try_from_wire(values[0], values[1], values[2], [0; 16]),
                Err(DeleteVerificationCountError::Negative)
            );
        }
        let max = i64::MAX as u64;
        let full = DeleteVerificationCounts::try_new(max, max, max, 0).unwrap();
        assert_eq!(full.into_wire().0, [i64::MAX; 3]);
        for (values, addition) in [
            ([max, 0, 0], [1, 0, 0]),
            ([0, max, 0], [0, 1, 0]),
            ([0, 0, max], [0, 0, 1]),
        ] {
            let a = DeleteVerificationCounts::try_new(values[0], values[1], values[2], 0).unwrap();
            let b = DeleteVerificationCounts::try_new(addition[0], addition[1], addition[2], 0)
                .unwrap();
            assert_eq!(
                a.checked_add(b),
                Err(DeleteVerificationCountError::Overflow)
            );
            assert_eq!(
                SummaryValue::DeleteVerification(a)
                    .checked_merge(SummaryValue::DeleteVerification(b)),
                Err(SummaryMergeError::DeleteVerification(
                    DeleteVerificationCountError::Overflow
                ))
            );
        }
        for values in [[u64::MAX, 0, 0], [0, u64::MAX, 0], [0, 0, u64::MAX]] {
            assert_eq!(
                DeleteVerificationCounts::try_new(values[0], values[1], values[2], 0),
                Err(DeleteVerificationCountError::Overflow)
            );
        }
        let write_max = SummaryValue::WriteCounts(LogicalChangeCounts::try_new(max, 0, 0).unwrap());
        assert_eq!(
            write_max.checked_merge(SummaryValue::WriteCounts(LogicalChangeCounts::INSERT)),
            Err(SummaryMergeError::WriteCounts(
                LogicalChangeCountError::Overflow
            ))
        );
    }

    #[test]
    fn verification_wire_roundtrip_retains_little_endian_fingerprint() {
        let fingerprint = 0x100f_0e0d_0c0b_0a09_0807_0605_0403_0201;
        let value = DeleteVerificationCounts::try_new(19, 17, 2, fingerprint).unwrap();
        let (counts, bytes) = value.into_wire();
        assert_eq!(counts, [19, 17, 2]);
        assert_eq!(
            bytes,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        assert_eq!(
            DeleteVerificationCounts::try_from_wire(counts[0], counts[1], counts[2], bytes),
            Ok(value)
        );
        assert_eq!(value.requested(), 19);
        assert_eq!(value.matched(), 17);
        assert_eq!(value.insufficient_keys(), 2);
        assert_eq!(value.fingerprint(), fingerprint);
    }

    #[test]
    fn fingerprint_modular_sum_preserves_multiplicity_and_merge_order() {
        let a = DeleteVerificationCounts::try_new(2, 1, 1, u128::MAX).unwrap();
        let b = DeleteVerificationCounts::try_new(5, 3, 1, 7).unwrap();
        let c = DeleteVerificationCounts::try_new(3, 2, 1, 2).unwrap();
        let abc = a.checked_add(b).unwrap().checked_add(c).unwrap();
        let cab = c.checked_add(a).unwrap().checked_add(b).unwrap();
        assert_eq!(abc, cab);
        assert_eq!(abc.into_wire(), ([10, 6, 3], 8_u128.to_le_bytes()));
        assert_eq!(b.checked_add(b).unwrap().fingerprint(), 14);
        assert_eq!(DeleteVerificationCounts::ZERO.checked_add(a), Ok(a));
    }

    #[test]
    fn per_kind_slots_preserve_kind_order_and_independent_values() {
        let mut values = PerKind::<u32>::default();
        assert!(values.is_empty());
        assert_eq!(values.get(SummaryKind::WriteCounts), None);
        assert_eq!(values.insert(SummaryKind::DeleteVerification, 8), None);
        assert_eq!(values.insert(SummaryKind::WriteCounts, 3), None);
        assert_eq!(values.insert(SummaryKind::WriteCounts, 4), Some(3));
        assert_eq!(values.get(SummaryKind::WriteCounts), Some(&4));
        *values.get_mut(SummaryKind::DeleteVerification).unwrap() += 1;
        assert_eq!(
            values
                .iter()
                .map(|(kind, value)| (kind, *value))
                .collect::<Vec<_>>(),
            vec![
                (SummaryKind::WriteCounts, 4),
                (SummaryKind::DeleteVerification, 9)
            ]
        );
        for (_, value) in values.iter_mut() {
            *value += 1;
        }
        let mapped = values.map(|kind, value| (kind.wire_value(), value));
        assert_eq!(mapped.write_counts, Some((1, 5)));
        assert_eq!(mapped.delete_verification, Some((2, 10)));
        assert!(!mapped.is_empty());
    }
}
