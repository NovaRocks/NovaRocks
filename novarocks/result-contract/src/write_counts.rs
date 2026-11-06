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

//! Logical write results remain independent of physical writer multiplicity.

use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinishCountMode {
    LogicalChanges,
    WriterReported,
    ExternalOwner,
}

/// Node input positions, disjoint from provider write-target ordinals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinishInputs {
    pub writer_results: u32,
    pub summaries: crate::PerKind<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalChangeCountError {
    Negative,
    Overflow,
}
impl fmt::Display for LogicalChangeCountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Negative => "logical change count must be nonnegative",
            Self::Overflow => "logical change count exceeds the Int64 result domain",
        })
    }
}
impl std::error::Error for LogicalChangeCountError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalChangeCounts {
    inserted: u64,
    replaced: u64,
    deleted: u64,
}

impl LogicalChangeCounts {
    pub const ZERO: Self = Self {
        inserted: 0,
        replaced: 0,
        deleted: 0,
    };
    pub const INSERT: Self = Self {
        inserted: 1,
        replaced: 0,
        deleted: 0,
    };
    pub const REPLACE: Self = Self {
        inserted: 0,
        replaced: 1,
        deleted: 0,
    };
    pub const DELETE: Self = Self {
        inserted: 0,
        replaced: 0,
        deleted: 1,
    };
    pub fn try_new(
        inserted: u64,
        replaced: u64,
        deleted: u64,
    ) -> Result<Self, LogicalChangeCountError> {
        let total = inserted
            .checked_add(replaced)
            .and_then(|n| n.checked_add(deleted))
            .ok_or(LogicalChangeCountError::Overflow)?;
        if total > i64::MAX as u64 {
            return Err(LogicalChangeCountError::Overflow);
        }
        Ok(Self {
            inserted,
            replaced,
            deleted,
        })
    }
    pub fn try_from_wire(
        inserted: i64,
        replaced: i64,
        deleted: i64,
    ) -> Result<Self, LogicalChangeCountError> {
        if inserted < 0 || replaced < 0 || deleted < 0 {
            return Err(LogicalChangeCountError::Negative);
        }
        Self::try_new(inserted as u64, replaced as u64, deleted as u64)
    }
    pub const fn inserted(self) -> u64 {
        self.inserted
    }
    pub const fn replaced(self) -> u64 {
        self.replaced
    }
    pub const fn deleted(self) -> u64 {
        self.deleted
    }
    pub const fn affected_rows(self) -> u64 {
        self.inserted + self.replaced + self.deleted
    }
    pub const fn into_wire(self) -> [i64; 3] {
        [
            self.inserted as i64,
            self.replaced as i64,
            self.deleted as i64,
        ]
    }
    pub fn checked_add(self, rhs: Self) -> Result<Self, LogicalChangeCountError> {
        Self::try_new(
            self.inserted
                .checked_add(rhs.inserted)
                .ok_or(LogicalChangeCountError::Overflow)?,
            self.replaced
                .checked_add(rhs.replaced)
                .ok_or(LogicalChangeCountError::Overflow)?,
            self.deleted
                .checked_add(rhs.deleted)
                .ok_or(LogicalChangeCountError::Overflow)?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mixed_changes_count_each_logical_action_once() {
        let counts = LogicalChangeCounts::try_new(2, 3, 4).unwrap();
        assert_eq!(counts.affected_rows(), 9);
        assert_eq!(counts.into_wire(), [2, 3, 4]);
        assert_eq!(
            LogicalChangeCounts::ZERO.checked_add(counts).unwrap(),
            counts
        );
    }
    #[test]
    fn accumulation_refuses_negative_and_overflow_without_saturation() {
        assert_eq!(
            LogicalChangeCounts::try_from_wire(0, -1, 0),
            Err(LogicalChangeCountError::Negative)
        );
        let max = LogicalChangeCounts::try_new(i64::MAX as u64, 0, 0).unwrap();
        assert_eq!(
            max.checked_add(LogicalChangeCounts::DELETE),
            Err(LogicalChangeCountError::Overflow)
        );
        assert_eq!(
            LogicalChangeCounts::try_new(u64::MAX, 1, 0),
            Err(LogicalChangeCountError::Overflow)
        );
    }
}
