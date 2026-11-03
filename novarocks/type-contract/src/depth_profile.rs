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

//! Explicit semantic and physical traversal coordinates. Observation grants
//! neither allocation receipts, runtime capabilities nor memory capacity.

use crate::{MAX_VALUE_TYPE_DEPTH, ValueTypeError};
use arrow_schema::DataType;

/// A boundary selects its existing policy explicitly. Value profiles do not
/// grant support for a particular encoding; that stays with its real consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrowTypeProfile {
    /// The original physical-tree policy. Constant/root/provider owners keep
    /// their existing separately selected format and encoding limits.
    Physical,
    /// A complete value declaration; runtime wrappers retain physical visits.
    /// Existing type-node and metadata admission remains with the type owner.
    Value,
    /// A normalized immutable ValueLayout, with no runtime encoding wrappers.
    CanonicalValue,
}
impl ArrowTypeProfile {
    /// Only the original physical-tree consumer has a physical depth policy.
    /// Value declarations keep their inherited logical/node contracts; a
    /// derived physical depth is stack-test geometry, never another quota.
    pub const fn physical_depth_limit(self) -> Option<usize> {
        match self {
            Self::Physical => Some(MAX_VALUE_TYPE_DEPTH),
            Self::Value | Self::CanonicalValue => None,
        }
    }
    pub fn check_physical_depth(self, physical_depth: usize) -> Result<(), ValueTypeError> {
        if self
            .physical_depth_limit()
            .is_some_and(|limit| physical_depth > limit)
        {
            Err(ValueTypeError::TooDeep)
        } else {
            Ok(())
        }
    }
}

/// One checked traversal coordinate. Field/Map scaffolding and encoding edges
/// retain their physical visits even when they do not increase logical depth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArrowTypeDepth {
    logical: usize,
    physical: usize,
    encodings: usize,
}
impl ArrowTypeDepth {
    pub const ROOT: Self = Self {
        logical: 1,
        physical: 1,
        encodings: 0,
    };
    pub const fn logical(self) -> usize {
        self.logical
    }
    pub const fn physical(self) -> usize {
        self.physical
    }
    pub const fn encoding_wrappers(self) -> usize {
        self.encodings
    }
    pub fn check(self, profile: ArrowTypeProfile) -> Result<(), ValueTypeError> {
        profile.check_physical_depth(self.physical)?;
        if profile != ArrowTypeProfile::Physical && self.logical > MAX_VALUE_TYPE_DEPTH {
            return Err(ValueTypeError::TooDeep);
        }
        match profile {
            ArrowTypeProfile::Physical => Ok(()),
            ArrowTypeProfile::CanonicalValue if self.encodings != 0 => Err(
                ValueTypeError::InvalidType("canonical value layout contains runtime encoding"),
            ),
            _ => Ok(()),
        }
    }
    // The borrowed comparator's generic observer need not accept type errors.
    // Carry a saturated coordinate to its existing validation callback. The
    // existing node contract bounds coordinates on checked entry points;
    // callback-only comparison retains its already validated source invariant.
    pub(crate) fn comparison_child(self, logical: bool, encoding: bool) -> Self {
        Self {
            logical: self.logical.saturating_add(usize::from(logical)),
            physical: self.physical.saturating_add(1),
            encodings: self.encodings.saturating_add(usize::from(encoding)),
        }
    }
    fn increment(self, logical: bool, encoding: bool) -> Result<Self, ValueTypeError> {
        Ok(Self {
            logical: self
                .logical
                .checked_add(usize::from(logical))
                .ok_or(ValueTypeError::TooDeep)?,
            physical: self
                .physical
                .checked_add(1)
                .ok_or(ValueTypeError::TooDeep)?,
            encodings: self
                .encodings
                .checked_add(usize::from(encoding))
                .ok_or(ValueTypeError::TooDeep)?,
        })
    }
    pub fn value_child(self) -> Result<Self, ValueTypeError> {
        self.increment(true, false)
    }
    pub fn map_entries(self) -> Result<Self, ValueTypeError> {
        self.increment(false, false)
    }
    pub fn encoding_child(self) -> Result<Self, ValueTypeError> {
        self.increment(false, true)
    }
}

/// The actual fixed/dynamic pending element. Resource owners must use this
/// layout, including Option occupancy, rather than mirror the former tuple.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypeTraversalFrame<'a> {
    pub(crate) data_type: &'a DataType,
    pub(crate) depth: ArrowTypeDepth,
}
impl<'a> TypeTraversalFrame<'a> {
    pub const fn root(data_type: &'a DataType) -> Self {
        Self {
            data_type,
            depth: ArrowTypeDepth::ROOT,
        }
    }
}
