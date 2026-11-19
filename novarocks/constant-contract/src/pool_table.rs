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

//! Sparse addresses into checked immutable constant backings.

use crate::{ConstantError, ConstantPool, ConstantValue};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, FunctionValueType};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConstantPoolId(u32);
impl ConstantPoolId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConstantReference {
    pub pool: ConstantPoolId,
    pub ordinal: u32,
}

/// This map deliberately has no Eq or Hash implementation: two addresses do
/// not prove value equality and checked backings are not raw-byte values.
#[derive(Clone, Debug)]
pub struct ConstantPools {
    entries: BTreeMap<ConstantPoolId, ConstantPool>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}

impl ConstantPools {
    pub fn empty() -> Self {
        Self {
            entries: BTreeMap::new(),
            storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        }
    }
    /// The map's finite construction responsibility is independent of each pool.
    /// Moving this marker neither allocates nor authorizes another map request.
    pub fn with_storage_lifetime(
        mut self,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Self {
        self.storage = storage;
        self
    }
    pub fn entries(&self) -> &BTreeMap<ConstantPoolId, ConstantPool> {
        &self.entries
    }
    pub fn insert(
        &mut self,
        id: ConstantPoolId,
        pool: ConstantPool,
    ) -> Result<(), ConstantTableError> {
        use std::collections::btree_map::Entry;
        match self.entries.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(pool);
                Ok(())
            }
            Entry::Occupied(_) => Err(ConstantTableError::DuplicatePool(id)),
        }
    }
    pub fn resolve_observed(
        &self,
        reference: ConstantReference,
        expected: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantValue, ConstantTableError> {
        // The caller owns the encompassing finish; an originating control or
        // resource refusal must never be replaced by a later observation.
        let pool = self.pool_observed(reference.pool, work)?;
        if !expected.exactly_equals_observed(pool.value_type(), || {
            work.step().map_err(ConstantTableError::from)
        })? {
            return Err(ConstantTableError::SourceTypeMismatch(reference));
        }
        work.step()?;
        pool.value(reference.ordinal)
            .map_err(ConstantTableError::from)
    }
    fn pool_observed(
        &self,
        id: ConstantPoolId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&ConstantPool, ConstantTableError> {
        work.step()?;
        self.entries
            .get(&id)
            .ok_or(ConstantTableError::MissingPool(id))
    }
    /// Borrow the selected row and its actual admitted source type. This
    /// resolves an address only: each consumer must independently prove its
    /// complete type/profile and output relation. No expected type is guessed
    /// from a joined output, and no source Field or backing is reconstructed.
    pub fn resolve_source_observed(
        &self,
        reference: ConstantReference,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantValue, ConstantTableError> {
        self.resolve_source_core::<ConstantTableError>(reference, false, &mut |_, _| Ok(()), work)
    }
    /// Capture the actual selected source before its completed lookup steps.
    /// The caller has admitted lookup work and owns entry and finish. The
    /// original pool author creates only its Arc-backed ordinal handle; no
    /// type, Field, array or scalar is reconstructed and no grant is minted.
    pub fn resolve_source_captured_observed<E: From<ConstantTableError>>(
        &self,
        reference: ConstantReference,
        capture: &mut impl FnMut(&ConstantValue, &mut CompileCheckpoints<'_>) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantValue, E> {
        self.resolve_source_core(reference, true, capture, work)
    }
    fn resolve_source_core<E: From<ConstantTableError>>(
        &self,
        reference: ConstantReference,
        captured: bool,
        capture: &mut impl FnMut(&ConstantValue, &mut CompileCheckpoints<'_>) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantValue, E> {
        if !captured {
            work.step().map_err(ConstantTableError::from)?;
        }
        let pool = self.entries.get(&reference.pool);
        let Some(pool) = pool else {
            if captured {
                work.step().map_err(ConstantTableError::from)?;
            }
            return Err(ConstantTableError::MissingPool(reference.pool).into());
        };
        // Only the caller-owned path selects before completed observations.
        // An invalid ordinal has no source capture and keeps both old steps.
        let selected = if captured {
            let selected = pool.value(reference.ordinal);
            if let Ok(value) = &selected {
                capture(value, work)?;
            }
            work.step().map_err(ConstantTableError::from)?;
            Some(selected)
        } else {
            None
        };
        work.step().map_err(ConstantTableError::from)?;
        selected
            .unwrap_or_else(|| pool.value(reference.ordinal))
            .map_err(ConstantTableError::from)
            .map_err(E::from)
    }
    /// Project only the already admitted backings referenced by this closure.
    /// All references are checked, including repeat ordinals and full types;
    /// one pool handle is retained once per sparse key.
    pub fn project_observed<'a>(
        &self,
        references: impl IntoIterator<Item = (ConstantReference, &'a FunctionValueType)>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantTableError> {
        self.project_optional_references_observed(references.into_iter().map(Some), work)
    }
    /// Observe every visited definition, including definitions without a
    /// constant reference. Filtering before this boundary would hide work.
    pub fn project_optional_references_observed<'a>(
        &self,
        references: impl IntoIterator<Item = Option<(ConstantReference, &'a FunctionValueType)>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantTableError> {
        let mut projected = Self::empty();
        for definition in references {
            work.step()?;
            let Some((reference, expected)) = definition else {
                continue;
            };
            let value = self.resolve_observed(reference, expected, work)?;
            work.step()?;
            projected
                .entries
                .entry(reference.pool)
                .or_insert_with(|| value.pool().clone());
        }
        Ok(projected)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstantTableError {
    Control(CompileControlError),
    Constant(ConstantError),
    MissingPool(ConstantPoolId),
    DuplicatePool(ConstantPoolId),
    SourceTypeMismatch(ConstantReference),
}
impl From<novarocks_type_contract::ValueTypeError> for ConstantTableError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Constant(ConstantError::Type(error))
    }
}
impl From<CompileControlError> for ConstantTableError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ConstantError> for ConstantTableError {
    fn from(error: ConstantError) -> Self {
        match error {
            ConstantError::Control(error) => Self::Control(error),
            error => Self::Constant(error),
        }
    }
}
impl fmt::Display for ConstantTableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::Constant(error) => error.fmt(formatter),
            Self::MissingPool(id) => write!(formatter, "missing constant pool {}", id.get()),
            Self::DuplicatePool(id) => write!(formatter, "duplicate constant pool {}", id.get()),
            Self::SourceTypeMismatch(reference) => write!(
                formatter,
                "constant {}:{} differs from its expected source type",
                reference.pool.get(),
                reference.ordinal
            ),
        }
    }
}
impl std::error::Error for ConstantTableError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Constant(source) => Some(source),
            Self::MissingPool(_) | Self::DuplicatePool(_) | Self::SourceTypeMismatch(_) => None,
        }
    }
}
