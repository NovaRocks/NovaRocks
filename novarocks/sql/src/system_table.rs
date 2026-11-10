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

//! Pure system-relation declarations and conservative key constraints.
//!
//! These facts carry neither a provider nor statement access capabilities.
//! Predicate lowering supplies value sets; this module interprets no expression.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_type_contract::FunctionValueType;
use novarocks_types::naming::SYSTEM_CATALOG;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SystemTableOccurrenceId(u32);
impl SystemTableOccurrenceId {
    pub(crate) const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Closed declaration-lookup failures, separate from provider execution errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SystemTableLookupError {
    CatalogNotFound,
    SchemaNotFound,
    TableNotFound,
}

/// The reserved catalog wins over the catalog-local information schema.
pub fn is_system_table_name(catalog: &str, schema: &str) -> bool {
    catalog == SYSTEM_CATALOG || schema == novarocks_types::naming::INFORMATION_SCHEMA
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SystemTableBinding {
    System,
    Catalog(String),
}

impl SystemTableBinding {
    pub fn catalog_name(&self) -> &str {
        match self {
            Self::System => SYSTEM_CATALOG,
            Self::Catalog(catalog) => catalog,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemTableColumn {
    pub name: String,
    pub value_type: FunctionValueType,
}

/// The complete immutable column contract for one catalog-bound relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemTableDeclaration {
    pub binding: SystemTableBinding,
    pub schema: String,
    pub table: String,
    pub columns: Arc<[SystemTableColumn]>,
    pub key_columns: Arc<[usize]>,
}

impl SystemTableDeclaration {
    pub fn qualified_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.binding.catalog_name(),
            self.schema,
            self.table
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemTableKeyValues {
    Any,
    Values(BTreeSet<String>),
    None,
}

impl SystemTableKeyValues {
    pub fn intersect(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::None, _) | (_, Self::None) => Self::None,
            (Self::Any, values) | (values, Self::Any) => values.clone(),
            (Self::Values(left), Self::Values(right)) => {
                let values: BTreeSet<_> = left.intersection(right).cloned().collect();
                if values.is_empty() {
                    Self::None
                } else {
                    Self::Values(values)
                }
            }
        }
    }

    pub fn allows(&self, value: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Values(values) => values.contains(value),
            Self::None => false,
        }
    }
}

/// Missing ordinals mean Any; a None in any column makes the relation empty.
/// String values are compared exactly and are never identifier-normalized.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SystemTableKeyConstraint {
    columns: BTreeMap<usize, SystemTableKeyValues>,
}

impl SystemTableKeyConstraint {
    pub fn columns(&self) -> &BTreeMap<usize, SystemTableKeyValues> {
        &self.columns
    }

    pub fn set_values(&mut self, ordinal: usize, values: SystemTableKeyValues) {
        let values = match values {
            SystemTableKeyValues::Values(values) if values.is_empty() => SystemTableKeyValues::None,
            values => values,
        };
        self.columns
            .entry(ordinal)
            .and_modify(|current| *current = current.intersect(&values))
            .or_insert(values);
    }

    pub fn intersect(&mut self, other: &Self) {
        for (&ordinal, values) in &other.columns {
            self.set_values(ordinal, values.clone());
        }
    }

    pub fn is_none(&self) -> bool {
        self.columns
            .values()
            .any(|values| matches!(values, SystemTableKeyValues::None))
    }

    pub fn allows(&self, ordinal: usize, value: &str) -> bool {
        !self.is_none()
            && self
                .columns
                .get(&ordinal)
                .is_none_or(|values| values.allows(value))
    }

    pub fn values_for(&self, ordinal: usize) -> Option<&SystemTableKeyValues> {
        self.columns.get(&ordinal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pure correctness coverage belongs to the declaration owner. No fixture,
    // external process or cleanup is needed; normal package tests consume it.
    #[test]
    fn conjunction_intersects_exact_values_without_normalizing_literals() {
        let mut constraint = SystemTableKeyConstraint::default();
        constraint.set_values(
            0,
            SystemTableKeyValues::Values(BTreeSet::from(["db".into(), "DB".into()])),
        );
        let mut other = SystemTableKeyConstraint::default();
        other.set_values(
            0,
            SystemTableKeyValues::Values(BTreeSet::from(["db".into()])),
        );
        constraint.intersect(&other);
        assert!(constraint.allows(0, "db"));
        assert!(!constraint.allows(0, "DB"));
        assert!(constraint.allows(1, "anything"));
        constraint.set_values(0, SystemTableKeyValues::Values(BTreeSet::new()));
        assert!(constraint.is_none());
        assert!(!constraint.allows(1, "anything"));
    }
}
