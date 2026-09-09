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

//! Query-local opaque identities for SQL table facts.
//!
//! These values deliberately have no serialization implementation. A table
//! binding is meaningful only in the application-owned store that allocated
//! its scope, so forwarding it across a request/process boundary is invalid.

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::atomic::{AtomicU64, Ordering},
};

/// Process-local identity of one application binding store.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SqlTableBindingScopeId(NonZeroU64);

impl SqlTableBindingScopeId {
    pub(crate) fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub fn get(self) -> NonZeroU64 {
        self.0
    }
}

/// One table fact allocated by a query-local binding store.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SqlTableBindingId {
    scope: SqlTableBindingScopeId,
    ordinal: NonZeroU32,
}

impl SqlTableBindingId {
    pub(crate) fn new(scope: SqlTableBindingScopeId, ordinal: NonZeroU32) -> Self {
        Self { scope, ordinal }
    }

    pub fn scope(self) -> SqlTableBindingScopeId {
        self.scope
    }

    pub fn ordinal(self) -> NonZeroU32 {
        self.ordinal
    }

    pub fn belongs_to(self, scope: SqlTableBindingScopeId) -> bool {
        self.scope == scope
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(ordinal: u32) -> Self {
        let ordinal = NonZeroU32::new(ordinal).expect("test binding ordinal is nonzero");
        let mut allocator = SqlTableBindingAllocator::try_new_for_test(
            NonZeroU64::new(1).expect("test binding scope is nonzero"),
        )
        .expect("test binding allocator must be valid");
        for _ in 1..ordinal.get() {
            allocator
                .allocate()
                .expect("test binding ordinal must be valid");
        }
        allocator
            .allocate()
            .expect("test binding ordinal must be valid")
    }
}

/// Opaque request-local token allocator.
///
/// The application owns the globally unique nonzero seed. SQL owns conversion
/// of that seed into binding tokens, so consumers cannot construct a token
/// from a scope and ordinal independently. Tokens retain no provider, plan,
/// wire, or lifecycle data.
pub struct SqlTableBindingAllocator {
    scope: SqlTableBindingScopeId,
    next_ordinal: u32,
}

impl SqlTableBindingAllocator {
    /// Mint one process-unique request-local scope.
    ///
    /// Callers cannot choose the scope value, so observing a binding token
    /// does not provide a way to recreate its mint authority.
    pub fn new_unique() -> Result<Self, String> {
        static NEXT_SCOPE: AtomicU64 = AtomicU64::new(1);
        let scope = NEXT_SCOPE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| "SQL table binding scope space is exhausted".to_string())?;
        let scope = NonZeroU64::new(scope)
            .ok_or_else(|| "SQL table binding scope space is exhausted".to_string())?;
        Ok(Self {
            scope: SqlTableBindingScopeId::new(scope),
            next_ordinal: 0,
        })
    }

    /// Construct a named scope only for cross-crate fixtures whose sealed SQL
    /// plan already contains deterministic binding tokens.
    #[cfg(any(test, feature = "test-support"))]
    pub fn try_new_for_test(scope_seed: NonZeroU64) -> Result<Self, String> {
        Ok(Self {
            scope: SqlTableBindingScopeId::new(scope_seed),
            next_ordinal: 0,
        })
    }

    pub fn scope(&self) -> SqlTableBindingScopeId {
        self.scope
    }

    /// Mint exactly one next token in this request-local scope.
    pub fn allocate(&mut self) -> Result<SqlTableBindingId, String> {
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| "SQL table binding ordinal space is exhausted".to_string())?;
        let ordinal = NonZeroU32::new(self.next_ordinal)
            .ok_or_else(|| "SQL table binding ordinal space is exhausted".to_string())?;
        Ok(SqlTableBindingId::new(self.scope, ordinal))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::SqlTableBindingAllocator;

    #[test]
    fn sqlx2_binding_token_is_scoped_and_nonzero() {
        let mut first = SqlTableBindingAllocator::try_new_for_test(NonZeroU64::new(17).unwrap())
            .expect("first allocator");
        let second = SqlTableBindingAllocator::try_new_for_test(NonZeroU64::new(18).unwrap())
            .expect("second allocator");
        let first_scope = first.scope();
        let second_scope = second.scope();
        let binding = first.allocate().expect("first binding");

        assert_eq!(binding.scope(), first_scope);
        assert_eq!(binding.ordinal().get(), 1);
        assert!(binding.belongs_to(first_scope));
        assert!(!binding.belongs_to(second_scope));
        assert_eq!(first_scope.get(), NonZeroU64::new(17).unwrap());
    }
}
