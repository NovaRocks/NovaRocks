// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

/// The closed hash-join condition, independent of expression representation.
/// `K` is the owner's ordered key collection. NULL-aware value comparisons and
/// eligibility scopes have distinct ordinal domains and must never be flattened
/// into ordinary equality or network repartition keys.
#[derive(Clone, Debug, PartialEq)]
pub enum HashJoinCondition<K, E> {
    Ordinary {
        keys: K,
        residual: Option<E>,
    },
    NullAwareEqual {
        value_equal_keys: K,
        scope_keys: K,
        eligibility: Option<E>,
    },
}

impl<K, E> HashJoinCondition<K, E> {
    pub fn keys(&self) -> &K {
        match self {
            Self::Ordinary { keys, .. } => keys,
            Self::NullAwareEqual {
                value_equal_keys, ..
            } => value_equal_keys,
        }
    }

    pub fn keys_mut(&mut self) -> &mut K {
        match self {
            Self::Ordinary { keys, .. } => keys,
            Self::NullAwareEqual {
                value_equal_keys, ..
            } => value_equal_keys,
        }
    }

    pub fn scope_keys(&self) -> Option<&K> {
        match self {
            Self::Ordinary { .. } => None,
            Self::NullAwareEqual { scope_keys, .. } => Some(scope_keys),
        }
    }

    pub fn scope_keys_mut(&mut self) -> Option<&mut K> {
        match self {
            Self::Ordinary { .. } => None,
            Self::NullAwareEqual { scope_keys, .. } => Some(scope_keys),
        }
    }

    pub fn residual(&self) -> &Option<E> {
        match self {
            Self::Ordinary { residual, .. } => residual,
            Self::NullAwareEqual { eligibility, .. } => eligibility,
        }
    }

    pub fn residual_mut(&mut self) -> &mut Option<E> {
        match self {
            Self::Ordinary { residual, .. } => residual,
            Self::NullAwareEqual { eligibility, .. } => eligibility,
        }
    }

    pub fn is_null_aware(&self) -> bool {
        matches!(self, Self::NullAwareEqual { .. })
    }

    pub fn try_map<L, F, X>(
        self,
        mut keys: impl FnMut(K) -> Result<L, X>,
        mut expression: impl FnMut(E) -> Result<F, X>,
    ) -> Result<HashJoinCondition<L, F>, X> {
        Ok(match self {
            Self::Ordinary {
                keys: input,
                residual,
            } => HashJoinCondition::Ordinary {
                keys: keys(input)?,
                residual: residual.map(&mut expression).transpose()?,
            },
            Self::NullAwareEqual {
                value_equal_keys,
                scope_keys,
                eligibility,
            } => HashJoinCondition::NullAwareEqual {
                value_equal_keys: keys(value_equal_keys)?,
                scope_keys: keys(scope_keys)?,
                eligibility: eligibility.map(&mut expression).transpose()?,
            },
        })
    }
}
