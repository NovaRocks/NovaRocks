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

//! Finite local dependency collection. Canonical validation borrows D's
//! occurrences rather than constructing a second owned expected inventory.

use crate::dependency::{MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine};
use crate::persistence::dependency::StoredMvDependency;
use crate::repository::{
    MvDependencyReadBound, MvProjectionInventoryEntry, MvRepositoryError, MvRepositoryErrorKind,
};

pub(crate) struct DependencyCollector {
    entries: Vec<StoredMvDependency>,
    payload_bytes: usize,
    bound: MvDependencyReadBound,
}

impl DependencyCollector {
    pub(crate) fn new(bound: MvDependencyReadBound) -> Result<Self, MvRepositoryError> {
        if bound.entries == 0 || bound.collection_bytes == 0 {
            return Err(refusal(
                "MV dependency reads require nonzero explicit bounds",
            ));
        }
        Ok(Self {
            entries: Vec::new(),
            payload_bytes: 0,
            bound,
        })
    }

    /// The decoded datum belongs to the single page. Check retained capacities
    /// and the next vector allocation before moving it into the collector.
    pub(crate) fn admit(
        &mut self,
        dependency: &StoredMvDependency,
    ) -> Result<(), MvRepositoryError> {
        if self.entries.len() >= self.bound.entries {
            return Err(refusal("MV dependencies exceed their entry bound"));
        }
        let object = &dependency.upstream;
        let payload = [
            dependency.upstream_object_id.capacity(),
            object.catalog.as_ref().map_or(0, String::capacity),
            object.database_or_namespace.capacity(),
            object.name.capacity(),
            4 * 64,
        ]
        .into_iter()
        .try_fold(self.payload_bytes, usize::checked_add)
        .ok_or_else(|| refusal("MV dependency collection size overflows"))?;
        let capacity = if self.entries.len() == self.entries.capacity() {
            self.entries
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(self.bound.entries)
        } else {
            self.entries.capacity()
        };
        // Two vector capacities cover either growth overlap or stable sort
        // scratch; the payload is moved, never cloned by sorting.
        let peak = capacity
            .checked_mul(2)
            .and_then(|n| n.checked_mul(std::mem::size_of::<StoredMvDependency>()))
            .and_then(|n| n.checked_add(payload))
            .ok_or_else(|| refusal("MV dependency collection size overflows"))?;
        if peak > self.bound.collection_bytes {
            return Err(refusal(
                "MV dependencies exceed their collection byte bound",
            ));
        }
        if capacity != self.entries.capacity() {
            self.entries
                .try_reserve_exact(capacity - self.entries.len())
                .map_err(|_| refusal("MV dependency collection allocation failed"))?;
        }
        self.payload_bytes = payload;
        Ok(())
    }

    pub(crate) fn push(&mut self, dependency: StoredMvDependency) -> Result<(), MvRepositoryError> {
        self.admit(&dependency)?;
        self.entries.push(dependency);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Vec<StoredMvDependency> {
        self.entries.sort_by(|left, right| {
            display_bytes(&left.upstream)
                .cmp(display_bytes(&right.upstream))
                .then(left.occurrence_id.cmp(&right.occurrence_id))
        });
        self.entries
    }
}

fn display_bytes(object: &MvDependencyObjectRef) -> impl Iterator<Item = u8> + '_ {
    let prefix = if object.object_type == MvDependencyObjectType::MaterializedView {
        "mv:"
    } else {
        ""
    };
    prefix
        .bytes()
        .chain(
            object
                .catalog
                .iter()
                .flat_map(|c| c.bytes().chain(std::iter::once(b'.'))),
        )
        .chain(object.database_or_namespace.bytes())
        .chain(std::iter::once(b'.'))
        .chain(object.name.bytes())
}

pub(crate) fn validate_canonical_parts(
    mv_id: i64,
    definition: &crate::persistence::codec::DefinitionDocument,
    dependencies: &[StoredMvDependency],
) -> Result<(), MvRepositoryError> {
    let occurrences = &definition.relation_occurrences;
    if dependencies.len() != occurrences.len()
        || dependencies.iter().any(|dependency| {
            dependency.downstream_mv_id != mv_id
                || dependency.created_at_ms != definition.created_at_ms
                || dependency.upstream.object_type != MvDependencyObjectType::Unclassified
                || dependency.upstream.storage_engine != MvDependencyStorageEngine::Unclassified
                || !occurrences.iter().any(|occurrence| {
                    occurrence.occurrence_id == dependency.occurrence_id
                        && occurrence.object_id.as_bytes()
                            == dependency.upstream_object_id.as_slice()
                        && dependency.upstream.catalog.as_deref()
                            == Some(occurrence.catalog_at_binding.as_str())
                        && dependency.upstream.database_or_namespace
                            == occurrence.namespace_at_binding
                        && dependency.upstream.name == occurrence.relation_at_binding
                })
        })
    {
        return Err(MvRepositoryError::new(
            MvRepositoryErrorKind::Corruption,
            "MV dependency index is incomplete or differs from its canonical occurrences",
        ));
    }
    // A provider must not replay an occurrence as a second range row. This
    // also protects the test repository's independently stored index.
    for (index, dependency) in dependencies.iter().enumerate() {
        if dependencies[..index]
            .iter()
            .any(|previous| previous.occurrence_id == dependency.occurrence_id)
        {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Corruption,
                "MV dependency index repeats a canonical occurrence",
            ));
        }
    }
    Ok(())
}

pub(crate) fn classify(
    dependencies: &mut [StoredMvDependency],
    inventory: &[MvProjectionInventoryEntry],
) {
    for dependency in dependencies {
        // Open the application envelope once, retain no full projection, and
        // compare the provider object byte-for-byte within the catalog scope.
        let object = crate::persistence::identity::ObjectIdentity::try_new(
            dependency.upstream_object_id.to_vec(),
        )
        .ok()
        .and_then(|identity| {
            crate::persistence::exact_revision::restore_persisted_object(&identity).ok()
        });
        let is_mv = object.as_ref().is_some_and(|object| {
            inventory.iter().any(|entry| {
                dependency.upstream.catalog.as_deref() == entry.target.catalog()
                    && object == &entry.object_id
            })
        });
        dependency.upstream.object_type = match (object.is_some(), is_mv) {
            (false, _) => MvDependencyObjectType::Unclassified,
            (true, true) => MvDependencyObjectType::MaterializedView,
            (true, false) => MvDependencyObjectType::Table,
        };
        dependency.upstream.storage_engine = match (object.is_some(), is_mv) {
            (false, _) => MvDependencyStorageEngine::Unclassified,
            (true, true) => MvDependencyStorageEngine::Iceberg,
            (true, false) => MvDependencyStorageEngine::ExternalTable,
        };
    }
}

fn refusal(message: &'static str) -> MvRepositoryError {
    MvRepositoryError::new(MvRepositoryErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_display_sort_matches_the_existing_concatenated_name_order() {
        let mut names = Vec::new();
        for catalog in [None, Some("a"), Some("a."), Some("名")] {
            for namespace in ["a", "a.", "a_b", "名"] {
                for object_type in [
                    MvDependencyObjectType::Table,
                    MvDependencyObjectType::MaterializedView,
                    MvDependencyObjectType::Unclassified,
                ] {
                    names.push(MvDependencyObjectRef {
                        catalog: catalog.map(str::to_string),
                        database_or_namespace: namespace.to_string(),
                        name: "base".to_string(),
                        object_type,
                        storage_engine: MvDependencyStorageEngine::Unclassified,
                    });
                }
            }
        }
        for left in &names {
            for right in &names {
                assert_eq!(
                    display_bytes(left).cmp(display_bytes(right)),
                    left.display_name().cmp(&right.display_name())
                );
            }
        }
    }
}
