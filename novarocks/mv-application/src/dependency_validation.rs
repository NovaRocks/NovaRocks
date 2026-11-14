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

//! Canonical dependency validation borrows definition occurrences; classification
//! uses the same read snapshot inventory without a local collection quota.

use crate::dependency::{MvDependencyObjectType, MvDependencyStorageEngine};
use crate::persistence::dependency::StoredMvDependency;
use crate::repository::{MvProjectionInventoryEntry, MvRepositoryError, MvRepositoryErrorKind};

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
