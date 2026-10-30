// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Keyed projections on the single Accelerator owner and canonical indexes.
use super::codec::decode_keyed_projection;
use super::*;
use crate::persistence::keyed_documents::{MvKeyedDocumentProjection, StoredKeyedMvProjection};
use crate::repository::MvProjectionVersion;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedKeyedMvProjection {
    pub projection: StoredKeyedMvProjection,
    pub version: MvProjectionVersion,
}

impl StateStoreMvRepository {
    pub async fn find_keyed_by_target(
        &self,
        target: &MvTarget,
        budget: crate::persistence::validation::PersistenceDecodeBudget,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvRepositoryError> {
        self.read_repository()
            .find_keyed_by_target(target, budget)
            .await
    }
    /// The existing sequence, lookup and dependency indexes are updated in the
    /// same transaction. A stale expected root is a conflict, never overwritten.
    pub async fn install_keyed_projection(
        &self,
        operation_id: Uuid,
        facts: MvKeyedDocumentProjection,
        expected: Option<LoadedKeyedMvProjection>,
    ) -> Result<LoadedKeyedMvProjection, MvRepositoryError> {
        self.write_repository()
            .install_keyed_projection(operation_id, facts, expected)
            .await
    }
}
pub(super) fn keyed_dependencies(
    id: i64,
    facts: &MvKeyedDocumentProjection,
) -> Vec<StoredMvDependency> {
    use crate::dependency::{MvDependencyObjectType, MvDependencyStorageEngine};
    facts
        .definition()
        .relation_occurrences
        .iter()
        .map(|r| StoredMvDependency {
            downstream_mv_id: id,
            occurrence_id: r.occurrence_id,
            upstream_object_id: serde_bytes::ByteBuf::from(r.object_id.as_bytes().to_vec()),
            upstream: MvDependencyObjectRef {
                catalog: Some(r.catalog_at_binding.clone()),
                database_or_namespace: r.namespace_at_binding.clone(),
                name: r.relation_at_binding.clone(),
                object_type: MvDependencyObjectType::Unclassified,
                storage_engine: MvDependencyStorageEngine::Unclassified,
            },
            created_at_ms: facts.definition().created_at_ms,
        })
        .collect()
}

impl StateStoreMvRepository {
    pub async fn list_keyed_projections(
        &self,
    ) -> Result<Vec<LoadedKeyedMvProjection>, MvRepositoryError> {
        self.read_repository().list_keyed_projections().await
    }
    pub async fn list_keyed_projection_inventory(
        &self,
        bound: MvProjectionInventoryBound,
    ) -> Result<Vec<MvProjectionInventoryEntry>, MvRepositoryError> {
        self.read_repository()
            .list_keyed_projection_inventory(bound)
            .await
    }
    pub async fn find_keyed_by_target_bounded(
        &self,
        target: &MvTarget,
        bound: MvProjectionInventoryBound,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvRepositoryError> {
        self.read_repository()
            .find_keyed_by_target_bounded(target, bound)
            .await
    }
    pub async fn delete_keyed_projection(
        &self,
        operation_id: Uuid,
        expected: LoadedKeyedMvProjection,
    ) -> Result<bool, MvRepositoryError> {
        self.write_repository()
            .delete_keyed_projection(operation_id, expected)
            .await
    }
    pub async fn list_keyed_dependencies_by_downstream_bounded(
        &self,
        loaded: &LoadedKeyedMvProjection,
        bound: MvDependencyReadBound,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.read_repository()
            .list_keyed_dependencies_by_downstream_bounded(loaded, bound)
            .await
    }
}
pub(super) async fn bounded_keyed_inventory<T: ReadAccess + ?Sized>(
    tx: &mut T,
    bound: MvProjectionInventoryBound,
) -> Result<Vec<MvProjectionInventoryEntry>, MvRepositoryError> {
    let mut inventory = MvProjectionInventoryBuilder::new(bound)?;
    let mut range = BoundedRecordRange::new(projection_prefix().map_err(corruption)?)?;
    while let Some(row) = range.next(tx, bound).await? {
        let projection = decode_keyed_projection(&row.key, &row.value, bound.decode)
            .map_err(corruption)?
            .value;
        if row.key != projection_by_id_key(projection.mv_id).map_err(corruption)? {
            return Err(corruption("keyed inventory key differs from projection"));
        }
        inventory.push_facts(
            projection.mv_id,
            &projection.facts.target(),
            &projection.facts.source_revision().target_object_id,
        )?;
    }
    Ok(inventory.finish())
}

impl StateStoreMvRepository {
    pub async fn find_keyed_by_target_object(
        &self,
        object: &novarocks_spi::connector::ConnectorTableObjectId,
    ) -> Result<Option<LoadedKeyedMvProjection>, MvRepositoryError> {
        self.read_repository()
            .find_keyed_by_target_object(object)
            .await
    }
    pub async fn list_keyed_dependencies_by_downstream(
        &self,
        loaded: &LoadedKeyedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.read_repository()
            .list_keyed_dependencies_by_downstream(loaded)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{keyed_documents::*, validation::PersistenceDecodeBudget};
    #[tokio::test]
    async fn keyed_projection_roundtrips_on_existing_owner_with_guarded_cas() {
        let (_, repository) = super::super::tests_definition::repository().await;
        let (catalog, view) = crate::persistence::keyed_documents::tests::fixture(Some(17), true);
        let observed =
            decode_current_keyed_documents(view, catalog, PersistenceDecodeBudget::default())
                .unwrap();
        let facts = MvKeyedDocumentProjection::try_from_current(observed).unwrap();
        let target = facts.target();
        let first = repository
            .install_keyed_projection(Uuid::now_v7(), facts.clone(), None)
            .await
            .unwrap();
        assert_eq!(
            repository
                .find_keyed_by_target(&target, PersistenceDecodeBudget::default())
                .await
                .unwrap(),
            Some(first.clone())
        );
        let second = repository
            .install_keyed_projection(Uuid::now_v7(), facts.clone(), Some(first.clone()))
            .await
            .unwrap();
        assert_eq!(second.projection.mv_id, first.projection.mv_id);
        assert_eq!(
            repository
                .install_keyed_projection(Uuid::now_v7(), facts, Some(first))
                .await
                .unwrap_err()
                .kind(),
            MvRepositoryErrorKind::Conflict
        );
    }
}
