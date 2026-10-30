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

//! Completed source facts owned by one refresh Root. Pending work stays in its I/O gate.

use super::async_facts::PreparationError;
use novarocks_spi::connector::read_stack::{
    ConnectorPreparedSourceRead, ConnectorReadSourceSelection,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorExactSemanticRevision, ConnectorProviderBindingKey,
    ConnectorTableMetadata, ConnectorTableObjectId,
};
use novarocks_types::naming::TableIdentity;
use std::sync::{Arc, Mutex};

pub(crate) struct CapturedRefreshSource {
    pub(crate) table: TableIdentity,
    pub(crate) lease: ConnectorControlPlanningLease,
    pub(crate) observed: ConnectorTableMetadata,
    pub(crate) object: ConnectorTableObjectId,
    pub(crate) observation: crate::mv::domain::storage_observation::MvRefreshBaseObservation,
    pub(crate) snapshot: Option<i64>,
    pub(crate) revision: ConnectorExactSemanticRevision,
    receipts: Mutex<Vec<ConnectorPreparedSourceRead>>,
}
impl CapturedRefreshSource {
    pub(crate) fn new(
        table: TableIdentity,
        lease: ConnectorControlPlanningLease,
        observed: ConnectorTableMetadata,
        observation: crate::mv::domain::storage_observation::MvRefreshBaseObservation,
        snapshot: Option<i64>,
        revision: ConnectorExactSemanticRevision,
    ) -> Self {
        Self {
            table,
            lease,
            observed,
            object: observation.object_id().clone(),
            observation,
            snapshot,
            revision,
            receipts: Mutex::new(Vec::new()),
        }
    }
    fn binding(&self) -> ConnectorProviderBindingKey {
        ConnectorProviderBindingKey {
            instance_id: self.lease.binding().descriptor().instance_id.clone(),
            incarnation: self.lease.binding().incarnation(),
        }
    }
    pub(crate) fn receipt(
        &self,
        from: Option<&ConnectorExactSemanticRevision>,
    ) -> Option<ConnectorPreparedSourceRead> {
        self.receipts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find(|v| {
                v.request().from_revision() == from && v.request().to_revision() == &self.revision
            })
            .cloned()
    }
    pub(crate) fn install(
        &self,
        value: ConnectorPreparedSourceRead,
    ) -> Result<(), PreparationError> {
        let request = value.request();
        request
            .observed_source_facts()
            .validate_owner(&self.binding())?;
        if value.binding().descriptor() != self.lease.binding().descriptor()
            || value.binding().catalog_handle() != self.lease.binding().catalog_handle()?
            || request.object_id() != &self.object
            || request.to_revision() != &self.revision
            || request.observed_table() != &self.observed.table
        {
            return Err("prepared MV source differs from its original pin".into());
        }
        let mut receipts = self.receipts.lock().unwrap_or_else(|e| e.into_inner());
        if receipts.iter().any(|v| {
            v.request().from_revision() == request.from_revision()
                && v.request().to_revision() == request.to_revision()
        }) {
            return Err("prepared MV source was installed twice".into());
        }
        receipts.push(value);
        Ok(())
    }
    pub(crate) fn to_receipt(&self) -> Result<ConnectorPreparedSourceRead, PreparationError> {
        let receipts = self.receipts.lock().unwrap_or_else(|e| e.into_inner());
        let first = receipts
            .first()
            .ok_or("MV source has no completed preparation")?;
        // Every request shares this exact To and observation. Prefer the actual
        // window result; first/full preparation has only a snapshot receipt.
        Ok(receipts
            .iter()
            .find(|v| {
                matches!(
                    v.request().selection(),
                    ConnectorReadSourceSelection::ChangeWindow(_)
                )
            })
            .unwrap_or(first)
            .clone())
    }
    pub(crate) fn metadata(&self) -> Result<ConnectorTableMetadata, PreparationError> {
        Ok(self.to_receipt()?.to_metadata().clone())
    }
    pub(crate) fn receipts(&self) -> Vec<ConnectorPreparedSourceRead> {
        self.receipts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

/// No CPU source, pending future, resource grant or cross-attempt cache is stored here.
#[derive(Default)]
pub(crate) struct CapturedRefreshSources {
    sources: Mutex<Vec<Arc<CapturedRefreshSource>>>,
    aliases: Mutex<Vec<(TableIdentity, Arc<CapturedRefreshSource>)>>,
}
impl CapturedRefreshSources {
    pub(crate) fn by_table(&self, table: &TableIdentity) -> Option<Arc<CapturedRefreshSource>> {
        self.aliases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find(|(name, _)| name == table)
            .map(|(_, value)| value.clone())
    }
    pub(crate) fn by_definition_object(
        &self,
        object: &novarocks_mv_application::persistence::identity::ObjectIdentity,
        catalog: &str,
    ) -> Result<Option<Arc<CapturedRefreshSource>>, PreparationError> {
        let sources = self.sources.lock().unwrap_or_else(|e| e.into_inner());
        for value in sources.iter().filter(|v| v.table.catalog == catalog) {
            let (observed, _) = novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision(&value.revision)?;
            if &observed == object {
                return Ok(Some(value.clone()));
            }
        }
        Ok(None)
    }
    pub(crate) fn bind_alias(
        &self,
        name: TableIdentity,
        source: Arc<CapturedRefreshSource>,
    ) -> Result<(), PreparationError> {
        let mut aliases = self.aliases.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, prior)) = aliases.iter().find(|(v, _)| v == &name) {
            if !Arc::ptr_eq(prior, &source) {
                return Err("MV source locator names two original captures".into());
            }
            return Ok(());
        }
        aliases.push((name, source));
        Ok(())
    }
    pub(crate) fn bindings(&self) -> Vec<(TableIdentity, Arc<CapturedRefreshSource>)> {
        self.aliases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub(crate) fn install(
        &self,
        value: CapturedRefreshSource,
    ) -> Result<Arc<CapturedRefreshSource>, PreparationError> {
        let mut sources = self.sources.lock().unwrap_or_else(|e| e.into_inner());
        if sources.iter().any(|v| {
            v.table == value.table || (v.binding() == value.binding() && v.object == value.object)
        }) {
            return Err("MV source pin was installed twice".into());
        }
        let value = Arc::new(value);
        sources.push(value.clone());
        drop(sources);
        self.bind_alias(value.table.clone(), value.clone())?;
        Ok(value)
    }
    pub(crate) fn require(
        &self,
        table: &TableIdentity,
    ) -> Result<Arc<CapturedRefreshSource>, PreparationError> {
        self.by_table(table).ok_or_else(|| {
            PreparationError::user(format!("MV source {} has no original capture", table.fqn()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_spi::connector::*;

    fn observed(lease: ConnectorControlPlanningLease, snapshot: i64) -> CapturedRefreshSource {
        let context = crate::connector::test_request_context();
        let identity = ConnectorTableIdentity {
            instance_id: lease.binding().descriptor().instance_id.clone(),
            namespace: Arc::from("ns"),
            table: Arc::from("source"),
        };
        let object = ConnectorTableObjectId::try_new(Bytes::from_static(b"stable-object")).unwrap();
        let revision = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            lease.binding().descriptor().provider_id.clone(),
            &object,
            Some(snapshot),
        )
        .unwrap();
        let observation =
            crate::mv::domain::storage_observation::MvRefreshBaseObservation::try_new(
                identity.clone(),
                object,
                Some(snapshot),
                &context,
            )
            .unwrap();
        let metadata = crate::connector::schema_creation::declared_test_metadata(
            ConnectorTableMetadataHeader {
                identity,
                planning_facts: ConnectorTablePlanningFacts::empty(),
                definition_facts: ConnectorTableDefinitionFacts::empty(),
                version: None,
                statistics_data_version: None,
                table: ConnectorTableHandle::try_new(
                    lease.binding().descriptor().instance_id.clone(),
                    Bytes::from_static(b"frozen-observation"),
                )
                .unwrap(),
            },
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            )],
        );
        CapturedRefreshSource::new(
            TableIdentity {
                catalog: "ice".into(),
                namespace: "ns".into(),
                table: "source".into(),
            },
            lease,
            metadata,
            observation,
            Some(snapshot),
            revision,
        )
    }
    fn lease(epoch: u8) -> ConnectorControlPlanningLease {
        ConnectorControlPlanningLease::new(
            Arc::new(
                novarocks_catalog_application::test_support::test_control_binding_for(
                    ConnectorInstanceId::parse("ice").unwrap(),
                    epoch,
                ),
            ),
            || {},
        )
    }

    #[test]
    fn an_observed_current_schema_cannot_substitute_for_a_completed_to_preparation() {
        let original = observed(lease(7), 17);
        // The complete initial metadata is available, yet no Provider receipt
        // has certified the selected To schema or executable provenance.
        assert!(original.metadata().is_err());
        assert!(original.to_receipt().is_err());
        assert!(original.receipt(None).is_none());
    }

    #[test]
    fn occurrence_aliases_share_the_original_pin_but_other_roots_do_not() {
        let stock = CapturedRefreshSources::default();
        let first = stock.install(observed(lease(7), 17)).unwrap();
        let alias = TableIdentity {
            catalog: "ice".into(),
            namespace: "ns".into(),
            table: "alias".into(),
        };
        stock.bind_alias(alias.clone(), first.clone()).unwrap();
        assert!(Arc::ptr_eq(&first, &stock.require(&alias).unwrap()));
        assert_eq!(stock.require(&alias).unwrap().snapshot, Some(17));
        // A second Current observation cannot replace facts already selected
        // inside this Root, even when its logical name still matches.
        assert!(stock.install(observed(lease(7), 18)).is_err());
        assert_eq!(stock.require(&alias).unwrap().snapshot, Some(17));
        let same_facts_other_root = CapturedRefreshSources::default();
        let independent = same_facts_other_root
            .install(observed(first.lease.clone(), 17))
            .unwrap();
        assert_eq!(first.binding(), independent.binding());
        assert_eq!(first.object, independent.object);
        assert_eq!(first.revision, independent.revision);
        assert!(!Arc::ptr_eq(&first, &independent));
        let other = CapturedRefreshSources::default();
        let second = other.install(observed(lease(8), 18)).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_ne!(
            first.lease.binding().incarnation(),
            second.lease.binding().incarnation()
        );
        assert_eq!(second.snapshot, Some(18));
    }
}
