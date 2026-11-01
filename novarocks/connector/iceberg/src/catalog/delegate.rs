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

//! Shared delegation onto one concrete Iceberg catalog client.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)
//!
//! Hadoop and HMS share bounded catalog reads. Publication is owned by
//! their prepared mechanisms and cannot be invoked through this delegate.
//!
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorListingBound, ConnectorListingCollector,
};
use novarocks_types::naming::normalize_identifier;

use crate::iceberg::{Catalog, NamespaceIdent, TableIdent};

use super::error::map_read_error;
use super::{CatalogNamespaceName, CatalogTableName};

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

pub(crate) fn namespace_ident(
    name: &CatalogNamespaceName,
) -> Result<NamespaceIdent, ConnectorError> {
    let normalized = normalize_identifier(&name.namespace).map_err(invalid)?;
    Ok(NamespaceIdent::new(normalized))
}

pub(super) fn table_ident(name: &CatalogTableName) -> Result<TableIdent, ConnectorError> {
    let namespace = normalize_identifier(&name.namespace).map_err(invalid)?;
    let table = normalize_identifier(&name.name).map_err(invalid)?;
    TableIdent::from_strs([namespace.as_str(), table.as_str()])
        .map_err(|error| invalid(format!("build Iceberg identity for {name}: {error}")))
}

/// Order a bounded listing and drop duplicate names.
pub(crate) fn sorted_unique(mut names: Vec<String>) -> Vec<String> {
    names.sort();
    names.dedup();
    names
}

/// Delegation onto one concrete catalog client.
#[derive(Debug)]
pub(super) struct CatalogDelegate {
    client: Arc<dyn Catalog>,
    pub(super) publication_generation: super::publication::PublicationGeneration,
    pub(super) evidence_policy: super::publication::CatalogEvidencePolicy,
    pub(super) listing: Arc<super::listing_admission::ListingAdmission>,
}

impl CatalogDelegate {
    pub(super) fn new(client: Arc<dyn Catalog>) -> Self {
        Self {
            client,
            publication_generation: Default::default(),
            evidence_policy: Default::default(),
            listing: Default::default(),
        }
    }

    // ---- Reads ----------------------------------------------------------

    /// The vendored clients expose namespace enumeration only as one complete
    /// listing, so the bound is enforced on that listing as it is retained.
    pub(super) async fn list_namespaces(
        &self,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.list_namespaces_inner(
            bound,
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            None,
        )
        .await
    }

    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(super) async fn list_namespaces_observed(
        &self,
        bound: ConnectorListingBound,
        invocation: &super::hms_listing_observer::Invocation,
    ) -> Result<Vec<String>, ConnectorError> {
        self.list_namespaces_inner(bound, Some(invocation)).await
    }

    async fn list_namespaces_inner(
        &self,
        bound: ConnectorListingBound,
        #[cfg(feature = "mem-1-m07-hms-listing-observe")] invocation: Option<
            &super::hms_listing_observer::Invocation,
        >,
    ) -> Result<Vec<String>, ConnectorError> {
        let mut collector = ConnectorListingCollector::new(bound)?;
        let namespaces = {
            let sdk = self.client.list_namespaces(None);
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            let result = super::hms_listing_observer::sdk_call(invocation, sdk).await;
            #[cfg(not(feature = "mem-1-m07-hms-listing-observe"))]
            let result = sdk.await;
            result
        }
        .map_err(|error| map_read_error(&error))?;
        for name in namespaces
            .into_iter()
            .flat_map(|ident| ident.inner())
            // A leading dot marks catalog-internal bookkeeping namespaces,
            // including the CTAS staging root, which SQL must never see.
            .filter(|name| !name.starts_with('.'))
        {
            collector.push(name)?;
        }
        Ok(sorted_unique(collector.finish()?))
    }

    pub(super) async fn namespace_exists(
        &self,
        namespace: &CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        let ident = namespace_ident(namespace)?;
        self.client
            .namespace_exists(&ident)
            .await
            .map_err(|error| map_read_error(&error))
    }

    /// One complete table listing from a client without a paging capability,
    /// bounded as it is retained.
    pub(super) async fn list_tables(
        &self,
        namespace: &CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.list_tables_inner(
            namespace,
            bound,
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            None,
        )
        .await
    }

    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(super) async fn list_tables_observed(
        &self,
        namespace: &CatalogNamespaceName,
        bound: ConnectorListingBound,
        invocation: &super::hms_listing_observer::Invocation,
    ) -> Result<Vec<String>, ConnectorError> {
        self.list_tables_inner(namespace, bound, Some(invocation))
            .await
    }

    async fn list_tables_inner(
        &self,
        namespace: &CatalogNamespaceName,
        bound: ConnectorListingBound,
        #[cfg(feature = "mem-1-m07-hms-listing-observe")] invocation: Option<
            &super::hms_listing_observer::Invocation,
        >,
    ) -> Result<Vec<String>, ConnectorError> {
        let ident = namespace_ident(namespace)?;
        let mut collector = ConnectorListingCollector::new(bound)?;
        let tables = {
            let sdk = self.client.list_tables(&ident);
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            let result = super::hms_listing_observer::sdk_call(invocation, sdk).await;
            #[cfg(not(feature = "mem-1-m07-hms-listing-observe"))]
            let result = sdk.await;
            result
        }
        .map_err(|error| map_read_error(&error))?;
        for table in tables {
            collector.push(table.name)?;
        }
        Ok(sorted_unique(collector.finish()?))
    }

    pub(super) async fn table_exists(
        &self,
        table: &CatalogTableName,
    ) -> Result<bool, ConnectorError> {
        let ident = table_ident(table)?;
        self.client
            .table_exists(&ident)
            .await
            .map_err(|error| map_read_error(&error))
    }

    pub(super) async fn load_table(
        &self,
        table: &CatalogTableName,
    ) -> Result<crate::iceberg::table::Table, ConnectorError> {
        let ident = table_ident(table)?;
        self.client
            .load_table(&ident)
            .await
            .map_err(|error| map_read_error(&error))
    }
}
