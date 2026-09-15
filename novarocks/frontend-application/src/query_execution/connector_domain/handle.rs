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

//! Catalog-scoped engine wrappers around validated connector carriers.

use novarocks_spi::connector::CatalogHandle;
use novarocks_spi::connector::read_stack::{
    ConnectorReadRelation, ConnectorReadRelationKind, ConnectorReadSplit,
};

/// One relation a scan reads: catalog, connector transaction, and the concrete
/// connector handle, already validated by the protocol layer.
#[derive(Clone, Debug)]
pub(crate) struct TableHandle {
    catalog: CatalogHandle,
    relation: ConnectorReadRelation,
}

impl TableHandle {
    pub(crate) fn new(catalog: CatalogHandle, relation: ConnectorReadRelation) -> Self {
        Self { catalog, relation }
    }

    pub(crate) const fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }

    pub(crate) const fn relation(&self) -> &ConnectorReadRelation {
        &self.relation
    }

    /// Which relation family this handle names. The engine branches on this to
    /// choose a distribution; it never reads inside the provider variant.
    pub(crate) fn relation_kind(&self) -> ConnectorReadRelationKind {
        self.relation.kind()
    }
}

/// One unit of schedulable read work, bound to the catalog that produced it.
#[derive(Clone, Debug)]
/// One split as the coordinator sees it, for placement only.
///
/// It carries no catalog: which connector generation a split belongs to is
/// decided by the scan node it was enumerated for, and the backend resolves the
/// provider from that node's own frozen table handle. Repeating it here would
/// create a second authority that could disagree with the first.
pub(crate) struct Split {
    split: ConnectorReadSplit,
}

impl Split {
    pub(crate) fn new(split: ConnectorReadSplit) -> Self {
        Self { split }
    }

    pub(crate) const fn split(&self) -> &ConnectorReadSplit {
        &self.split
    }

    /// Relative scheduling cost. Weight only influences placement; it never
    /// changes what the split reads.
    pub(crate) fn weight_raw(&self) -> u64 {
        self.split.facts().split_weight().raw_value()
    }

    pub(crate) fn retained_size_in_bytes(&self) -> u64 {
        self.split.facts().retained_size_in_bytes()
    }

    pub(crate) fn is_remotely_accessible(&self) -> bool {
        self.split.facts().remotely_accessible()
    }

    pub(crate) fn affinity_key(&self) -> Option<&str> {
        self.split.facts().affinity_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{CatalogVersion, ConnectorInstanceId};

    fn handle(version: u8) -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("ice").expect("fixture catalog name"),
            CatalogVersion::from_bytes([version; 32]),
        )
    }

    #[test]
    fn a_catalog_handle_is_identified_by_name_and_immutable_version() {
        let first = handle(1);
        let same = handle(1);
        let replaced = handle(2);
        assert_eq!(first, same);
        assert_ne!(first, replaced);
    }
}
