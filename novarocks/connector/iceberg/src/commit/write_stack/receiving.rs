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

//! Provider custody of root write records before the publication handoff.

pub(crate) mod validation;

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::write_stack::{
    ConnectorCommitFragment, ConnectorWriteProductionProof, ConnectorWriteQueryIdentity,
    ConnectorWriterPhysicalContext, WriteTargetOrdinal, validate_query_target_ordinals,
    validate_write_fragment_targets,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use super::domain::{IcebergSealedWriteTarget, invalid};

type WriteFragment = (WriteTargetOrdinal, ConnectorCommitFragment);

/// Retained as whole fragments until each flavor moves to compact receiving.
#[derive(Debug, Default)]
pub(crate) struct IcebergReceiving {
    queries: BTreeMap<ConnectorWriteQueryIdentity, QueryReceiving>,
    records: Vec<ReceivedRecord>,
}

struct QueryReceiving {
    targets: Vec<WriteTargetOrdinal>,
    production: Option<Arc<dyn ConnectorWriteProductionProof>>,
    confirmed: bool,
}

impl std::fmt::Debug for QueryReceiving {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryReceiving")
            .field("targets", &self.targets)
            .field("has_production", &self.production.is_some())
            .field("confirmed", &self.confirmed)
            .finish()
    }
}

#[derive(Debug)]
struct ReceivedRecord {
    query: ConnectorWriteQueryIdentity,
    fragments: Vec<WriteFragment>,
}

pub(crate) struct ReceivedFragment<'a> {
    pub(crate) target: WriteTargetOrdinal,
    pub(crate) fragment: &'a ConnectorCommitFragment,
    production: Option<&'a dyn ConnectorWriteProductionProof>,
}

impl ReceivedFragment<'_> {
    pub(crate) fn admits_creation(
        &self,
        physical: &ConnectorWriterPhysicalContext,
    ) -> Result<bool, ConnectorError> {
        match self.production {
            Some(production) => production.admits(self.target, physical),
            None => Ok(false),
        }
    }
}

impl IcebergReceiving {
    /// A failed registration leaves the session unchanged and the proposed
    /// production proof with its caller.
    pub(crate) fn begin_query(
        &mut self,
        query: ConnectorWriteQueryIdentity,
        targets: &[WriteTargetOrdinal],
        production: &mut Option<Arc<dyn ConnectorWriteProductionProof>>,
        sealed: &[IcebergSealedWriteTarget],
    ) -> Result<(), ConnectorError> {
        validate_query_target_ordinals(targets)?;
        if targets
            .iter()
            .any(|ordinal| !sealed.iter().any(|target| target.ordinal() == *ordinal))
        {
            return Err(invalid(
                "Iceberg write query targets are outside its original sealed session",
            ));
        }
        let std::collections::btree_map::Entry::Vacant(entry) = self.queries.entry(query) else {
            return Err(invalid("Iceberg write query is already registered"));
        };
        entry.insert(QueryReceiving {
            targets: targets.to_vec(),
            production: production.take(),
            confirmed: false,
        });
        Ok(())
    }

    /// Check the entire root record before transferring any fragment.
    pub(crate) fn arrive(
        &mut self,
        query: ConnectorWriteQueryIdentity,
        fragments: &mut Vec<WriteFragment>,
    ) -> Result<(), ConnectorError> {
        let registration = self
            .queries
            .get(&query)
            .ok_or_else(|| invalid("Iceberg write arrival names an unregistered query"))?;
        if registration.confirmed {
            return Err(invalid("Iceberg write query already completed receiving"));
        }
        validate_write_fragment_targets(fragments, &registration.targets)?;
        if !fragments.is_empty() {
            self.records.push(ReceivedRecord {
                query,
                fragments: std::mem::take(fragments),
            });
        }
        Ok(())
    }

    pub(crate) fn confirm(
        &mut self,
        query: ConnectorWriteQueryIdentity,
    ) -> Result<(), ConnectorError> {
        let registration = self
            .queries
            .get_mut(&query)
            .ok_or_else(|| invalid("Iceberg write confirmation names an unregistered query"))?;
        if registration.confirmed {
            return Err(invalid("Iceberg write query already completed receiving"));
        }
        registration.confirmed = true;
        Ok(())
    }

    pub(crate) fn require_complete(&self) -> Result<(), ConnectorError> {
        if self.queries.values().any(|query| !query.confirmed) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Iceberg write session has queries that have not completed receiving",
            ));
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(crate) fn is_unstarted(&self) -> bool {
        self.queries.is_empty()
    }

    pub(crate) fn fragments(&self) -> impl Iterator<Item = ReceivedFragment<'_>> {
        self.records.iter().flat_map(|record| {
            let production = self.queries[&record.query].production.as_deref();
            record
                .fragments
                .iter()
                .map(move |(target, fragment)| ReceivedFragment {
                    target: *target,
                    fragment,
                    production,
                })
        })
    }
}

#[cfg(test)]
mod tests;
