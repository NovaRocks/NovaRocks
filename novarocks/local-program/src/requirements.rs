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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_connector_contract::ConnectorEnvelopeHeader;

use crate::StaticLayout;

/// Dense index in one local program. It is distinct from a native plan node
/// ID, which need not be dense and is checked separately during lowering.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProgramNodeId(usize);

impl ProgramNodeId {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug)]
pub enum ScanSourceKind {
    File,
    BrokerFile,
    SchemaSelection,
    TypedConnector { relation: ConnectorEnvelopeHeader },
}

/// A pure program declares every task-owned capability it will consume.
/// Instantiation must match the full set and each expected kind and layout.
#[derive(Clone, Debug)]
pub enum BindingRequirement {
    Scan {
        node: ProgramNodeId,
        kind: ScanSourceKind,
        layout: StaticLayout,
    },
    ExchangeInput {
        node: ProgramNodeId,
        layout: StaticLayout,
    },
    ExchangeOutput {
        branch: usize,
        layout: StaticLayout,
    },
    RuntimeFilter {
        binding_id: i32,
    },
    TableWriter {
        node: ProgramNodeId,
        layout: StaticLayout,
    },
    TableFinish {
        node: ProgramNodeId,
        layout: StaticLayout,
    },
    ResultSink {
        layout: StaticLayout,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum BindingKey {
    Scan(ProgramNodeId),
    ExchangeInput(ProgramNodeId),
    ExchangeOutput(usize),
    RuntimeFilter(i32),
    TableWriter(ProgramNodeId),
    TableFinish(ProgramNodeId),
    ResultSink,
}

impl BindingRequirement {
    fn key(&self) -> BindingKey {
        match self {
            Self::Scan { node, .. } => BindingKey::Scan(*node),
            Self::ExchangeInput { node, .. } => BindingKey::ExchangeInput(*node),
            Self::ExchangeOutput { branch, .. } => BindingKey::ExchangeOutput(*branch),
            Self::RuntimeFilter { binding_id } => BindingKey::RuntimeFilter(*binding_id),
            Self::TableWriter { node, .. } => BindingKey::TableWriter(*node),
            Self::TableFinish { node, .. } => BindingKey::TableFinish(*node),
            Self::ResultSink { .. } => BindingKey::ResultSink,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BindingRequirements {
    entries: Arc<[BindingRequirement]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindingRequirementsError {
    Duplicate,
}

impl fmt::Display for BindingRequirementsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("local program contains duplicate binding requirements")
    }
}

impl std::error::Error for BindingRequirementsError {}

impl BindingRequirements {
    pub fn try_new(entries: Vec<BindingRequirement>) -> Result<Self, BindingRequirementsError> {
        let mut keys = BTreeSet::new();
        for entry in &entries {
            if !keys.insert(entry.key()) {
                return Err(BindingRequirementsError::Duplicate);
            }
        }
        Ok(Self {
            entries: Arc::from(entries),
        })
    }

    pub fn entries(&self) -> &[BindingRequirement] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_ambiguous_binding_slot() {
        let requirements = BindingRequirements::try_new(vec![
            BindingRequirement::RuntimeFilter { binding_id: 7 },
            BindingRequirement::RuntimeFilter { binding_id: 7 },
        ]);
        assert!(matches!(
            requirements,
            Err(BindingRequirementsError::Duplicate)
        ));
    }
}
