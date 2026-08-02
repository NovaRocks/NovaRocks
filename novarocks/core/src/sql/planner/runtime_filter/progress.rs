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

use std::collections::BTreeMap;

use crate::sql::planner::runtime_filter::contract::{BindingId, ChannelId};

/// Neutral projection of a planner-owned fragment input edge. The deployment
/// compiler validates this projection against the sealed planner edge set.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct FrontierEdge {
    pub source_fragment: u32,
    pub target_exchange_node: i32,
}
/// Planner-sealed proof that one hash-join producer can publish its runtime
/// filter after only its build-side frontier completes, independent of the
/// probe side and of the rest of the fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinBuildProgressProof {
    pub channel: ChannelId,
    pub producer_binding: BindingId,
    pub producer_fragment: u32,
    pub join_node_id: i32,
    pub build_frontier: Vec<FrontierEdge>,
    pub non_build_inputs: Vec<FrontierEdge>,
}

/// Why a join was skipped (no proof sealed). Diagnostic only; deployment keeps
/// the coarse-grained wait edges and runs the final cycle guard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontierSkip {
    NoRfSides,
    MissingChild,
    UnauditedNode { node_id: i32 },
}

/// Planner provenance for one producer that could not seal a build-frontier
/// proof. Deployment keeps the coarse edge and uses this only for diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JoinBuildProgressSkip {
    pub join_node_id: i32,
    pub rule: FrontierSkip,
}

type JoinBuildProgressKey = (ChannelId, BindingId, u32);

/// Planner-sealed build-progress facts. Successful proofs and fail-closed skip
/// provenance share the same expected producer tuple without conflating their
/// semantics.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JoinBuildProgressCatalog {
    proofs: BTreeMap<JoinBuildProgressKey, JoinBuildProgressProof>,
    skips: BTreeMap<JoinBuildProgressKey, JoinBuildProgressSkip>,
}

impl JoinBuildProgressCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.proofs.len() + self.skips.len()
    }

    #[cfg(test)]
    pub fn values(&self) -> impl Iterator<Item = &JoinBuildProgressProof> {
        self.proofs.values()
    }

    pub fn get(&self, key: &JoinBuildProgressKey) -> Option<&JoinBuildProgressProof> {
        self.proofs.get(key)
    }

    pub fn skipped(&self, key: &JoinBuildProgressKey) -> Option<&JoinBuildProgressSkip> {
        self.skips.get(key)
    }

    pub fn insert_proof(&mut self, key: JoinBuildProgressKey, proof: JoinBuildProgressProof) {
        self.skips.remove(&key);
        self.proofs.insert(key, proof);
    }

    pub fn insert_skip(&mut self, key: JoinBuildProgressKey, skip: JoinBuildProgressSkip) {
        self.proofs.remove(&key);
        self.skips.insert(key, skip);
    }
}

impl FromIterator<(JoinBuildProgressKey, JoinBuildProgressProof)> for JoinBuildProgressCatalog {
    fn from_iter<T: IntoIterator<Item = (JoinBuildProgressKey, JoinBuildProgressProof)>>(
        iter: T,
    ) -> Self {
        let mut catalog = Self::new();
        for (key, proof) in iter {
            catalog.insert_proof(key, proof);
        }
        catalog
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FrontierEdge, FrontierSkip, JoinBuildProgressCatalog, JoinBuildProgressProof,
        JoinBuildProgressSkip,
    };
    use crate::sql::planner::runtime_filter::contract::{BindingId, ChannelId};

    type Key = (ChannelId, BindingId, u32);

    fn key() -> Key {
        (ChannelId::new(7), BindingId::new(11), 13)
    }

    fn proof() -> JoinBuildProgressProof {
        JoinBuildProgressProof {
            channel: ChannelId::new(7),
            producer_binding: BindingId::new(11),
            producer_fragment: 13,
            join_node_id: 17,
            build_frontier: vec![FrontierEdge {
                source_fragment: 19,
                target_exchange_node: 23,
            }],
            non_build_inputs: vec![FrontierEdge {
                source_fragment: 29,
                target_exchange_node: 31,
            }],
        }
    }

    fn skip() -> JoinBuildProgressSkip {
        JoinBuildProgressSkip {
            join_node_id: 37,
            rule: FrontierSkip::UnauditedNode { node_id: 41 },
        }
    }

    #[test]
    fn insert_proof_removes_skip_for_same_key() {
        let mut catalog = JoinBuildProgressCatalog::new();
        catalog.insert_skip(key(), skip());
        catalog.insert_proof(key(), proof());

        assert_eq!(catalog.get(&key()), Some(&proof()));
        assert_eq!(catalog.skipped(&key()), None);
    }

    #[test]
    fn insert_skip_removes_proof_for_same_key() {
        let mut catalog = JoinBuildProgressCatalog::new();
        catalog.insert_proof(key(), proof());
        catalog.insert_skip(key(), skip());

        assert_eq!(catalog.get(&key()), None);
        assert_eq!(catalog.skipped(&key()), Some(&skip()));
    }

    #[test]
    fn get_and_skipped_keep_distinct_entries() {
        let proof_key = key();
        let skip_key = (ChannelId::new(43), BindingId::new(47), 53);
        let mut catalog = JoinBuildProgressCatalog::new();
        catalog.insert_proof(proof_key, proof());
        catalog.insert_skip(skip_key, skip());

        assert_eq!(catalog.get(&proof_key), Some(&proof()));
        assert_eq!(catalog.skipped(&proof_key), None);
        assert_eq!(catalog.get(&skip_key), None);
        assert_eq!(catalog.skipped(&skip_key), Some(&skip()));
    }

    #[test]
    fn from_iterator_creates_proof_entries_only() {
        let catalog: JoinBuildProgressCatalog = [(key(), proof())].into_iter().collect();

        assert_eq!(catalog.get(&key()), Some(&proof()));
        assert_eq!(catalog.skipped(&key()), None);
    }

    #[test]
    fn entries_preserve_proof_and_skip_provenance() {
        let mut catalog = JoinBuildProgressCatalog::new();
        let proof_key = key();
        let skip_key = (ChannelId::new(43), BindingId::new(47), 53);
        catalog.insert_proof(proof_key, proof());
        catalog.insert_skip(skip_key, skip());

        let stored_proof = catalog.get(&proof_key).expect("proof should be stored");
        assert_eq!(stored_proof.channel, ChannelId::new(7));
        assert_eq!(stored_proof.producer_binding, BindingId::new(11));
        assert_eq!(stored_proof.producer_fragment, 13);
        assert_eq!(stored_proof.join_node_id, 17);
        assert_eq!(
            stored_proof.build_frontier,
            vec![FrontierEdge {
                source_fragment: 19,
                target_exchange_node: 23,
            }]
        );
        assert_eq!(
            stored_proof.non_build_inputs,
            vec![FrontierEdge {
                source_fragment: 29,
                target_exchange_node: 31,
            }]
        );
        assert_eq!(
            catalog.skipped(&skip_key),
            Some(&JoinBuildProgressSkip {
                join_node_id: 37,
                rule: FrontierSkip::UnauditedNode { node_id: 41 },
            })
        );
    }
}
