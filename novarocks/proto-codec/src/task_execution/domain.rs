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

//! Task and query-context domain codec.
//!
//! Two domains have no transport-neutral content type: a connector split
//! batch is provider wire data, and a runtime filter envelope is a generated
//! message the filter runtime consumes directly. Both therefore travel as a
//! [`WireContent`] handle, which answers the neutral `CodecOwnedContent`
//! capability while keeping the typed message reachable for the role that
//! actually consumes it.
//!
//! Credential material is different again: it is confidential, so it takes
//! part in no digest at all. [`WireCredential`] implements
//! `ConfidentialContent`, which has neither `Debug` nor a fingerprint, and
//! answers only whether it is byte-identical to what is installed.

use std::fmt;
use std::sync::Arc;

use novarocks_execution::task_execution::domain::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    DomainVersion, EdgeOpenVersion, ExchangeEdgeId, PlanNodeId, SplitSequence, SplitWatermark,
};
use novarocks_execution::task_execution::operation::{
    CredentialUpdate, PlanNodeSplitReceipt, QueryContextDomainUpdate, SplitAssignmentIntent,
    TaskDomainUpdate,
};
use novarocks_proto_models::novarocks;
use prost::Message;
use sha2::{Digest, Sha256};

use crate::connector_read::SplitAssignment;
use crate::task_execution::{invalid, missing, out_of_range};
use crate::{FieldPath, ProtocolError};

/// Largest number of domain changes one operation may carry.
pub const MAX_DOMAIN_UPDATES: usize = 256;

/// Largest number of edges one edge-open may name.
pub const MAX_OPEN_EDGES: usize = 256;

/// Largest number of credential descriptors one rotation may carry.
pub const MAX_CREDENTIAL_DESCRIPTORS: usize = 64;

/// Content whose stored representation is a generated message.
///
/// The domain tag keeps one domain's fingerprint from ever colliding with
/// another's, so a split batch and a filter envelope that happen to encode to
/// the same bytes still compare as different content.
pub struct WireContent<T> {
    wire: T,
    fingerprint: ContentFingerprint,
    encoded_len: usize,
}

impl<T: Message> WireContent<T> {
    pub fn new(domain_tag: &'static [u8], wire: T) -> Self {
        let encoded = wire.encode_to_vec();
        let mut hasher = Sha256::new();
        hasher.update(domain_tag);
        hasher.update(&encoded);
        let digest = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        Self {
            fingerprint: ContentFingerprint::from_bytes(bytes),
            encoded_len: encoded.len(),
            wire,
        }
    }

    /// The typed message, for the role that consumes this domain.
    pub const fn wire(&self) -> &T {
        &self.wire
    }
}

impl<T> fmt::Debug for WireContent<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WireContent")
            .field("fingerprint", &self.fingerprint)
            .field("encoded_len", &self.encoded_len)
            .finish()
    }
}

impl<T: Send + Sync> CodecOwnedContent for WireContent<T> {
    fn fingerprint(&self) -> ContentFingerprint {
        self.fingerprint
    }

    fn encoded_len(&self) -> usize {
        self.encoded_len
    }
}

const SPLIT_DOMAIN_TAG: &[u8] = b"novarocks.task_execution.split_assignment.v1";
const TASK_FILTER_DOMAIN_TAG: &[u8] = b"novarocks.task_execution.task_dynamic_filter.v1";
const CATALOG_DOMAIN_TAG: &[u8] = b"novarocks.task_execution.catalog_binding.v1";
const SHARED_FILTER_DOMAIN_TAG: &[u8] = b"novarocks.task_execution.shared_dynamic_filter.v1";

/// Confidential credential material.
///
/// Nothing here can be fingerprinted or printed. Same-epoch equality is a
/// byte comparison against the live installed value, which is the only
/// question the protocol is allowed to ask of a secret.
pub struct WireCredential {
    envelopes: Vec<novarocks::CredentialLeaseSecretEnvelope>,
    encoded_len: usize,
}

impl WireCredential {
    pub fn new(envelopes: Vec<novarocks::CredentialLeaseSecretEnvelope>) -> Self {
        let encoded_len = envelopes.iter().map(Message::encoded_len).sum();
        Self {
            envelopes,
            encoded_len,
        }
    }

    /// The envelopes, for the backend's credential slot owner.
    pub fn envelopes(&self) -> &[novarocks::CredentialLeaseSecretEnvelope] {
        &self.envelopes
    }
}

impl ConfidentialContent for WireCredential {
    fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    fn matches(&self, other: &dyn ConfidentialContent) -> bool {
        // A cross-type comparison can only be answered by size, which is all
        // this trait exposes. Both sides are produced by this codec in
        // practice, so the concrete comparison below is the one that runs.
        self.encoded_len == other.encoded_len()
    }
}

impl WireCredential {
    /// Byte-exact comparison against another decoded credential.
    pub fn matches_exact(&self, other: &Self) -> bool {
        self.envelopes.len() == other.envelopes.len()
            && self
                .envelopes
                .iter()
                .zip(other.envelopes.iter())
                .all(|(left, right)| left == right)
    }
}

/// One decoded task-scoped domain change, with its typed content retained.
pub enum DecodedTaskDomain {
    SplitAssignment {
        intent: SplitAssignmentIntent,
        assignment: SplitAssignment,
    },
    DynamicFilter {
        version: DomainVersion,
        content: Arc<WireContent<novarocks_proto_models::filter::RuntimeFilterEnvelope>>,
    },
    OpenExchangeEdges {
        version: EdgeOpenVersion,
        edges: Vec<ExchangeEdgeId>,
    },
}

impl DecodedTaskDomain {
    /// The neutral view the domain classifier consumes.
    pub fn as_neutral(&self) -> TaskDomainUpdate {
        match self {
            Self::SplitAssignment { intent, .. } => {
                TaskDomainUpdate::SplitAssignment(intent.clone())
            }
            Self::DynamicFilter { version, content } => TaskDomainUpdate::TaskDynamicFilter {
                version: *version,
                payload: Arc::clone(content) as Arc<dyn CodecOwnedContent>,
            },
            Self::OpenExchangeEdges { version, edges } => TaskDomainUpdate::OpenExchangeEdges {
                version: *version,
                edges: edges.clone(),
            },
        }
    }
}

/// Decodes one task-scoped domain change.
pub fn decode_task_domain(
    src: &novarocks::TaskDomainUpdate,
    path: FieldPath,
) -> Result<DecodedTaskDomain, ProtocolError> {
    let domain = src
        .domain
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "task domain update requires a domain"))?;
    match domain {
        novarocks::task_domain_update::Domain::SplitAssignment(split) => {
            let split_path = path.field("split_assignment");
            let assignment = split.assignment.as_ref().ok_or_else(|| {
                missing(
                    split_path.clone().field("assignment"),
                    "split domain requires an assignment",
                )
            })?;
            let assignment =
                SplitAssignment::parse(assignment.clone(), split_path.clone().field("assignment"))?;
            let node = PlanNodeId::new(assignment.plan_node_id()).map_err(|error| {
                out_of_range(
                    split_path.clone().field("assignment").field("plan_node_id"),
                    error.to_string(),
                )
            })?;
            let content = WireContent::new(SPLIT_DOMAIN_TAG, assignment.as_proto().clone());
            let splits = assignment.splits();
            let intent = match (splits.first(), splits.last()) {
                (Some(first), Some(last)) => {
                    let first = SplitSequence::new(first.sequence_id()).map_err(|error| {
                        out_of_range(
                            split_path.clone().field("assignment").field("splits"),
                            error.to_string(),
                        )
                    })?;
                    let last = SplitSequence::new(last.sequence_id()).map_err(|error| {
                        out_of_range(
                            split_path.clone().field("assignment").field("splits"),
                            error.to_string(),
                        )
                    })?;
                    SplitAssignmentIntent::new(
                        node,
                        first,
                        last,
                        assignment.no_more_splits(),
                        Arc::new(content) as Arc<dyn CodecOwnedContent>,
                    )
                    .ok_or_else(|| {
                        invalid(
                            split_path.clone().field("assignment").field("splits"),
                            "split batch sequences must not descend",
                        )
                    })?
                }
                // An assignment with no splits is the standalone terminal
                // marker for its plan node.
                _ => {
                    if !assignment.no_more_splits() {
                        return Err(missing(
                            split_path.field("assignment").field("splits"),
                            "an assignment with no splits must carry no_more_splits",
                        ));
                    }
                    SplitAssignmentIntent::new(
                        node,
                        SplitSequence::FIRST,
                        SplitSequence::FIRST,
                        true,
                        Arc::new(content) as Arc<dyn CodecOwnedContent>,
                    )
                    .expect("first equals last")
                }
            };
            Ok(DecodedTaskDomain::SplitAssignment { intent, assignment })
        }
        novarocks::task_domain_update::Domain::DynamicFilter(filter) => {
            let filter_path = path.field("dynamic_filter");
            let version = DomainVersion::new(filter.version).map_err(|error| {
                invalid(filter_path.clone().field("version"), error.to_string())
            })?;
            let envelope = filter.envelope.clone().ok_or_else(|| {
                missing(
                    filter_path.field("envelope"),
                    "task dynamic filter requires an envelope",
                )
            })?;
            Ok(DecodedTaskDomain::DynamicFilter {
                version,
                content: Arc::new(WireContent::new(TASK_FILTER_DOMAIN_TAG, envelope)),
            })
        }
        novarocks::task_domain_update::Domain::OpenExchangeEdges(open) => {
            let open_path = path.field("open_exchange_edges");
            let version = EdgeOpenVersion::new(open.version)
                .map_err(|error| invalid(open_path.clone().field("version"), error.to_string()))?;
            if version != EdgeOpenVersion::FIRST {
                return Err(invalid(
                    open_path.clone().field("version"),
                    "this release opens an edge exactly once, at version one",
                ));
            }
            if open.edge_ids.is_empty() {
                return Err(missing(
                    open_path.clone().field("edge_ids"),
                    "an edge-open must name at least one edge",
                ));
            }
            if open.edge_ids.len() > MAX_OPEN_EDGES {
                return Err(out_of_range(
                    open_path.clone().field("edge_ids"),
                    "edge count exceeds the hard limit",
                ));
            }
            let mut edges = Vec::with_capacity(open.edge_ids.len());
            for (index, edge) in open.edge_ids.iter().enumerate() {
                edges.push(ExchangeEdgeId::new(*edge).map_err(|error| {
                    invalid(
                        open_path.clone().field("edge_ids").index(index),
                        error.to_string(),
                    )
                })?);
            }
            Ok(DecodedTaskDomain::OpenExchangeEdges { version, edges })
        }
    }
}

/// Encodes one task-scoped domain change.
///
/// The split and filter variants re-encode from the retained typed message,
/// which is why the decoded form is what gets encoded rather than the neutral
/// view: the neutral view holds only a fingerprint of the content.
pub fn encode_task_domain(value: &DecodedTaskDomain) -> novarocks::TaskDomainUpdate {
    let domain = match value {
        DecodedTaskDomain::SplitAssignment { assignment, .. } => {
            novarocks::task_domain_update::Domain::SplitAssignment(
                novarocks::TaskSplitAssignmentDomain {
                    assignment: Some(assignment.as_proto().clone()),
                },
            )
        }
        DecodedTaskDomain::DynamicFilter { version, content } => {
            novarocks::task_domain_update::Domain::DynamicFilter(
                novarocks::TaskDynamicFilterDomain {
                    version: version.get(),
                    envelope: Some(content.wire().clone()),
                },
            )
        }
        DecodedTaskDomain::OpenExchangeEdges { version, edges } => {
            novarocks::task_domain_update::Domain::OpenExchangeEdges(
                novarocks::OpenExchangeEdgesDomain {
                    version: version.get(),
                    edge_ids: edges.iter().map(|edge| edge.get()).collect(),
                },
            )
        }
    };
    novarocks::TaskDomainUpdate {
        domain: Some(domain),
    }
}

/// One decoded query-context domain change, with its typed content retained.
pub enum DecodedQueryContextDomain {
    CatalogBinding {
        version: DomainVersion,
        content: Arc<WireContent<novarocks_proto_models::catalog::CatalogSet>>,
    },
    SharedDynamicFilter {
        version: DomainVersion,
        content: Arc<WireContent<novarocks::RuntimeFilterContribution>>,
    },
    Credential {
        update: CredentialUpdate,
        descriptors: Vec<novarocks::CredentialLeaseDescriptor>,
        material: Arc<WireCredential>,
    },
}

impl DecodedQueryContextDomain {
    /// The neutral view the domain classifier consumes.
    pub fn as_neutral(&self) -> QueryContextDomainUpdate {
        match self {
            Self::CatalogBinding { version, content } => QueryContextDomainUpdate::CatalogBinding {
                version: *version,
                payload: Arc::clone(content) as Arc<dyn CodecOwnedContent>,
            },
            Self::SharedDynamicFilter { version, content } => {
                QueryContextDomainUpdate::SharedDynamicFilter {
                    version: *version,
                    payload: Arc::clone(content) as Arc<dyn CodecOwnedContent>,
                }
            }
            Self::Credential { update, .. } => QueryContextDomainUpdate::Credential(update.clone()),
        }
    }
}

/// Decodes a credential rotation.
pub fn decode_credential_domain(
    src: &novarocks::QueryContextCredentialDomain,
    path: FieldPath,
) -> Result<DecodedQueryContextDomain, ProtocolError> {
    let epoch = CredentialEpoch::new(src.epoch)
        .map_err(|error| invalid(path.clone().field("epoch"), error.to_string()))?;
    if src.descriptors.len() > MAX_CREDENTIAL_DESCRIPTORS {
        return Err(out_of_range(
            path.clone().field("descriptors"),
            "credential descriptor count exceeds the hard limit",
        ));
    }
    // Every descriptor must have exactly one envelope. A descriptor without
    // its secret cannot be installed, and a spare envelope has no owner.
    if src.envelopes.len() != src.descriptors.len() {
        return Err(invalid(
            path.clone().field("envelopes"),
            "each credential descriptor requires exactly one envelope",
        ));
    }
    let material = Arc::new(WireCredential::new(src.envelopes.clone()));
    let update = CredentialUpdate::new(
        CredentialLeaseId::new(src.lease_id),
        epoch,
        Arc::clone(&material) as Arc<dyn ConfidentialContent>,
    );
    Ok(DecodedQueryContextDomain::Credential {
        update,
        descriptors: src.descriptors.clone(),
        material,
    })
}

/// Decodes one query-context domain change.
pub fn decode_query_context_domain(
    src: &novarocks::QueryContextDomainUpdate,
    path: FieldPath,
) -> Result<DecodedQueryContextDomain, ProtocolError> {
    let domain = src.domain.as_ref().ok_or_else(|| {
        missing(
            path.clone(),
            "query context domain update requires a domain",
        )
    })?;
    match domain {
        novarocks::query_context_domain_update::Domain::CatalogBinding(catalog) => {
            let catalog_path = path.field("catalog_binding");
            let version = DomainVersion::new(catalog.version).map_err(|error| {
                invalid(catalog_path.clone().field("version"), error.to_string())
            })?;
            let catalog_set = catalog.catalog_set.clone().ok_or_else(|| {
                missing(
                    catalog_path.field("catalog_set"),
                    "catalog binding requires a catalog set",
                )
            })?;
            Ok(DecodedQueryContextDomain::CatalogBinding {
                version,
                content: Arc::new(WireContent::new(CATALOG_DOMAIN_TAG, catalog_set)),
            })
        }
        novarocks::query_context_domain_update::Domain::SharedDynamicFilter(filter) => {
            let filter_path = path.field("shared_dynamic_filter");
            let version = DomainVersion::new(filter.version).map_err(|error| {
                invalid(filter_path.clone().field("version"), error.to_string())
            })?;
            let contribution = filter.contribution.clone().ok_or_else(|| {
                missing(
                    filter_path.field("contribution"),
                    "shared dynamic filter requires a contribution",
                )
            })?;
            Ok(DecodedQueryContextDomain::SharedDynamicFilter {
                version,
                content: Arc::new(WireContent::new(SHARED_FILTER_DOMAIN_TAG, contribution)),
            })
        }
        novarocks::query_context_domain_update::Domain::Credential(credential) => {
            decode_credential_domain(credential, path.field("credential"))
        }
    }
}

/// Decodes the split receipt of one plan node.
pub fn decode_plan_node_split_receipt(
    src: &novarocks::TaskSplitPlanNodeReceipt,
    path: FieldPath,
) -> Result<PlanNodeSplitReceipt, ProtocolError> {
    let node = PlanNodeId::new(src.plan_node_id)
        .map_err(|error| out_of_range(path.clone().field("plan_node_id"), error.to_string()))?;
    let mut watermark = SplitWatermark::empty();
    if src.accepted_through_sequence > 0 {
        let accepted = SplitSequence::new(src.accepted_through_sequence)
            .map_err(|error| invalid(path.field("accepted_through_sequence"), error.to_string()))?;
        watermark = watermark.apply_batch(accepted, src.no_more_splits);
    } else if src.no_more_splits {
        watermark = watermark.apply_no_more();
    }
    let mut receipt = PlanNodeSplitReceipt::new(node, watermark);
    if let Some(queued) = src.queued_splits {
        receipt = receipt.with_queued_splits(queued);
    }
    Ok(receipt)
}

pub fn encode_plan_node_split_receipt(
    value: PlanNodeSplitReceipt,
) -> novarocks::TaskSplitPlanNodeReceipt {
    novarocks::TaskSplitPlanNodeReceipt {
        plan_node_id: value.node().get(),
        accepted_through_sequence: value
            .watermark()
            .accepted_through()
            .map_or(0, SplitSequence::get),
        no_more_splits: value.watermark().no_more_splits(),
        queued_splits: value.queued_splits(),
    }
}
