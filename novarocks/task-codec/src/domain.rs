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

use novarocks_execution_contract::TaskIdentity;
use novarocks_execution_contract::task_execution::domain::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    DomainVersion, EdgeOpenVersion, ExchangeEdgeId, PlanNodeId, SplitOffer, SplitSequence,
    SplitWatermark,
};
use novarocks_execution_contract::task_execution::operation::{
    CredentialUpdate, PlanNodeSplitReceipt, QueryContextDomainUpdate, SplitAssignmentIntent,
    TaskDomainUpdate,
};
use novarocks_proto_models::novarocks;
use prost::Message;
use sha2::{Digest, Sha256};

use novarocks_proto_codec::connector_read::SplitAssignment;
use novarocks_proto_codec::lifecycle::{
    decode_credential_lease_descriptor, validate_initial_credential_lease_descriptors,
};
use novarocks_proto_codec::{FieldPath, ProtocolError};

use crate::{invalid, missing, out_of_range};
use novarocks_spi::connector::CredentialLeaseDescriptor;

/// Largest number of domain changes one operation may carry.
pub const MAX_DOMAIN_UPDATES: usize = 256;

/// Largest number of edges one edge-open may name.
pub const MAX_OPEN_EDGES: usize = 256;

/// Largest number of credential descriptors one rotation may carry.
pub const MAX_CREDENTIAL_DESCRIPTORS: usize = 64;

/// Largest encoded dynamic filter payload one domain may carry.
///
/// This is enforced where the payload *arrives*, not only where it is read
/// back. A task retains what it accepted and a fetch has to be able to return
/// it, so accepting a payload larger than a fetch may carry would build a
/// domain that can be advertised and never delivered.
pub const MAX_DYNAMIC_FILTER_ENCODED_BYTES: usize = 16 * 1024 * 1024;

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

impl<T: Send + Sync + 'static> CodecOwnedContent for WireContent<T> {
    fn fingerprint(&self) -> ContentFingerprint {
        self.fingerprint
    }

    fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    fn stored_representation(&self) -> Option<&(dyn std::any::Any + 'static)> {
        Some(self)
    }
}

/// Recovers the generated message stored behind a neutral content handle.
///
/// The type parameter is the whole contract: a caller states which message it
/// expects for the domain it is handling, and a handle carrying anything else
/// answers `None`. There is no "closest match" and no untyped byte view, so a
/// consumer that names the wrong type fails closed rather than misreading a
/// neighbouring domain's payload.
///
/// A `None` therefore means one of two things, and both are the same verdict
/// for the caller: the handle is not one this codec produced, or it holds a
/// different message than the one asked for.
pub fn stored_message<T: Message + 'static>(content: &dyn CodecOwnedContent) -> Option<&T> {
    content
        .stored_representation()?
        .downcast_ref::<WireContent<T>>()
        .map(WireContent::wire)
}

/// Recovers the credential material stored behind a neutral confidential
/// handle.
///
/// Separate from [`stored_message`] because what it returns is a secret. The
/// only legitimate caller is the owner that installs the material into its
/// slot; see [`ConfidentialContent::stored_representation`].
pub fn stored_credential(content: &dyn ConfidentialContent) -> Option<&WireCredential> {
    content
        .stored_representation()?
        .downcast_ref::<WireCredential>()
}

const SPLIT_DOMAIN_TAG: &[u8] = b"novarocks.task_execution.split_assignment.v1";
const TASK_FILTER_DOMAIN_TAG: &[u8] = b"novarocks.task_execution.task_dynamic_filter.v1";
const CATALOG_DOMAIN_TAG: &[u8] = b"novarocks.task_execution.catalog_binding.v1";
const SHARED_FILTER_DOMAIN_TAG: &[u8] = b"novarocks.task_execution.shared_dynamic_filter.v1";

/// The storage scopes one rotation announces, and how each is acquired.
///
/// This used to carry confidential material paired with the descriptors that
/// scoped it. Material no longer travels at all: the node that consumes it
/// acquires it under its own catalog identity (CAD-1 D1), so what remains is
/// an announcement — which scopes an attempt touches and where each one's
/// credentials come from.
///
/// Same-epoch equality is still a byte comparison against the live installed
/// value. That is not a property of secrecy but of the domain: a rotation that
/// re-announced a different scope set under the same epoch is a conflict, and
/// comparing a digest would let two different announcements of equal size pass
/// as the same one.
pub struct WireCredential {
    leases: Vec<CredentialLeaseDescriptor>,
    /// The received bytes, retained so a rotation re-encodes to exactly what
    /// arrived rather than to whatever a re-encode of the decoded value would
    /// produce.
    descriptors: Vec<novarocks::CredentialLeaseDescriptor>,
    encoded_len: usize,
}

/// Renders how many scopes are announced and how large the announcement is.
impl fmt::Debug for WireCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WireCredential")
            .field("leases", &self.leases.len())
            .field("encoded_len", &self.encoded_len)
            .finish()
    }
}

impl WireCredential {
    /// Decodes and validates one rotation's descriptor list.
    ///
    /// Cardinality, ordering and per-entry bounds are delegated to the
    /// lifecycle validator, which is the single authority over that shape.
    pub fn decode(
        descriptors: &[novarocks::CredentialLeaseDescriptor],
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        validate_initial_credential_lease_descriptors(descriptors, path.clone())?;
        let mut leases = Vec::with_capacity(descriptors.len());
        for (index, descriptor) in descriptors.iter().enumerate() {
            leases.push(decode_credential_lease_descriptor(
                descriptor.clone(),
                path.clone().field("descriptors").index(index),
            )?);
        }
        Ok(Self {
            leases,
            descriptors: descriptors.to_vec(),
            encoded_len: descriptors.iter().map(Message::encoded_len).sum(),
        })
    }

    /// The announced scopes, for the backend's credential slot owner.
    pub fn leases(&self) -> &[CredentialLeaseDescriptor] {
        &self.leases
    }

    /// Whether this rotation announces any scope at all.
    ///
    /// A query with no vended catalog establishes with an empty rotation, so
    /// "has a credential domain" and "announces a scope" are different
    /// questions.
    pub fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }

    pub fn descriptors(&self) -> &[novarocks::CredentialLeaseDescriptor] {
        &self.descriptors
    }
}

impl ConfidentialContent for WireCredential {
    fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    fn matches(&self, other: &dyn ConfidentialContent) -> bool {
        // Both sides are produced by this codec in practice, so the byte-exact
        // comparison is the one that runs. A handle that is not one of ours
        // cannot be compared at all: answering by size would call two
        // different secrets of equal length identical, which would turn a
        // same-epoch rotation conflict into a silent acknowledgement.
        match stored_credential(other) {
            Some(other) => self.matches_exact(other),
            None => false,
        }
    }

    fn stored_representation(&self) -> Option<&(dyn std::any::Any + 'static)> {
        Some(self)
    }
}

impl WireCredential {
    /// Byte-exact comparison against another decoded announcement.
    pub fn matches_exact(&self, other: &Self) -> bool {
        self.descriptors.len() == other.descriptors.len()
            && self
                .descriptors
                .iter()
                .zip(other.descriptors.iter())
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
    CloseExchangeDestination {
        version: DomainVersion,
        edge: ExchangeEdgeId,
        destination: TaskIdentity,
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
            Self::CloseExchangeDestination {
                version,
                edge,
                destination,
            } => TaskDomainUpdate::CloseExchangeDestination {
                version: *version,
                edge: *edge,
                destination: *destination,
            },
        }
    }
}

/// The split-domain offer one wire assignment names.
///
/// Both sides of the boundary derive the offer here, from the same message the
/// frontend puts on the wire and the backend takes off it. A second derivation
/// -- however carefully mirrored -- would be a second authority over one wire
/// fact, and the two sides would then classify the same assignment against
/// different watermarks.
///
/// An assignment carrying no splits is the standalone terminal marker for its
/// plan node: it names no range, so it decodes to [`SplitOffer::Seal`] rather
/// than to a placeholder range that a genuine one-split batch could also
/// produce.
pub fn split_offer(
    assignment: &novarocks_proto_models::connector_read::SplitAssignment,
    path: FieldPath,
) -> Result<SplitOffer, ProtocolError> {
    let (Some(first), Some(last)) = (assignment.splits.first(), assignment.splits.last()) else {
        if !assignment.no_more_splits {
            return Err(missing(
                path.field("splits"),
                "an assignment with no splits must carry no_more_splits",
            ));
        }
        return Ok(SplitOffer::Seal);
    };
    let sequence = |raw: u64| {
        SplitSequence::new(raw)
            .map_err(|error| out_of_range(path.clone().field("splits"), error.to_string()))
    };
    SplitOffer::batch(
        sequence(first.sequence_id)?,
        sequence(last.sequence_id)?,
        assignment.no_more_splits,
    )
    .ok_or_else(|| {
        invalid(
            path.field("splits"),
            "split batch sequences must not descend",
        )
    })
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
            let offer = split_offer(assignment.as_proto(), split_path.field("assignment"))?;
            let intent = SplitAssignmentIntent::new(
                node,
                offer,
                Arc::new(content) as Arc<dyn CodecOwnedContent>,
            );
            Ok(DecodedTaskDomain::SplitAssignment { intent, assignment })
        }
        novarocks::task_domain_update::Domain::DynamicFilter(filter) => {
            let filter_path = path.field("dynamic_filter");
            let version = DomainVersion::new(filter.version).map_err(|error| {
                invalid(filter_path.clone().field("version"), error.to_string())
            })?;
            let envelope = filter.envelope.clone().ok_or_else(|| {
                missing(
                    filter_path.clone().field("envelope"),
                    "task dynamic filter requires an envelope",
                )
            })?;
            let content = WireContent::new(TASK_FILTER_DOMAIN_TAG, envelope);
            if content.encoded_len > MAX_DYNAMIC_FILTER_ENCODED_BYTES {
                return Err(out_of_range(
                    filter_path.field("envelope"),
                    "dynamic filter payload exceeds what a fetch can return",
                ));
            }
            Ok(DecodedTaskDomain::DynamicFilter {
                version,
                content: Arc::new(content),
            })
        }
        novarocks::task_domain_update::Domain::OpenExchangeEdges(open) => {
            let open_path = path.field("open_exchange_edges");
            // Any nonzero version decodes. One version means one exact edge
            // set, and a producer whose edges are decided one at a time mints
            // a fresh version per decision, so pinning the wire to version one
            // refused every edge after a multi-edge producer's first. Whether
            // a version is a legal progression is the receiving domain's
            // question, not this decoder's.
            let version = EdgeOpenVersion::new(open.version)
                .map_err(|error| invalid(open_path.clone().field("version"), error.to_string()))?;
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
        novarocks::task_domain_update::Domain::CloseExchangeDestination(close) => {
            let close_path = path.field("close_exchange_destination");
            let version = DomainVersion::new(close.version)
                .map_err(|error| invalid(close_path.clone().field("version"), error.to_string()))?;
            let edge = ExchangeEdgeId::new(close.edge_id)
                .map_err(|error| invalid(close_path.clone().field("edge_id"), error.to_string()))?;
            let destination = close.destination_task.as_ref().ok_or_else(|| {
                missing(
                    close_path.clone().field("destination_task"),
                    "destination close requires a task identity",
                )
            })?;
            let destination = crate::identity::decode_task_identity(
                destination,
                close_path.field("destination_task"),
            )?;
            Ok(DecodedTaskDomain::CloseExchangeDestination {
                version,
                edge,
                destination,
            })
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
        DecodedTaskDomain::CloseExchangeDestination {
            version,
            edge,
            destination,
        } => novarocks::task_domain_update::Domain::CloseExchangeDestination(
            novarocks::CloseExchangeDestinationDomain {
                version: version.get(),
                edge_id: edge.get(),
                destination_task: Some(crate::identity::encode_task_identity(*destination)),
            },
        ),
    };
    novarocks::TaskDomainUpdate {
        domain: Some(domain),
    }
}

/// Encodes one query-context domain change the owner holds neutrally.
///
/// The credential arm is the reason this cannot be shared with its task-scoped
/// sibling: its payload is a secret, so it is recovered through the
/// confidential projection rather than the ordinary one, and the whole lease
/// table travels with every rotation.
pub fn encode_neutral_query_context_domain(
    value: &QueryContextDomainUpdate,
    path: FieldPath,
) -> Result<novarocks::QueryContextDomainUpdate, ProtocolError> {
    let domain = match value {
        QueryContextDomainUpdate::CatalogBinding { version, payload } => {
            let catalog_set =
                stored_message::<novarocks_proto_models::catalog::CatalogSet>(payload.as_ref())
                    .ok_or_else(|| {
                        invalid(
                            path.field("catalog_binding"),
                            "catalog binding payload is not a codec-produced catalog set",
                        )
                    })?;
            novarocks::query_context_domain_update::Domain::CatalogBinding(
                novarocks::QueryContextCatalogDomain {
                    version: version.get(),
                    catalog_set: Some(catalog_set.clone()),
                },
            )
        }
        QueryContextDomainUpdate::SharedDynamicFilter { version, payload } => {
            let contribution =
                stored_message::<novarocks::RuntimeFilterContribution>(payload.as_ref())
                    .ok_or_else(|| {
                        invalid(
                            path.field("shared_dynamic_filter"),
                            "shared filter payload is not a codec-produced contribution",
                        )
                    })?;
            novarocks::query_context_domain_update::Domain::SharedDynamicFilter(
                novarocks::QueryContextSharedDynamicFilterDomain {
                    version: version.get(),
                    contribution: Some(contribution.clone()),
                },
            )
        }
        QueryContextDomainUpdate::Credential(update) => {
            let material = stored_credential(update.material().as_ref()).ok_or_else(|| {
                invalid(
                    path.field("credential"),
                    "credential payload is not codec-produced material",
                )
            })?;
            novarocks::query_context_domain_update::Domain::Credential(
                novarocks::QueryContextCredentialDomain {
                    lease_id: update.lease_id().get(),
                    epoch: update.epoch().get(),
                    descriptors: material.descriptors().to_vec(),
                },
            )
        }
    };
    Ok(novarocks::QueryContextDomainUpdate {
        domain: Some(domain),
    })
}

/// Wraps one split assignment as the codec-owned payload a domain carries.
///
/// The domain separation tag stays private here. A caller that built its own
/// `WireContent` would have to name the tag, and a wrong tag produces a
/// payload that decodes as a different domain's content -- which is exactly
/// the confusion the tag exists to prevent.
pub fn wire_split_assignment(
    assignment: novarocks_proto_models::connector_read::SplitAssignment,
) -> Arc<dyn CodecOwnedContent> {
    Arc::new(WireContent::new(SPLIT_DOMAIN_TAG, assignment))
}

/// Wraps one runtime filter envelope as the codec-owned payload a task's
/// dynamic filter domain carries.
///
/// The same tag serves both directions of that domain: a frontend push decodes
/// through [`decode_task_domain`], and a backend that advertised a domain
/// retains this so [`encode_task_dynamic_filter_domain`] can project it back
/// onto a fetch response. Keeping the tag private is what stops a producer from
/// retaining content under a tag that would decode as another domain.
pub fn wire_task_dynamic_filter(
    envelope: novarocks_proto_models::filter::RuntimeFilterEnvelope,
) -> Arc<dyn CodecOwnedContent> {
    Arc::new(WireContent::new(TASK_FILTER_DOMAIN_TAG, envelope))
}

/// Encodes one task-scoped domain change the owner holds neutrally.
///
/// The neutral update carries its payload behind a fingerprint, so this
/// projects it back to the message it was built from. A payload this codec did
/// not produce, or one holding a different message than its domain implies, is
/// refused: encoding it as an empty or default body would put a request on the
/// wire that says something the owner never decided.
pub fn encode_neutral_task_domain(
    value: &TaskDomainUpdate,
    path: FieldPath,
) -> Result<novarocks::TaskDomainUpdate, ProtocolError> {
    let domain = match value {
        TaskDomainUpdate::SplitAssignment(intent) => {
            let assignment = stored_message::<
                novarocks_proto_models::connector_read::SplitAssignment,
            >(intent.payload().as_ref())
            .ok_or_else(|| {
                invalid(
                    path.field("split_assignment"),
                    "split assignment payload is not a codec-produced assignment",
                )
            })?;
            novarocks::task_domain_update::Domain::SplitAssignment(
                novarocks::TaskSplitAssignmentDomain {
                    assignment: Some(assignment.clone()),
                },
            )
        }
        TaskDomainUpdate::TaskDynamicFilter { version, payload } => {
            let envelope = stored_message::<novarocks_proto_models::filter::RuntimeFilterEnvelope>(
                payload.as_ref(),
            )
            .ok_or_else(|| {
                invalid(
                    path.field("dynamic_filter"),
                    "dynamic filter payload is not a codec-produced runtime filter envelope",
                )
            })?;
            novarocks::task_domain_update::Domain::DynamicFilter(
                novarocks::TaskDynamicFilterDomain {
                    version: version.get(),
                    envelope: Some(envelope.clone()),
                },
            )
        }
        TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
            novarocks::task_domain_update::Domain::OpenExchangeEdges(
                novarocks::OpenExchangeEdgesDomain {
                    version: version.get(),
                    edge_ids: edges.iter().map(|edge| edge.get()).collect(),
                },
            )
        }
        TaskDomainUpdate::CloseExchangeDestination {
            version,
            edge,
            destination,
        } => novarocks::task_domain_update::Domain::CloseExchangeDestination(
            novarocks::CloseExchangeDestinationDomain {
                version: version.get(),
                edge_id: edge.get(),
                destination_task: Some(crate::identity::encode_task_identity(*destination)),
            },
        ),
    };
    Ok(novarocks::TaskDomainUpdate {
        domain: Some(domain),
    })
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
    // Cardinality, ordering, bounds, and exact envelope-to-descriptor pairing
    // are the lifecycle validator's contract; a rotation whose secret does not
    // match the scope it claims is refused here rather than installed and
    // discovered at first use.
    let material = Arc::new(WireCredential::decode(&src.descriptors, path.clone())?);
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

/// Why a retained dynamic filter domain cannot be put on the wire.
///
/// Both variants are invariant violations inside this process rather than
/// something a caller can cause, which is why neither has an in-band form: a
/// payload this codec did not produce, or one it accepted above the fetch
/// bound, means an owner somewhere stored content it could never deliver.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DynamicFilterProjectionError {
    /// The handle does not carry a filter envelope this codec stored.
    NotAFilterEnvelope,
    /// The retained payload is larger than a fetch may carry.
    PayloadTooLarge { limit: usize, actual: usize },
}

impl fmt::Display for DynamicFilterProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAFilterEnvelope => formatter
                .write_str("retained dynamic filter payload is not a runtime filter envelope"),
            Self::PayloadTooLarge { limit, actual } => write!(
                formatter,
                "retained dynamic filter payload is {actual} bytes, above the {limit} byte fetch \
                 bound"
            ),
        }
    }
}

impl std::error::Error for DynamicFilterProjectionError {}

/// Projects one task's retained dynamic filter domain back onto the wire.
///
/// This is the read side of [`decode_task_domain`]'s filter branch: the owner
/// retains what a task advertised as codec-owned content, and this recovers
/// the envelope that content actually is. The version travels with it because
/// a domain payload without its token would not tell the frontend what it just
/// acknowledged.
pub fn encode_task_dynamic_filter_domain(
    version: DomainVersion,
    payload: &dyn CodecOwnedContent,
) -> Result<novarocks::TaskDynamicFilterDomain, DynamicFilterProjectionError> {
    let encoded_len = payload.encoded_len();
    if encoded_len > MAX_DYNAMIC_FILTER_ENCODED_BYTES {
        return Err(DynamicFilterProjectionError::PayloadTooLarge {
            limit: MAX_DYNAMIC_FILTER_ENCODED_BYTES,
            actual: encoded_len,
        });
    }
    let envelope = stored_message::<novarocks_proto_models::filter::RuntimeFilterEnvelope>(payload)
        .ok_or(DynamicFilterProjectionError::NotAFilterEnvelope)?;
    Ok(novarocks::TaskDynamicFilterDomain {
        version: version.get(),
        envelope: Some(envelope.clone()),
    })
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

#[cfg(test)]
mod tests {
    use super::{
        DynamicFilterProjectionError, MAX_DYNAMIC_FILTER_ENCODED_BYTES, WireContent,
        WireCredential, decode_task_domain, encode_task_dynamic_filter_domain, stored_credential,
        stored_message,
    };

    use novarocks_execution_contract::task_execution::domain::{
        CodecOwnedContent, ConfidentialContent, DomainVersion, ExchangeEdgeId,
    };
    use novarocks_execution_contract::{TaskDomainUpdate, TaskIdentity};
    use novarocks_proto_models::{catalog, filter, novarocks};

    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::lifecycle::encode_credential_lease_descriptor;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor,
        CredentialLeaseId, CredentialLeaseProvider, StorageAccessDomainId,
        StorageCredentialScopePrefix,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    #[test]
    fn destination_close_wire_requires_and_preserves_exact_task_identity() {
        let destination = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(51, 52), AttemptId::new(1).expect("attempt"))
                .expect("query"),
            StageId::new(2).expect("stage"),
            TaskId::new(3).expect("task"),
            BackendProcessId::new_v7(),
        );
        let update = TaskDomainUpdate::CloseExchangeDestination {
            version: DomainVersion::FIRST,
            edge: ExchangeEdgeId::new(7).expect("edge"),
            destination,
        };
        let wire =
            super::encode_neutral_task_domain(&update, FieldPath::root("domain")).expect("encodes");
        assert!(matches!(
            decode_task_domain(&wire, FieldPath::root("domain"))
                .expect("decodes").as_neutral(),
            TaskDomainUpdate::CloseExchangeDestination { destination: decoded, .. } if decoded == destination
        ));
        let mut missing = wire;
        if let Some(novarocks::task_domain_update::Domain::CloseExchangeDestination(close)) =
            missing.domain.as_mut()
        {
            close.destination_task = None;
        }
        assert!(decode_task_domain(&missing, FieldPath::root("domain")).is_err());
    }

    fn descriptor_for(epoch: u64, prefix: &str) -> novarocks::CredentialLeaseDescriptor {
        encode_credential_lease_descriptor(
            &CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                epoch,
                CatalogHandle::new(
                    ConnectorInstanceId::parse("warehouse").expect("instance"),
                    CatalogVersion::from_bytes([7; 32]),
                ),
                CredentialLeaseProvider::S3,
                vec![StorageCredentialScopePrefix::try_from_normalized(prefix).expect("prefix")],
                99,
                true,
                None,
                StorageAccessDomainId::from_bytes([8; 32]),
            )
            .expect("descriptor"),
        )
    }

    /// Two announcements differing only in scope, at equal encoded length.
    fn credential(prefix: &str) -> WireCredential {
        WireCredential::decode(&[descriptor_for(3, prefix)], FieldPath::root("credential"))
            .expect("legal rotation")
    }

    #[test]
    fn a_stored_message_is_recoverable_only_as_the_type_it_is() {
        let envelope = filter::RuntimeFilterEnvelope {
            channel_id: 5,
            ..filter::RuntimeFilterEnvelope::default()
        };
        let content = WireContent::new(b"tag", envelope.clone());

        assert_eq!(
            stored_message::<filter::RuntimeFilterEnvelope>(&content),
            Some(&envelope)
        );
        assert_eq!(stored_message::<catalog::CatalogSet>(&content), None);
    }

    #[test]
    fn a_neutral_handle_this_codec_did_not_produce_answers_nothing() {
        // The hook is opt-in, so an implementation from another crate cannot
        // be downcast at all. This is what keeps the projection a codec
        // capability rather than a general escape hatch.
        #[derive(Debug)]
        struct Foreign;

        impl CodecOwnedContent for Foreign {
            fn fingerprint(&self) -> novarocks_execution_contract::ContentFingerprint {
                novarocks_execution_contract::ContentFingerprint::from_bytes([0; 16])
            }

            fn encoded_len(&self) -> usize {
                0
            }
        }

        assert!(Foreign.stored_representation().is_none());
        assert_eq!(stored_message::<catalog::CatalogSet>(&Foreign), None);
    }

    #[test]
    fn same_epoch_announcements_of_equal_length_are_a_conflict_not_a_replay() {
        // Both scopes encode to the same number of bytes. Comparing by size
        // would call them identical, which would turn a same-epoch rotation
        // announcing a different scope into an idempotent acknowledgement
        // instead of the conflict the protocol requires.
        let installed = credential("s3://bucket/orders");
        // Same byte length as the first, by construction.
        let different = credential("s3://bucket/return");
        assert_eq!(
            installed.encoded_len(),
            different.encoded_len(),
            "the test only means something if the lengths match"
        );

        assert!(installed.matches(&credential("s3://bucket/orders")));
        assert!(!installed.matches(&different));
    }

    #[test]
    fn a_foreign_confidential_handle_never_compares_equal() {
        struct Foreign;

        impl ConfidentialContent for Foreign {
            fn encoded_len(&self) -> usize {
                credential("s3://bucket/orders").encoded_len()
            }

            fn matches(&self, _other: &dyn ConfidentialContent) -> bool {
                true
            }
        }

        assert!(
            !credential("s3://bucket/orders").matches(&Foreign),
            "a handle whose contents cannot be read must not be judged equal"
        );
        assert!(stored_credential(&Foreign).is_none());
    }

    #[test]
    fn a_rotation_whose_scopes_repeat_a_lease_is_refused() {
        // Ordering and uniqueness by lease id are what keep one announcement
        // from silently replacing another inside the same rotation.
        let repeated = descriptor_for(3, "s3://bucket/orders");
        let rejection =
            WireCredential::decode(&[repeated.clone(), repeated], FieldPath::root("credential"))
                .expect_err("a repeated lease id is refused");
        assert!(rejection.to_string().contains("strictly sorted"));
    }

    #[test]
    fn an_advertised_filter_domain_projects_back_to_its_envelope() {
        let envelope = filter::RuntimeFilterEnvelope {
            channel_id: 9,
            deployment_epoch: 2,
            ..filter::RuntimeFilterEnvelope::default()
        };
        let content = WireContent::new(b"tag", envelope.clone());
        let version = DomainVersion::new(6).expect("nonzero");

        assert_eq!(
            encode_task_dynamic_filter_domain(version, &content).expect("projectable"),
            novarocks::TaskDynamicFilterDomain {
                version: 6,
                envelope: Some(envelope),
            }
        );
    }

    #[test]
    fn a_payload_of_another_domain_has_no_filter_projection() {
        let content = WireContent::new(b"tag", catalog::CatalogSet::default());
        assert_eq!(
            encode_task_dynamic_filter_domain(DomainVersion::FIRST, &content),
            Err(DynamicFilterProjectionError::NotAFilterEnvelope)
        );
    }

    #[test]
    fn a_filter_payload_above_the_fetch_bound_is_refused_where_it_arrives() {
        // Accepting it would build a domain the task can advertise and no
        // fetch can ever return.
        let oversized = filter::RuntimeFilterEnvelope {
            payload: vec![0_u8; MAX_DYNAMIC_FILTER_ENCODED_BYTES + 1],
            ..filter::RuntimeFilterEnvelope::default()
        };
        let update = novarocks::TaskDomainUpdate {
            domain: Some(novarocks::task_domain_update::Domain::DynamicFilter(
                novarocks::TaskDynamicFilterDomain {
                    version: 1,
                    envelope: Some(oversized),
                },
            )),
        };

        assert!(decode_task_domain(&update, FieldPath::root("domain")).is_err());
    }
}
