//! Frontend-owned runtime-filter lifecycle contribution encoder.

use std::collections::{BTreeMap, BTreeSet};

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use arrow::datatypes::DataType;
use novarocks_proto_models::{filter, novarocks as service};
use novarocks_types::BackendProcessId;

use super::model::{FrontendRuntimeFilterDeployment, FrontendRuntimeFilterParticipant};

fn encoding_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// A Frontend-owned, deterministically ordered contribution table.  The caller
/// seals it with the Core schedule view; this type cannot attach itself to an
/// arbitrary query artifact.
pub(crate) struct EncodedRuntimeFilterDeployment {
    contributions: BTreeMap<usize, service::RuntimeFilterContribution>,
    /// Backends present only to satisfy the retiring `InitQuery` coverage
    /// rule. They host no task and own no filter role.
    padded_backends: BTreeSet<usize>,
    feedback_declaration: FrontendRuntimeFilterFeedbackDeclaration,
}

/// FE-private authority compiled from the same sealed deployment as the BE
/// install.  It is intentionally not a protocol DTO: later lifecycle
/// admission owns its mutable terminal state, while this value remains a
/// query-attempt-local immutable declaration.
#[derive(Clone, Debug, Default)]
pub(crate) struct FrontendRuntimeFilterFeedbackDeclaration {
    channels: BTreeMap<u32, FrontendRuntimeFilterFeedbackChannel>,
}

impl FrontendRuntimeFilterFeedbackDeclaration {
    pub(crate) fn new(
        channels: impl IntoIterator<Item = FrontendRuntimeFilterFeedbackChannel>,
    ) -> Result<Self, DistributedQueryError> {
        let mut by_channel = BTreeMap::new();
        for channel in channels {
            if by_channel.insert(channel.channel_id, channel).is_some() {
                return Err(encoding_error(
                    "runtime filter feedback declaration repeats a channel",
                ));
            }
        }
        Ok(Self {
            channels: by_channel,
        })
    }

    pub(crate) fn channels(
        &self,
    ) -> impl ExactSizeIterator<Item = &FrontendRuntimeFilterFeedbackChannel> {
        self.channels.values()
    }

    pub(crate) fn channel(&self, channel_id: u32) -> Option<&FrontendRuntimeFilterFeedbackChannel> {
        self.channels.get(&channel_id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FrontendRuntimeFilterFeedbackChannel {
    pub(crate) channel_id: u32,
    pub(crate) contract_digest: [u8; 32],
    pub(crate) max_encoded_domain_bytes: u64,
    pub(crate) publishers: Vec<FrontendRuntimeFilterFeedbackPublisherSlot>,
    pub(crate) scan_bindings: Vec<FrontendRuntimeFilterFeedbackScanBinding>,
    pub(crate) wait_eligibility: FrontendRuntimeFilterFeedbackWaitEligibility,
}

impl FrontendRuntimeFilterFeedbackChannel {
    pub(crate) const fn channel_id(&self) -> u32 {
        self.channel_id
    }

    pub(crate) const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }

    pub(crate) const fn max_encoded_domain_bytes(&self) -> u64 {
        self.max_encoded_domain_bytes
    }

    pub(crate) fn publishers(&self) -> &[FrontendRuntimeFilterFeedbackPublisherSlot] {
        &self.publishers
    }

    pub(crate) fn scan_bindings(&self) -> &[FrontendRuntimeFilterFeedbackScanBinding] {
        &self.scan_bindings
    }

    pub(crate) const fn wait_eligibility(&self) -> &FrontendRuntimeFilterFeedbackWaitEligibility {
        &self.wait_eligibility
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrontendRuntimeFilterFeedbackPublisherOwner {
    DirectSource,
    Aggregator,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrontendRuntimeFilterFeedbackPublisherSlot {
    pub(crate) participant_id: u32,
    pub(crate) backend_process_id: BackendProcessId,
    pub(crate) owner: FrontendRuntimeFilterFeedbackPublisherOwner,
}

#[derive(Clone, Debug)]
pub(crate) struct FrontendRuntimeFilterFeedbackScanBinding {
    pub(crate) fragment_id: u32,
    pub(crate) plan_node_id: i32,
    pub(crate) binding_id: u32,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FrontendRuntimeFilterFeedbackWaitEligibility {
    Eligible,
    IneligibleCycle { witness: Vec<u32> },
}

#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
impl EncodedRuntimeFilterDeployment {
    pub(crate) fn contributions(
        &self,
    ) -> impl ExactSizeIterator<Item = (usize, service::RuntimeFilterContribution)> + '_ {
        self.contributions
            .iter()
            .map(|(backend_idx, contribution)| (*backend_idx, contribution.clone()))
    }

    /// The contributions of backends that actually host a task.
    ///
    /// This is the deployment's own truth. [`Self::contributions`] can be
    /// wider, because the retiring `InitQuery` path requires a manifest for
    /// every frozen live backend; see [`Self::pad_to_frozen_live_backends`].
    pub(crate) fn task_hosting_contributions(
        &self,
    ) -> impl Iterator<Item = (usize, &service::RuntimeFilterContribution)> + '_ {
        self.contributions
            .iter()
            .filter(|(backend_idx, _)| !self.padded_backends.contains(*backend_idx))
            .map(|(backend_idx, contribution)| (*backend_idx, contribution))
    }

    /// Adds an explicitly empty contribution for every frozen live backend
    /// that hosts no task.
    ///
    /// `InitQuery` admits a participant only if it carries fragments or a
    /// filter contribution, and its attachment demands one entry per frozen
    /// live backend, so a task-free backend still has to appear there. The
    /// deployment no longer has a participant for such a backend: this pads
    /// the wire table alone, so the padding is confined to the boundary that
    /// requires it and is deleted with that path rather than living on in the
    /// participant model.
    pub(crate) fn pad_to_frozen_live_backends(
        &mut self,
        frozen_live_backend_ids: impl IntoIterator<Item = usize>,
        lifecycle: filter::RuntimeFilterQueryLifecycleOptions,
    ) -> Result<(), DistributedQueryError> {
        if self.contributions.is_empty() {
            // An empty graph contributes nothing at all, and the attachment
            // accepts that as a whole. Padding it would invent a deployment.
            return Ok(());
        }
        for backend_idx in frozen_live_backend_ids {
            if self.contributions.contains_key(&backend_idx) {
                continue;
            }
            let participant = FrontendRuntimeFilterParticipant::without_local_role(backend_idx)
                .map_err(|error| encoding_error(error.to_string()))?;
            self.contributions
                .insert(backend_idx, encode_participant(lifecycle, &participant));
            self.padded_backends.insert(backend_idx);
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.contributions.is_empty()
    }

    pub(crate) fn feedback_declaration(&self) -> &FrontendRuntimeFilterFeedbackDeclaration {
        &self.feedback_declaration
    }
}

/// Encode the already-validated Frontend deployment into the canonical
/// Protocol contributions. Identity belongs to the enclosing participant
/// manifest, which the coordinator and the backend each derive by descriptor
/// traversal; the contribution carries content only.
pub(crate) fn encode_install_contributions(
    deployment: &FrontendRuntimeFilterDeployment,
    feedback_declaration: FrontendRuntimeFilterFeedbackDeclaration,
) -> Result<EncodedRuntimeFilterDeployment, DistributedQueryError> {
    let mut contributions = BTreeMap::new();
    let lifecycle = deployment.lifecycle().to_wire();
    for participant in deployment.participants() {
        let contribution = encode_participant(lifecycle, participant);
        if contributions
            .insert(participant.backend_idx(), contribution)
            .is_some()
        {
            return Err(encoding_error(format!(
                "runtime filter deployment encoder repeats backend {}",
                participant.backend_idx()
            )));
        }
    }
    Ok(EncodedRuntimeFilterDeployment {
        contributions,
        padded_backends: BTreeSet::new(),
        feedback_declaration,
    })
}

fn encode_participant(
    lifecycle: filter::RuntimeFilterQueryLifecycleOptions,
    participant: &FrontendRuntimeFilterParticipant,
) -> service::RuntimeFilterContribution {
    service::RuntimeFilterContribution {
        participant_id: participant.participant_id(),
        lifecycle: Some(lifecycle),
        install: Some(participant.install().clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EncodedRuntimeFilterDeployment, FrontendRuntimeFilterFeedbackDeclaration,
        encode_participant,
    };

    use std::collections::{BTreeMap, BTreeSet};

    use crate::runtime_filter::model::{
        FrontendRuntimeFilterLifecycle, FrontendRuntimeFilterParticipant,
    };
    use novarocks_proto_models::filter;

    #[test]
    fn a_participant_without_a_local_role_carries_the_typed_empty_install() {
        // Keep this assertion focused on the owner-local contribution shape;
        // constructing a sealed artifact belongs to the schedule-view seam.
        let participant = FrontendRuntimeFilterParticipant::without_local_role(3)
            .expect("a participant with no local role is valid");
        let lifecycle = FrontendRuntimeFilterLifecycle::new(10, 20, 30, 2, 40, 50, 60)
            .expect("lifecycle is valid");

        let contribution = encode_participant(lifecycle.to_wire(), &participant);

        assert_eq!(contribution.participant_id, participant.participant_id());
        assert_eq!(contribution.lifecycle, Some(lifecycle.to_wire()));
        let install = contribution.install.expect("typed install is required");
        assert!(install.core_channels.is_empty());
        assert!(install.routing_channels.is_empty());
    }

    #[test]
    fn padding_addresses_only_backends_the_deployment_left_out() {
        let lifecycle = FrontendRuntimeFilterLifecycle::new(10, 20, 30, 2, 40, 50, 60)
            .expect("lifecycle is valid");
        let install = filter::RuntimeFilterParticipantInstall {
            core_channels: vec![filter::RuntimeFilterChannelDeployment {
                channel_id: 1,
                ..filter::RuntimeFilterChannelDeployment::default()
            }],
            routing_channels: Vec::new(),
        };
        let participant = FrontendRuntimeFilterParticipant::active(1, install)
            .expect("an active participant is valid");
        let mut encoded = EncodedRuntimeFilterDeployment {
            contributions: BTreeMap::from([(
                1,
                encode_participant(lifecycle.to_wire(), &participant),
            )]),
            padded_backends: BTreeSet::new(),
            feedback_declaration: FrontendRuntimeFilterFeedbackDeclaration::default(),
        };

        encoded
            .pad_to_frozen_live_backends([0, 1, 2], lifecycle.to_wire())
            .expect("padding is legal");

        // The wire table covers every live backend, because the retiring
        // attachment demands it. The deployment's own truth does not: a
        // padded backend hosts no task and owns no role.
        assert_eq!(
            encoded
                .contributions()
                .map(|(idx, _)| idx)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            encoded
                .task_hosting_contributions()
                .map(|(idx, _)| idx)
                .collect::<Vec<_>>(),
            vec![1],
            "padding must not be mistaken for a task-hosting participant"
        );
        let padded = encoded
            .contributions()
            .find(|(idx, _)| *idx == 0)
            .expect("backend zero was padded")
            .1;
        let padded_install = padded.install.expect("typed install is required");
        assert!(padded_install.core_channels.is_empty());
        assert!(padded_install.routing_channels.is_empty());
    }

    #[test]
    fn an_empty_deployment_is_never_padded_into_existence() {
        let lifecycle = FrontendRuntimeFilterLifecycle::new(10, 20, 30, 2, 40, 50, 60)
            .expect("lifecycle is valid");
        let mut encoded = EncodedRuntimeFilterDeployment {
            contributions: BTreeMap::new(),
            padded_backends: BTreeSet::new(),
            feedback_declaration: FrontendRuntimeFilterFeedbackDeclaration::default(),
        };

        encoded
            .pad_to_frozen_live_backends([0, 1, 2], lifecycle.to_wire())
            .expect("padding an empty graph is a no-op");

        assert!(
            encoded.is_empty(),
            "a query with no filter graph must contribute nothing at all"
        );
    }
}
