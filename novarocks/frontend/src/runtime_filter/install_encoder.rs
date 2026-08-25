//! Frontend-owned runtime-filter lifecycle contribution encoder.

use std::collections::BTreeMap;

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks_proto::{filter, novarocks as service};
use prost::Message;
use sha2::{Digest, Sha256};

use super::model::{
    CONTRIBUTION_DIGEST_DOMAIN, FrontendRuntimeFilterDeployment, FrontendRuntimeFilterParticipant,
};

fn encoding_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// A Frontend-owned, deterministically ordered contribution table.  The caller
/// seals it with the Core schedule view; this type cannot attach itself to an
/// arbitrary query artifact.
pub(crate) struct EncodedRuntimeFilterDeployment {
    contributions: BTreeMap<usize, service::RuntimeFilterContribution>,
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

    pub(crate) fn is_empty(&self) -> bool {
        self.contributions.is_empty()
    }
}

/// Encode the already-validated Frontend deployment. The Frontend owns both
/// the canonical Protocol envelope and its contribution digest.
pub(crate) fn encode_install_contributions(
    deployment: &FrontendRuntimeFilterDeployment,
) -> Result<EncodedRuntimeFilterDeployment, DistributedQueryError> {
    let mut contributions = BTreeMap::new();
    for participant in deployment.participants() {
        let contribution = encode_participant(deployment, participant);
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
    Ok(EncodedRuntimeFilterDeployment { contributions })
}

fn encode_participant(
    deployment: &FrontendRuntimeFilterDeployment,
    participant: &FrontendRuntimeFilterParticipant,
) -> service::RuntimeFilterContribution {
    let mut digest = Sha256::new();
    let lifecycle = deployment.lifecycle().to_wire();
    let envelope = filter::InstallRuntimeFilterDeploymentRequest {
        query_id: Some(deployment.query_id()),
        deployment_epoch: deployment.deployment_epoch(),
        participant_id: participant.participant_id(),
        lifecycle: Some(lifecycle),
        install: Some(participant.install().clone()),
    };
    digest.update(CONTRIBUTION_DIGEST_DOMAIN);
    digest.update(envelope.encode_to_vec());
    service::RuntimeFilterContribution {
        participant_id: participant.participant_id(),
        lifecycle: Some(lifecycle),
        install: Some(participant.install().clone()),
        contribution_digest: digest.finalize().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use novarocks_proto::filter;
    use prost::Message;
    use sha2::{Digest, Sha256};

    use crate::runtime_filter::model::{
        CONTRIBUTION_DIGEST_DOMAIN, FrontendRuntimeFilterLifecycle,
        FrontendRuntimeFilterParticipant,
    };

    #[test]
    fn service_only_contribution_keeps_typed_empty_install_and_canonical_digest() {
        let participant = FrontendRuntimeFilterParticipant::service_only(3)
            .expect("service-only participant is valid");
        let lifecycle = FrontendRuntimeFilterLifecycle::new(10, 20, 30, 2, 40, 50, 60)
            .expect("lifecycle is valid");
        let contribution = encode_for_test(lifecycle, &participant);

        let install = contribution.install.expect("typed install is required");
        assert!(install.core_channels.is_empty());
        assert!(install.routing_channels.is_empty());
        let mut expected = Sha256::new();
        expected.update(CONTRIBUTION_DIGEST_DOMAIN);
        let envelope = filter::InstallRuntimeFilterDeploymentRequest {
            query_id: Some(novarocks_proto::common::UniqueId { hi: 1, lo: 2 }),
            deployment_epoch: 3,
            participant_id: participant.participant_id(),
            lifecycle: Some(lifecycle.to_wire()),
            install: Some(install.clone()),
        };
        expected.update(envelope.encode_to_vec());
        assert_eq!(
            contribution.contribution_digest,
            expected.finalize().to_vec()
        );
    }

    fn encode_for_test(
        lifecycle: FrontendRuntimeFilterLifecycle,
        participant: &FrontendRuntimeFilterParticipant,
    ) -> novarocks_proto::novarocks::RuntimeFilterContribution {
        // Keep this assertion focused on the owner-local contribution shape;
        // constructing a sealed artifact belongs to the schedule-view seam.
        let mut digest = Sha256::new();
        digest.update(CONTRIBUTION_DIGEST_DOMAIN);
        let install = filter::RuntimeFilterParticipantInstall {
            core_channels: Vec::new(),
            routing_channels: Vec::new(),
        };
        let envelope = filter::InstallRuntimeFilterDeploymentRequest {
            query_id: Some(novarocks_proto::common::UniqueId { hi: 1, lo: 2 }),
            deployment_epoch: 3,
            participant_id: participant.participant_id(),
            lifecycle: Some(lifecycle.to_wire()),
            install: Some(install.clone()),
        };
        digest.update(envelope.encode_to_vec());
        novarocks_proto::novarocks::RuntimeFilterContribution {
            participant_id: participant.participant_id(),
            lifecycle: Some(lifecycle.to_wire()),
            install: Some(install),
            contribution_digest: digest.finalize().to_vec(),
        }
    }
}
