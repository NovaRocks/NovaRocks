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

//! Safe terminal observations for DML publication attempts.

use sha2::{Digest, Sha256};

use novarocks_spi::connector::LakePublicationTerminal;

use crate::dml::attempt::{
    DmlPublicationFinalization, DmlPublicationPhase, publication_disposition_name,
    publication_next_action_name,
};

pub(crate) fn record_terminal(
    terminal: &LakePublicationTerminal,
    phase: DmlPublicationPhase,
    finalization: DmlPublicationFinalization,
) {
    let header = terminal.header();
    crate::metrics::dml_publication::observe_terminal(
        header.family(),
        phase,
        terminal.disposition(),
        finalization,
    );
    tracing::info!(
        publication_id = %header.publication_id(),
        family = header.family().as_str(),
        target_fingerprint = %safe_target_fingerprint(terminal),
        phase = phase.as_str(),
        disposition = publication_disposition_name(terminal.disposition()),
        next_action = publication_next_action_name(terminal.next_action()),
        finalization = finalization.as_str(),
        "DML publication attempt reached terminal state"
    );
}

/// Produces a stable, non-reversible log correlation token without recording
/// target components, provider evidence, or payloads.
pub(crate) fn safe_target_fingerprint(terminal: &LakePublicationTerminal) -> String {
    let target = terminal.target();
    let mut digest = Sha256::new();
    digest.update(target.catalog().as_bytes());
    digest.update([0]);
    digest.update(target.namespace().as_bytes());
    digest.update([0]);
    digest.update(target.table().unwrap_or_default().as_bytes());
    digest.update([0]);
    digest.update(target.reference().unwrap_or_default().as_bytes());
    let digest = digest.finalize();
    hex::encode(&digest[..12])
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::{
        LakePublicationDisposition, LakePublicationFamily, LakePublicationId,
        LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationTarget,
        LakePublicationTerminal,
    };

    use super::*;

    #[test]
    fn target_fingerprint_is_stable_without_exposing_target_text() {
        let terminal = LakePublicationTerminal::new(
            LakePublicationMarkerHeader::new(
                LakePublicationId::new_v7(),
                LakePublicationFamily::Write,
            ),
            LakePublicationTarget::try_new(
                "catalog-with-secret".to_string(),
                "namespace".to_string(),
                Some("s3://access:credential@example/path".to_string()),
                Some("branch".to_string()),
            )
            .expect("target"),
            LakePublicationDisposition::CommitUnknown,
            LakePublicationNextAction::InspectPublishedState,
            None,
        );

        let fingerprint = safe_target_fingerprint(&terminal);
        assert_eq!(fingerprint.len(), 24);
        assert_eq!(fingerprint, safe_target_fingerprint(&terminal));
        assert!(!fingerprint.contains("credential"));
        assert!(!fingerprint.contains("catalog"));
        assert!(!fingerprint.contains("path"));
    }
}
