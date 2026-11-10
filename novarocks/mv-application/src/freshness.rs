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

//! Pure publication freshness. Observation does not grant scheduling legality.

use crate::persistence::codec::{PublicationInput, RelationOccurrence};
use crate::persistence::identity::{NativeDataVersion, ObjectIdentity};
use novarocks_failure_contract::SharedFailureCause;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub enum MvSourceState {
    Present {
        object_id: ObjectIdentity,
        native_data_version: NativeDataVersion,
    },
    Missing {
        cause: SharedFailureCause,
    },
}
#[derive(Clone, Debug)]
pub struct MvSourceObservation {
    pub relation_occurrence_id: u32,
    pub state: MvSourceState,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvPublicationFreshness {
    Fresh,
    Stale,
}
impl MvPublicationFreshness {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "FRESH",
            Self::Stale => "STALE",
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvObservationCoverageError;
impl std::fmt::Display for MvObservationCoverageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MV source observations must cover every definition occurrence exactly once")
    }
}
impl std::error::Error for MvObservationCoverageError {}

pub fn exact_inputs_match(published: &[PublicationInput], current: &[PublicationInput]) -> bool {
    let by_occurrence = current
        .iter()
        .map(|input| (input.relation_occurrence_id, input))
        .collect::<BTreeMap<_, _>>();
    let published_ids = published
        .iter()
        .map(|input| input.relation_occurrence_id)
        .collect::<BTreeSet<_>>();
    published.len() == current.len()
        && by_occurrence.len() == current.len()
        && published_ids.len() == published.len()
        && published
            .iter()
            .all(|input| by_occurrence.get(&input.relation_occurrence_id).copied() == Some(input))
}

pub fn mv_publication_freshness(
    publication: Option<&[PublicationInput]>,
    occurrences: &[RelationOccurrence],
    observed: &[MvSourceObservation],
) -> Result<MvPublicationFreshness, MvObservationCoverageError> {
    let expected = occurrences
        .iter()
        .map(|item| item.occurrence_id)
        .collect::<BTreeSet<_>>();
    let seen = observed
        .iter()
        .map(|item| item.relation_occurrence_id)
        .collect::<BTreeSet<_>>();
    if expected.len() != occurrences.len() || seen.len() != observed.len() || seen != expected {
        return Err(MvObservationCoverageError);
    }
    let Some(publication) = publication else {
        return Ok(MvPublicationFreshness::Stale);
    };
    let mut current = Vec::with_capacity(observed.len());
    for observation in observed {
        let MvSourceState::Present {
            object_id,
            native_data_version,
        } = &observation.state
        else {
            return Ok(MvPublicationFreshness::Stale);
        };
        current.push(PublicationInput {
            relation_occurrence_id: observation.relation_occurrence_id,
            object_id: object_id.clone(),
            native_data_version: native_data_version.clone(),
        });
    }
    Ok(if exact_inputs_match(publication, &current) {
        MvPublicationFreshness::Fresh
    } else {
        MvPublicationFreshness::Stale
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::identity::SchemaVersion;
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    fn occurrence(id: u32) -> RelationOccurrence {
        RelationOccurrence {
            occurrence_id: id,
            catalog_at_binding: "ice".into(),
            namespace_at_binding: "db".into(),
            relation_at_binding: "t".into(),
            qualifier_at_binding: "t".into(),
            object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
            schema_version: SchemaVersion::try_new(vec![1]).unwrap(),
            fields: vec![],
        }
    }
    fn input(object: u8, version: u8) -> PublicationInput {
        PublicationInput {
            relation_occurrence_id: 1,
            object_id: ObjectIdentity::try_new(vec![object]).unwrap(),
            native_data_version: NativeDataVersion::try_new(vec![version]).unwrap(),
        }
    }
    fn present(input: PublicationInput) -> MvSourceObservation {
        MvSourceObservation {
            relation_occurrence_id: input.relation_occurrence_id,
            state: MvSourceState::Present {
                object_id: input.object_id,
                native_data_version: input.native_data_version,
            },
        }
    }
    #[test]
    fn freshness_compares_exact_publication_facts_and_requires_complete_coverage() {
        let occurrences = [occurrence(1)];
        let published = [input(1, 1)];
        assert_eq!(
            mv_publication_freshness(None, &occurrences, &[present(input(1, 1))]).unwrap(),
            MvPublicationFreshness::Stale
        );
        assert_eq!(
            mv_publication_freshness(Some(&published), &occurrences, &[present(input(1, 1))])
                .unwrap(),
            MvPublicationFreshness::Fresh
        );
        for changed in [input(1, 2), input(2, 1)] {
            assert_eq!(
                mv_publication_freshness(Some(&published), &occurrences, &[present(changed)])
                    .unwrap(),
                MvPublicationFreshness::Stale
            );
        }
        let missing = MvSourceObservation {
            relation_occurrence_id: 1,
            state: MvSourceState::Missing {
                cause: SharedFailureCause::new(ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "original missing",
                )),
            },
        };
        assert_eq!(
            mv_publication_freshness(Some(&published), &occurrences, &[missing]).unwrap(),
            MvPublicationFreshness::Stale
        );
        assert!(mv_publication_freshness(None, &occurrences, &[]).is_err());
        assert!(
            mv_publication_freshness(
                Some(&published),
                &occurrences,
                &[present(input(1, 1)), present(input(1, 1))]
            )
            .is_err()
        );
        let mut unknown = present(input(1, 1));
        unknown.relation_occurrence_id = 2;
        assert!(mv_publication_freshness(Some(&published), &occurrences, &[unknown]).is_err());
    }
}
