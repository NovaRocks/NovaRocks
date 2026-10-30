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

//! Catalog-granted publication stages. Statement stop controls preparation;
//! a possible external effect is observed with its own finite budget.

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorRequestContext};
use std::time::{Duration, Instant};

pub(crate) const EVIDENCE_TIMEOUT: &str = "novarocks.publication.evidence-timeout-ms";
pub(crate) const FINALIZATION_TIMEOUT: &str = "novarocks.publication.finalization-timeout-ms";
pub(crate) const RECLAMATION_TIMEOUT: &str = "novarocks.publication.reclamation-timeout-ms";
pub(crate) const RECLAMATION_FRESHNESS: &str = "novarocks.publication.reclamation-freshness-ms";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CatalogEvidencePolicy {
    pub evidence: Duration,
    pub finalization: Duration,
    pub reclamation: Duration,
    pub freshness: Duration,
}
impl Default for CatalogEvidencePolicy {
    fn default() -> Self {
        Self {
            evidence: Duration::from_secs(300),
            finalization: Duration::from_secs(30),
            reclamation: Duration::from_secs(300),
            freshness: Duration::from_secs(60),
        }
    }
}
impl CatalogEvidencePolicy {
    /// Only persistent catalog properties grant these budgets. Config endpoint
    /// defaults/overrides do not participate in publication authority.
    pub(crate) fn from_properties(properties: &[(String, String)]) -> Result<Self, String> {
        let mut policy = Self::default();
        for (key, value) in properties {
            let slot = match key.as_str() {
                EVIDENCE_TIMEOUT => &mut policy.evidence,
                FINALIZATION_TIMEOUT => &mut policy.finalization,
                RECLAMATION_TIMEOUT => &mut policy.reclamation,
                RECLAMATION_FRESHNESS => &mut policy.freshness,
                _ => continue,
            };
            let millis = value
                .parse::<u32>()
                .ok()
                .filter(|millis| *millis > 0 && *millis <= i32::MAX as u32)
                .ok_or_else(|| {
                    format!("{key} must be an integer in 1..=2147483647 milliseconds")
                })?;
            let duration = Duration::from_millis(u64::from(millis));
            if Instant::now().checked_add(duration).is_none() {
                return Err(format!("{key} exceeds the monotonic clock range"));
            }
            *slot = duration;
        }
        Ok(policy)
    }
    pub(crate) fn begin_evidence(
        &self,
        preparation: &ConnectorRequestContext,
    ) -> Result<EvidenceContext, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        preparation.check_active()?;
        let grant = EvidenceContext::new(self.evidence, EvidencePhase::Evidence)?
            .with_finalization_deadline_cap(preparation.necessary_finalization_deadline_cap());
        if grant.expired() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "Necessary finalization deadline elapsed before nested publication",
            ));
        }
        Ok(grant)
    }
    pub(crate) fn begin_finalization(&self) -> Result<EvidenceContext, ConnectorError> {
        EvidenceContext::new(self.finalization, EvidencePhase::Finalization)
    }
    pub(crate) fn begin_reclamation(&self) -> Result<EvidenceContext, ConnectorError> {
        EvidenceContext::new(self.reclamation, EvidencePhase::Reclamation)
    }
}

/// The purpose of the original catalog phase grant, not a second authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EvidencePhase {
    Evidence,
    Finalization,
    Reclamation,
}

/// No statement cancellation view is retained: this capability only observes
/// the one request already permitted to enter the external-effect interval.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EvidenceContext {
    #[cfg(test)]
    started: Instant,
    deadline: Instant,
    phase: EvidencePhase,
}
impl EvidenceContext {
    fn new(budget: Duration, phase: EvidencePhase) -> Result<Self, ConnectorError> {
        let started = Instant::now();
        let deadline = started.checked_add(budget).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Publication deadline exceeds the monotonic clock range",
            )
        })?;
        Ok(Self {
            #[cfg(test)]
            started,
            deadline,
            phase,
        })
    }
    pub(super) const fn phase(&self) -> EvidencePhase {
        self.phase
    }
    pub(crate) fn with_finalization_deadline_cap(mut self, cap: Option<Instant>) -> Self {
        if let Some(cap) = cap {
            self.deadline = self.deadline.min(cap);
        }
        self
    }
    pub(crate) const fn deadline(&self) -> Instant {
        self.deadline
    }
    #[cfg(test)]
    pub(crate) const fn started(&self) -> Instant {
        self.started
    }
    pub(crate) fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorStopOwner;
    #[test]
    fn grants_are_independent_and_stop_only_prevents_starting_evidence() {
        let policy = CatalogEvidencePolicy::default();
        let stop = ConnectorStopOwner::new();
        let preparation = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            stop.view(),
            64,
            128,
        )
        .unwrap();
        let evidence = policy.begin_evidence(&preparation).unwrap();
        assert!(evidence.deadline() > preparation.deadline());
        stop.request_stop();
        assert!(!evidence.expired());
        assert!(policy.begin_evidence(&preparation).is_err());
        let finalization = policy.begin_finalization().unwrap();
        assert_eq!(
            finalization
                .deadline()
                .duration_since(finalization.started()),
            Duration::from_secs(30)
        );
        let reclamation = policy.begin_reclamation().unwrap();
        assert_eq!(
            reclamation.deadline().duration_since(reclamation.started()),
            Duration::from_secs(300)
        );
    }
    #[test]
    fn finite_positive_catalog_values_are_required() {
        for value in ["0", "-1", "2147483648", "1.5", "unlimited"] {
            assert!(
                CatalogEvidencePolicy::from_properties(&[(EVIDENCE_TIMEOUT.into(), value.into())])
                    .is_err()
            );
        }
        let explicit =
            CatalogEvidencePolicy::from_properties(&[(EVIDENCE_TIMEOUT.into(), "100".into())])
                .unwrap();
        assert_eq!(explicit.evidence, Duration::from_millis(100));
        assert_eq!(explicit.finalization, Duration::from_secs(30));
        assert_eq!(
            CatalogEvidencePolicy::default().evidence,
            Duration::from_secs(300)
        );
    }
    // Functional policy lower-bound input, fixed before confirmation. This
    // uses the native HTTP/mock owners; elapsed time is not a performance claim.
    #[tokio::test]
    async fn supported_metadata_compute_and_slow_chunks_fit_the_default_evidence_budget() {
        use crate::iceberg::spec::FormatVersion;
        use crate::rest_protocol::http::{RequestContext, RestHttpClient, RestRequest};
        use crate::rest_protocol::json::scan_complete;
        use crate::rest_protocol::minimal_view::{TableResponseSink, TableViewRequest};
        use crate::rest_protocol::test_server::{
            BodyPlan, Reply, Script, ScriptedResponse, TestServer,
        };
        let fixture = crate::commit::overwrite::preparer_tests::Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V2);
        let mut json = serde_json::to_value(&metadata).unwrap();
        let snapshots: Vec<_> = (1..=50000_i64)
            .map(|id| {
                serde_json::json!({
                    "snapshot-id":id,"parent-snapshot-id":(id>1).then_some(id-1),
                    "sequence-number":id,"timestamp-ms":1700000000000_i64+id,
                    "manifest-list":format!("s3://bucket/table/metadata/{id}.avro"),
                    "schema-id":metadata.current_schema_id(),"summary":{"operation":"append"}
                })
            })
            .collect();
        json["snapshots"] = serde_json::to_value(snapshots).unwrap();
        json["current-snapshot-id"] = serde_json::json!(50000);
        json["last-sequence-number"] = serde_json::json!(50000);
        json["last-updated-ms"] = serde_json::json!(1700000050000_i64);
        json["refs"] = serde_json::json!({"main":{"snapshot-id":50000,"type":"branch"}});
        let _: crate::iceberg::spec::TableMetadata = serde_json::from_value(json.clone()).unwrap();
        let bytes=serde_json::to_vec(&serde_json::json!({"metadata-location":"s3://bucket/table/metadata/v1.json","metadata":json})).unwrap();
        let policy = CatalogEvidencePolicy::default();
        let client = RestHttpClient::new().unwrap();
        let piece = bytes.len().div_ceil(64);
        let slow = BodyPlan::Chunked(
            bytes
                .chunks(piece)
                .map(|chunk| (Duration::from_millis(5), chunk.to_vec()))
                .collect(),
        );
        for (name, body, delay) in [
            ("large", BodyPlan::Fixed(bytes.clone()), Duration::ZERO),
            (
                "compute",
                BodyPlan::Fixed(bytes.clone()),
                Duration::from_millis(200),
            ),
            ("slow", slow, Duration::ZERO),
        ] {
            let server = TestServer::start(Script::new().on(
                "POST",
                "/commit",
                Reply::Respond(ScriptedResponse {
                    status: 200,
                    headers: vec![],
                    body,
                    delay,
                }),
            ));
            let started = Instant::now();
            let (stage, body) = client
                .send_once(
                    Ok(RestRequest::new(
                        reqwest::Method::POST,
                        format!("{}/commit", server.url()),
                        Default::default(),
                    )),
                    &[200],
                    &RequestContext::with_deadline((started + policy.evidence).into()),
                )
                .await
                .expect("the original transport fixture has no MIO admission source");
            assert!(
                matches!(
                    stage,
                    crate::rest_protocol::evidence::DispatchStage::Status { code: 200, .. }
                ),
                "{name}"
            );
            let body = body.unwrap().read_to_end().await.unwrap();
            scan_complete(
                TableResponseSink::new(
                    TableViewRequest::new(Some(50000), Some("main".into()), vec![]).unwrap(),
                ),
                &body,
            )
            .unwrap();
            assert!(
                started.elapsed() < policy.evidence / 10,
                "supported {name} response lacks the required budget margin"
            );
            assert_eq!(server.count("POST", "/commit"), 1);
        }
        let stalled = TestServer::start(Script::new().on("POST", "/commit", Reply::NeverAnswer));
        let started = Instant::now();
        let (stage, body) = client
            .send_once(
                Ok(RestRequest::new(
                    reqwest::Method::POST,
                    format!("{}/commit", stalled.url()),
                    Default::default(),
                )),
                &[200],
                &RequestContext::with_deadline((started + Duration::from_millis(100)).into()),
            )
            .await
            .expect("the original transport fixture has no MIO admission source");
        assert!(matches!(
            stage,
            crate::rest_protocol::evidence::DispatchStage::WrittenNoStatus(_)
        ));
        assert!(body.is_none());
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(stalled.count("POST", "/commit"), 1);
    }
}

/// Bound once by the provider materializer before a generation is exposed.
/// A caller's epoch is never authority to relabel an existing client/session.
#[derive(Debug, Default)]
pub(crate) struct PublicationGeneration(
    std::sync::OnceLock<novarocks_spi::connector::ProviderBindingEpoch>,
);
impl PublicationGeneration {
    pub(crate) fn bind(
        &self,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        let bound = self.0.get_or_init(|| generation);
        self.validate(*bound, generation)
    }
    fn validate(
        &self,
        bound: novarocks_spi::connector::ProviderBindingEpoch,
        actual: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        if bound != actual {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "Publication client belongs to another provider generation",
            ));
        }
        Ok(())
    }
    pub(crate) fn require(
        &self,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        let bound = self.0.get().ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "Publication client has no materialized provider generation",
            )
        })?;
        self.validate(*bound, generation)
    }
}
