// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information regarding copyright
// ownership.  The ASF licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Frozen distributed-writer planning seam.
//!
//! Placement creates a fragment-instance identity.  Consequently a provider
//! control binding cannot plan one writer handle per logical writer before the
//! frontend has validated the placement manifest.  This module owns that
//! narrow conversion and validates the returned opaque plan.  It deliberately
//! does not name a provider, patch a native sink, or run a transaction; those
//! are the composition and protocol responsibilities layered after this seam.

use std::collections::BTreeSet;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExecutionDeclaration, ConnectorWriteCohortDescriptor, ConnectorWriteCohortId,
    ConnectorWriteLease, ConnectorWriteOperationId, ConnectorWritePlan,
    ConnectorWritePlanningRequest, ConnectorWriterIdentity,
};
use sha2::{Digest, Sha256};

use crate::common::types::UniqueId;
use crate::query_execution::lifecycle::QueryExecutionId;
use crate::query_execution::schedule::SchedulingPlan;
use crate::sql::planner::distributed::FragmentId;

/// One exact, placement-frozen writer manifest.  C1 gives each terminal sink
/// one logical writer, so `sink_ordinal` is zero until a physical fragment can
/// carry more than one connector sink.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteManifest {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: novarocks_spi::connector::ConnectorWriteExecutionId,
    writers: Vec<ConnectorWriterIdentity>,
    digest: [u8; 32],
}

impl ConnectorWriteManifest {
    pub(crate) fn freeze(
        schedule: &SchedulingPlan,
        terminal_write_fragment_ids: &BTreeSet<FragmentId>,
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        owner: ConnectorExecutionBindingKey,
        execution_id: QueryExecutionId,
    ) -> Result<Self, ConnectorError> {
        if terminal_write_fragment_ids.is_empty() {
            return Err(invalid(
                "connector write manifest requires a terminal write fragment",
            ));
        }
        let execution_id = connector_execution_id(execution_id);
        let mut writers = Vec::new();
        for &fragment_id in terminal_write_fragment_ids {
            let placements = schedule.by_fragment.get(&fragment_id).ok_or_else(|| {
                invalid(format!(
                    "connector write manifest terminal fragment {fragment_id} is absent from the validated schedule"
                ))
            })?;
            for placement in placements {
                let fragment_id = i32::try_from(fragment_id)
                    .map_err(|_| invalid("connector write fragment ID exceeds i32 width"))?;
                let backend_num = i32::try_from(placement.instance_index)
                    .map_err(|_| invalid("connector write backend number exceeds i32 width"))?;
                writers.push(ConnectorWriterIdentity::new(
                    operation_id,
                    cohort_id,
                    execution_id,
                    unique_id_bytes(placement.finst_id),
                    fragment_id,
                    backend_num,
                    0,
                    owner.clone(),
                ));
            }
        }
        if writers.is_empty() {
            return Err(invalid("connector write manifest has no scheduled writers"));
        }
        writers.sort();
        let unique = writers.iter().cloned().collect::<BTreeSet<_>>();
        if unique.len() != writers.len() {
            return Err(invalid(
                "connector write manifest contains duplicate writer identities",
            ));
        }
        let digest =
            writer_manifest_digest(&owner, operation_id, cohort_id, execution_id, &writers);
        Ok(Self {
            owner,
            operation_id,
            cohort_id,
            execution_id,
            writers,
            digest,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub const fn execution_id(&self) -> novarocks_spi::connector::ConnectorWriteExecutionId {
        self.execution_id
    }

    pub fn writers(&self) -> &[ConnectorWriterIdentity] {
        &self.writers
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Fill the frozen writer set and obtain a provider-neutral plan while
    /// retaining the exact control-generation lease.  The caller keeps the
    /// returned attachment until its eventual commit/abort/reconcile path.
    pub fn plan(
        &self,
        lease: ConnectorWriteLease,
        mut request: ConnectorWritePlanningRequest,
    ) -> Result<ConnectorWritePlanAttachment, ConnectorError> {
        if lease.binding_key() != &self.owner {
            return Err(invalid(
                "connector write lease does not match the frozen writer manifest generation",
            ));
        }
        if request.table.owner() != &self.owner.instance_id
            || request.operation_id != self.operation_id
            || request.cohort_id != self.cohort_id
            || request.execution_id != self.execution_id
            || !request.expected_writers.is_empty()
        {
            return Err(invalid(
                "connector write planning request does not match the frozen writer manifest",
            ));
        }
        request.expected_writers = self.writers.clone();
        let descriptor = ConnectorWriteCohortDescriptor::new(
            self.cohort_id,
            request.intent,
            request.stable_digest(&self.owner)?,
        );
        // The same bounded/cancellable request context must accompany the
        // terminal control decision.  Reconstructing it later could silently
        // extend a write past its planning deadline or swap cancellation
        // state while retaining an otherwise exact generation lease.
        let context = request.context.clone();
        let execution_declaration = lease.execution_declaration(&context)?;
        let plan = lease.control().plan_write(request)?;
        validate_returned_plan(self, &plan)?;
        Ok(ConnectorWritePlanAttachment {
            manifest: self.clone(),
            plan,
            context,
            execution_declaration,
            descriptor,
            _lease: lease,
        })
    }

    /// Verify that an attachment still belongs to the exact, immutable
    /// placement manifest that produced it.  This is intentionally stricter
    /// than matching a query ID: a retry can retain a query ID but always
    /// receives a distinct attempt and fragment-instance set.
    pub(crate) fn validate_schedule(
        &self,
        schedule: &SchedulingPlan,
        execution_id: QueryExecutionId,
    ) -> Result<(), ConnectorError> {
        if self.execution_id != connector_execution_id(execution_id) {
            return Err(invalid(
                "connector write attachment execution does not match the validated schedule",
            ));
        }
        for writer in &self.writers {
            let fragment_id = u32::try_from(writer.fragment_id()).map_err(|_| {
                invalid("connector write attachment contains a negative fragment ID")
            })?;
            let placements = schedule.by_fragment.get(&fragment_id).ok_or_else(|| {
                invalid("connector write attachment references a fragment absent from the validated schedule")
            })?;
            let expected = placements.iter().any(|placement| {
                i32::try_from(placement.instance_index).ok() == Some(writer.backend_num())
                    && unique_id_bytes(placement.finst_id) == writer.fragment_instance_id()
            });
            if !expected {
                return Err(invalid(
                    "connector write attachment writer does not match a validated fragment placement",
                ));
            }
        }
        Ok(())
    }
}

/// A typed handoff from FE control planning to the later native-sink patcher.
/// The plan's per-writer handles are opaque and the exact control lease is
/// intentionally retained, preventing a current incarnation from taking over
/// an older write operation.
pub struct ConnectorWritePlanAttachment {
    manifest: ConnectorWriteManifest,
    plan: ConnectorWritePlan,
    context: novarocks_spi::connector::ConnectorRequestContext,
    execution_declaration: ConnectorExecutionDeclaration,
    descriptor: ConnectorWriteCohortDescriptor,
    _lease: ConnectorWriteLease,
}

impl ConnectorWritePlanAttachment {
    pub fn manifest(&self) -> &ConnectorWriteManifest {
        &self.manifest
    }

    pub fn plan(&self) -> &ConnectorWritePlan {
        &self.plan
    }

    /// Request bounds and cancellation captured at write planning.  Terminal
    /// control calls must reuse this context with the exact retained lease.
    pub fn context(&self) -> &novarocks_spi::connector::ConnectorRequestContext {
        &self.context
    }

    /// Bounded exact-generation declaration to install on each BE that owns a
    /// writer in this placement-frozen manifest.
    pub fn execution_declaration(&self) -> &ConnectorExecutionDeclaration {
        &self.execution_declaration
    }

    pub fn descriptor(&self) -> &ConnectorWriteCohortDescriptor {
        &self.descriptor
    }

    /// The exact FE control capability retained from placement through the
    /// terminal commit/abort decision. This intentionally exposes no registry
    /// lookup, so a newer incarnation cannot take over the operation.
    pub fn control(&self) -> &std::sync::Arc<dyn novarocks_spi::connector::ConnectorWriteControl> {
        self._lease.control()
    }
}

fn validate_returned_plan(
    manifest: &ConnectorWriteManifest,
    plan: &ConnectorWritePlan,
) -> Result<(), ConnectorError> {
    if plan.owner() != manifest.owner()
        || plan.operation_id() != manifest.operation_id()
        || plan.cohort_id() != manifest.cohort_id()
        || plan.execution_id() != manifest.execution_id()
    {
        return Err(invalid(
            "connector write control returned a plan for a different operation or generation",
        ));
    }
    let expected = manifest.writers.iter().cloned().collect::<BTreeSet<_>>();
    let actual = plan
        .handles()
        .iter()
        .map(|handle| handle.writer().clone())
        .collect::<BTreeSet<_>>();
    if expected != actual || plan.handles().len() != expected.len() {
        return Err(invalid(
            "connector write control returned handles that do not exactly cover the frozen writer manifest",
        ));
    }
    Ok(())
}

fn connector_execution_id(
    execution_id: QueryExecutionId,
) -> novarocks_spi::connector::ConnectorWriteExecutionId {
    let query_id = execution_id.query_id();
    novarocks_spi::connector::ConnectorWriteExecutionId::new(
        unique_id_bytes(UniqueId::new(query_id.high(), query_id.low())),
        execution_id.attempt_id().get(),
    )
}

fn writer_manifest_digest(
    owner: &ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: novarocks_spi::connector::ConnectorWriteExecutionId,
    writers: &[ConnectorWriterIdentity],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-write-manifest.v1\0");
    hasher.update(operation_id.to_bytes());
    hasher.update(cohort_id.to_bytes());
    hasher.update(execution_id.query_id());
    hasher.update(execution_id.attempt_id().to_be_bytes());
    hasher.update((owner.instance_id.as_str().len() as u64).to_be_bytes());
    hasher.update(owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
    hasher.update((writers.len() as u64).to_be_bytes());
    for writer in writers {
        hasher.update(writer.fragment_instance_id());
        hasher.update(writer.fragment_id().to_be_bytes());
        hasher.update(writer.backend_num().to_be_bytes());
        hasher.update(writer.sink_ordinal().to_be_bytes());
    }
    hasher.finalize().into()
}

fn unique_id_bytes(value: UniqueId) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&value.high().to_be_bytes());
    bytes[8..].copy_from_slice(&value.low().to_be_bytes());
    bytes
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::query_execution::schedule::FragmentInstancePlacement;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorInstanceIncarnation};

    fn placement(
        fragment_id: FragmentId,
        instance_index: usize,
        finst_id: UniqueId,
        backend_idx: usize,
    ) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id,
            backend_idx,
            endpoint: RuntimeEndpoint::new("127.0.0.1", 19040 + backend_idx as i32)
                .expect("valid endpoint"),
            scan_ranges: BTreeMap::new(),
            connector_splits: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    fn owner() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg").expect("valid instance"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
        }
    }

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(
            crate::query_execution::contract::QueryId::new(13, 17),
            crate::query_execution::lifecycle::AttemptId::new(5).expect("valid attempt"),
        )
        .expect("valid execution")
    }

    #[test]
    fn manifest_is_deterministic_and_binds_each_placement_to_the_attempt() {
        let schedule = SchedulingPlan {
            root_fragment_id: 9,
            by_fragment: BTreeMap::from([
                (
                    3,
                    vec![
                        placement(3, 0, UniqueId::new(3, 30), 8),
                        placement(3, 1, UniqueId::new(3, 31), 2),
                    ],
                ),
                (9, vec![placement(9, 0, UniqueId::new(9, 90), 8)]),
            ]),
            root_finst_id: UniqueId::new(9, 90),
            root_backend_idx: 8,
        };
        let operation_id = ConnectorWriteOperationId::from_bytes([7; 16]);
        let terminal = BTreeSet::from([3]);
        let manifest = ConnectorWriteManifest::freeze(
            &schedule,
            &terminal,
            operation_id,
            ConnectorWriteCohortId::primary(operation_id),
            owner(),
            execution(),
        )
        .expect("freeze manifest");

        assert_eq!(manifest.writers().len(), 2);
        assert_eq!(manifest.writers()[0].fragment_id(), 3);
        assert_eq!(manifest.writers()[0].backend_num(), 0);
        assert_eq!(manifest.writers()[1].backend_num(), 1);
        assert_eq!(manifest.writers()[0].sink_ordinal(), 0);
        assert_eq!(manifest.writers()[0].operation_id(), operation_id);
        assert_eq!(
            manifest.writers()[0].cohort_id(),
            ConnectorWriteCohortId::primary(operation_id)
        );
        assert_eq!(
            manifest.writers()[0].execution_id(),
            manifest.execution_id()
        );
        assert_ne!(
            manifest.writers()[0].fragment_instance_id(),
            manifest.writers()[1].fragment_instance_id()
        );

        let other_cohort = ConnectorWriteCohortId::derive(operation_id, b"rewrite", [4; 32])
            .expect("cohort identity");
        let other = ConnectorWriteManifest::freeze(
            &schedule,
            &terminal,
            operation_id,
            other_cohort,
            owner(),
            execution(),
        )
        .expect("freeze second cohort manifest");
        assert_ne!(manifest.digest(), other.digest());
    }

    #[test]
    fn manifest_rejects_missing_terminal_fragment_and_empty_writer_set() {
        let schedule = SchedulingPlan {
            root_fragment_id: 9,
            by_fragment: BTreeMap::from([(9, vec![placement(9, 0, UniqueId::new(9, 90), 8)])]),
            root_finst_id: UniqueId::new(9, 90),
            root_backend_idx: 8,
        };
        let operation_id = ConnectorWriteOperationId::from_bytes([7; 16]);
        let error = ConnectorWriteManifest::freeze(
            &schedule,
            &BTreeSet::from([3]),
            operation_id,
            ConnectorWriteCohortId::primary(operation_id),
            owner(),
            execution(),
        )
        .expect_err("missing terminal fragment must fail closed");
        assert!(
            error
                .to_string()
                .contains("absent from the validated schedule")
        );
    }
}
