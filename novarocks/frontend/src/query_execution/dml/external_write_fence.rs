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

//! The Core half of the frontend-owned external write fence (CP-3B spec D2).
//!
//! A control-plane owner cannot withdraw a Connector commit it has already
//! dispatched, so the external operation fence must be established before any
//! writer or commit dispatch that can produce an irreversible external effect.
//!
//! Neither side of the boundary can build that fence alone. The frontend owns
//! cluster identity, the CP-1 generation scalars and the coordination attempt;
//! only the exact connector write authority a prepared statement retained can
//! name the write operation and the fenced resource. This module is where the
//! two halves meet: the frontend hands down an incomplete proposal, and each
//! reverse-port engine completes it with the identity of the authority that will
//! later commit, abort or reconcile.
//!
//! The fence is recorded on the [`ConnectorWriteLease`] rather than on an
//! operation session, so every terminal provider call of one coordination
//! attempt is covered by it -- including the pre-registration aborts that hold
//! only the lease and never obtain a session.

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorEstablishedWriteFence,
    ConnectorExternalFenceFailure, ConnectorExternalOperationFence, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorWriteLease, ConnectorWriteOperationId,
    ConnectorWriteTargetRef,
};

/// The control-plane half of one external operation fence.
///
/// The frontend implements this over its own coordination attempt. Core never
/// mints a fence: it only asks the proposal to complete itself with the
/// connector-owned identity, which keeps the frontend from inventing a write
/// operation or a fenced resource and keeps Core from inventing a generation.
pub trait ExternalWriteFenceProposal {
    fn seal(
        &self,
        operation_id: ConnectorWriteOperationId,
        table: ConnectorTableIdentity,
        target_ref: ConnectorWriteTargetRef,
    ) -> Result<ConnectorExternalOperationFence, ConnectorError>;
}

/// Any sealing function is a proposal.
///
/// This exists so the frontend does not have to place a trait implementation in
/// an arbitrary route module: each route passes the one closure that forwards to
/// its own coordination attempt, and the port signature stays a named type.
impl<F> ExternalWriteFenceProposal for F
where
    F: Fn(
        ConnectorWriteOperationId,
        ConnectorTableIdentity,
        ConnectorWriteTargetRef,
    ) -> Result<ConnectorExternalOperationFence, ConnectorError>,
{
    fn seal(
        &self,
        operation_id: ConnectorWriteOperationId,
        table: ConnectorTableIdentity,
        target_ref: ConnectorWriteTargetRef,
    ) -> Result<ConnectorExternalOperationFence, ConnectorError> {
        self(operation_id, table, target_ref)
    }
}

/// The exact write authority one prepared statement will use for every terminal
/// provider call, together with the connector-owned identity its fence binds.
///
/// Holding the lease here is the whole point: a route that derived a *second*
/// lease for staging would fence an authority nobody commits through, and the
/// commit would then fail closed on a missing fence. Every engine therefore
/// resolves this from the same lease its staging and terminal calls use.
#[derive(Clone)]
pub struct ExternalWriteFenceAuthority {
    lease: ConnectorWriteLease,
    operation_id: ConnectorWriteOperationId,
    table: ConnectorTableIdentity,
    target_ref: ConnectorWriteTargetRef,
    context: ConnectorRequestContext,
}

impl ExternalWriteFenceAuthority {
    /// Bind one exact write lease to the resource identity its fence must name.
    ///
    /// The connector instance comes from the lease's own binding generation, not
    /// from a catalog name: the fence marker must belong to the provider
    /// incarnation that will compare it at the external linearization point.
    pub(crate) fn try_new(
        lease: ConnectorWriteLease,
        operation_id: ConnectorWriteOperationId,
        namespace: &str,
        table: &str,
        target_ref: ConnectorWriteTargetRef,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        // The target ref is already validated by construction: it only ever
        // arrives here from a provider-signed preparation. Re-validating would
        // reach into an SPI-internal method for no added guarantee.
        let table = ConnectorTableIdentity {
            instance_id: lease.binding_key().instance_id.clone(),
            namespace: Arc::from(namespace),
            table: Arc::from(table),
        };
        Ok(Self {
            lease,
            operation_id,
            table,
            target_ref,
            context,
        })
    }

    pub(crate) fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    /// Establish this coordination attempt's fence on the exact write authority.
    ///
    /// Replaying the identical fence is idempotent and a generation behind the
    /// established one is refused as a typed stale conflict; both rules live on
    /// the lease. A refusal keeps its `ConnectorExternalFenceFailure`
    /// classification all the way back to the frontend, which is what stops a
    /// superseded owner from being mistaken for an unknown external effect.
    pub fn establish(
        &self,
        proposal: &dyn ExternalWriteFenceProposal,
    ) -> Result<ConnectorEstablishedWriteFence, ConnectorError> {
        let fence = proposal.seal(
            self.operation_id,
            self.table.clone(),
            self.target_ref.clone(),
        )?;
        // Refuse a fence sealed for another write operation before the provider
        // can publish a marker for it.
        fence.validate_for_operation(self.operation_id)?;
        self.lease
            .establish_external_fence(fence, self.context.clone())
    }
}

/// The fail-closed answer of a route whose write authority cannot be resolved.
///
/// `NotEstablished` is deliberate: nothing was dispatched and nothing is known
/// about external truth, so the operation stays recoverable for historical write
/// recovery instead of being reported as a stale authority.
pub(crate) fn external_fence_authority_unavailable(reason: impl Into<String>) -> ConnectorError {
    ConnectorError::external_fence(ConnectorExternalFenceFailure::NotEstablished, reason)
}

/// Map a reverse-port handle rejection onto a connector error.
///
/// A foreign or already-consumed handle is a caller contract violation, not a
/// fence conflict, so it must not be classified as a stale fence.
pub(crate) fn invalid_fence_request(reason: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, reason)
}
