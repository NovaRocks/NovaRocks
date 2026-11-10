// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use novarocks_attempt_protocol::effect::{AttemptEffect, EffectBody};
use novarocks_control_runtime::exit_work::HandoffCell;
use novarocks_coordination_contract::{
    AttemptActionId, AttemptAuthorization, AttemptToLogicalBody,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock, Weak},
};

/// Carries the committed passive effect and, for transport, the one cell
/// that arbitrates withdrawal against the actual executor start.
pub(crate) struct AttemptEffectEnvelope {
    pub(crate) protocol: AttemptEffect,
    handoff: Option<Arc<HandoffCell>>,
    routes: OnceLock<Weak<TransportWithdrawalRoutes>>,
}
impl AttemptEffectEnvelope {
    pub(crate) fn new(protocol: AttemptEffect) -> Arc<Self> {
        let handoff = matches!(
            protocol.body,
            EffectBody::SendBatch { .. } | EffectBody::AdmitQueuedInput { .. }
        )
        .then(|| Arc::new(HandoffCell::new()));
        Arc::new(Self {
            protocol,
            handoff,
            routes: OnceLock::new(),
        })
    }

    pub(crate) fn handoff(&self) -> Option<&Arc<HandoffCell>> {
        self.handoff.as_ref()
    }

    pub(crate) fn is_abort_settlement(&self) -> bool {
        matches!(&self.protocol.body, EffectBody::Port(port)
            if matches!(port.body, AttemptToLogicalBody::AbortSettlement { .. }))
    }

    pub(crate) fn is_qa_call(&self) -> bool {
        match &self.protocol.body {
            EffectBody::EstablishAuthorization { .. } => true,
            EffectBody::Port(port) => matches!(
                port.body,
                AttemptToLogicalBody::EstablishSettlement { .. }
                    | AttemptToLogicalBody::DetachEstablish { .. }
                    | AttemptToLogicalBody::PreReadyReplacementQuery { .. }
                    | AttemptToLogicalBody::AttemptTerminal(_)
                    | AttemptToLogicalBody::ConvergenceProgress { .. }
                    | AttemptToLogicalBody::AbortSettlement { .. }
                    | AttemptToLogicalBody::AuthorizationRequest {
                        request: AttemptAuthorization::BeginAdmission(_)
                            | AttemptAuthorization::SettleAdmission { .. }
                            | AttemptAuthorization::ReauthorizeEstablish { .. },
                        ..
                    }
            ),
            _ => false,
        }
    }

    pub(crate) fn uses_join_custody(&self) -> bool {
        self.handoff.is_some()
            || self.is_qa_call()
            || self.is_process_call()
            || self.is_publication_call()
    }

    pub(crate) fn is_process_call(&self) -> bool {
        matches!(self.protocol.body, EffectBody::ObserveProcess { .. })
    }

    pub(crate) fn is_root_control_reply(&self) -> bool {
        matches!(&self.protocol.body, EffectBody::Port(port)
            if crate::task_execution::bridge::root_control::reply_kind(&port.body).is_some())
    }

    pub(crate) fn is_publication_call(&self) -> bool {
        matches!(self.protocol.body, EffectBody::Publish(_)) || self.is_root_control_reply()
    }

    pub(crate) fn retire_handoff(&self) {
        if let Some(routes) = self.routes.get().and_then(Weak::upgrade) {
            let mut originals = routes.originals.lock().unwrap_or_else(|e| e.into_inner());
            if originals
                .get(&self.protocol.id)
                .is_some_and(|original| std::ptr::eq(original.as_ptr(), self))
            {
                originals.remove(&self.protocol.id);
            }
        }
    }
}
impl std::fmt::Debug for AttemptEffectEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.protocol.fmt(f)
    }
}
impl Drop for AttemptEffectEnvelope {
    fn drop(&mut self) {
        self.retire_handoff();
    }
}

/// One entry per live original committed transport effect. Runtime metadata
/// retains it while pending; the original Join/result owner retains it after
/// handoff. The index itself owns no captures, permits or operation payloads.
#[derive(Default)]
pub(crate) struct TransportWithdrawalRoutes {
    originals: Mutex<BTreeMap<AttemptActionId, Weak<AttemptEffectEnvelope>>>,
}
impl TransportWithdrawalRoutes {
    pub(crate) fn index(self: &Arc<Self>, original: &Arc<AttemptEffectEnvelope>) {
        if original.handoff.is_none() {
            return;
        }
        let owner = original.routes.get_or_init(|| Arc::downgrade(self));
        assert!(
            owner.ptr_eq(&Arc::downgrade(self)),
            "same original attempt host"
        );
        let mut originals = self.originals.lock().unwrap_or_else(|e| e.into_inner());
        let weak = Arc::downgrade(original);
        let indexed = originals
            .entry(original.protocol.id)
            .or_insert_with(|| weak.clone());
        assert!(
            indexed.ptr_eq(&weak),
            "one original transport effect per id"
        );
    }

    pub(crate) fn withdraw(&self, id: AttemptActionId) {
        let original = self
            .originals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .and_then(Weak::upgrade);
        if let Some(original) = original {
            original
                .handoff()
                .expect("indexed transport effect")
                .withdraw();
        }
    }
}
