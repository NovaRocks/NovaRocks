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

//! Concrete application dispatch under the carrier's original work credential.
use super::{
    adapter::LogicalHostEffect, failure::LogicalInfrastructureFailure,
    installed::LogicalInstalledState, work::LogicalJobExecutor,
};
use novarocks_coordination_contract::{QueryExecutionError, logical_port::LogicalToAttempt};
use novarocks_logical_execution_protocol::{
    LogicalRequestToken, LogicalSourceId,
    effect::{AttemptLifecycleDiagnostic, LogicalEffectBody, NativeEffect},
    input::ActionResult,
};
use novarocks_query_application::{
    api::logical_runtime::{InstalledLogicalExecution, LogicalFuture, LogicalSourceKind},
    coordination::executor::LogicalExecutors,
};
use std::sync::Arc;

/// P10 binds this endpoint to the SAME actual attempt host. No direct attempt
/// Core access, Scope stop, process-global lookup or transport fallback exists.
pub(crate) trait LogicalHostPorts: Send + Sync {
    /// Commit the original handoff under this still-live source ticket. This
    /// method performs only metadata publication; no Native I/O or rich Drop.
    fn deliver(
        &self,
        runtime: &novarocks_control_runtime::actor_turn::ActorRuntime<
            crate::control_runtime::actor::FrontendActor,
        >,
        ticket: &novarocks_control_runtime::actor_turn::EffectTicket<
            crate::control_runtime::actor::FrontendEffect,
        >,
        original: LogicalToAttempt,
    ) -> Result<(), QueryExecutionError>;
    fn publish_lifecycle(
        &self,
        original: AttemptLifecycleDiagnostic,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>>;
}
/// Committed host replies have no decision policy. They complete the original
/// registry probe and expose progress only after the original Core turn commits.
pub(super) trait LogicalHostReplies: Send + Sync {
    fn readiness_changed(&self, ready: bool);
    fn registry_reply(&self, source: LogicalSourceId, token: LogicalRequestToken, ready: bool);
    fn snapshot_reply(&self, source: LogicalSourceId, token: LogicalRequestToken);
    fn retirable(&self);
}
pub(super) trait LogicalBootstrapPort: Send + Sync {
    fn publish_started(
        &self,
    ) -> LogicalFuture<
        '_,
        Result<(), novarocks_query_application::api::logical_runtime::LogicalSourceClosed>,
    >;
}
pub(super) struct LogicalApplicationExecutor {
    pub(super) original: LogicalExecutors,
    pub(super) ports: Arc<dyn LogicalHostPorts>,
    pub(super) replies: Arc<LogicalInstalledState>,
    pub(super) bootstrap: Arc<dyn LogicalBootstrapPort>,
    pub(super) consumer_attached: tokio::sync::OnceCell<()>,
    pub(super) failure: Arc<LogicalInfrastructureFailure>,
}
impl LogicalApplicationExecutor {
    async fn attach_consumer(&self) -> bool {
        let result = self
            .consumer_attached
            .get_or_try_init(|| async {
                let source = match self.replies.source(LogicalSourceKind::Consumer) {
                    Ok(source) => source,
                    Err(original) => {
                        self.failure.retain(Box::new(original));
                        return Err(());
                    }
                };
                match self.original.consumer.attach_source(source).await {
                    Ok(()) => Ok(()),
                    Err(original) => {
                        self.failure.retain_typed(original);
                        Err(())
                    }
                }
            })
            .await;
        result.is_ok()
    }
}
impl LogicalJobExecutor for LogicalApplicationExecutor {
    fn deliver_port_now(
        &self,
        runtime: &novarocks_control_runtime::actor_turn::ActorRuntime<
            crate::control_runtime::actor::FrontendActor,
        >,
        ticket: &novarocks_control_runtime::actor_turn::EffectTicket<
            crate::control_runtime::actor::FrontendEffect,
        >,
        original: LogicalToAttempt,
    ) -> Option<Result<(), QueryExecutionError>> {
        Some(self.ports.deliver(runtime, ticket, original))
    }
    fn failure_executors(&self) -> Option<Arc<dyn super::failure::LogicalFailureOriginals>> {
        Some(Arc::new(self.original.clone()))
    }
    fn native_executor(
        &self,
    ) -> Option<Arc<novarocks_query_application::coordination::executor::NativeExecutor>> {
        Some(self.original.native.clone())
    }
    fn take_joined_relay_cut(
        &self,
        activation: novarocks_coordination_contract::AttemptActivationIdentity,
    ) -> Option<novarocks_query_application::coordination::StreamedRowsComputedCut> {
        self.original.relay.take_joined_cut(activation)
    }
    fn execute(&self, body: LogicalHostEffect) -> LogicalFuture<'_, Option<ActionResult>> {
        Box::pin(async move {
            let effect = match body {
                LogicalHostEffect::JoinReadiness(ready) => {
                    self.replies.readiness_changed(ready);
                    return None;
                }
                LogicalHostEffect::PortDelivered(_) => return None,
                LogicalHostEffect::Bootstrap => {
                    if !self.attach_consumer().await {
                        return None;
                    }
                    if let Err(original) = self.bootstrap.publish_started().await {
                        self.failure.retain(Box::new(original));
                        return None;
                    }
                    let mut source = match self.replies.source(LogicalSourceKind::Cancellation) {
                        Ok(source) => source,
                        Err(original) => {
                            self.failure.retain(Box::new(original));
                            return None;
                        }
                    };
                    let _ = self.original.cancel.observe(&mut *source).await;
                    return None;
                }
                LogicalHostEffect::ClientCloseObservation => {
                    if !self.attach_consumer().await {
                        return None;
                    }
                    tokio::select! {
                        biased;
                        _ = self.replies.stopped() => {},
                        _ = self.original.consumer.observe_client_closed() => {},
                    }
                    return None;
                }
                LogicalHostEffect::Protocol(effect) => *effect,
            };
            match effect.body {
                LogicalEffectBody::Native(body) => {
                    if let NativeEffect::ActivateAttempt { activation, .. } = &body {
                        let source = match self
                            .replies
                            .source(LogicalSourceKind::AttemptData(*activation))
                        {
                            Ok(source) => source,
                            Err(original) => {
                                self.failure.retain(Box::new(original));
                                return None;
                            }
                        };
                        if let Err(original) =
                            self.original.relay.attach_source(*activation, source)
                        {
                            self.failure.retain_typed(original);
                            return None;
                        }
                    }
                    Some(ActionResult::Native(
                        self.original.native.execute(body).await,
                    ))
                }
                LogicalEffectBody::Relay(body) => {
                    Some(ActionResult::Relay(self.original.relay.execute(body).await))
                }
                LogicalEffectBody::Consumer(body) => Some(ActionResult::Consumer(
                    self.original.consumer.execute(body).await,
                )),
                LogicalEffectBody::Governance(body) => Some(ActionResult::Governance(
                    self.original.governance.execute(body).await,
                )),
                LogicalEffectBody::AttemptPort(_) => {
                    panic!("port publication escaped its original source ticket")
                }
                LogicalEffectBody::AttemptLifecycleDiagnostic(body) => {
                    if let Err(original) = self.ports.publish_lifecycle(*body).await {
                        self.failure.retain_typed(original);
                    }
                    None
                }
                LogicalEffectBody::SnapshotReply { source, token } => {
                    self.replies.snapshot_reply(source, token);
                    None
                }
                LogicalEffectBody::RegistryJoinReply {
                    source,
                    token,
                    ready,
                } => {
                    self.replies.registry_reply(source, token, ready);
                    None
                }
                LogicalEffectBody::Retirable => {
                    self.original.cancel.stop();
                    self.replies.retirable();
                    None
                }
                LogicalEffectBody::SupervisionFailed(original) => {
                    self.failure.retain_typed(original);
                    None
                }
            }
        })
    }
}

impl LogicalBootstrapPort for super::inputs::LogicalBootstrapPublisher {
    fn publish_started(
        &self,
    ) -> LogicalFuture<
        '_,
        Result<(), novarocks_query_application::api::logical_runtime::LogicalSourceClosed>,
    > {
        super::inputs::LogicalBootstrapPublisher::publish_started(self)
    }
}
