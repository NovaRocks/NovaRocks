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

use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::exec::fragment::program::{FragmentProgram, FragmentSinkKind};
use crate::runtime::fragment::error::{
    FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::instance::FragmentInstanceSpec;
use crate::runtime::fragment::io::{
    FragmentResultSession, FragmentResultWriter, ResultAbort, ResultWriteSpec,
};
use crate::runtime::{exchange, sink_commit};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ResourceKind {
    SinkCommit,
    Result,
    Exchange,
}

impl ResourceKind {
    fn cleanup_failure_detail(self) -> &'static str {
        match self {
            Self::SinkCommit => "injected sink commit cleanup failure",
            Self::Result => "injected result registration cleanup failure",
            Self::Exchange => "injected exchange registration cleanup failure",
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct ResourceCleanupFaults {
    failed: std::collections::BTreeSet<ResourceKind>,
}

impl ResourceCleanupFaults {
    #[cfg(test)]
    pub(crate) fn with_failure(mut self, resource: ResourceKind) -> Self {
        self.failed.insert(resource);
        self
    }

    fn should_fail(&self, resource: ResourceKind) -> bool {
        self.failed.contains(&resource)
    }
}

pub(crate) struct SinkCommitLease {
    finst_id: UniqueId,
    active: bool,
    cleanup_should_fail: bool,
}

impl SinkCommitLease {
    pub(crate) fn acquire(
        finst_id: UniqueId,
        cleanup_should_fail: bool,
    ) -> Result<Self, FragmentLaunchError> {
        if !sink_commit::try_register(finst_id) {
            return Err(registration_error(format!(
                "sink commit already registered for fragment instance {finst_id}"
            )));
        }
        Ok(Self {
            finst_id,
            active: true,
            cleanup_should_fail,
        })
    }

    fn rollback(&mut self) -> Result<(), String> {
        if self.active {
            sink_commit::unregister(self.finst_id);
            self.active = false;
        }
        if self.cleanup_should_fail {
            self.cleanup_should_fail = false;
            return Err(ResourceKind::SinkCommit
                .cleanup_failure_detail()
                .to_string());
        }
        Ok(())
    }

    fn handoff(&mut self) {
        self.active = false;
        self.cleanup_should_fail = false;
    }
}

impl Drop for SinkCommitLease {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

pub(crate) struct ResultRegistration {
    session: Arc<dyn FragmentResultSession>,
    active: bool,
    cleanup_should_fail: bool,
}

impl ResultRegistration {
    pub(crate) fn acquire(
        writer: &Arc<dyn FragmentResultWriter>,
        spec: ResultWriteSpec,
        cleanup_should_fail: bool,
    ) -> Result<Self, FragmentLaunchError> {
        let session = writer
            .open(spec)
            .map_err(|error| registration_error(error.to_string()))?;
        Ok(Self {
            session,
            active: true,
            cleanup_should_fail,
        })
    }

    fn rollback(&mut self) -> Result<(), String> {
        if self.active {
            self.session.abort(ResultAbort::PrepareRollback);
            self.active = false;
        }
        if self.cleanup_should_fail {
            self.cleanup_should_fail = false;
            return Err(ResourceKind::Result.cleanup_failure_detail().to_string());
        }
        Ok(())
    }

    fn finish_success(&mut self) {
        self.active = false;
        self.cleanup_should_fail = false;
    }

    fn finish_failure(&mut self, error: String) {
        if self.active {
            self.session.abort(ResultAbort::Failed(error));
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }

    fn finish_cancelled(&mut self, reason: String) {
        if self.active {
            self.session.abort(ResultAbort::Cancelled(reason));
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }
}

impl Drop for ResultRegistration {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

pub(crate) struct ExchangeRegistration {
    keys: Vec<exchange::ExchangeKey>,
    active: bool,
    cleanup_should_fail: bool,
}

impl ExchangeRegistration {
    pub(crate) fn acquire(
        program: &FragmentProgram,
        instance: &FragmentInstanceSpec,
        cleanup_should_fail: bool,
    ) -> Result<Option<Self>, FragmentLaunchError> {
        if program.exchange_inputs().is_empty() {
            return Ok(None);
        }
        let finst_id = instance.fragment_instance_id().get();
        let mut registration = Self {
            keys: Vec::with_capacity(program.exchange_inputs().len()),
            active: true,
            cleanup_should_fail,
        };
        for (node_id, contract) in program.exchange_inputs() {
            let assignment = instance
                .exchange_inputs()
                .get(node_id)
                .expect("validated submission has every exchange assignment");
            let key = exchange::ExchangeKey {
                finst_id_hi: finst_id.high(),
                finst_id_lo: finst_id.low(),
                node_id: node_id.get(),
            };
            if let Err(error) = exchange::try_register_expected_chunk_schema(
                key,
                assignment.sender_count().get(),
                Arc::clone(contract.expected_schema()),
            ) {
                let diagnostics = registration.rollback().err().into_iter().collect();
                return Err(registration_error(error).with_cleanup_diagnostics(diagnostics));
            }
            registration.keys.push(key);
        }
        Ok(Some(registration))
    }

    fn rollback(&mut self) -> Result<(), String> {
        if self.active {
            for key in self.keys.drain(..).rev() {
                exchange::cancel_exchange_key(key);
            }
            self.active = false;
        }
        if self.cleanup_should_fail {
            self.cleanup_should_fail = false;
            return Err(ResourceKind::Exchange.cleanup_failure_detail().to_string());
        }
        Ok(())
    }

    fn finish_success(&mut self) {
        if self.active {
            for key in self.keys.drain(..).rev() {
                exchange::remove_exchange_key(key);
            }
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }

    fn finish_cancelled(&mut self) {
        if self.active {
            for key in self.keys.drain(..).rev() {
                exchange::cancel_exchange_key(key);
            }
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }
}

impl Drop for ExchangeRegistration {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

pub(crate) struct FragmentResources {
    sink_commit: Option<SinkCommitLease>,
    result: Option<ResultRegistration>,
    exchange: Option<ExchangeRegistration>,
    cleanup_faults: ResourceCleanupFaults,
}

impl FragmentResources {
    pub(crate) fn new(cleanup_faults: ResourceCleanupFaults) -> Self {
        Self {
            sink_commit: None,
            result: None,
            exchange: None,
            cleanup_faults,
        }
    }

    pub(crate) fn acquire_sink_commit(
        &mut self,
        finst_id: UniqueId,
    ) -> Result<(), FragmentLaunchError> {
        self.sink_commit = Some(SinkCommitLease::acquire(
            finst_id,
            self.cleanup_faults.should_fail(ResourceKind::SinkCommit),
        )?);
        Ok(())
    }

    pub(crate) fn acquire_result(
        &mut self,
        program: &FragmentProgram,
        writer: &Arc<dyn FragmentResultWriter>,
        spec: ResultWriteSpec,
    ) -> Result<(), FragmentLaunchError> {
        if program.sink().kind() != FragmentSinkKind::Result {
            return Ok(());
        }
        self.result = Some(ResultRegistration::acquire(
            writer,
            spec,
            self.cleanup_faults.should_fail(ResourceKind::Result),
        )?);
        Ok(())
    }

    pub(crate) fn result_session(&self) -> Option<Arc<dyn FragmentResultSession>> {
        self.result
            .as_ref()
            .map(|registration| Arc::clone(&registration.session))
    }

    pub(crate) fn acquire_exchange(
        &mut self,
        program: &FragmentProgram,
        instance: &FragmentInstanceSpec,
    ) -> Result<(), FragmentLaunchError> {
        self.exchange = ExchangeRegistration::acquire(
            program,
            instance,
            self.cleanup_faults.should_fail(ResourceKind::Exchange),
        )?;
        Ok(())
    }

    pub(crate) fn rollback(&mut self) -> Vec<String> {
        let mut diagnostics = Vec::new();
        if let Some(mut exchange) = self.exchange.take() {
            if let Err(error) = exchange.rollback() {
                diagnostics.push(error);
            }
        }
        if let Some(mut result) = self.result.take() {
            if let Err(error) = result.rollback() {
                diagnostics.push(error);
            }
        }
        if let Some(mut sink_commit) = self.sink_commit.take() {
            if let Err(error) = sink_commit.rollback() {
                diagnostics.push(error);
            }
        }
        diagnostics
    }

    pub(crate) fn finish_success(&mut self) {
        if let Some(mut exchange) = self.exchange.take() {
            exchange.finish_success();
        }
        if let Some(mut result) = self.result.take() {
            result.finish_success();
        }
    }

    pub(crate) fn handoff_sink_commit(&mut self) {
        if let Some(mut sink_commit) = self.sink_commit.take() {
            sink_commit.handoff();
        }
    }

    pub(crate) fn finish_failure(&mut self, error: String) {
        if let Some(mut exchange) = self.exchange.take() {
            exchange.finish_cancelled();
        }
        if let Some(mut result) = self.result.take() {
            result.finish_failure(error);
        }
    }

    pub(crate) fn finish_cancelled(&mut self, reason: String) {
        if let Some(mut exchange) = self.exchange.take() {
            exchange.finish_cancelled();
        }
        if let Some(mut result) = self.result.take() {
            result.finish_cancelled(reason);
        }
    }
}

impl Drop for FragmentResources {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

fn registration_error(detail: impl Into<String>) -> FragmentLaunchError {
    FragmentLaunchError::new(
        FragmentLaunchStage::Register,
        FragmentLaunchErrorKind::DuplicateRegistration,
        detail,
    )
}
