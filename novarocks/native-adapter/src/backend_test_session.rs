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

//! Exact BE role-launch test composition and joined provider cleanup.
use novarocks_failpoint::liveness::{PreparedRoleSession, RoleSessionOwner};
use novarocks_failpoint::scope::{ProcessBinding, SessionScope};
use novarocks_types::BackendProcessId;
use std::sync::Arc;

pub type ProviderCloseHook = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;
pub type ProviderBindHook =
    Arc<dyn Fn(&SessionScope) -> Result<Option<ProviderCloseHook>, String> + Send + Sync>;

#[derive(Clone)]
pub struct BackendTestSessionConfig {
    prepared: PreparedRoleSession,
    provider_bind: Option<ProviderBindHook>,
}
impl BackendTestSessionConfig {
    pub fn new(prepared: PreparedRoleSession) -> Self {
        Self {
            prepared,
            provider_bind: None,
        }
    }
    pub fn with_provider_bind(mut self, bind: ProviderBindHook) -> Self {
        self.provider_bind = Some(bind);
        self
    }
    #[expect(
        clippy::disallowed_methods,
        reason = "The authorized development session owns its liveness carrier; see ADR-0218."
    )]
    pub(crate) fn bind(&self, process: BackendProcessId) -> Result<BackendTestSession, String> {
        let role = self
            .prepared
            .bind(ProcessBinding::Backend(process))
            .map_err(|error| error.to_string())?;
        let scope = role.scope();
        let allowed = [
            "native",
            "native-observation",
            "iceberg-observation",
            "storage-authority",
        ];
        if scope.domain_names().any(|name| !allowed.contains(&name)) {
            return Err("unsupported BE test-session domain".into());
        }
        let native = Arc::new(crate::test_controls::NativeTestControls::from_grants(
            scope.grants_for("native"),
        )?);
        let observation_grants = scope.grants_for("native-observation");
        let observation = if observation_grants.is_empty() {
            None
        } else {
            Some(Arc::new(
                crate::test_observation_controls::NativeObservationControls::from_grants(
                    observation_grants,
                )?,
            ))
        };
        let close = match &self.provider_bind {
            Some(bind) => bind(scope)?,
            None => {
                if !scope.grants_for("iceberg-observation").is_empty()
                    || !scope.grants_for("storage-authority").is_empty()
                {
                    return Err("provider test capabilities require the Server binding".into());
                }
                None
            }
        };
        let cleanup = match close {
            Some(close) => {
                let observed = scope.clone();
                let rollback = close.clone();
                match std::thread::Builder::new()
                    .name("backend-test-provider-cleanup".into())
                    .spawn(move || {
                        observed.wait_closed();
                        close()
                    }) {
                    Ok(worker) => Some(worker),
                    Err(error) => {
                        role.close();
                        let primary = error.to_string();
                        return Err(match rollback() {
                            Ok(()) => primary,
                            Err(cleanup) => {
                                format!("{primary}; provider cleanup rollback: {cleanup}")
                            }
                        });
                    }
                }
            }
            None => None,
        };
        Ok(BackendTestSession {
            role,
            native,
            observation,
            cleanup,
            failures: Vec::new(),
        })
    }
}
pub(crate) struct BackendTestSession {
    role: RoleSessionOwner,
    pub(crate) native: Arc<crate::test_controls::NativeTestControls>,
    pub(crate) observation:
        Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
    cleanup: Option<std::thread::JoinHandle<Result<(), String>>>,
    failures: Vec<String>,
}
impl BackendTestSession {
    pub(crate) fn close_and_join(&mut self) -> Result<(), String> {
        if let Err(error) = self.role.close_and_join() {
            self.failures.push(error.to_string());
        }
        if let Some(worker) = self.cleanup.take() {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => self.failures.push(error),
                Err(_) => self
                    .failures
                    .push("backend test provider cleanup panicked".into()),
            }
        }
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(self.failures.join("; "))
        }
    }
}
impl Drop for BackendTestSession {
    fn drop(&mut self) {
        if let Err(error) = self.close_and_join() {
            tracing::error!(%error, "backend test-session cleanup failed");
        }
    }
}
