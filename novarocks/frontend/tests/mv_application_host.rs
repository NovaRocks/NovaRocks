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

use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendApplicationErrorKind, FrontendApplicationHost,
    FrontendExecutionConfig, FrontendNativeTransport,
};
use novarocks_native_trust::{
    DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
};
use novarocks_secret::SecretValue;
mod common;
use common::state_store_fixture;
use std::time::Duration;
use tempfile::TempDir;

fn test_native_trust() -> std::sync::Arc<NativeTrust> {
    std::sync::Arc::new(NativeTrust::new(
        DeploymentId::parse("frontend-mv-integration-test").expect("deployment"),
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
            .expect("secret"),
        NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("subject"),
        NativeTransportMode::Disabled,
    ))
}

fn state_store_input(temp: &TempDir) -> novarocks_frontend::StateStoreHostInput {
    state_store_fixture::input(format!("frontend-mv-host-{}", temp.path().display()))
}

fn execution_config() -> FrontendExecutionConfig {
    FrontendExecutionConfig::new_for_test(
        "127.0.0.1",
        19090,
        std::num::NonZeroUsize::new(1).unwrap(),
        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        std::sync::Arc::new(
            novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin function catalog"),
        ),
    )
}

async fn open_host(
    input: Option<novarocks_frontend::StateStoreHostInput>,
) -> Result<FrontendApplicationHost, novarocks_frontend::FrontendApplicationError> {
    let registry = state_store_fixture::registry();
    FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
        input,
        &registry,
        execution_config(),
        backend_config(),
        Vec::new(),
        tokio::runtime::Handle::current(),
        test_native_trust(),
        FrontendNativeTransport::plaintext(),
    )
    .await
}

fn backend_config() -> ClusterBackendOpenConfig {
    ClusterBackendOpenConfig::new(
        novarocks_types::ClusterRole::Fe,
        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        Duration::from_secs(1),
        1,
        Duration::from_secs(1),
    )
    .expect("valid frontend backend config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_sqlite_opens_and_reopens_mv_repository() {
    let temp = TempDir::new().expect("temporary SQLite deployment");
    let config = state_store_input(&temp);

    let mut host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open its MV repository");
    assert!(host.mv_repository().list_projections().await.is_ok());
    let repository = host.mv_repository();
    drop(repository);
    host.shutdown()
        .await
        .expect("shutdown must release MV repository first");

    let mut reopened = open_host(Some(config))
        .await
        .expect("same SQLite store must reopen its MV repository");
    assert!(reopened.mv_repository().list_projections().await.is_ok());
    reopened.shutdown().await.expect("reopened host shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_state_store_rejects_mv_services_open() {
    let error = match open_host(None).await {
        Ok(mut host) => {
            host.shutdown().await.expect("shutdown unexpected host");
            panic!("role=fe requires durable StateStore before MV services open");
        }
        Err(error) => error,
    };
    assert_eq!(error.kind(), FrontendApplicationErrorKind::MvServiceOpen);
}
