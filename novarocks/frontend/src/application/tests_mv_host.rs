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

use super::{
    FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
    FrontendExecutionConfig,
};
use crate::{state_store::testing as state_store_fixture, topology::ClusterBackendOpenConfig};
use novarocks_native_adapter::FrontendNativeTransport;
use novarocks_native_trust::{
    DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
};
use novarocks_secret::SecretValue;
use novarocks_state_store_runtime::StateStoreHostInput;
use std::time::Duration;
use uuid::Uuid;

fn test_native_trust() -> std::sync::Arc<NativeTrust> {
    std::sync::Arc::new(NativeTrust::new(
        DeploymentId::parse("frontend-mv-integration-test").expect("deployment"),
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
            .expect("secret"),
        NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("subject"),
        NativeTransportMode::Disabled,
    ))
}

fn state_store_input() -> StateStoreHostInput {
    state_store_fixture::persistent_input(format!("frontend-mv-host-{}", Uuid::now_v7()))
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
    input: Option<StateStoreHostInput>,
) -> Result<FrontendApplicationHost, FrontendApplicationError> {
    let registry = state_store_fixture::persistent_registry();
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
async fn configured_state_store_transfers_mv_repository_to_role_product_construction() {
    let config = state_store_input();

    let mut host = open_host(Some(config.clone()))
        .await
        .expect("configured host must open its MV repository");
    let repository = host
        .take_mv_repository_for_role_product_construction()
        .expect("configured host transfers the MV repository once");
    assert!(repository.list_projections().await.is_ok());
    assert!(
        host.take_mv_repository_for_role_product_construction()
            .is_err(),
        "Host must not retain a second MV repository capability after transfer"
    );
    drop(repository);
    host.shutdown().await.expect(
        "shutdown must release the host-owned StateStore after the product repository drops",
    );

    let mut reopened = open_host(Some(config))
        .await
        .expect("same StateStore must reopen its MV repository");
    let repository = reopened
        .take_mv_repository_for_role_product_construction()
        .expect("reopened host transfers the MV repository once");
    assert!(repository.list_projections().await.is_ok());
    drop(repository);
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
