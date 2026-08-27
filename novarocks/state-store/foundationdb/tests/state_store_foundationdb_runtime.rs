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

#![cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use novarocks_spi::state_store::StateStoreErrorKind;
use novarocks_state_store_foundationdb::{
    FoundationDbClientConfig, FoundationDbProviderTestHarness, FoundationDbTestLimitOverrides,
    FoundationDbTestProviderConfig, FoundationDbTestStoreConfig,
};
use tempfile::TempDir;
use uuid::Uuid;

fn client_config() -> FoundationDbClientConfig {
    FoundationDbClientConfig {
        disable_multi_version_client: true,
        tls_cert_path: None,
        tls_key_path: None,
        tls_ca_path: None,
        tls_verify_peers: None,
        tls_password: None,
    }
}

fn fdb_store_config() -> FoundationDbTestStoreConfig {
    FoundationDbTestStoreConfig {
        cluster_id: "runtime-cluster".to_owned(),
        limits: FoundationDbTestLimitOverrides::default(),
        provider: FoundationDbTestProviderConfig::Foundationdb {
            cluster_file: PathBuf::from(
                std::env::var("NOVAROCKS_FDB_CLUSTER_FILE")
                    .expect("FoundationDB fixture cluster file"),
            ),
            keyspace_id: Uuid::parse_str(
                &std::env::var("NOVAROCKS_FDB_KEYSPACE_ID")
                    .expect("FoundationDB fixture keyspace id"),
            )
            .expect("valid FoundationDB fixture keyspace id"),
        },
    }
}

#[tokio::test]
async fn foundationdb_runtime_lifecycle() {
    let mut harness = FoundationDbProviderTestHarness::boot(client_config())
        .expect("boot process-owned FoundationDB runtime");

    let duplicate = match FoundationDbProviderTestHarness::boot(client_config()) {
        Ok(_) => panic!("a process cannot boot FoundationDB twice"),
        Err(error) => error,
    };
    assert_eq!(duplicate.kind(), StateStoreErrorKind::InvalidConfiguration);
    assert!(duplicate.to_string().contains("already"));

    let tls = TempDir::new().expect("TLS config temp dir");
    let cert = tls.path().join("client.crt");
    let key = tls.path().join("client.key");
    let ca = tls.path().join("ca.crt");
    std::fs::write(&cert, b"cert").expect("write cert fixture");
    std::fs::write(&key, b"key").expect("write key fixture");
    std::fs::write(&ca, b"ca").expect("write CA fixture");
    let different = match FoundationDbProviderTestHarness::boot(FoundationDbClientConfig {
        disable_multi_version_client: true,
        tls_cert_path: Some(cert),
        tls_key_path: Some(key),
        tls_ca_path: Some(ca),
        tls_verify_peers: Some("Check.Valid=1".to_owned()),
        tls_password: None,
    }) {
        Ok(_) => panic!("a different process-global client config must fail closed"),
        Err(error) => error,
    };
    assert_eq!(different.kind(), StateStoreErrorKind::InvalidConfiguration);
    assert!(different.to_string().contains("different"));

    let child = Command::new(std::env::current_exe().expect("current test binary"))
        .args([
            "--ignored",
            "--exact",
            "runtime_child_boot",
            "--nocapture",
            "--test-threads=1",
        ])
        .status()
        .expect("exec runtime child");
    assert!(
        child.success(),
        "fresh exec process must boot independently"
    );

    let store = harness
        .open_store(fdb_store_config(), Instant::now() + Duration::from_secs(5))
        .await
        .expect("open FoundationDB runtime handle");
    let held = harness
        .shutdown(Instant::now() + Duration::from_millis(50))
        .await
        .expect_err("shutdown must retain ownership while a store handle is alive");
    assert_eq!(held.kind(), StateStoreErrorKind::DeadlineExceeded);

    let blocked = match harness
        .open_store(fdb_store_config(), Instant::now() + Duration::from_secs(5))
        .await
    {
        Ok(_) => panic!("draining harness must not open another store"),
        Err(error) => error,
    };
    assert_eq!(blocked.kind(), StateStoreErrorKind::ProviderUnavailable);
    drop(store);
    harness
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown is retryable after provider handles drain");

    let restart = match FoundationDbProviderTestHarness::boot(client_config()) {
        Ok(_) => panic!("FoundationDB cannot restart after a successful stop"),
        Err(error) => error,
    };
    assert_eq!(restart.kind(), StateStoreErrorKind::InvalidConfiguration);
    assert!(restart.to_string().contains("stopped"));
}

#[tokio::test]
#[ignore = "exec helper used by foundationdb_runtime_lifecycle"]
async fn runtime_child_boot() {
    let mut harness = FoundationDbProviderTestHarness::boot(client_config())
        .expect("fresh exec process boots FoundationDB");
    harness
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("fresh exec process stops FoundationDB");
}
