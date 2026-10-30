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

use super::*;

fn configured_client(builder: &HdfsServiceBuilder, directory: &std::path::Path) -> ClientBuilder {
    builder
        .client_builder()
        .unwrap()
        .with_config_dir(directory.to_str().unwrap())
        .with_config([("hadoop.security.authentication", "simple")])
}

#[tokio::test]
async fn public_direct_and_effective_user_construction_needs_no_cluster() {
    let directory = tempfile::tempdir().unwrap();
    for user in [None, Some("alice"), Some("alice:team"), Some("alice@realm")] {
        let builder = HdfsServiceBuilder::new(
            "hdfs://namenode.invalid:8020",
            user,
            tokio::runtime::Handle::current(),
        );
        assert_eq!(
            builder.config.endpoint.as_deref(),
            Some("hdfs://namenode.invalid:8020")
        );
        assert_eq!(builder.config.user.as_deref(), user);
        let _client = configured_client(&builder, directory.path())
            .build()
            .unwrap();
    }
    // These are public constructor facts, not observations of RPC user fields.
}

#[tokio::test]
async fn public_logical_ha_construction_uses_the_original_namespace() {
    let directory = tempfile::tempdir().unwrap();
    let builder = HdfsServiceBuilder::new(
        "hdfs://warehouse-ha",
        Some("alice"),
        tokio::runtime::Handle::current(),
    );
    let _client = configured_client(&builder, directory.path())
        .with_config([
            ("hadoop.security.authentication", "simple"),
            ("dfs.ha.namenodes.warehouse-ha", "left,right"),
            (
                "dfs.namenode.rpc-address.warehouse-ha.left",
                "left.invalid:8020",
            ),
            (
                "dfs.namenode.rpc-address.warehouse-ha.right",
                "right.invalid:8020",
            ),
            (
                "dfs.client.failover.proxy.provider.warehouse-ha",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
            ),
        ])
        .build()
        .unwrap();
    let error = configured_client(&builder, directory.path())
        .build()
        .unwrap_err();
    assert!(matches!(error, HdfsError::InvalidArgument(_)));
    // No inferred default port, synthetic namespace, DNS lookup, or RPC is used.
}

#[test]
fn error_projection_keeps_established_kinds_and_the_real_source() {
    use std::error::Error as _;
    for (error, kind) in [
        (
            HdfsError::FileNotFound("original".into()),
            ErrorKind::NotFound,
        ),
        (
            HdfsError::BlocksNotFound("original".into()),
            ErrorKind::NotFound,
        ),
        (
            HdfsError::AlreadyExists("original".into()),
            ErrorKind::AlreadyExists,
        ),
        (
            HdfsError::IsADirectoryError("original".into()),
            ErrorKind::IsADirectory,
        ),
        (
            HdfsError::RPCError("java.io.FileNotFoundException".into(), "original".into()),
            ErrorKind::NotFound,
        ),
        (
            HdfsError::OperationFailed("original".into()),
            ErrorKind::Unexpected,
        ),
    ] {
        let projected = map_error(error);
        assert_eq!(projected.kind(), kind);
        assert!(projected.source().is_some());
    }
}

#[tokio::test]
async fn original_capabilities_do_not_gain_append_copy_or_presign() {
    let directory = tempfile::tempdir().unwrap();
    let builder = HdfsServiceBuilder::new(
        "hdfs://namenode.invalid:8020",
        Some("alice"),
        tokio::runtime::Handle::current(),
    );
    let client = configured_client(&builder, directory.path())
        .build()
        .unwrap();
    let service = HdfsService {
        client: Arc::new(client),
    };
    let capability = raw::Service::capability(&service);
    assert!(!capability.write_can_append);
    assert!(!capability.copy);
    assert!(!capability.presign);
    let result = raw::Service::write(
        &service,
        &OperationContext::default(),
        "file",
        raw::OpWrite::default().with_append(true),
    );
    let Err(error) = result else {
        panic!("unchecked HDFS append was accepted")
    };
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[test]
fn missing_runtime_configuration_refuses_before_client_construction() {
    let builder = opendal::Configurator::into_builder(HdfsServiceConfig {
        endpoint: Some("hdfs://namenode.invalid:8020".into()),
        user: Some("alice".into()),
    });
    let error = builder
        .client_builder()
        .err()
        .expect("runtime is mandatory");
    assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
}

#[test]
fn public_client_built_off_runtime_uses_the_supplied_role_handle_on_last_drop() {
    let role = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    // There is deliberately no current Tokio context here. Without the public
    // with_io_runtime consumption, the SDK would create an owned Runtime.
    let builder = HdfsServiceBuilder::new(
        "hdfs://namenode.invalid:8020",
        Some("alice"),
        role.handle().clone(),
    );
    let client = configured_client(&builder, directory.path())
        .build()
        .unwrap();
    // Real last-client Drop on another nonblocking runtime thread must not
    // attempt to synchronously shut down an SDK-owned Runtime. No RPC is sent.
    let exited = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        observer.block_on(async move {
            drop(client);
        });
    }));
    let role_still_owned = role.block_on(async { 29 });
    drop(builder);
    drop(observer);
    drop(role);
    assert!(exited.is_ok());
    assert_eq!(role_still_owned, 29);
}
