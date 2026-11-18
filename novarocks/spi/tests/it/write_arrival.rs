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

use novarocks_spi::connector::write_stack::{
    ConnectorCommitFragment, ConnectorWriteQueryIdentity, ProviderWriteRuntime,
    WriteRuntimeAdapter, WriteTargetOrdinal, validate_write_fragment_targets,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId,
};

struct TestProvider {
    descriptor: ConnectorInstanceDescriptor,
    catalog: CatalogHandle,
}

impl ProviderWriteRuntime for TestProvider {
    type CommitSession = ();
    type WriterHandle = ();
    type CommitFragment = usize;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog
    }
}

fn adapter() -> WriteRuntimeAdapter<TestProvider> {
    let instance_id = ConnectorInstanceId::parse("write_arrival_test").expect("instance id");
    WriteRuntimeAdapter::new(Arc::new(TestProvider {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("test").expect("provider id"),
            instance_id: instance_id.clone(),
        },
        catalog: CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
    }))
}

fn ordinal(value: u32) -> WriteTargetOrdinal {
    WriteTargetOrdinal::try_new(value).expect("bounded target")
}

#[test]
fn a_write_query_requires_exact_nonzero_execution_and_plan() {
    let execution = [1; 16];
    let plan = [2; 16];
    let query = ConnectorWriteQueryIdentity::try_new(execution, 3, plan).expect("query identity");
    assert_ne!(
        query,
        ConnectorWriteQueryIdentity::try_new(execution, 4, plan).expect("different attempt")
    );
    assert_ne!(
        query,
        ConnectorWriteQueryIdentity::try_new(execution, 3, [5; 16]).expect("different plan")
    );
    assert!(ConnectorWriteQueryIdentity::try_new(execution, 0, plan).is_err());
    assert!(ConnectorWriteQueryIdentity::try_new([0; 16], 3, plan).is_err());
    assert!(ConnectorWriteQueryIdentity::try_new(execution, 3, [0; 16]).is_err());
}

#[test]
fn empty_results_still_require_a_query_target_set() {
    assert!(validate_write_fragment_targets(&[], &[ordinal(0)]).is_ok());
    assert!(validate_write_fragment_targets(&[], &[]).is_err());
    assert!(validate_write_fragment_targets(&[], &[ordinal(0), ordinal(0)]).is_err());
}

#[test]
fn a_query_can_reference_one_nonzero_session_target() {
    let target = ordinal(2);
    let fragments = [(target, adapter().wrap_commit_fragment(17))];
    assert!(validate_write_fragment_targets(&fragments, &[target]).is_ok());
}

#[test]
fn a_foreign_target_refusal_preserves_the_original_fragments() {
    let adapter = adapter();
    let admitted = ordinal(1);
    let foreign = ordinal(2);
    let fragments = [
        (admitted, adapter.wrap_commit_fragment(17)),
        (foreign, adapter.wrap_commit_fragment(23)),
    ];
    assert_eq!(
        validate_write_fragment_targets(&fragments, &[admitted])
            .expect_err("foreign target")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert_eq!(
        fragments.map(|(target, fragment)| (target, *adapter.commit_fragment(&fragment).unwrap())),
        [(admitted, 17), (foreign, 23)]
    );
}

#[test]
fn target_validation_does_not_limit_the_number_of_fragments() {
    let adapter = adapter();
    let target = ordinal(0);
    let fragments: Vec<(WriteTargetOrdinal, ConnectorCommitFragment)> = (0..16_385)
        .map(|id| (target, adapter.wrap_commit_fragment(id)))
        .collect();
    validate_write_fragment_targets(&fragments, &[target]).expect("exact target");
    assert_eq!(fragments.len(), 16_385);
}
