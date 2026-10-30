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

#[test]
fn request_uses_original_safe_source_without_a_metadata_capacity_invoice() {
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(arrow::array::StringArray::from(vec!["value"])) as ArrayRef,
        DataType::Utf8,
        novarocks_types::schema::SqlType::String,
    );
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let context = connector_context_for_test();
    let deadline = context.deadline();
    let selection = collect_admitted_cow_selection(&fixture, &binding, context.clone());
    let held = capacity.snapshot().held_positions;
    cow_fe_begin_receipt::before_request(&selection, &binding, &context).unwrap();
    let held_unchanged = capacity.snapshot().held_positions == held;
    drop(selection);
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let exited = capacity.snapshot().held_positions;
    drop(control);
    assert!(held_unchanged);
    assert_eq!(context.deadline(), deadline);
    assert_eq!(exited, [0; 4]);
}

#[test]
fn legacy_selection_without_safe_receipts_never_enters_request_construction() {
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(arrow::array::StringArray::from(vec!["value"])) as ArrayRef,
        DataType::Utf8,
        novarocks_types::schema::SqlType::String,
    );
    assert!(!fixture.selection.has_owned_sources());
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let entered = std::cell::Cell::new(false);
    let result = cow_fe_begin_receipt::before_request(
        &fixture.selection,
        &binding,
        &connector_context_for_test(),
    )
    .map(|_| entered.set(true));
    let rejected = matches!(result, Err(cow_necessary_before_begin::Error::Control(ref error))
        if error.kind() == spi::ConnectorErrorKind::InvalidRequest);
    drop(result);
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let exited = capacity.snapshot().held_positions;
    drop(control);
    assert!(rejected);
    assert!(!entered.get());
    assert_eq!(exited, [0; 4]);
}

#[test]
fn original_control_failure_precedes_missing_source_receipts_at_request_boundary() {
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(arrow::array::StringArray::from(vec!["value"])) as ArrayRef,
        DataType::Utf8,
        novarocks_types::schema::SqlType::String,
    );
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let stop = spi::ConnectorStopOwner::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let context =
        spi::ConnectorRequestContext::try_new(deadline, stop.view(), 64 * 1024, 1024 * 1024)
            .unwrap();
    stop.request_stop();
    let result = cow_fe_begin_receipt::before_request(&fixture.selection, &binding, &context);
    let first_control = matches!(&result, Err(cow_necessary_before_begin::Error::Control(error))
        if error.kind() == spi::ConnectorErrorKind::Cancelled);
    drop(result);
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let exited = capacity.snapshot().held_positions;
    drop(control);
    assert!(first_control);
    assert_eq!(context.deadline(), deadline);
    assert_eq!(exited, [0; 4]);
}

struct ContextCloneReceiptSink;
impl spi::ConnectorVendedCredentialLeaseSink for ContextCloneReceiptSink {
    fn offer_vended_s3_credential_lease(
        &self,
        _: &spi::CatalogProperties,
        _: spi::VendedS3CredentialLeaseContribution,
    ) -> Result<(), spi::ConnectorError> {
        panic!("component context clone must not offer a credential contribution")
    }
}
fn nonempty_context_for_clone_receipt(
    original: spi::ConnectorRequestContext,
) -> spi::ConnectorRequestContext {
    let properties = spi::CatalogProperties::new(
        spi::CatalogHandle::new(
            spi::ConnectorInstanceId::parse("cow-context-component").unwrap(),
            spi::CatalogVersion::from_bytes([22; 32]),
        ),
        spi::ConnectorProviderId::parse("iceberg").unwrap(),
        1,
        vec![
            spi::CatalogProperty::new("warehouse", "component-warehouse").unwrap(),
            spi::CatalogProperty::new("format", "component-format").unwrap(),
        ],
        vec![
            spi::CatalogCredentialBinding::try_new(
                spi::CatalogCredentialPurpose::ObjectStoreMetadata,
                spi::CredentialConsumerRole::Frontend,
                spi::CatalogCredentialMode::Static(
                    spi::StaticCredentialReference::try_new("metadata-principal", "generation-1")
                        .unwrap(),
                ),
            )
            .unwrap(),
            spi::CatalogCredentialBinding::try_new(
                spi::CatalogCredentialPurpose::ObjectStoreData,
                spi::CredentialConsumerRole::Backend,
                spi::CatalogCredentialMode::Vended,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    original
        .with_vended_credential_lease_sink(Arc::new(ContextCloneReceiptSink))
        .with_vended_credential_lease_collection(properties)
        .unwrap()
}

#[test]
fn real_context_clone_preserves_original_properties_credentials_and_shared_strings() {
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(arrow::array::StringArray::from(vec!["value"])) as ArrayRef,
        DataType::Utf8,
        novarocks_types::schema::SqlType::String,
    );
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let original = connector_context_for_test();
    let deadline = original.deadline();
    let selection = collect_admitted_cow_selection(&fixture, &binding, original.clone());
    let context = nonempty_context_for_clone_receipt(original);
    let props = context
        .vended_credential_lease_collection()
        .unwrap()
        .catalog_properties();
    let held = capacity.snapshot().held_positions;
    cow_fe_begin_receipt::before_request(&selection, &binding, &context).unwrap();
    // This is the actual production type's Clone, after actual before_request.
    let copied = context.clone();
    let copied_props = copied
        .vended_credential_lease_collection()
        .unwrap()
        .catalog_properties();
    let separate_properties =
        props.execution_properties().as_ptr() != copied_props.execution_properties().as_ptr();
    let separate_bindings =
        props.credential_bindings().as_ptr() != copied_props.credential_bindings().as_ptr();
    let same_lengths = props.execution_properties().len()
        == copied_props.execution_properties().len()
        && props.credential_bindings().len() == copied_props.credential_bindings().len();
    let shared_properties = props
        .execution_properties()
        .iter()
        .zip(copied_props.execution_properties())
        .all(|(a, b)| {
            a.key().as_ptr() == b.key().as_ptr() && a.value().as_ptr() == b.value().as_ptr()
        });
    let shared_static = props
        .credential_bindings()
        .iter()
        .zip(copied_props.credential_bindings())
        .all(
            |(a, b)| match (a.static_reference(), b.static_reference()) {
                (Some(a), Some(b)) => {
                    a.name().as_ptr() == b.name().as_ptr()
                        && a.generation().as_ptr() == b.generation().as_ptr()
                }
                (None, None) => true,
                _ => false,
            },
        );
    let same_deadline = copied.deadline() == deadline && context.deadline() == deadline;
    let positions_unchanged = capacity.snapshot().held_positions == held;
    drop(copied);
    drop(context);
    drop(selection);
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let exited = capacity.snapshot().held_positions;
    drop(control);
    assert!(positions_unchanged && same_deadline);
    assert!(
        separate_properties
            && separate_bindings
            && same_lengths
            && shared_properties
            && shared_static
    );
    assert_eq!(exited, [0; 4]);
}

#[test]
fn original_stop_refuses_nonempty_context_before_the_request_clone() {
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(arrow::array::StringArray::from(vec!["value"])) as ArrayRef,
        DataType::Utf8,
        novarocks_types::schema::SqlType::String,
    );
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let selection =
        collect_admitted_cow_selection(&fixture, &binding, connector_context_for_test());
    let stop = spi::ConnectorStopOwner::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let context = nonempty_context_for_clone_receipt(
        spi::ConnectorRequestContext::try_new(deadline, stop.view(), 64 * 1024, 1024 * 1024)
            .unwrap(),
    );
    stop.request_stop();
    let entered = std::cell::Cell::new(false);
    let held = capacity.snapshot().held_positions;
    let result = cow_fe_begin_receipt::before_request(&selection, &binding, &context).map(|_| {
        entered.set(true);
        context.clone()
    });
    let cancelled = matches!(&result,Err(cow_necessary_before_begin::Error::Control(e)) if e.kind()==spi::ConnectorErrorKind::Cancelled);
    let same_control = context.is_cancelled() && context.deadline() == deadline;
    let positions_unchanged = capacity.snapshot().held_positions == held;
    drop(result);
    drop(context);
    drop(selection);
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let exited = capacity.snapshot().held_positions;
    drop(control);
    assert!(cancelled && same_control && positions_unchanged);
    assert!(!entered.get());
    assert_eq!(exited, [0; 4]);
}
