use prost::Message;
use prost_reflect::DescriptorPool;

use novarocks_proto_models::{
    FILE_DESCRIPTOR_SET, SCHEMA_LEDGER_VERSION, catalog, common, expr, filter, novarocks, plan,
};

#[test]
fn generated_dtos_and_descriptor_match_the_native_schema_contract() {
    assert_eq!(SCHEMA_LEDGER_VERSION, 1);

    let _ = common::UniqueId::default();
    let _ = catalog::CatalogSet::default();
    let _ = expr::Expr::default();
    let _ = filter::LookupRequest::default();
    let _ = plan::PlanFragment::default();
    let _ = novarocks::StageFragmentsRequest::default();

    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    assert!(
        pool.get_message_by_name("novarocks.plan.PlanFragment")
            .is_some()
    );
    assert!(
        pool.get_service_by_name("novarocks.NovaRocksGrpc")
            .is_some()
    );
}

#[test]
fn catalog_lifecycle_contract_is_carried_by_init_and_the_existing_control_stream() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let manifest = pool
        .get_message_by_name("novarocks.ParticipantManifest")
        .expect("ParticipantManifest descriptor");
    let catalog_set = manifest
        .get_field_by_name("catalog_set")
        .expect("catalog contribution field");
    assert_eq!(catalog_set.number(), 12);
    assert_eq!(
        catalog_set
            .kind()
            .as_message()
            .expect("CatalogSet message")
            .full_name(),
        "novarocks.catalog.CatalogSet"
    );

    let ready = pool
        .get_message_by_name("novarocks.QueryControlReady")
        .expect("QueryControlReady descriptor");
    assert_eq!(
        ready
            .get_field_by_name("catalog_load_state")
            .expect("closed catalog state")
            .number(),
        1
    );
    let response = pool
        .get_message_by_name("novarocks.QueryControlResponse")
        .expect("QueryControlResponse descriptor");
    assert_eq!(
        response
            .get_field_by_name("catalog_ready")
            .expect("cold completion")
            .number(),
        9
    );
    assert_eq!(
        response
            .get_field_by_name("catalog_load_failed")
            .expect("cold failure")
            .number(),
        10
    );

    let service = pool
        .get_service_by_name("novarocks.NovaRocksGrpc")
        .expect("service descriptor");
    assert!(
        service
            .methods()
            .any(|method| method.name() == "PruneCatalogs"),
        "catalog pruning has one explicit best-effort control-plane RPC"
    );
}

#[test]
fn native_compatibility_identity_fields_are_exact_and_append_only() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    for (message_name, field_name, field_number) in [
        (
            "novarocks.BackendProcessDescriptor",
            "native_compatibility_id",
            5,
        ),
        (
            "novarocks.ParticipantManifest",
            "native_compatibility_id",
            11,
        ),
    ] {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        let field = message
            .get_field_by_name(field_name)
            .unwrap_or_else(|| panic!("{message_name}.{field_name} descriptor"));
        assert_eq!(field.number(), field_number);
        assert_eq!(
            field.kind().as_message().unwrap().full_name(),
            "novarocks.NativeCompatibilityId"
        );
    }
    let identity = pool
        .get_message_by_name("novarocks.NativeCompatibilityId")
        .expect("NativeCompatibilityId descriptor");
    assert_eq!(identity.fields().count(), 1);
    let value = identity
        .get_field_by_name("value")
        .expect("identity value field");
    assert_eq!(value.number(), 1);

    let outcome = pool
        .get_enum_by_name("novarocks.QueryInitOutcome")
        .expect("QueryInitOutcome descriptor");
    assert_eq!(
        outcome
            .get_value_by_name("QUERY_INIT_REJECTED_COMPATIBILITY_MISMATCH")
            .expect("compatibility mismatch outcome")
            .number(),
        10
    );
}

#[test]
fn retired_starrocks_native_scan_fields_remain_reserved() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    let scan_source = pool
        .get_message_by_name("novarocks.plan.ScanSource")
        .expect("ScanSource descriptor");
    assert!(
        scan_source
            .reserved_ranges()
            .any(|range| range.contains(&7)),
        "ScanSource field 7 must remain reserved"
    );
    assert!(
        scan_source
            .reserved_names()
            .any(|name| name == "starrocks_table"),
        "ScanSource starrocks_table name must remain reserved"
    );

    let scan_range = pool
        .get_message_by_name("novarocks.ScanRange")
        .expect("ScanRange descriptor");
    assert!(
        scan_range.reserved_ranges().any(|range| range.contains(&2)),
        "ScanRange field 2 must remain reserved"
    );
    assert!(
        scan_range
            .reserved_names()
            .any(|name| name == "starrocks_tablet"),
        "ScanRange starrocks_tablet name must remain reserved"
    );
}

#[test]
fn retired_mv_native_scan_fields_remain_reserved_and_fail_closed() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let scan_source = pool
        .get_message_by_name("novarocks.plan.ScanSource")
        .expect("ScanSource descriptor");

    for field_number in [5, 6] {
        assert!(
            scan_source
                .reserved_ranges()
                .any(|range| range.contains(&field_number)),
            "ScanSource field {field_number} must remain reserved"
        );
    }
    for field_name in ["iceberg_mv_target_state", "iceberg_mv_target_locator"] {
        assert!(
            scan_source.reserved_names().any(|name| name == field_name),
            "ScanSource {field_name} name must remain reserved"
        );
    }

    for encoded in [&[0x2a, 0x00][..], &[0x32, 0x00][..]] {
        let source = plan::ScanSource::decode(encoded)
            .expect("retired source field remains decodable as an unknown field");
        assert!(source.kind.is_none());
    }
}

#[test]
fn retired_starrocks_native_scan_wire_fields_fail_closed() {
    let source = plan::ScanSource::decode(&[0x3a, 0x00][..])
        .expect("retired source field remains decodable as an unknown field");
    assert!(source.kind.is_none());

    let range = novarocks::ScanRange::decode(&[0x12, 0x00][..])
        .expect("retired range field remains decodable as an unknown field");
    assert!(range.kind.is_none());
}

#[test]
fn runtime_filter_membership_contract_is_closed_and_legacy_fields_stay_reserved() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let membership = pool
        .get_message_by_name("novarocks.plan.RuntimeFilterMembershipContract")
        .expect("RuntimeFilterMembershipContract descriptor");

    for (field_number, field_name) in [(1, "canonical_schema"), (2, "schema_digest")] {
        assert!(
            membership
                .reserved_ranges()
                .any(|range| range.contains(&field_number)),
            "RuntimeFilterMembershipContract field {field_number} must remain reserved"
        );
        assert!(
            membership.reserved_names().any(|name| name == field_name),
            "RuntimeFilterMembershipContract {field_name} must remain reserved"
        );
        assert!(
            membership
                .fields()
                .all(|field| field.number() != field_number),
            "RuntimeFilterMembershipContract must not reuse tag {field_number}"
        );
        assert!(
            membership.fields().all(|field| field.name() != field_name),
            "RuntimeFilterMembershipContract must not reuse name {field_name}"
        );
    }

    let null_semantics = membership
        .get_field_by_name("null_semantics")
        .expect("RuntimeFilterMembershipContract.null_semantics descriptor");
    assert_eq!(null_semantics.number(), 3);
    assert_eq!(
        null_semantics.kind().as_enum().unwrap().full_name(),
        "novarocks.plan.RuntimeFilterMembershipNullSemantics"
    );

    let semantics = pool
        .get_enum_by_name("novarocks.plan.RuntimeFilterMembershipNullSemantics")
        .expect("RuntimeFilterMembershipNullSemantics descriptor");
    let values = semantics
        .values()
        .map(|value| (value.name().to_owned(), value.number()))
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        [
            (
                "RUNTIME_FILTER_MEMBERSHIP_NULL_SEMANTICS_UNSPECIFIED".to_owned(),
                0,
            ),
            (
                "RUNTIME_FILTER_MEMBERSHIP_NULL_SEMANTICS_NEVER_MATCHES".to_owned(),
                1,
            ),
            (
                "RUNTIME_FILTER_MEMBERSHIP_NULL_SEMANTICS_NULL_SAFE_EQUAL".to_owned(),
                2,
            ),
        ]
    );

    for encoded in [&[0x0a, 0x00][..], &[0x12, 0x00][..]] {
        let membership = plan::RuntimeFilterMembershipContract::decode(encoded)
            .expect("retired membership field remains decodable as an unknown field");
        assert_eq!(membership.null_semantics, 0);
    }
}

#[test]
fn retired_terminal_self_attestation_fields_remain_reserved() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    for (message_name, field_numbers, field_names, participant_field) in [
        (
            "novarocks.QueryTerminalSnapshot",
            &[2, 3, 4, 5][..],
            &["execution_id", "backend", "init_digest", "digest"][..],
            8,
        ),
        (
            "novarocks.TerminalizationProof",
            &[2, 3, 4, 5][..],
            &["execution_id", "backend", "init_digest", "digest"][..],
            7,
        ),
        (
            "novarocks.NegativeAttestation",
            &[1, 2, 3, 7][..],
            &["execution_id", "backend", "init_digest", "digest"][..],
            8,
        ),
        (
            "novarocks.QueryControlTerminalAck",
            &[1, 2, 3, 4][..],
            &[
                "execution_id",
                "init_digest",
                "snapshot_version",
                "snapshot_digest",
            ][..],
            5,
        ),
    ] {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        for field_number in field_numbers {
            assert!(
                message
                    .reserved_ranges()
                    .any(|range| range.contains(field_number)),
                "{message_name} field {field_number} must remain reserved"
            );
            assert!(
                message
                    .fields()
                    .all(|field| field.number() != *field_number),
                "{message_name} must not reuse retired tag {field_number}"
            );
        }
        for field_name in field_names {
            assert!(
                message.reserved_names().any(|name| name == *field_name),
                "{message_name} field name {field_name} must remain reserved"
            );
            assert!(
                message.fields().all(|field| field.name() != *field_name),
                "{message_name} must not reuse retired name {field_name}"
            );
        }
        let participant = message
            .get_field(participant_field)
            .unwrap_or_else(|| panic!("{message_name} participant field"));
        assert_eq!(participant.name(), "participant");
        assert_eq!(
            participant
                .kind()
                .as_message()
                .map(|message| message.full_name()),
            Some("novarocks.ParticipantAttemptRef".into())
        );
    }
}

#[test]
fn participant_attempt_ref_has_only_allocated_identity_leaves() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let message = pool
        .get_message_by_name("novarocks.ParticipantAttemptRef")
        .expect("ParticipantAttemptRef descriptor");
    assert_eq!(message.fields().count(), 2);
    assert_eq!(
        message.get_field(1).expect("execution field").name(),
        "execution_id"
    );
    assert_eq!(
        message.get_field(2).expect("process field").name(),
        "backend_process_id"
    );
}

#[test]
fn nonterminal_lifecycle_carriers_reserve_retired_fences_and_use_participant_refs() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let exact_ref = |message_name: &str, field_name: &str, field_number: u32| {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        let field = message
            .get_field(field_number)
            .unwrap_or_else(|| panic!("{message_name}.{field_name} field"));
        assert_eq!(field.name(), field_name);
        assert_eq!(
            field.kind().as_message().map(|message| message.full_name()),
            Some("novarocks.ParticipantAttemptRef".into())
        );
    };
    let reserved = |message_name: &str, field_numbers: &[u32], field_names: &[&str]| {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        for field_number in field_numbers {
            assert!(
                message
                    .reserved_ranges()
                    .any(|range| range.contains(field_number)),
                "{message_name} retired tag {field_number} must remain reserved"
            );
            assert!(
                message
                    .fields()
                    .all(|field| field.number() != *field_number)
            );
        }
        for field_name in field_names {
            assert!(
                message.reserved_names().any(|name| name == *field_name),
                "{message_name} retired field {field_name} must remain reserved"
            );
            assert!(message.fields().all(|field| field.name() != *field_name));
        }
    };

    reserved(
        "novarocks.StageFragmentsRequest",
        &[1, 2, 3, 4],
        &[
            "execution_id",
            "init_digest",
            "stage_digest_version",
            "stage_digest",
        ],
    );
    exact_ref("novarocks.StageFragmentsRequest", "participant", 6);
    reserved(
        "novarocks.StageFragmentsResponse",
        &[2],
        &["stage_digest_version"],
    );
    reserved(
        "novarocks.StartPreparedQueryRequest",
        &[2],
        &["stage_digest_version"],
    );
    reserved(
        "novarocks.StartPreparedQueryResponse",
        &[2],
        &["stage_digest_version"],
    );
    reserved("novarocks.AbortQueryRequest", &[1], &["execution_id"]);
    exact_ref("novarocks.AbortQueryRequest", "participant", 4);
    reserved(
        "novarocks.QueryControlAttach",
        &[1, 2, 3],
        &["execution_id", "init_digest", "frontend_owner_epoch"],
    );
    exact_ref("novarocks.QueryControlAttach", "participant", 4);
    reserved(
        "novarocks.FragmentLiveObservation",
        &[1, 2, 3],
        &["execution_id", "init_digest", "backend"],
    );
    exact_ref("novarocks.FragmentLiveObservation", "participant", 10);
    reserved(
        "novarocks.RuntimeFilterFeedbackEvent",
        &[1, 2, 3],
        &["execution_id", "init_digest", "backend"],
    );
    exact_ref(
        "novarocks.RuntimeFilterFeedbackEvent",
        "participant_attempt",
        10,
    );
}

#[test]
fn retired_request_self_attestation_fields_remain_reserved() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    // Each entry named a digest whose derivation inputs were entirely present in
    // the same message. The receiver derives the identity instead; other
    // messages keep carrying it as a cross-message reference.
    for (message_name, field_number, field_name) in [
        ("novarocks.InitQueryRequest", 2, "init_digest"),
        ("novarocks.StageFragmentsRequest", 4, "stage_digest"),
        (
            "novarocks.RuntimeFilterContribution",
            4,
            "contribution_digest",
        ),
    ] {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        assert!(
            message
                .reserved_ranges()
                .any(|range| range.contains(&field_number)),
            "{message_name} field {field_number} must remain reserved"
        );
        assert!(
            message.reserved_names().any(|name| name == field_name),
            "{message_name} {field_name} name must remain reserved"
        );
        assert!(
            message.fields().all(|field| field.number() != field_number),
            "{message_name} must not reuse retired tag {field_number}"
        );
        assert!(
            message.fields().all(|field| field.name() != field_name),
            "{message_name} must not reuse retired name {field_name}"
        );
    }
}

#[test]
fn typed_connector_read_handle_and_split_oneofs_are_closed() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    // Every handle and split family selects its provider through its own closed
    // oneof. A generic consumer must never be able to reach a variant by class
    // id, message name, or an escape hatch field, so the exact variant list is
    // part of the contract.
    for (message_name, oneof_name, expected_variants) in [
        (
            "novarocks.connector_read.ColumnHandle",
            "handle",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.ConnectorTransactionHandle",
            "handle",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.ConnectorTableHandle",
            "handle",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.ConnectorTableFunctionHandle",
            "handle",
            &["iceberg_table_changes"][..],
        ),
        (
            "novarocks.connector_read.ConnectorChangeWindowHandle",
            "handle",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.ConnectorSystemTableReference",
            "reference",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.ConnectorTableExecuteHandle",
            "handle",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.ConnectorMergeTableHandle",
            "handle",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.DataSplit",
            "provider",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.TableChangesSplitCategory",
            "provider",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.ChangeWindowSplitCategory",
            "provider",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.SystemFilesSplitCategory",
            "provider",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.RewritePositionDeleteFilesSplitCategory",
            "provider",
            &["iceberg"][..],
        ),
        (
            "novarocks.connector_read.ConnectorSplit",
            "category",
            &[
                "data",
                "table_changes",
                "change_window",
                "system_files",
                "rewrite_position_delete_files",
            ][..],
        ),
        (
            "novarocks.connector_read.CatalogTableHandle",
            "relation",
            &[
                "table",
                "table_function",
                "change_window",
                "system_table",
                "table_execute",
                "merge_table",
            ][..],
        ),
        (
            "novarocks.connector_read.IcebergChangeSplit",
            "rows",
            &[
                "added_rows",
                "position_deleted_rows",
                "equality_deleted_rows",
                "deleted_data_file_rows",
            ][..],
        ),
        (
            "novarocks.connector_read.IcebergTableExecuteHandle",
            "procedure_handle",
            &["optimize", "rewrite_position_delete_files"][..],
        ),
    ] {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        let oneof = message
            .oneofs()
            .find(|oneof| oneof.name() == oneof_name)
            .unwrap_or_else(|| panic!("{message_name} must declare the {oneof_name} oneof"));
        let variants = oneof
            .fields()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            variants, expected_variants,
            "{message_name}.{oneof_name} variant set changed"
        );
    }
}

#[test]
fn the_typed_connector_scan_source_carries_no_split_list_or_private_payload() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    let scan_source = pool
        .get_message_by_name("novarocks.connector_read.ConnectorTableScanSource")
        .expect("ConnectorTableScanSource descriptor");
    let fields = scan_source
        .fields()
        .map(|field| (field.number(), field.name().to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(
        fields,
        vec![
            (1, "table".to_owned()),
            (2, "assignments".to_owned()),
            (3, "enforced_predicate".to_owned()),
            (4, "unenforced_predicate".to_owned()),
            (5, "remaining_expression".to_owned()),
            (6, "dynamic_filters".to_owned()),
            (7, "max_batch_rows".to_owned()),
            (8, "max_batch_bytes".to_owned()),
            (9, "work_source".to_owned()),
        ]
    );

    // The whole point of the typed source: no eager split list, no provider
    // payload, and no Arrow IPC schema crossing the boundary. `work_source`
    // is a neutral scheduling fact, not provider-private scan content.
    for forbidden in [
        "splits",
        "scan_payload",
        "split_payload",
        "expected_schema_ipc",
    ] {
        assert!(
            scan_source.fields().all(|field| field.name() != forbidden),
            "ConnectorTableScanSource must not carry {forbidden}"
        );
    }
}

#[test]
fn the_split_envelope_exposes_only_neutral_scheduling_facts() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    let split = pool
        .get_message_by_name("novarocks.connector_read.ConnectorSplit")
        .expect("ConnectorSplit descriptor");
    let neutral = split
        .fields()
        .filter(|field| field.containing_oneof().is_none())
        .map(|field| (field.number(), field.name().to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(
        neutral,
        vec![
            (1, "split_weight_raw".to_owned()),
            (2, "remotely_accessible".to_owned()),
            (3, "addresses".to_owned()),
            (5, "retained_size_in_bytes".to_owned()),
        ]
    );
    // `affinity_key` is optional, so proto3 places it in a synthetic oneof; it
    // is still part of the neutral envelope.
    assert!(
        split
            .fields()
            .any(|field| field.number() == 4 && field.name() == "affinity_key")
    );

    // A split never carries a digest or a self-attested identity: scheduling
    // identity is the task-attempt-scoped sequence alone.
    for forbidden in ["digest", "content_id", "membership_digest", "split_id"] {
        assert!(
            split.fields().all(|field| field.name() != forbidden),
            "ConnectorSplit must not carry {forbidden}"
        );
    }
}

#[test]
fn runtime_split_assignment_messages_carry_sequence_and_terminal_facts() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    let scheduled = pool
        .get_message_by_name("novarocks.connector_read.ScheduledSplit")
        .expect("ScheduledSplit descriptor");
    assert_eq!(
        scheduled
            .fields()
            .map(|field| (field.number(), field.name().to_owned()))
            .collect::<Vec<_>>(),
        vec![
            (1, "sequence_id".to_owned()),
            (2, "plan_node_id".to_owned()),
            (3, "split".to_owned()),
        ]
    );

    let assignment = pool
        .get_message_by_name("novarocks.connector_read.SplitAssignment")
        .expect("SplitAssignment descriptor");
    assert_eq!(
        assignment
            .fields()
            .map(|field| (field.number(), field.name().to_owned()))
            .collect::<Vec<_>>(),
        vec![
            (1, "plan_node_id".to_owned()),
            (2, "splits".to_owned()),
            (3, "no_more_splits".to_owned()),
        ]
    );

    let request = pool
        .get_message_by_name("novarocks.TaskUpdateRequest")
        .expect("TaskUpdateRequest descriptor");
    assert_eq!(
        request
            .fields()
            .map(|field| (field.number(), field.name().to_owned()))
            .collect::<Vec<_>>(),
        vec![
            (1, "execution_id".to_owned()),
            (2, "fragment_instance_id".to_owned()),
            (3, "assignments".to_owned()),
        ]
    );

    let service = pool
        .get_service_by_name("novarocks.NovaRocksGrpc")
        .expect("service descriptor");
    assert!(
        service
            .methods()
            .any(|method| method.name() == "TaskUpdate"),
        "the runtime split-assignment RPC must exist"
    );
}

#[test]
fn the_worker_system_relation_set_stays_closed() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    let system_table_type = pool
        .get_enum_by_name("novarocks.connector_read.IcebergSystemTableType")
        .expect("IcebergSystemTableType descriptor");
    assert_eq!(
        system_table_type
            .values()
            .map(|value| value.name().to_owned())
            .collect::<Vec<_>>(),
        vec![
            "ICEBERG_SYSTEM_TABLE_TYPE_UNSPECIFIED".to_owned(),
            "ICEBERG_SYSTEM_TABLE_TYPE_FILES".to_owned(),
            "ICEBERG_SYSTEM_TABLE_TYPE_ENTRIES".to_owned(),
            "ICEBERG_SYSTEM_TABLE_TYPE_SNAPSHOTS".to_owned(),
            "ICEBERG_SYSTEM_TABLE_TYPE_HISTORY".to_owned(),
            "ICEBERG_SYSTEM_TABLE_TYPE_REFS".to_owned(),
            "ICEBERG_SYSTEM_TABLE_TYPE_MANIFESTS".to_owned(),
            "ICEBERG_SYSTEM_TABLE_TYPE_PARTITIONS".to_owned(),
        ],
        "the worker set is exact, with no ALL_* or unknown system-table variant"
    );
}

#[test]
fn retired_participant_role_projection_remains_reserved() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    // `participant_roles` was a projection the sender mechanically derived from
    // two other fields of the same message: FragmentExecutor followed from a
    // non-empty `expected_fragment_instance_ids` (field 4) and
    // RuntimeFilterService from the presence of `runtime_filter` (field 8). Both
    // derivation inputs travel inside `ParticipantManifest` itself, so the
    // receiver can rebuild the role set unaided and validating the carried copy
    // produced no fact it did not already hold. The payload is now the sole
    // participant role authority (ADR-0114).
    let manifest = pool
        .get_message_by_name("novarocks.ParticipantManifest")
        .expect("ParticipantManifest descriptor");
    assert!(
        manifest.reserved_ranges().any(|range| range.contains(&3)),
        "ParticipantManifest field 3 must remain reserved"
    );
    assert!(
        manifest
            .reserved_names()
            .any(|name| name == "participant_roles"),
        "ParticipantManifest participant_roles name must remain reserved"
    );
    assert!(
        manifest.fields().all(|field| field.number() != 3),
        "ParticipantManifest must not reuse retired tag 3"
    );
    assert!(
        manifest
            .fields()
            .all(|field| field.name() != "participant_roles"),
        "ParticipantManifest must not reuse retired name participant_roles"
    );

    // The projection's role vocabulary was retired with it. Nothing else on the
    // wire names these values, so the enum must stay out of the contract rather
    // than linger as a second, drift-prone role authority.
    assert!(
        pool.get_enum_by_name("novarocks.QueryParticipantRole")
            .is_none(),
        "retired QueryParticipantRole enum must not return to the wire contract"
    );
}

#[test]
fn write_dataflow_nodes_are_appended_after_the_existing_distributed_payloads() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let node = pool
        .get_message_by_name("novarocks.plan.DistributedNode")
        .expect("DistributedNode descriptor");
    // The two overlay-only write nodes are appended; the pre-existing payload
    // arms keep their numbers so an older plan still parses the same way.
    for (field_name, field_number) in [
        ("physical", 10),
        ("exchange", 11),
        ("table_writer", 12),
        ("table_finish", 13),
    ] {
        let field = node
            .get_field_by_name(field_name)
            .unwrap_or_else(|| panic!("DistributedNode.{field_name} descriptor"));
        assert_eq!(field.number(), field_number);
    }
}

#[test]
fn the_connector_write_carriers_are_closed_single_provider_oneofs() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    // Iceberg is the only provider that can write today. StarRocks deliberately
    // has no arm here: an unused placeholder would advertise a capability the
    // provider does not have, and `write: None` must stay a real refusal.
    for (message_name, oneof_name, arm_name) in [
        (
            "novarocks.connector_write.ConnectorWriterHandle",
            "handle",
            "iceberg",
        ),
        (
            "novarocks.connector_write.ConnectorCommitFragment",
            "fragment",
            "iceberg",
        ),
    ] {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        let oneof = message
            .oneofs()
            .find(|oneof| oneof.name() == oneof_name)
            .unwrap_or_else(|| panic!("{message_name}.{oneof_name} oneof"));
        let arms = oneof
            .fields()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(arms, vec![arm_name.to_string()]);
        let arm = message
            .get_field_by_name(arm_name)
            .unwrap_or_else(|| panic!("{message_name}.{arm_name} descriptor"));
        // Provider arms start at 10 by repository convention, leaving 1..9 for
        // neutral envelope fields if one is ever needed.
        assert_eq!(arm.number(), 10);
    }
}

#[test]
fn an_iceberg_commit_fragment_describes_exactly_one_artifact() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let fragment = pool
        .get_message_by_name("novarocks.connector_write.IcebergCommitFragment")
        .expect("IcebergCommitFragment descriptor");
    let artifact = fragment
        .oneofs()
        .find(|oneof| oneof.name() == "artifact")
        .expect("IcebergCommitFragment.artifact oneof");
    let arms = artifact
        .fields()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        arms,
        vec![
            "data_file".to_string(),
            "position_delete_file".to_string(),
            "deletion_vector".to_string(),
            "equality_delete_file".to_string(),
        ]
    );
    // A fragment carries no writer identity, attempt id, or aggregate summary:
    // those belong to an execution, not to an artifact.
    let field_names = fragment
        .fields()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(field_names.len(), 4);
    for forbidden in [
        "writer",
        "operation_id",
        "cohort_id",
        "summary",
        "row_count",
    ] {
        assert!(
            !field_names.iter().any(|name| name.contains(forbidden)),
            "commit fragment must not carry {forbidden}"
        );
    }
}

/// The write-operation aggregate carried three plan messages and one terminal
/// report list. They are gone; their tags must stay reserved so a later field
/// cannot silently occupy a number an older encoder still fills.
#[test]
fn retired_write_operation_aggregate_fields_remain_reserved() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    for (message_name, field_number, field_name) in [
        ("novarocks.plan.DataSink", 7, "connector_write"),
        (
            "novarocks.QueryTerminalFragmentSnapshot",
            11,
            "connector_staged_report_frames",
        ),
    ] {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        assert!(
            message
                .reserved_ranges()
                .any(|range| range.contains(&field_number)),
            "{message_name} field {field_number} must remain reserved"
        );
        assert!(
            message.reserved_names().any(|name| name == field_name),
            "{message_name} {field_name} must remain reserved"
        );
        assert!(
            message.fields().all(|field| field.number() != field_number),
            "{message_name} must not reuse tag {field_number}"
        );
        assert!(
            message.fields().all(|field| field.name() != field_name),
            "{message_name} must not reuse name {field_name}"
        );
    }

    // The aggregate's own carriers are gone, not merely unreferenced.
    for retired in [
        "novarocks.plan.ConnectorWriterIdentity",
        "novarocks.plan.ConnectorWriterHandleEnvelope",
        "novarocks.plan.ConnectorWriteFragmentSink",
        "novarocks.ConnectorStagedReportFrame",
    ] {
        assert!(
            pool.get_message_by_name(retired).is_none(),
            "{retired} must not exist in the native schema"
        );
    }
}

#[test]
fn retired_write_operation_aggregate_wire_fields_fail_closed() {
    // DataSink field 7, wire type 2: the retired `connector_write` sink arm.
    let sink = plan::DataSink::decode(&[0x3a, 0x00][..])
        .expect("retired sink field remains decodable as an unknown field");
    assert!(sink.kind.is_none());

    // QueryTerminalFragmentSnapshot field 11, wire type 2: the retired staged
    // report frame list. It decodes away instead of reviving a staged writer.
    let snapshot = novarocks::QueryTerminalFragmentSnapshot::decode(&[0x5a, 0x00][..])
        .expect("retired snapshot field remains decodable as an unknown field");
    assert_eq!(snapshot.backend_num, 0);
    assert!(snapshot.tablet_commit_infos.is_empty());
}

/// The type-level separation of create from update is the whole reason this
/// protocol cannot be misassembled at runtime, so it is asserted on the
/// descriptor rather than trusted to review. A descriptor is required on
/// exactly one message, and no update may name one under any field.
#[test]
fn the_task_protocol_separates_create_from_update_on_the_descriptor() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    let create = pool
        .get_message_by_name("novarocks.CreateTaskRequest")
        .expect("CreateTaskRequest descriptor");
    let descriptor = create
        .get_field_by_name("descriptor")
        .expect("a create carries a descriptor");
    assert_eq!(
        descriptor
            .kind()
            .as_message()
            .expect("TaskDescriptor message")
            .full_name(),
        "novarocks.TaskDescriptor"
    );
    assert!(
        !descriptor.is_list() && !descriptor.is_map(),
        "a task is created from exactly one descriptor"
    );
    assert!(
        create.get_field_by_name("query_context").is_some(),
        "a create must address the query context of its own backend"
    );

    // Nothing that updates may carry a descriptor, under that name or any
    // other. Checking the field type as well as the name is what makes this a
    // contract rather than a naming convention.
    for message_name in [
        "novarocks.UpdateTaskRequest",
        "novarocks.AdvanceQueryContextDomainRequest",
        "novarocks.RenewQueryExecutionLeaseRequest",
        "novarocks.EstablishQueryContextRequest",
        "novarocks.CancelTaskRequest",
        "novarocks.AbortQueryContextRequest",
        "novarocks.ReleaseQueryContextRequest",
    ] {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        assert!(
            message.get_field_by_name("descriptor").is_none(),
            "{message_name} must not carry a descriptor field"
        );
        for field in message.fields() {
            let names_a_descriptor = field
                .kind()
                .as_message()
                .is_some_and(|message| message.full_name() == "novarocks.TaskDescriptor");
            assert!(
                !names_a_descriptor,
                "{message_name}.{} must not name a TaskDescriptor",
                field.name()
            );
        }
    }
}

/// Only an establish may create a query context, and the command set that can
/// reach a context is closed.
#[test]
fn the_task_operation_and_query_context_command_sets_are_closed() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    for (message_name, oneof_name, expected_variants) in [
        (
            "novarocks.TaskOperation",
            "operation",
            &[
                "create_task",
                "update_task",
                "update_query_context",
                "cancel_task",
                "abort_query_context",
                "release_query_context",
            ][..],
        ),
        (
            "novarocks.UpdateQueryContextRequest",
            "command",
            &["establish", "advance_domain", "renew_lease"][..],
        ),
        (
            "novarocks.TaskDomainUpdate",
            "domain",
            &["split_assignment", "dynamic_filter", "open_exchange_edges"][..],
        ),
        (
            "novarocks.QueryContextDomainUpdate",
            "domain",
            &["catalog_binding", "shared_dynamic_filter", "credential"][..],
        ),
        (
            "novarocks.TaskTermination",
            "cause",
            &["canceled", "aborted", "failed"][..],
        ),
        (
            "novarocks.TaskStatusStreamEvent",
            "event",
            &["task_status", "task_gone"][..],
        ),
        (
            "novarocks.TaskDomainReceipt",
            "receipt",
            &["split_assignment", "dynamic_filter", "open_exchange_edges"][..],
        ),
        (
            "novarocks.QueryContextDomainReceipt",
            "receipt",
            &["catalog_binding", "shared_dynamic_filter", "credential"][..],
        ),
    ] {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        let oneof = message
            .oneofs()
            .find(|oneof| oneof.name() == oneof_name)
            .unwrap_or_else(|| panic!("{message_name} must declare the {oneof_name} oneof"));
        let variants = oneof
            .fields()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            variants, expected_variants,
            "{message_name}.{oneof_name} variant set changed"
        );
    }
}

/// A frozen exchange endpoint carries both addresses. The task identity is the
/// process fence; the fragment instance id is what an actual exchange frame
/// carries. Losing either one makes a frame uncheckable.
#[test]
fn the_task_exchange_topology_freezes_both_addresses() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    for message_name in [
        "novarocks.TaskExchangeDestination",
        "novarocks.TaskExchangeSource",
    ] {
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("{message_name} descriptor"));
        assert_eq!(
            message
                .get_field_by_name("task")
                .expect("the protocol fence")
                .kind()
                .as_message()
                .expect("TaskIdentity message")
                .full_name(),
            "novarocks.TaskIdentity",
            "{message_name} must be fenced by an exact task identity"
        );
        assert_eq!(
            message
                .get_field_by_name("fragment_instance_id")
                .expect("the kernel key")
                .kind()
                .as_message()
                .expect("UniqueId message")
                .full_name(),
            "novarocks.common.UniqueId",
            "{message_name} must carry the kernel key an exchange frame uses"
        );
    }

    // The sender count of an inbound node is its frozen source set, so there
    // is deliberately no separate count field that could disagree with it.
    let inbound = pool
        .get_message_by_name("novarocks.TaskExchangeInbound")
        .expect("TaskExchangeInbound descriptor");
    assert!(
        inbound.get_field_by_name("expected_sender_count").is_none(),
        "the source set is the sender count; a second field could disagree"
    );
    assert!(
        inbound
            .get_field_by_name("sources")
            .expect("frozen source set")
            .is_list()
    );
}

/// A status snapshot advertises a dynamic filter version and never carries its
/// payload, so a large filter cannot inflate the lifecycle channel.
#[test]
fn a_task_status_advertises_a_filter_version_but_never_a_payload() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let status = pool
        .get_message_by_name("novarocks.TaskStatus")
        .expect("TaskStatus descriptor");
    assert!(status.get_field_by_name("dynamic_filter_version").is_some());
    assert!(
        status
            .get_field_by_name("dynamic_filter_domain_count")
            .is_some()
    );
    for field in status.fields() {
        if let Some(message) = field.kind().as_message() {
            assert_ne!(
                message.full_name(),
                "novarocks.filter.RuntimeFilterEnvelope",
                "TaskStatus.{} must not carry a filter payload",
                field.name()
            );
        }
        assert!(
            !matches!(field.kind(), prost_reflect::Kind::Bytes),
            "TaskStatus.{} must not carry an opaque payload",
            field.name()
        );
    }
}

/// Final task info is observation only: bounded, redacted, and free of every
/// payload that has its own owner and data plane.
#[test]
fn final_task_info_carries_no_result_credential_or_commit_payload() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let info = pool
        .get_message_by_name("novarocks.FinalTaskInfo")
        .expect("FinalTaskInfo descriptor");
    assert_eq!(
        info.get_field_by_name("final_status")
            .expect("the terminal status it must agree with")
            .kind()
            .as_message()
            .expect("TaskStatus message")
            .full_name(),
        "novarocks.TaskStatus"
    );
    assert!(
        info.get_field_by_name("operator_statistics_truncated")
            .is_some(),
        "truncation is reported explicitly, never silently"
    );
    for field in info.fields() {
        assert!(
            !matches!(field.kind(), prost_reflect::Kind::Bytes),
            "FinalTaskInfo.{} must not carry an opaque payload",
            field.name()
        );
        if let Some(message) = field.kind().as_message() {
            for forbidden in [
                "novarocks.CredentialLeaseSecretEnvelope",
                "novarocks.CredentialLeaseDescriptor",
                "novarocks.FetchResultResponse",
            ] {
                assert_ne!(
                    message.full_name(),
                    forbidden,
                    "FinalTaskInfo.{} must not carry {forbidden}",
                    field.name()
                );
            }
        }
    }
}

/// The root result poll is fenced against an exact task and process, which is
/// what the fragment-instance-addressed form it replaces could not do.
#[test]
fn the_root_result_poll_is_addressed_by_task_identity() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let request = pool
        .get_message_by_name("novarocks.FetchTaskResultRequest")
        .expect("FetchTaskResultRequest descriptor");
    assert_eq!(
        request
            .get_field_by_name("root_task")
            .expect("root task identity")
            .kind()
            .as_message()
            .expect("TaskIdentity message")
            .full_name(),
        "novarocks.TaskIdentity"
    );
    assert!(
        request.get_field_by_name("finst_id").is_none(),
        "the root result is no longer addressed by a fragment instance id"
    );
    assert!(
        request.get_field_by_name("max_wait_millis").is_some(),
        "the poll budget is a duration the backend times itself"
    );
}

/// A credential rotation keeps its non-secret descriptors and its confidential
/// envelopes in separate fields, and reports back only an epoch.
#[test]
fn a_credential_domain_separates_descriptors_from_envelopes() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let domain = pool
        .get_message_by_name("novarocks.QueryContextCredentialDomain")
        .expect("QueryContextCredentialDomain descriptor");
    assert!(domain.get_field_by_name("descriptors").expect("descriptors").is_list());
    assert!(domain.get_field_by_name("envelopes").expect("envelopes").is_list());
    assert!(domain.get_field_by_name("epoch").is_some());

    let receipt = pool
        .get_message_by_name("novarocks.QueryContextCredentialReceipt")
        .expect("QueryContextCredentialReceipt descriptor");
    let reported = receipt
        .fields()
        .map(|field| field.name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        reported,
        vec!["lease_id".to_owned(), "accepted_epoch".to_owned()],
        "a credential receipt reports only an epoch, never material or a digest of it"
    );
}

/// The unknown-transport outcome has no server-reported value on purpose: it is
/// what a client concludes when no receipt arrives at all. Reserving the name
/// keeps it from being reintroduced as something a backend can claim.
#[test]
fn the_operation_outcome_enum_reserves_the_client_only_category() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let outcome = pool
        .get_enum_by_name("novarocks.TaskOperationOutcome")
        .expect("TaskOperationOutcome descriptor");
    assert!(
        outcome
            .reserved_names()
            .any(|name| name == "TASK_OPERATION_OUTCOME_RETRYABLE_TRANSPORT_UNKNOWN"),
        "the unknown-transport name must stay reserved"
    );
    assert!(
        outcome.reserved_ranges().any(|range| range.contains(&16)),
        "the unknown-transport value must stay reserved"
    );
    // Every remaining value is a real category a backend can report.
    for expected in [
        "TASK_OPERATION_OUTCOME_ACCEPTED",
        "TASK_OPERATION_OUTCOME_IDEMPOTENT",
        "TASK_OPERATION_OUTCOME_OPERATION_TIMED_OUT",
        "TASK_OPERATION_OUTCOME_IDENTITY_MISMATCH",
        "TASK_OPERATION_OUTCOME_CREATE_CONFLICT",
        "TASK_OPERATION_OUTCOME_DOMAIN_CONFLICT",
        "TASK_OPERATION_OUTCOME_LEASE_EXPIRED",
        "TASK_OPERATION_OUTCOME_RELEASE_NOT_READY",
        "TASK_OPERATION_OUTCOME_GONE",
        "TASK_OPERATION_OUTCOME_RESOURCE_EXHAUSTED",
    ] {
        assert!(
            outcome.values().any(|value| value.name() == expected),
            "{expected} must exist"
        );
    }
}

/// A batch is a transport convenience. Its response carries one receipt per
/// item and no batch-global verdict that could collapse them.
#[test]
fn an_operation_batch_has_no_batch_global_outcome() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let request = pool
        .get_message_by_name("novarocks.ApplyTaskOperationsRequest")
        .expect("ApplyTaskOperationsRequest descriptor");
    let request_fields = request
        .fields()
        .map(|field| field.name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(request_fields, vec!["operations".to_owned()]);

    let response = pool
        .get_message_by_name("novarocks.ApplyTaskOperationsResponse")
        .expect("ApplyTaskOperationsResponse descriptor");
    let response_fields = response
        .fields()
        .map(|field| field.name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        response_fields,
        vec!["receipts".to_owned()],
        "a batch response has no shared outcome, revision, or digest"
    );

    // Every item carries its own envelope, so a batch cannot impose one
    // deadline or one identity on all of them.
    let operation = pool
        .get_message_by_name("novarocks.TaskOperation")
        .expect("TaskOperation descriptor");
    assert_eq!(
        operation
            .get_field_by_name("envelope")
            .expect("per-item envelope")
            .kind()
            .as_message()
            .expect("TaskOperationEnvelope message")
            .full_name(),
        "novarocks.TaskOperationEnvelope"
    );
}

/// Establish installs the shared facts and the initial lease together, and it
/// deliberately does not carry an expected task manifest or a task set digest.
#[test]
fn establish_installs_shared_facts_without_a_task_manifest_or_digest() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");
    let establish = pool
        .get_message_by_name("novarocks.EstablishQueryContextRequest")
        .expect("EstablishQueryContextRequest descriptor");
    for required in [
        "query_context",
        "catalog_set",
        "initial_runtime_filter",
        "initial_credential",
        "initial_lease",
    ] {
        assert!(
            establish.get_field_by_name(required).is_some(),
            "establish must install {required} atomically"
        );
    }
    for forbidden in [
        "expected_fragment_instance_ids",
        "expected_task_identities",
        "task_set_digest",
        "init_digest",
        "pre_start_timeout_ms",
        "report_endpoint",
        "query_deadline_unix_ms",
    ] {
        assert!(
            establish.get_field_by_name(forbidden).is_none(),
            "establish must not carry {forbidden}"
        );
    }

    let release = pool
        .get_message_by_name("novarocks.ReleaseQueryContextRequest")
        .expect("ReleaseQueryContextRequest descriptor");
    let release_fields = release
        .fields()
        .map(|field| field.name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        release_fields,
        vec!["query_context".to_owned()],
        "release is the frontend's closure statement, not a manifest"
    );
}
