use prost::Message;
use prost_reflect::DescriptorPool;

use novarocks_proto_models::{
    FILE_DESCRIPTOR_SET, SCHEMA_LEDGER_VERSION, common, expr, filter, novarocks, plan,
};

#[test]
fn generated_dtos_and_descriptor_match_the_native_schema_contract() {
    assert_eq!(SCHEMA_LEDGER_VERSION, 1);

    let _ = common::UniqueId::default();
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
fn retired_terminal_self_attestation_fields_remain_reserved() {
    let pool =
        DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("protocol descriptor set must decode");

    for (message_name, field_number) in [
        ("novarocks.QueryTerminalSnapshot", 5),
        ("novarocks.TerminalizationProof", 5),
        ("novarocks.NegativeAttestation", 7),
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
            message.reserved_names().any(|name| name == "digest"),
            "{message_name} digest name must remain reserved"
        );
        assert!(
            message.fields().all(|field| field.number() != field_number),
            "{message_name} must not reuse retired digest tag {field_number}"
        );
        assert!(
            message.fields().all(|field| field.name() != "digest"),
            "{message_name} must not reuse retired digest name"
        );
    }
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
        ],
        "PARTITIONS is a view over the pinned FILES relation, and there is no \
         ALL_* or unknown worker variant"
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
