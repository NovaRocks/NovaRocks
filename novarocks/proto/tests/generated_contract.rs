use prost::Message;
use prost_reflect::DescriptorPool;

use novarocks_proto::{
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
