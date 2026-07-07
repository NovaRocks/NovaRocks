use prost::Message;

use crate::proto::{common, novarocks};
use crate::runtime::profile::{ProfileUnit, RuntimeProfile};

fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

#[test]
fn runtime_profile_tree_survives_proto_roundtrip() {
    let root = RuntimeProfile::new("FragmentRoot");
    root.set_metadata(10);
    root.add_info_string("query_id", "q-1");

    let z_root = root.add_unit_counter("ZRoot");
    z_root.set(300);
    let none_counter = root.add_child_counter("NoUnitCounter", ProfileUnit::None, "ZRoot");
    none_counter.set(0);

    let total_time = root.add_timer("TotalTime");
    total_time.set(123);
    total_time.set_min(100);
    total_time.set_max(200);

    let scan_time = root.add_child_timer("ScanTime", "TotalTime");
    scan_time.set(70);
    scan_time.set_min(60);
    scan_time.set_max(90);

    let scan = root.child("SCAN (plan_node_id=1)");
    scan.set_metadata(1);
    scan.add_info_string("table", "lineitem");
    scan.counter_set_bytes("DataCacheReadBytes", 4096);

    let rows_read = scan.add_child_counter("RowsRead", ProfileUnit::Unit, "DataCacheReadBytes");
    rows_read.set(8);
    rows_read.set_min(4);
    rows_read.set_max(12);

    let exchange = root.child("EXCHANGE (plan_node_id=2)");
    exchange.set_metadata(2);
    exchange.add_info_string("partition", "HASH");
    exchange.counter_set("NetworkTime", ProfileUnit::TimeMs, 9);

    let decoded: novarocks::RuntimeProfileTree = roundtrip_message(&root.to_proto());
    let decoded_root = decoded.root.expect("profile root");
    assert_eq!(decoded_root.name, "FragmentRoot");
    assert_eq!(decoded_root.node_id, 10);
    assert_eq!(
        decoded_root.info_strings.get("query_id"),
        Some(&"q-1".to_string())
    );
    assert_eq!(
        decoded_root
            .children
            .iter()
            .map(|child| child.name.as_str())
            .collect::<Vec<_>>(),
        vec!["SCAN (plan_node_id=1)", "EXCHANGE (plan_node_id=2)"]
    );
    assert_eq!(
        decoded_root
            .counters
            .iter()
            .map(|counter| (counter.parent_name.as_str(), counter.name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("", "TotalTime"),
            ("", "ZRoot"),
            ("TotalTime", "ScanTime"),
            ("ZRoot", "NoUnitCounter"),
        ]
    );

    let root_total = decoded_root
        .counters
        .iter()
        .find(|c| c.name == "TotalTime")
        .expect("TotalTime counter");
    assert_eq!(root_total.parent_name, "");
    assert_eq!(root_total.unit, novarocks::ProfileUnit::TimeNs as i32);
    assert_eq!(root_total.value, 123);
    assert_eq!(root_total.min_value, Some(100));
    assert_eq!(root_total.max_value, Some(200));

    let root_scan_time = decoded_root
        .counters
        .iter()
        .find(|c| c.name == "ScanTime")
        .expect("ScanTime counter");
    assert_eq!(root_scan_time.parent_name, "TotalTime");
    assert_eq!(root_scan_time.unit, novarocks::ProfileUnit::TimeNs as i32);
    assert_eq!(root_scan_time.min_value, Some(60));
    assert_eq!(root_scan_time.max_value, Some(90));

    let no_unit_counter = decoded_root
        .counters
        .iter()
        .find(|c| c.name == "NoUnitCounter")
        .expect("NoUnitCounter counter");
    assert_eq!(no_unit_counter.parent_name, "ZRoot");
    assert_eq!(no_unit_counter.unit, novarocks::ProfileUnit::None as i32);

    let scan_node = decoded_root
        .children
        .iter()
        .find(|child| child.name == "SCAN (plan_node_id=1)")
        .expect("scan child");
    assert_eq!(scan_node.node_id, 1);
    assert_eq!(
        scan_node.info_strings.get("table"),
        Some(&"lineitem".to_string())
    );
    let scan_bytes = scan_node
        .counters
        .iter()
        .find(|c| c.name == "DataCacheReadBytes")
        .expect("DataCacheReadBytes counter");
    assert_eq!(scan_bytes.parent_name, "");
    assert_eq!(scan_bytes.unit, novarocks::ProfileUnit::Bytes as i32);
    assert_eq!(scan_bytes.value, 4096);

    let rows_read = scan_node
        .counters
        .iter()
        .find(|c| c.name == "RowsRead")
        .expect("RowsRead counter");
    assert_eq!(rows_read.parent_name, "DataCacheReadBytes");
    assert_eq!(rows_read.unit, novarocks::ProfileUnit::Unit as i32);
    assert_eq!(rows_read.value, 8);
    assert_eq!(rows_read.min_value, Some(4));
    assert_eq!(rows_read.max_value, Some(12));

    let exchange_node = decoded_root
        .children
        .iter()
        .find(|child| child.name == "EXCHANGE (plan_node_id=2)")
        .expect("exchange child");
    assert_eq!(
        exchange_node.info_strings.get("partition"),
        Some(&"HASH".to_string())
    );
    let network_time = exchange_node
        .counters
        .iter()
        .find(|c| c.name == "NetworkTime")
        .expect("NetworkTime counter");
    assert_eq!(network_time.unit, novarocks::ProfileUnit::TimeMs as i32);
    assert_eq!(network_time.value, 9);
}

#[test]
fn exec_status_report_survives_proto_roundtrip() {
    let report = novarocks::ExecStatusReport {
        query_id: Some(common::UniqueId { hi: 11, lo: 12 }),
        fragment_instance_id: Some(common::UniqueId { hi: 21, lo: 22 }),
        backend_num: 3,
        status: Some(common::Status {
            code: 0,
            message: String::new(),
        }),
        done: true,
        iceberg_commits: vec![novarocks::IcebergCommitInfo {
            iceberg_data_file: Some(novarocks::IcebergDataFile {
                path: Some("s3://warehouse/db/t/data-1.parquet".to_string()),
                format: Some("parquet".to_string()),
                record_count: Some(9),
                file_size_in_bytes: Some(90),
                partition_path: Some("region=us".to_string()),
                split_offsets: Some(novarocks::Int64List { values: vec![4, 8] }),
                column_stats: Some(novarocks::IcebergColumnStats {
                    column_sizes: [(1, 100)].into_iter().collect(),
                    value_counts: [(1, 9)].into_iter().collect(),
                    null_value_counts: [(1, 0)].into_iter().collect(),
                    nan_value_counts: [(1, 0)].into_iter().collect(),
                    lower_bounds: [(1, vec![0x01])].into_iter().collect(),
                    upper_bounds: [(1, vec![0x09])].into_iter().collect(),
                }),
                partition_null_fingerprint: Some("0".to_string()),
                file_content: novarocks::IcebergFileContent::Data as i32,
                referenced_data_file: Some("s3://warehouse/db/t/base.parquet".to_string()),
                first_row_id: Some(77),
                equality_ids: Some(novarocks::Int32List { values: vec![1, 2] }),
                key_metadata: Some(vec![0xaa, 0xbb]),
                partition_spec_id: Some(5),
                partition_values_descriptor: Some(novarocks::IcebergPartitionDescriptor {
                    values: vec![novarocks::IcebergPartitionValue {
                        is_null: Some(false),
                        datum_bytes: Some(b"us".to_vec()),
                    }],
                }),
                content_offset: Some(128),
                content_size_in_bytes: Some(256),
                cardinality: Some(4),
            }),
            is_overwrite: Some(true),
            is_rewrite: Some(false),
        }],
        loaded_rows: 9,
        sink_load_bytes: 90,
        filtered_rows: 1,
        profile: Some(RuntimeProfile::new("FragmentRoot").to_proto()),
    };

    let decoded: novarocks::ExecStatusReport = roundtrip_message(&report);
    assert_eq!(decoded.query_id.expect("query id").hi, 11);
    assert_eq!(
        decoded
            .fragment_instance_id
            .expect("fragment instance id")
            .lo,
        22
    );
    assert_eq!(decoded.backend_num, 3);
    assert_eq!(decoded.status.expect("status").code, 0);
    assert!(decoded.done);
    assert_eq!(decoded.loaded_rows, 9);
    assert_eq!(decoded.sink_load_bytes, 90);
    assert_eq!(decoded.filtered_rows, 1);
    assert!(decoded.profile.and_then(|tree| tree.root).is_some());

    let commit = decoded.iceberg_commits.into_iter().next().expect("commit");
    assert_eq!(commit.is_overwrite, Some(true));
    assert_eq!(commit.is_rewrite, Some(false));
    let data_file = commit.iceberg_data_file.expect("data file");
    assert_eq!(
        data_file.path.as_deref(),
        Some("s3://warehouse/db/t/data-1.parquet")
    );
    assert_eq!(data_file.format.as_deref(), Some("parquet"));
    assert_eq!(data_file.record_count, Some(9));
    assert_eq!(data_file.file_size_in_bytes, Some(90));
    assert_eq!(data_file.partition_spec_id, Some(5));
    assert_eq!(
        data_file.file_content,
        novarocks::IcebergFileContent::Data as i32
    );
    assert_eq!(
        data_file.split_offsets.expect("split offsets").values,
        vec![4, 8]
    );
    assert_eq!(
        data_file.equality_ids.expect("equality ids").values,
        vec![1, 2]
    );
    assert_eq!(data_file.key_metadata, Some(vec![0xaa, 0xbb]));
    assert_eq!(data_file.content_size_in_bytes, Some(256));
    let stats = data_file.column_stats.expect("column stats");
    assert_eq!(stats.column_sizes.get(&1), Some(&100));
    assert_eq!(stats.lower_bounds.get(&1), Some(&vec![0x01]));
    let partition = data_file
        .partition_values_descriptor
        .expect("partition descriptor");
    assert_eq!(partition.values.len(), 1);
    assert_eq!(partition.values[0].is_null, Some(false));
    assert_eq!(partition.values[0].datum_bytes, Some(b"us".to_vec()));
}
