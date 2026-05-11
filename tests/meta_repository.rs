use bytes::Bytes;
use novarocks::meta::MetaKey;
use novarocks::meta::repository::{
    RepositoryError, decode_json_payload, encode_json_payload, id_scopes,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SamplePayload {
    id: i64,
    name: String,
}

#[test]
fn repository_payload_json_round_trips() {
    let payload = SamplePayload {
        id: 7,
        name: "orders".to_string(),
    };
    let encoded = encode_json_payload(1, &payload).expect("encode payload");
    assert_eq!(encoded.schema_version, 1);
    assert_eq!(
        encoded.bytes,
        Bytes::from_static(br#"{"id":7,"name":"orders"}"#)
    );

    let decoded: SamplePayload = decode_json_payload(&encoded).expect("decode payload");
    assert_eq!(decoded, payload);
}

#[test]
fn repository_id_scopes_are_stable_strings() {
    assert_eq!(id_scopes::managed_db().as_str(), "managed.db");
    assert_eq!(id_scopes::managed_table().as_str(), "managed.table");
    assert_eq!(id_scopes::managed_partition().as_str(), "managed.partition");
    assert_eq!(id_scopes::managed_index().as_str(), "managed.index");
    assert_eq!(id_scopes::managed_tablet().as_str(), "managed.tablet");
    assert_eq!(id_scopes::managed_txn().as_str(), "managed.txn");
    assert_eq!(id_scopes::mv_id().as_str(), "mv.id");
    assert_eq!(id_scopes::refresh_id().as_str(), "refresh.id");
    assert_eq!(id_scopes::erase_job().as_str(), "job.erase");
    assert_eq!(
        id_scopes::iceberg_optimize_job().as_str(),
        "job.iceberg_optimize"
    );
}

#[test]
fn repository_error_display_is_domain_facing() {
    let err = RepositoryError::conflict("managed txn state changed");
    assert_eq!(
        err.to_string(),
        "metadata repository conflict: managed txn state changed"
    );
}

#[test]
fn key_helpers_reject_unescaped_path_separators() {
    let err = MetaKey::new("managed", ["table", "bad/name"]).expect_err("slash must fail");
    assert!(
        err.to_string()
            .contains("invalid metadata key path segment")
    );
}
