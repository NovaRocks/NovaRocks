// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use super::*;
use serde_json::json;

const GOLDEN_ID: &str = "b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e";
const GOLDEN_VERSION: &str = r#"{"v":2,"owner":"novarocks.mv","name":"definition","format":{"owner":"novarocks.mv","name":"definition","version":1},"refs":[],"content":"YWJj","id":"b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e"}"#;
const B_HEX: &str = "080112240a07696365626572671215636f6e6e6563746f722d736e617073686f742d69641801200912240a07696365626572671215636f6e6e6563746f722d736e617073686f742d696418012009";
const P_HEX: &str = "0111111111111111111111111111111111000000000000002a010000000000000007000200090100000000000000110009010000000000000011";
fn bytes(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixture.json")).unwrap()
}
fn frame(id: u8) -> String {
    let f = fixture();
    let mut raw = bytes(P_HEX);
    raw[1..17].fill(id);
    format!(
        "2.{}.{}",
        f["binding"].as_str().unwrap(),
        wire::base64url(&raw)
    )
}
fn snapshot(id: i64, parent: Option<i64>, operation: &str, frame: Option<String>) -> Value {
    let mut summary = json!({"operation":operation,"total-data-files":"1","total-delete-files":"0","total-records":"7","added-data-files":"0","added-records":"0"});
    if let Some(frame) = frame {
        summary[wire::PUBLICATION_KEY] = json!(frame);
    }
    if operation == "overwrite" {
        summary["added-data-files"] = json!("1");
        summary["deleted-data-files"] = json!("1");
    }
    let mut value = json!({"snapshot-id":id,"sequence-number":id,"summary":summary});
    if let Some(parent) = parent {
        value["parent-snapshot-id"] = json!(parent);
    }
    value
}
fn response(snapshots: Vec<Value>) -> Value {
    let mut f = fixture();
    let mut seen = BTreeSet::new();
    let mut latest_slot = None;
    for snapshot in &snapshots {
        let frame = snapshot["summary"][wire::PUBLICATION_KEY].as_str();
        if frame.is_none_or(|p| !seen.insert(p.to_string())) {
            latest_slot = snapshot["snapshot-id"].as_i64();
        }
    }
    if let Some(id) = latest_slot {
        f["properties"]["novarocks.doc.v2.slot.novarocks.mv~configuration"] =
            f["configuration_writers"][id.to_string()].clone();
    }
    let current = snapshots
        .last()
        .map(|s| s["snapshot-id"].clone())
        .unwrap_or(Value::Null);
    json!({"metadata":{"properties":f["properties"],"snapshots":snapshots,"current-snapshot-id":current,"refs":{"main":{"snapshot-id":current}}}})
}
#[test]
fn manual_version_identity_and_changed_rejections() {
    let key = format!("novarocks.doc.v2.rev.{GOLDEN_ID}");
    let mut p = json!({});
    p[&key] = json!(GOLDEN_VERSION);
    assert_eq!(
        wire::properties(&p).unwrap().versions[GOLDEN_ID].content,
        b"abc"
    );
    for raw in [
        format!(" {GOLDEN_VERSION}"),
        GOLDEN_VERSION.replace("YWJj", "YWJk"),
        GOLDEN_VERSION.replace("YWJj", "YWJj="),
        GOLDEN_VERSION.replace("\"v\":2", "\"v\":2,\"v\":2"),
    ] {
        p[&key] = json!(raw);
        assert!(wire::properties(&p).is_err());
    }
}
#[test]
fn manual_binding_and_compact_publication() {
    let b = wire::binding(&bytes(B_HEX)).unwrap();
    assert_eq!(b.len(), 2);
    assert_eq!(b[0], b[1]);
    assert_eq!(b[0].provider, "iceberg");
    assert_eq!(b[0].name, "connector-snapshot-id");
    assert_eq!(b[0].version, 1);
    assert_eq!(b[0].maximum, 9);
    let p = wire::publication(&bytes(P_HEX), &b).unwrap();
    assert_eq!(p.id, [0x11; 16]);
    assert_eq!(p.rows, Some(7));
    assert!(wire::publication(&bytes(P_HEX), &b[..1]).is_err());
    let mut trailing = bytes(P_HEX);
    trailing.push(0);
    assert!(wire::publication(&trailing, &b).is_err());
    let mut bounded = b.clone();
    bounded[0].maximum = 8;
    assert!(wire::publication(&bytes(P_HEX), &bounded).is_err());
    let mut unknown = bytes(B_HEX);
    unknown.extend([0x18, 1]);
    assert!(wire::binding(&unknown).is_err());
}

#[test]
fn large_inline_version_and_private_properties_keep_their_exact_content() {
    use sha2::{Digest, Sha256};

    // This size crosses the former document and private-property quotas;
    // it is a test input, not a supported-size declaration.
    let content = vec![b'x'; 196_609];
    let mut identity = Sha256::new();
    identity.update(b"novarocks-document-identity/v2\0");
    for part in ["novarocks.mv", "definition", "novarocks.mv", "definition"] {
        identity.update((part.len() as u32).to_be_bytes());
        identity.update(part.as_bytes());
    }
    identity.update(1u32.to_be_bytes());
    identity.update(0u32.to_be_bytes());
    identity.update((content.len() as u64).to_be_bytes());
    identity.update(&content);
    let id = wire::hex(&identity.finalize());
    let raw = GOLDEN_VERSION
        .replace("YWJj", &wire::base64url(&content))
        .replace(GOLDEN_ID, &id);
    assert!(raw.len() > 65_536);
    let key = format!("novarocks.doc.v2.rev.{id}");
    let mut properties = json!({});
    properties[&key] = json!(raw);
    let decoded = wire::properties(&properties).unwrap();
    assert!(decoded.private_bytes > 262_144);
    assert_eq!(decoded.versions[&id].content, content);

    let mut corrupt = properties;
    let raw = corrupt[&key].as_str().unwrap().to_string();
    corrupt[&key] = json!(raw.replacen("eHh4", "eHh5", 1));
    assert!(wire::properties(&corrupt).is_err());
}

#[test]
fn large_publication_keeps_the_binding_count_and_declared_value_bounds() {
    let formats = vec![wire::binding(&bytes(B_HEX)).unwrap()[0].clone(); 370];
    let mut payload = bytes(P_HEX)[..34].to_vec();
    payload.extend_from_slice(&370u16.to_be_bytes());
    for _ in &formats {
        payload.extend_from_slice(&9u16.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&17i64.to_be_bytes());
    }
    assert!(payload.len() > 4_096);
    let framed = format!("2.{GOLDEN_ID}.{}", wire::base64url(&payload));
    let mut summary = json!({});
    summary[wire::PUBLICATION_KEY] = json!(framed);
    let records = wire::records(&summary).unwrap();
    let decoded = wire::record(&records[wire::PUBLICATION_KEY]).unwrap();
    assert_eq!(decoded.binding.as_deref(), Some(GOLDEN_ID));
    assert_eq!(decoded.payload, payload);
    assert_eq!(
        wire::publication(&decoded.payload, &formats).unwrap().rows,
        Some(7)
    );
    let mut narrower = formats;
    narrower[0].maximum = 8;
    assert!(wire::publication(&decoded.payload, &narrower).is_err());
}
#[test]
fn two_publications_share_binding_and_current_properties() {
    let r = response(vec![
        snapshot(1, None, "append", Some(frame(0x11))),
        snapshot(2, Some(1), "append", Some(frame(0x22))),
    ]);
    assert!(
        verify_graph(
            &r,
            &parse_expectation("ns.mv,publications=2,slot-changes=0").unwrap()
        )
        .is_ok()
    );
    let mut missing = r.clone();
    let key = format!(
        "novarocks.doc.v2.rev.{}",
        fixture()["interpretation"].as_str().unwrap()
    );
    missing["metadata"]["properties"]
        .as_object_mut()
        .unwrap()
        .remove(&key);
    assert!(
        verify_graph(
            &missing,
            &parse_expectation("ns.mv,publications=2").unwrap()
        )
        .unwrap_err()
        .to_string()
        .contains(&key)
    );
}
#[test]
fn carrying_snapshot_is_not_another_publication() {
    let r = response(vec![
        snapshot(1, None, "append", Some(frame(0x11))),
        snapshot(2, Some(1), "replace", Some(frame(0x11))),
    ]);
    assert!(
        verify_graph(
            &r,
            &parse_expectation("ns.mv,publications=1,slot-changes=1").unwrap()
        )
        .is_ok()
    );
    assert!(
        verify_graph(
            &r,
            &parse_expectation("ns.mv,publications=2,slot-changes=0").unwrap()
        )
        .is_err()
    );
    let mut wrong = r.clone();
    wrong["metadata"]["snapshots"][1]["summary"]["operation"] = json!("append");
    assert!(
        verify_graph(
            &wrong,
            &parse_expectation("ns.mv,publications=1,slot-changes=1").unwrap()
        )
        .is_err()
    );
}
#[test]
fn empty_first_slot_then_first_publication() {
    let mut empty = snapshot(1, None, "append", None);
    empty["summary"]["total-data-files"] = json!("0");
    empty["summary"]["total-records"] = json!("0");
    let r = response(vec![
        empty,
        snapshot(2, Some(1), "append", Some(frame(0x11))),
    ]);
    assert!(
        verify_graph(
            &r,
            &parse_expectation("ns.mv,publications=1,slot-changes=1").unwrap()
        )
        .is_ok()
    );
}
#[test]
fn new_publication_must_not_be_replace() {
    let r = response(vec![snapshot(1, None, "replace", Some(frame(0x11)))]);
    assert!(verify_graph(&r, &parse_expectation("ns.mv,publications=1").unwrap()).is_err());
}
#[test]
fn directives_reject_retired_and_duplicate_options() {
    assert!(parse_expectation("ns.mv,publications=2,deferred-sidecars-min=1").is_err());
    assert!(parse_expectation("ns.mv,publications=1,slot-changes=1,slot-changes=1").is_err());
    assert!(parse_expectation("ns.mv,publications=1,slot-changes=2,table-commits=3").is_err());
    assert_eq!(
        parse_expectation("ns.mv,publications=1,slot-changes=2,table-commits=4")
            .unwrap()
            .slot_changes,
        2
    );
}
#[test]
fn logical_rows_are_distinct_from_physical_records() {
    let mut r = response(vec![snapshot(1, None, "append", Some(frame(0x11)))]);
    r["metadata"]["snapshots"][0]["summary"]["total-records"] = json!("9");
    assert!(
        verify_graph(
            &r,
            &parse_expectation("ns.mv,publications=1,logical-rows=7,total-records=9").unwrap()
        )
        .is_ok()
    );
    assert!(
        verify_graph(
            &r,
            &parse_expectation("ns.mv,publications=1,logical-rows=9,total-records=9").unwrap()
        )
        .is_err()
    );
}
#[test]
fn each_check_has_table_and_warehouse_document_prefixes() {
    let metadata = json!({"location":"s3://bucket/private/rest/_novarocks/ctas-staging/table"});
    let prefixes = document_prefixes(&metadata, "s3://bucket/private/rest").unwrap();
    assert_eq!(
        prefixes,
        [
            (
                "bucket",
                "private/rest/_novarocks/ctas-staging/table/metadata/novarocks-documents/".into()
            ),
            ("bucket", "private/rest/_novarocks/document-staging/".into())
        ]
    );
    assert!(document_prefixes(&metadata, "s3://bucket/other").is_err());
}
#[test]
fn manual_slot_and_marker_integrity() {
    let slot = r#"{"v":2,"owner":"novarocks.mv","name":"configuration","format":{"owner":"novarocks.mv","name":"definition","version":1},"writer":7,"content":"YWJj","digest":"cc51b62de09961dd9e155df07e266acbf053af1195934292386054f37fdfe689"}"#;
    let marker = r#"{"v":2,"owner":"novarocks.mv","name":"pending","binding":"attempt-1","format":{"owner":"novarocks.mv","name":"definition","version":1},"content":"YWJj","digest":"52ba21e0ca6e5859ebcd762f8817d0ee782471aaff67999b086c5df63d9b0f39"}"#;
    let mut p = json!({"novarocks.doc.v2.slot.novarocks.mv~configuration":slot,"novarocks.doc.v2.mark.novarocks.mv~pending~attempt-1":marker});
    assert_eq!(
        wire::properties(&p).unwrap().slots["novarocks.mv~configuration"].writer,
        Some(7)
    );
    p["novarocks.doc.v2.slot.novarocks.mv~configuration"] =
        json!(slot.replace("\"writer\":7", "\"writer\":8"));
    assert!(wire::properties(&p).is_err());
}
#[test]
fn repartition_resolves_old_interpretation_in_current_pool() {
    let f = fixture();
    let replacement = &f["repartition"];
    let mut second = frame(0x22);
    second = second.replacen(
        f["binding"].as_str().unwrap(),
        replacement["binding"].as_str().unwrap(),
        1,
    );
    let mut r = response(vec![
        snapshot(1, None, "append", Some(frame(0x11))),
        snapshot(2, Some(1), "overwrite", Some(second)),
    ]);
    let props = &mut r["metadata"]["properties"];
    props[format!(
        "novarocks.doc.v2.rev.{}",
        replacement["interpretation"].as_str().unwrap()
    )] = replacement["interpretation_value"].clone();
    props[format!(
        "novarocks.doc.v2.rev.{}",
        replacement["binding"].as_str().unwrap()
    )] = replacement["binding_value"].clone();
    props["novarocks.doc.v2.root.novarocks.mv"] = replacement["binding"].clone();
    r["metadata"]["metadata-log"] = json!([{"metadata-file":"s3://unreadable/old-metadata.json"}]);
    let expectation =
        parse_expectation("ns.mv,publications=2,interpretation-changes=true").unwrap();
    assert!(verify_graph(&r, &expectation).is_ok());
    r["metadata"]["properties"]
        .as_object_mut()
        .unwrap()
        .remove(&format!(
            "novarocks.doc.v2.rev.{}",
            f["interpretation"].as_str().unwrap()
        ));
    assert!(verify_graph(&r, &expectation).is_err());
}
#[test]
fn slot_must_carry_other_owners_and_write_its_snapshot_id() {
    let mut r = response(vec![
        snapshot(1, None, "append", Some(frame(0x11))),
        snapshot(2, Some(1), "replace", Some(frame(0x11))),
    ]);
    r["metadata"]["snapshots"][0]["summary"]["novarocks.doc.v2.record.another~audit"] =
        json!("2.-.YQ");
    let e = parse_expectation("ns.mv,publications=1,slot-changes=1").unwrap();
    assert!(verify_graph(&r, &e).is_err());
    r["metadata"]["snapshots"][1]["summary"]["novarocks.doc.v2.record.another~audit"] =
        json!("2.-.YQ");
    assert!(verify_graph(&r, &e).is_ok());
    r["metadata"]["properties"]["novarocks.doc.v2.slot.novarocks.mv~configuration"] =
        fixture()["properties"]["novarocks.doc.v2.slot.novarocks.mv~configuration"].clone();
    assert!(verify_graph(&r, &e).is_err());
}
#[test]
fn zero_file_publication_is_append_and_metadata_only_cannot_add_dv() {
    let mut first = snapshot(1, None, "append", Some(frame(0x11)));
    first["summary"]["operation"] = json!("overwrite");
    assert!(
        verify_graph(
            &response(vec![first]),
            &parse_expectation("ns.mv,publications=1").unwrap()
        )
        .is_err()
    );
    let mut second = snapshot(2, Some(1), "append", Some(frame(0x22)));
    second["summary"]["added-delete-files"] = json!("1");
    assert!(
        verify_graph(
            &response(vec![snapshot(1, None, "append", Some(frame(0x11))), second]),
            &parse_expectation("ns.mv,publications=2,metadata-only-last=true").unwrap()
        )
        .is_err()
    );
}
