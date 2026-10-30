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

// Functional tests for the private existing-serializer projection.
use super::*;
use crate::iceberg::spec::{
    FormatVersion, ListType, Literal, Map, MapType, NestedField, Operation, PartitionSpec,
    PrimitiveType, Schema, Snapshot, SortOrder, Summary, TableMetadataBuilder, Type,
};
use novarocks_spi::connector::{ConnectorErrorKind, ConnectorInstanceId};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
struct Witness(Arc<AtomicUsize>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Scope {
    stop: AtomicBool,
    guard: ConnectorPayloadRetentionGuard,
}
impl OriginalScope for Scope {
    fn check_active(&self) -> Result<(), ConnectorError> {
        if self.stop.load(Ordering::SeqCst) {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "original stopped projection",
            ))
        } else {
            Ok(())
        }
    }

    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.guard.clone()
    }
}
fn scope() -> (Scope, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Scope {
            stop: AtomicBool::new(false),
            guard: ConnectorPayloadRetentionGuard::new(Witness(drops.clone())),
        },
        drops,
    )
}
// Keep each stock format lawful: only v3 can backfill old rows from a
// non-null initial default; write defaults remain covered for every version.
fn with_initial_default_for_version(
    field: NestedField,
    default: Literal,
    version: FormatVersion,
) -> NestedField {
    if version == FormatVersion::V3 {
        field.with_initial_default(default)
    } else {
        field
    }
}

fn metadata(version: FormatVersion) -> TableMetadata {
    let map = Type::Map(MapType {
        key_field: Arc::new(NestedField::map_key_element(2, PrimitiveType::Int.into())),
        value_field: Arc::new(with_initial_default_for_version(
            NestedField::map_value_element(3, PrimitiveType::String.into(), false),
            Literal::string("synthetic map child"),
            version,
        )),
    });
    let default = Literal::Map(Map::from([
        (
            Literal::int(9),
            Some(Literal::string("nine\nquoted \"name\"")),
        ),
        (Literal::int(1), None),
    ]));
    let list = Type::List(ListType {
        element_field: Arc::new(with_initial_default_for_version(
            NestedField::list_element(6, PrimitiveType::Long.into(), false)
                .with_write_default(Literal::long(8)),
            Literal::long(7),
            version,
        )),
    });
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(with_initial_default_for_version(
                NestedField::optional(1, "attrs", map).with_write_default(default.clone()),
                default,
                version,
            )),
            Arc::new(with_initial_default_for_version(
                NestedField::optional(4, "nan", PrimitiveType::Double.into()),
                Literal::double(f64::NAN),
                version,
            )),
            Arc::new(NestedField::optional(5, "items", list)),
        ])
        .build()
        .expect("valid small stock schema");
    let base = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "memory://warehouse/db/defaults".to_string(),
        version,
        Default::default(),
    )
    .expect("stock metadata builder")
    .build()
    .expect("stock metadata")
    .metadata;
    let snapshot = Snapshot::builder()
        .with_snapshot_id(41)
        .with_sequence_number(if version == FormatVersion::V1 { 0 } else { 1 })
        .with_schema_id(base.current_schema_id())
        .with_timestamp_ms(base.last_updated_ms())
        .with_manifest_list("memory://warehouse/db/defaults/snap-41.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: Default::default(),
        })
        .with_row_range(base.next_row_id(), 1)
        .build();
    base.into_builder(None)
        .add_snapshot(snapshot)
        .expect("real snapshot admission")
        .build()
        .expect("snapshot metadata")
        .metadata
}
#[test]
fn stock_v1_v2_v3_actual_frozen_source_and_reader_defaults_are_unchanged() {
    for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
        let metadata = metadata(version);
        let schema = metadata
            .snapshot_by_id(41)
            .expect("actual snapshot")
            .schema(&metadata)
            .expect("snapshot schema");
        let (scope, _) = scope(); // Original control/retention component; it grants no capacity.
        let mut checked = Plan::inspect(&metadata, &schema, &scope)
            .expect("borrowed count")
            .for_copies(1, &scope)
            .expect("component checker");
        let projection = checked.build_one().expect("same original serializers");
        let catalog = ConnectorInstanceId::parse("iceberg").expect("catalog");
        let mut old = crate::metadata::frozen_copy_on_write_source_payload(
            &catalog,
            "db",
            "defaults",
            &metadata,
            41,
            crate::scan_model::IcebergDataFileInfo::for_test("memory://data.parquet", 80, 1),
        )
        .expect("actual original stock frozen source");
        let original_wire = serde_json::to_vec(&old).expect("finite original payload");
        let old_schema = old.table_info.as_ref().expect("table info").schema.clone();
        let info = old.table_info.as_mut().expect("table info");
        assert_eq!(
            info.serialized_metadata.as_deref(),
            Some(projection.metadata_json())
        );
        // New raw defaults are None only in this PRIVATE schema projection;
        // original public schema_facts and borrowed NestedField remain intact.
        info.schema = projection.schema().clone();
        assert_eq!(
            serde_json::to_vec(&old).expect("candidate frozen wire"),
            original_wire
        );
        let arrow =
            Arc::new(crate::iceberg::arrow::schema_to_arrow_schema(&schema).expect("stock Arrow"));
        let old_read =
            crate::schema_mapping::annotate_read_schema_from_scan_model(&arrow, &old_schema)
                .expect("original actual reader defaults");
        let new_read = crate::schema_mapping::annotate_read_schema_from_scan_model(
            &arrow,
            projection.schema(),
        )
        .expect("candidate actual reader defaults");
        assert_eq!(new_read, old_read);
        assert_eq!(
            projection.schema.fields[0].children[1]
                .initial_default_json
                .as_deref(),
            (version == FormatVersion::V3).then_some("\"synthetic map child\"")
        );
        assert_eq!(
            projection.schema.fields[1].initial_default_json.as_deref(),
            (version == FormatVersion::V3).then_some("null")
        );
        assert_eq!(
            projection.schema.fields[2].children[0]
                .initial_default_json
                .as_deref(),
            (version == FormatVersion::V3).then_some("7")
        );
        assert_eq!(
            projection.schema.fields[2].children[0]
                .write_default_json
                .as_deref(),
            Some("8")
        );
    }
}
#[test]
fn same_original_guard_stays_until_the_last_actual_projection_exits() {
    let metadata = metadata(FormatVersion::V3);
    let schema = metadata
        .snapshot_by_id(41)
        .unwrap()
        .schema(&metadata)
        .unwrap();
    let (scope, drops) = scope();
    let (first, last) = {
        let mut checked = Plan::inspect(&metadata, &schema, &scope)
            .unwrap()
            .for_copies(2, &scope)
            .unwrap();
        (checked.build_one().unwrap(), checked.build_one().unwrap())
    };
    drop(scope);
    drop(metadata);
    drop(schema);
    drop(first);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(last.metadata_json().contains("schemas"));
    drop(last);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    // This is opaque same-holder evidence. No Work/Native join or real window claim.
}
#[test]
fn exact_copy_count_refuses_an_extra_output_without_minting_a_guard() {
    let metadata = metadata(FormatVersion::V2);
    let schema = metadata
        .snapshot_by_id(41)
        .unwrap()
        .schema(&metadata)
        .unwrap();
    let (scope, drops) = scope();
    let (first, second) = {
        let mut checked = Plan::inspect(&metadata, &schema, &scope)
            .unwrap()
            .for_copies(2, &scope)
            .unwrap();
        let first = checked.build_one().unwrap();
        let second = checked.build_one().unwrap();
        assert!(matches!(checked.build_one(), Err(Error::ShapeChanged)));
        (first, second)
    };
    drop(scope);
    drop(first);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(second);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn stop_before_output_prevents_a_late_output_and_preserves_original_error() {
    let metadata = metadata(FormatVersion::V2);
    let schema = metadata
        .snapshot_by_id(41)
        .unwrap()
        .schema(&metadata)
        .unwrap();
    let (scope, _) = scope();
    let mut checked = Plan::inspect(&metadata, &schema, &scope)
        .unwrap()
        .for_copies(1, &scope)
        .unwrap();
    scope.stop.store(true, Ordering::SeqCst);
    match checked.build_one().err().expect("original stop") {
        Error::Control(error) => assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "original stopped projection"
            )
        ),
        _ => panic!("original control error lost"),
    }
}

struct MidWriteScope {
    checks: std::cell::Cell<usize>,
    original: std::cell::RefCell<Option<ConnectorError>>,
    guard: ConnectorPayloadRetentionGuard,
}
impl OriginalScope for MidWriteScope {
    fn check_active(&self) -> Result<(), ConnectorError> {
        self.checks.set(self.checks.get() + 1);
        if self.checks.get() >= 4 {
            return Err(self
                .original
                .borrow_mut()
                .take()
                .expect("write_all retried an original cancellation"));
        }
        Ok(())
    }

    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.guard.clone()
    }
}
#[test]
fn actual_serde_write_all_midwrite_cancellation_returns_first_error_without_retry() {
    let error = ConnectorError::new(ConnectorErrorKind::Cancelled, "original-midwrite-stop")
        .with_cleanup_context("original-provider-context");
    let message_ptr = error.message().as_ptr();
    let (ordinary, _) = scope();
    let original = MidWriteScope {
        checks: std::cell::Cell::new(0),
        original: std::cell::RefCell::new(Some(error)),
        guard: ordinary.guard.clone(),
    };
    let mut output = Vec::with_capacity(128);
    let result = serialize(
        &["first", "second", "third"],
        &original,
        Some(&mut output),
        128,
    );
    assert!(
        !output.is_empty(),
        "real output prefix preceded cancellation"
    );
    assert_eq!(original.checks.get(), 4, "cancelled writes must not retry");
    match result {
        Err(Error::Control(raw)) => {
            assert_eq!(raw.message().as_ptr(), message_ptr);
            assert_eq!(raw.kind(), ConnectorErrorKind::Cancelled);
            assert!(raw.to_string().contains("original-provider-context"));
        }
        _ => panic!("original cancellation was replaced"),
    }
}
#[test]
fn subsequent_direct_writer_call_cannot_replace_or_recheck_first_cancellation() {
    use std::io::Write;
    let error = ConnectorError::new(ConnectorErrorKind::DeadlineExceeded, "original-expiry");
    let message_ptr = error.message().as_ptr();
    let (ordinary, _) = scope();
    let original = MidWriteScope {
        checks: std::cell::Cell::new(3),
        original: std::cell::RefCell::new(Some(error)),
        guard: ordinary.guard.clone(),
    };
    let mut output = Vec::with_capacity(128);
    let mut writer = Writer {
        scope: &original,
        original: None,
        length: 0,
        output: Some(&mut output),
        ceiling: 128,
        changed: false,
    };
    assert_eq!(
        writer.write(b"first").unwrap_err().kind(),
        std::io::ErrorKind::Other
    );
    assert_eq!(
        writer.write(b"second").unwrap_err().kind(),
        std::io::ErrorKind::Other
    );
    assert_eq!(original.checks.get(), 4);
    assert_eq!(writer.original.unwrap().message().as_ptr(), message_ptr);
    assert!(output.is_empty());
}

struct OpaqueCause {
    drops: Arc<AtomicUsize>,
}
impl std::fmt::Debug for OpaqueCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("original opaque cause must not be formatted")
    }
}
impl std::fmt::Display for OpaqueCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("original opaque cause must not be formatted")
    }
}
impl Drop for OpaqueCause {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct OpaqueScope {
    checks: std::cell::Cell<usize>,
    cause: std::cell::RefCell<Option<Box<OpaqueCause>>>,
    guard: ConnectorPayloadRetentionGuard,
}
impl OriginalScope<Box<OpaqueCause>> for OpaqueScope {
    fn check_active(&self) -> Result<(), Box<OpaqueCause>> {
        self.checks.set(self.checks.get() + 1);
        if self.checks.get() == 4 {
            return Err(self
                .cause
                .borrow_mut()
                .take()
                .expect("original cause consumed once"));
        }
        assert!(
            self.checks.get() < 4,
            "serializer retried after cancellation"
        );
        Ok(())
    }

    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.guard.clone()
    }
}
#[test]
fn actual_serde_midwrite_moves_opaque_cause_once_without_formatting_or_retry() {
    let drops = Arc::new(AtomicUsize::new(0));
    let cause = Box::new(OpaqueCause {
        drops: drops.clone(),
    });
    let identity = (&*cause) as *const OpaqueCause;
    let (ordinary, _) = scope();
    let original = OpaqueScope {
        checks: std::cell::Cell::new(0),
        cause: std::cell::RefCell::new(Some(cause)),
        guard: ordinary.guard.clone(),
    };
    let mut output = Vec::with_capacity(128);
    let result = serialize(
        &["first", "second", "third"],
        &original,
        Some(&mut output),
        128,
    );
    assert!(!output.is_empty());
    assert_eq!(original.checks.get(), 4);
    match result {
        Err(error) => {
            assert_eq!(format!("{error:?}"), "Control");
            assert_eq!(format!("{error}"), "Control");
            match error {
                Error::Control(cause) => {
                    assert_eq!((&*cause) as *const OpaqueCause, identity);
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    drop(cause);
                }
                _ => panic!("original cause was replaced"),
            }
        }
        Ok(_) => panic!("cancelled serialization unexpectedly succeeded"),
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn actual_json_count_is_an_exact_serializer_ceiling_not_a_graph_allowance() {
    let (scope, _) = scope();
    let value = ["escaped\nquoted\"", "unchanged"];
    let length = count(&value, &scope).expect("original serializer count");
    let actual = exact_json(&value, length, &scope).expect("exact original output");
    assert_eq!(actual, serde_json::to_string(&value).unwrap());
    assert!(matches!(
        exact_json(&value, length - 1, &scope),
        Err(Error::ShapeChanged)
    ));
    assert!(matches!(
        exact_json(&value, length + 1, &scope),
        Err(Error::ShapeChanged)
    ));
}
