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
//! Source lifetime witnesses only. Scope witnesses are never memory grants.
use super::*;

fn named_source() -> Chunk {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        "original-stage-metadata".repeat(200),
        "original-value".repeat(200),
    );
    let schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(7),
            Field::new("original-stage-field".repeat(200), DataType::Int32, true)
                .with_metadata(metadata),
            None,
            None,
        )])
        .unwrap(),
    );
    Chunk::try_new_with_columns(schema, vec![Arc::new(Int32Array::from(vec![2, 1]))]).unwrap()
}
#[test]
fn by_runtime_memory_ready_original_regroup_schema_and_field_escape_are_independent() {
    let source = named_source();
    let original_schema = source.schema();
    let keys = vec![source.record_batch().column(0).clone()];
    let control = Control::new(None);
    let drops = Arc::new(AtomicUsize::new(0));
    let mut work = Work::new(&control, drops.clone());
    let stage = stages::regroup(&source, &keys, None, &mut work).unwrap();
    let schema = stage.value.schema();
    assert!(
        !Arc::ptr_eq(&schema, &original_schema),
        "the original changed-order constructor rebuilt Schema"
    );
    let field = Arc::clone(&schema.fields()[0]);
    let weak_schema = Arc::downgrade(&schema);
    let weak_field = Arc::downgrade(&field);
    assert_eq!(field.name(), original_schema.field(0).name());
    assert_eq!(field.metadata(), original_schema.field(0).metadata());
    drop(stage);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(keys);
    drop(source);
    drop(original_schema);
    assert!(
        weak_schema.upgrade().is_some(),
        "SchemaRef outlives Chunk and its stage scope"
    );
    drop(schema);
    assert!(weak_schema.upgrade().is_none());
    assert!(
        weak_field.upgrade().is_some(),
        "FieldRef separately outlives SchemaRef"
    );
    drop(field);
    assert!(weak_field.upgrade().is_none());
}
#[test]
fn by_runtime_memory_ready_original_split_schema_escape_outlives_all_stage_chunks() {
    let source = named_source();
    let original_schema = source.schema();
    let output = source.chunk_schema_ref();
    let columns = source.record_batch().columns().to_vec();
    let control = Control::new(None);
    let drops = Arc::new(AtomicUsize::new(0));
    let mut work = Work::new(&control, drops.clone());
    let stage = stages::split(output, &columns, &[source], &mut work).unwrap();
    let schema = stage.value.front().unwrap().schema();
    assert!(!Arc::ptr_eq(&schema, &original_schema));
    let weak = Arc::downgrade(&schema);
    drop(stage);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(columns);
    drop(original_schema);
    assert!(weak.upgrade().is_some());
    drop(schema);
    assert!(weak.upgrade().is_none());
}
