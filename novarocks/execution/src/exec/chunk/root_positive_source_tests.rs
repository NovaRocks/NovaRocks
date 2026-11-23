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

use super::*;
use arrow::array::new_empty_array;
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_type_contract::owned_resources::metadata_materialization::{
    MaterializedMetadataMap, MetadataMaterializationError, SchemaMetadataMaterializations,
};
use std::sync::Arc;

fn limits() -> RootArrayStorageLimits {
    RootArrayStorageLimits {
        bytes: 96 << 20,
        nodes: 8192,
        depth: 64,
    }
}

struct MaterializedLayoutControl;
impl novarocks_type_contract::PureCompileControl for MaterializedLayoutControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}

#[test]
fn root_positive_source_structural_local_compiler_unknown_owner_and_materialized_layout() {
    use crate::exec::expr::fixtures::{RandArgument, program};
    use novarocks_type_contract::owned_resources::metadata_materialization::{
        MaterializedFieldNamespace, TypedSchemaMaterializations, materialize_value_field,
    };
    let program = program(RandArgument::Unbounded, false);
    let structural = program.graph().nodes()[program.graph().root().index()].output_layout();
    // This canonical structural fixture does not decode a type namespace.
    assert!(structural.metadata_materializations().is_none());
    assert!(structural.field_metadata_origins().is_none());
    assert!(structural.schema_metadata_origin().is_none());
    let canonical = ChunkSchema::from_compiled_layout(structural).unwrap();
    assert!(canonical.is_typed());
    // The immutable typed author has no retained annotation table. Its actual
    // zero-capacity representation is a distinct bounded source; it claims no
    // provider/M07 construction receipt or capacity grant.
    assert!(borrowed_root_chunk_schema_storage(&canonical, limits()).is_ok());
    // An explicitly legacy field import does not elect that typed source just
    // because its visible Arrow representation is equal and empty.
    let legacy_slots = structural
        .schema()
        .fields()
        .iter()
        .zip(structural.slots())
        .map(|(field, slot)| {
            crate::exec::chunk::ChunkSlotSchema::new_with_field(
                *slot,
                field.as_ref().clone(),
                None,
                None,
            )
        })
        .collect();
    let unknown = ChunkSchema::try_new(legacy_slots).unwrap();
    assert!(!unknown.is_typed());
    assert_eq!(
        borrowed_root_chunk_schema_storage(&unknown, limits()),
        Err(RootArrayStorageError::UnknownMetadataOwner)
    );

    // Publish new fields through the ONE actual FVT materializer and original
    // typed-schema author. No equal Field or Schema is granted a source loan.
    // All fields in this fixture are primitive, so its nested namespace is empty.
    let fields = structural
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let ty = novarocks_type_contract::FunctionValueType::try_from_field(field).unwrap();
            assert!(matches!(
                ty.data_type(),
                DataType::Int64 | DataType::Float64
            ));
            materialize_value_field(&ty, field.name().clone()).unwrap()
        })
        .collect();
    let source = TypedSchemaMaterializations::new(
        fields,
        MaterializedFieldNamespace::from_original_loans(Arc::from([])),
    )
    .into_original_schema();
    let layout = novarocks_local_program::StaticLayout::try_new_materialized_for_compile(
        source,
        Arc::from(structural.slots()),
        &MaterializedLayoutControl,
    )
    .unwrap();
    assert_eq!(layout.schema(), structural.schema()); // Semantics only.
    let schema = ChunkSchema::from_compiled_layout(&layout).unwrap();
    let source = schema.metadata_materializations().unwrap();
    assert!(source.schema_owner().lends(&schema.arrow_schema_ref()));
    assert!(source.checked_field_index().is_some());
    let columns = schema
        .arrow_schema_ref()
        .fields()
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect();
    let chunk = Chunk::try_new_with_columns(schema, columns).unwrap();
    assert!(borrowed_root_chunk_storage(&chunk, limits()).is_ok());
    assert!(borrowed_root_chunk_schema_storage(chunk.chunk_schema(), limits()).is_ok());
    // Structural equality never substitutes for the actual published SchemaArc.
    let foreign = Arc::new(chunk.record_batch().schema().as_ref().clone());
    let batch =
        arrow::record_batch::RecordBatch::try_new(foreign, chunk.record_batch().columns().to_vec())
            .unwrap();
    let foreign = Chunk::try_new_with_chunk_schema(batch, chunk.chunk_schema_ref()).unwrap();
    assert_eq!(
        borrowed_root_chunk_storage(&foreign, limits()),
        Err(RootArrayStorageError::UnknownMetadataOwner)
    );
}

#[test]
fn root_positive_source_live_capacity_expired_weak_and_foreign_field() {
    use super::super::root_schema_backing::{RootFieldSource, RootSchemaSource};
    let mut name = String::with_capacity(8192);
    name.push('x');
    let mut key = String::with_capacity(4096);
    key.push('k');
    let mut value = String::with_capacity(2048);
    value.push('v');
    let mut map = MaterializedMetadataMap::with_capacity(1);
    map.insert(key, value);
    let live = map
        .into_field(Field::new(name, DataType::Utf8, true))
        .into_shared();
    let loan = live.loan();
    let mut work = || Ok::<(), MetadataMaterializationError>(());
    let backing = loan
        .original_backing_observed(live.field(), &mut work)
        .unwrap()
        .unwrap();
    assert!(backing >= 4096 + 2048);
    let foreign = Arc::new(live.field().as_ref().clone());
    assert!(
        loan.original_backing_observed(&foreign, &mut work)
            .is_none()
    );
    let root = MaterializedMetadataMap::new_for_original_reserve()
        .into_schema(Vec::<Arc<Field>>::new())
        .into_shared();
    let source = SchemaMetadataMaterializations::from_materialized_owners(root, Arc::from([loan]));
    let index = source.checked_field_index().unwrap();
    let mut before = RootSchemaInspection::new(96 << 20, 8192, 64);
    before.inspect_original_index(index, None).unwrap();
    let before = before.bytes();
    drop(live);
    assert!(index[0].borrow_original_field().is_none());
    let mut after = RootSchemaInspection::new(96 << 20, 8192, 64);
    after.inspect_original_index(index, None).unwrap();
    after
        .inspect_schema_source(
            source.schema_owner().schema(),
            RootSchemaSource::Original(source.schema_owner()),
            RootFieldSource::Original {
                index,
                attached: None,
            },
        )
        .unwrap();
    assert!(before > after.bytes() + 8192);
    assert!(after.bytes() > 0); // The weak Arc block is still retained.
}

#[test]
fn root_positive_source_statistics_nested_types_keep_actual_field_loans() {
    use super::super::root_schema_backing::{RootFieldSource, RootSchemaSource};
    let item = MaterializedMetadataMap::new_for_original_reserve()
        .into_field(Field::new("item", DataType::Int32, false))
        .into_shared();
    let key = MaterializedMetadataMap::new_for_original_reserve()
        .into_field(Field::new("key", DataType::Utf8, false))
        .into_shared();
    let value = MaterializedMetadataMap::new_for_original_reserve()
        .into_field(Field::new("value", DataType::Utf8, false))
        .into_shared();
    let entries = MaterializedMetadataMap::new_for_original_reserve()
        .into_field(Field::new(
            "entries",
            DataType::Struct(vec![key.field().clone(), value.field().clone()].into()),
            false,
        ))
        .into_shared();
    let roots = [
        MaterializedMetadataMap::new_for_original_reserve()
            .into_field(Field::new(
                "input_fields",
                DataType::List(item.field().clone()),
                false,
            ))
            .into_shared(),
        MaterializedMetadataMap::new_for_original_reserve()
            .into_field(Field::new("blob_type", DataType::Utf8, false))
            .into_shared(),
        MaterializedMetadataMap::new_for_original_reserve()
            .into_field(Field::new("body", DataType::Binary, false))
            .into_shared(),
        MaterializedMetadataMap::new_for_original_reserve()
            .into_field(Field::new(
                "properties",
                DataType::Map(entries.field().clone(), false),
                false,
            ))
            .into_shared(),
    ];
    let mut loans = vec![item.loan(), key.loan(), value.loan(), entries.loan()];
    loans.extend(roots.iter().map(|field| field.loan()));
    let root = MaterializedMetadataMap::new_for_original_reserve()
        .into_schema(roots.iter().map(|f| f.field().clone()).collect::<Vec<_>>())
        .into_shared();
    let source = SchemaMetadataMaterializations::from_materialized_owners(root, loans.into());
    let index = source.checked_field_index().unwrap();
    let mut inspection = RootSchemaInspection::new(96 << 20, 8192, 64);
    inspection.inspect_original_index(index, None).unwrap();
    inspection
        .inspect_schema_source(
            source.schema_owner().schema(),
            RootSchemaSource::Original(source.schema_owner()),
            RootFieldSource::Original {
                index,
                attached: None,
            },
        )
        .unwrap();
    assert!(inspection.bytes() > 0);
    let foreign = Arc::new(Schema::new(source.schema_owner().schema().fields().clone()));
    assert!(
        inspection
            .inspect_schema_source(
                &foreign,
                RootSchemaSource::Original(source.schema_owner()),
                RootFieldSource::Original {
                    index,
                    attached: None
                }
            )
            .is_err()
    );
}
