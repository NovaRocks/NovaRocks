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
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_execution::exec::chunk::ChunkSchema;
use novarocks_type_contract::{
    CompileControlError, FunctionValueType,
    owned_resources::metadata_materialization::{
        MaterializedFieldNamespace, OriginalFieldMaterialization, TypedSchemaMaterializations,
        materialize_value_field,
    },
};
use novarocks_types::SlotId;
use novarocks_types::arrow_metadata_owner::FieldMetadataOrigins;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Control {
    calls: AtomicUsize,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            refusal: None,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        let at = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some((wanted, error)) = self.refusal
            && wanted == at
        {
            return Err(error);
        }
        Ok(())
    }
}
fn positive(count: usize) -> StaticLayout {
    let ty = FunctionValueType::new(DataType::Int64, false);
    let fields = (0..count)
        .map(|index| materialize_value_field(&ty, format!("field_{index}")).unwrap())
        .collect();
    let source = TypedSchemaMaterializations::new(
        fields,
        MaterializedFieldNamespace::from_original_loans(Arc::from([])),
    )
    .into_original_schema();
    StaticLayout::try_new_materialized_for_compile(
        source,
        (0..count)
            .map(|index| SlotId::new(u32::try_from(index).unwrap() + 1))
            .collect::<Vec<_>>()
            .into(),
        &Control::good(),
    )
    .unwrap()
}
fn mixed() -> StaticLayout {
    let source = TypedSchemaMaterializations::from_original_fields(
        vec![OriginalFieldMaterialization::Plain(Field::new(
            "foreign",
            DataType::Int64,
            true,
        ))],
        MaterializedFieldNamespace::from_original_loans(Arc::from([])),
    )
    .into_original_schema();
    StaticLayout::try_new_materialized_for_compile(
        source,
        Arc::from([SlotId::new(1)]),
        &Control::good(),
    )
    .unwrap()
}
#[test]
fn project_metadata_route_uses_actual_namespace_loans_and_preserves_open_constructors() {
    for count in [0, 1, 320] {
        let layout = positive(count);
        assert_eq!(
            original_schema_route(&layout, &Control::good()),
            Ok(OriginalSchemaRoute::NormalPositive)
        );
        assert!(original_compiled_schema_metadata_request(&layout, &Control::good()).is_ok());
        ChunkSchema::from_compiled_layout(&layout).unwrap();
    }
    let foreign = StaticLayout::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "foreign",
            DataType::Int64,
            true,
        )])),
        Arc::from([SlotId::new(1)]),
    )
    .unwrap();
    let mixed = mixed();
    let shared = StaticLayout::try_new(Arc::new(Schema::empty()), Arc::from([]))
        .unwrap()
        .with_metadata_origins(FieldMetadataOrigins::try_new(vec![], 0).unwrap(), None)
        .unwrap();
    for (layout, expected) in [
        (foreign, OriginalSchemaRoute::Foreign),
        (mixed, OriginalSchemaRoute::Mixed),
        (shared, OriginalSchemaRoute::SharedOrigins),
    ] {
        assert_eq!(
            original_schema_route(&layout, &Control::good()),
            Ok(expected)
        );
        ChunkSchema::from_compiled_layout(&layout).unwrap();
    }
}
#[test]
fn project_metadata_route_control_refusal_cannot_become_an_open_source_route() {
    for layout in [positive(320), mixed()] {
        for at in [1, 2] {
            for error in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    calls: AtomicUsize::new(0),
                    refusal: Some((at, error)),
                };
                assert_eq!(
                    original_schema_route(&layout, &control),
                    Err(MetadataRequestError::Control(error))
                );
                assert_eq!(control.calls.load(Ordering::Relaxed), at);
            }
        }
    }
}
