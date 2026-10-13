// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Apply the final result occurrence's owned fields after carrier execution.

use std::sync::Arc;

use novarocks_execution::exec::expr::{ExprArena, ExprNode};
use novarocks_execution::exec::node::project::ProjectNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::common;
use novarocks_type_contract::ValueLogicalType;
use novarocks_types::logical::{LogicalType, NR_LOGICAL_TYPE_KEY};

/// Legacy writer columns are a compatibility projection only when a real
/// TableFinish owns the separately validated exact Arrow result relation.
pub(crate) fn legacy_result_uses_declared_fields(
    decoded: &NativeLoweredPlanNode,
    declared: &[common::OutputColumn],
) -> Result<bool, NativeFragmentDecodeError> {
    if declared.is_empty() {
        return Ok(false);
    }
    if !declared.iter().any(|column| column.is_internal) {
        return Ok(true);
    }
    if declared.iter().all(|column| column.is_internal)
        && matches!(decoded.node.kind, ExecNodeKind::TableFinish(_))
    {
        return Ok(false);
    }
    Err(NativeFragmentDecodeError::inconsistent(
        FieldPath::root("plan_fragment").field("output_columns"),
        "internal result projection requires an exact TableFinish result relation",
    ))
}

fn strict_source_domain(
    field: &arrow::datatypes::Field,
) -> Result<Option<ValueLogicalType>, NativeFragmentDecodeError> {
    let Some(value) = field.metadata().get(NR_LOGICAL_TYPE_KEY) else {
        return Ok(None);
    };
    let logical = ValueLogicalType::from_legacy_compatible_metadata_value(value).map_err(|_| {
        NativeFragmentDecodeError::inconsistent(
            FieldPath::root("plan_fragment").field("output_columns"),
            "decoded root contains an unknown logical domain marker",
        )
    })?;
    if matches!(
        logical,
        ValueLogicalType::LargeInt | ValueLogicalType::Variant | ValueLogicalType::Uuid
    ) {
        logical.validate_carrier(field.data_type()).map_err(|_| {
            NativeFragmentDecodeError::inconsistent(
                FieldPath::root("plan_fragment").field("output_columns"),
                "final result output carrier differs from the decoded root carrier",
            )
        })?;
    }
    Ok(Some(logical))
}

fn legacy_projection(domain: Option<ValueLogicalType>) -> Option<&'static str> {
    domain
        .filter(|domain| {
            !matches!(
                domain,
                ValueLogicalType::LargeInt | ValueLogicalType::Variant | ValueLogicalType::Uuid
            )
        })
        .and_then(ValueLogicalType::metadata_value)
}

use crate::fragment_error::NativeFragmentDecodeError;
use crate::fragment_layout::decode_output_layout;
use crate::fragment_plan_node::NativeLoweredPlanNode;

/// Publish only the final result's exact slot order and field semantics.
///
/// Expression bindings retain their original carrier types and slot facts.
/// This adapter permits a declared nullable output to receive a non-null
/// source, but never narrows nullability or casts a carrier. Its subordinate
/// processor shares the root's reporting owner, just like existing internal
/// join and set-operation projections; LocalProgram assigns its own node ID.
pub(crate) fn apply_final_root_output_layout(
    decoded: NativeLoweredPlanNode,
    declared: &[common::OutputColumn],
    arena: &mut ExprArena,
    root_node_id: i32,
) -> Result<NativeLoweredPlanNode, NativeFragmentDecodeError> {
    let path = FieldPath::root("plan_fragment").field("output_columns");
    if declared.len() != decoded.layout.order().len()
        || decoded.output_schema.slot_ids() != decoded.layout.order()
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "final result output width differs from the decoded root layout",
        ));
    }
    // Validate slot identity before constructing any declared field owners.
    for (index, (column, slot)) in declared.iter().zip(decoded.layout.order()).enumerate() {
        if column.column_id != slot.as_u32() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().index(index).field("column_id"),
                "final result output slot order differs from the decoded root layout",
            ));
        }
    }
    let actual =
        novarocks_execution::exec::pipeline::builder::output_chunk_schema_for_node(&decoded.node)
            .ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                path.clone(),
                "decoded root has no output schema",
            )
        })?;
    if actual.as_ref() != decoded.output_schema.as_ref() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            "decoded root schema differs from its actual execution node",
        ));
    }
    let output =
        decode_output_layout(declared, path.clone()).map_err(NativeFragmentDecodeError::from)?;
    let output_schema = output.chunk_schema();
    for (index, (source, target)) in decoded
        .output_schema
        .slots()
        .iter()
        .zip(output_schema.slots())
        .enumerate()
    {
        if source.data_type() != target.data_type() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().index(index).field("type"),
                "final result output carrier differs from the decoded root carrier",
            ));
        }
        let target_domain = strict_source_domain(target.field())?;
        let target_projection = legacy_projection(target_domain);
        if target
            .field_schema()
            .logical_type()
            .map(LogicalType::metadata_value)
            != target_projection
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().index(index).field("type"),
                "final result output contains conflicting logical domain facts",
            ));
        }
        // A carrier-only source may acquire the exact frozen root domain, but
        // an already known source domain cannot be erased or reinterpreted.
        let source_domain = strict_source_domain(source.field())?;
        let source_cache = source
            .field_schema()
            .logical_type()
            .map(LogicalType::metadata_value);
        if (source_domain.is_some() && source_domain != target_domain)
            || (source_cache.is_some() && source_cache != target_projection)
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().index(index).field("type"),
                "final result output logical domain differs from the decoded root domain",
            ));
        }
        if source.nullable() && !target.nullable() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().index(index).field("nullable"),
                "final result output cannot narrow source nullability",
            ));
        }
    }
    // Static schema equality does not prove an upstream operator publishes
    // the logical fields in its actual chunks. Materialize semantic/container
    // layouts at this final boundary even when the decoded facts already agree.
    let plain_leaf_layout = output_schema.slots().iter().all(|slot| {
        !slot.field().metadata().contains_key(NR_LOGICAL_TYPE_KEY)
            && slot.field_schema().logical_type().is_none()
            && slot.field_schema().children().is_empty()
    });
    if plain_leaf_layout && decoded.output_schema.as_ref() == output_schema.as_ref() {
        return Ok(decoded);
    }

    // Slot expressions read the source's facts. Only the materialized output
    // takes the declared occurrence's field names and logical markers.
    let exprs = decoded
        .output_schema
        .slots()
        .iter()
        .map(|source| {
            let id = arena.push_typed(
                ExprNode::SlotId(source.slot_id()),
                source.data_type().clone(),
            );
            arena.set_field_schema(id, source.field_schema().clone());
            id
        })
        .collect();
    let slots = decoded.layout.order().to_vec();
    Ok(NativeLoweredPlanNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(decoded.node),
                node_id: root_node_id,
                is_subordinate: true,
                validate_final_result_input: true,
                exprs,
                expr_slot_ids: slots,
                expr_slot_schemas: Some(decoded.output_schema.slots().to_vec()),
                output_indices: None,
                output_chunk_schema: Arc::clone(&output_schema),
            }),
        },
        layout: decoded.layout,
        output_schema,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BinaryArray, StringArray};
    use arrow::datatypes::DataType;
    use novarocks_execution::exec::chunk::{Chunk, SlotLayout};
    use novarocks_execution::exec::node::values::ValuesNode;
    use novarocks_execution::exec::node::{ExecPlanBuilder, ExternalSinkRequirement};
    use novarocks_execution::exec::operators::ProjectProcessorFactory;
    use novarocks_execution::exec::pipeline::operator_factory::OperatorFactory;
    use novarocks_execution::runtime::runtime_state::RuntimeState;
    use novarocks_types::logical::{LogicalType, logical_type_of_field};

    fn column(id: u32, primitive: common::PrimitiveType, nullable: bool) -> common::OutputColumn {
        common::OutputColumn {
            column_id: id,
            name: format!("field_{id}"),
            r#type: Some(common::TypeDesc {
                kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                    r#type: primitive as i32,
                    len: None,
                    precision: None,
                    scale: None,
                    time_unit: None,
                    time_zone: None,
                })),
            }),
            nullable,
            is_internal: false,
        }
    }

    fn values(columns: &[common::OutputColumn]) -> (NativeLoweredPlanNode, Chunk) {
        let output = decode_output_layout(columns, FieldPath::root("source")).unwrap();
        let schema = output.chunk_schema();
        let arrays = schema
            .slots()
            .iter()
            .map(|slot| match slot.data_type() {
                DataType::Binary => Arc::new(BinaryArray::from(vec![&b"state"[..]])) as ArrayRef,
                DataType::Utf8 => Arc::new(StringArray::from(vec!["{\"k\":1}"])) as ArrayRef,
                _ => panic!("test source requires a text or binary carrier"),
            })
            .collect();
        let chunk = Chunk::try_new_with_columns(Arc::clone(&schema), arrays).unwrap();
        (
            NativeLoweredPlanNode {
                node: ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode {
                        chunk: chunk.clone(),
                        node_id: 37,
                    }),
                },
                layout: SlotLayout::for_slots(output.slot_ids().iter().copied()),
                output_schema: schema,
            },
            chunk,
        )
    }

    #[test]
    fn nonempty_final_domain_is_owned_in_local_program_and_actual_ipc_output() {
        use common::PrimitiveType as P;
        for (primitive, plain, logical) in [
            (P::Bitmap, P::Varbinary, LogicalType::Bitmap),
            (P::Hll, P::Varbinary, LogicalType::Hll),
            (P::Percentile, P::Varbinary, LogicalType::Percentile),
            (P::Object, P::Varbinary, LogicalType::Object),
            (P::Json, P::Varchar, LogicalType::Json),
        ] {
            let (source, input) = values(&[column(1, plain, false)]);
            let original_array = Arc::clone(&input.columns()[0]);
            let mut declared = column(1, primitive, false);
            declared.name = "opaque_alias_without_a_function_name".into();
            let mut arena = ExprArena::default();
            let adapted =
                apply_final_root_output_layout(source, &[declared], &mut arena, 37).unwrap();
            let ExecNodeKind::Project(project) = &adapted.node.kind else {
                panic!("different root field semantics require an output adapter")
            };
            assert!(project.is_subordinate);
            assert!(project.validate_final_result_input);
            assert_eq!(project.node_id, 37);
            assert_eq!(
                arena.data_type(project.exprs[0]),
                Some(original_array.data_type())
            );
            assert_eq!(
                arena.field_schema(project.exprs[0]).unwrap().logical_type(),
                None
            );
            assert_eq!(
                project.expr_slot_schemas.as_ref().unwrap()[0]
                    .field_schema()
                    .logical_type(),
                None
            );

            // Run the real processor on a nonempty input, then use the actual
            // typed Arrow IPC path consumed by legacy SQL results.
            let factory = ProjectProcessorFactory::new(
                project.node_id,
                project.is_subordinate,
                Arc::new(arena.clone()),
                project.exprs.clone(),
                project.expr_slot_ids.clone(),
                project.expr_slot_schemas.clone(),
                project.output_indices.clone(),
                Arc::clone(&project.output_chunk_schema),
            )
            .with_final_result_input_validation(project.validate_final_result_input);
            let mut operator = factory.create(1, 0);
            let processor = operator.as_processor_mut().unwrap();
            let state = RuntimeState::default();
            processor.push_chunk(&state, input).unwrap();
            let output = processor.pull_chunk(&state).unwrap().unwrap();
            assert_eq!(output.len(), 1);
            // Chunk construction retags ArrayData and creates a fresh wrapper;
            // the actual payload and offset buffers must remain shared.
            let original_data = original_array.to_data();
            let output_data = output.columns()[0].to_data();
            assert_eq!(original_data, output_data);
            assert_eq!(original_data.buffers().len(), output_data.buffers().len());
            for (original, output) in original_data.buffers().iter().zip(output_data.buffers()) {
                assert!(original.ptr_eq(output));
            }
            let slot = &output.chunk_schema().slots()[0];
            assert_eq!(logical_type_of_field(slot.field()), Some(logical));
            assert_eq!(slot.field_schema().logical_type(), Some(logical));
            let origin = slot.metadata_origins().expect("owned declared field");
            assert!(origin.metadata_bytes_for(slot.field_ref()).is_some());
            let ipc =
                novarocks_execution::runtime::exchange::encode_chunks(&[output], true).unwrap();
            let received =
                novarocks_execution::runtime::exchange::decode_root_result_chunks(&ipc, None)
                    .unwrap();
            assert_eq!(received[0].len(), 1);
            assert_eq!(
                logical_type_of_field(received[0].chunk_schema().slots()[0].field()),
                Some(logical)
            );
            assert_eq!(
                novarocks_types::FieldRenderSchema::from_field(
                    received[0].chunk_schema().slots()[0].field(),
                )
                .renders_opaque_binary(),
                logical != LogicalType::Json,
            );
            assert_eq!(received[0].columns()[0].to_data(), original_array.to_data());

            let exec = ExecPlanBuilder::new(arena, adapted.node).finish().unwrap();
            let profile = exec
                .local_compile_profile(std::num::NonZeroUsize::new(1).unwrap(), None)
                .unwrap();
            let (local, _) = exec
                .into_local_program_and_bindings(
                    profile,
                    std::collections::BTreeMap::new(),
                    vec![ExternalSinkRequirement::Result],
                    novarocks_local_program::StaticSinkProgram::Result,
                )
                .unwrap();
            let root = &local.nodes()[local.root().index()];
            assert!(matches!(
                root.kind(),
                novarocks_local_program::ProgramNodeKind::Project {
                    validate_final_result_input: true,
                    ..
                }
            ));
            let layout = root.output_layout();
            assert_eq!(
                logical_type_of_field(&layout.schema().fields()[0]),
                Some(logical)
            );
            assert!(layout.field_metadata_origins().is_some());
            assert_eq!(local.nodes().len(), 2);
        }
    }

    #[test]
    fn internal_flags_cannot_bypass_an_ordinary_result_layout() {
        use common::PrimitiveType as P;
        let columns = [
            column(1, P::Varbinary, false),
            column(2, P::Varbinary, false),
        ];
        let (source, _) = values(&columns);
        assert!(legacy_result_uses_declared_fields(&source, &columns).unwrap());
        assert!(!legacy_result_uses_declared_fields(&source, &[]).unwrap());
        let mut internal = columns.clone();
        internal[0].is_internal = true;
        assert!(legacy_result_uses_declared_fields(&source, &internal).is_err());
        internal[1].is_internal = true;
        assert!(legacy_result_uses_declared_fields(&source, &internal).is_err());
    }

    #[test]
    fn unknown_source_markers_cannot_be_erased_or_reassigned() {
        use arrow::datatypes::Field;
        use common::PrimitiveType as P;
        use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSlotSchema};
        for value in ["unknown", "", "  "] {
            for target in [P::Varbinary, P::Bitmap] {
                let (mut source, input) = values(&[column(1, P::Varbinary, false)]);
                let field = Field::new("state", DataType::Binary, false)
                    .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), value.into())].into());
                let schema = Arc::new(
                    ChunkSchema::try_new(vec![
                        ChunkSlotSchema::try_new_with_field(
                            novarocks_types::SlotId::new(1),
                            field,
                            None,
                            None,
                        )
                        .unwrap(),
                    ])
                    .unwrap(),
                );
                let chunk = Chunk::try_new_with_columns(
                    Arc::clone(&schema),
                    vec![Arc::clone(&input.columns()[0])],
                )
                .unwrap();
                source.node = ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 37 }),
                };
                source.output_schema = schema;
                let mut arena = ExprArena::default();
                let error = apply_final_root_output_layout(
                    source,
                    &[column(1, target, false)],
                    &mut arena,
                    37,
                )
                .unwrap_err();
                assert!(error.contains("unknown logical domain"));
                assert!(
                    arena
                        .node(novarocks_execution::exec::expr::ExprId(0))
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn known_source_domains_cannot_be_erased_or_reinterpreted() {
        use common::PrimitiveType as P;
        for (source_type, target_type) in [
            (P::Bitmap, P::Hll),
            (P::Bitmap, P::Varbinary),
            (P::Json, P::Varchar),
        ] {
            let (source, _) = values(&[column(1, source_type, false)]);
            let mut arena = ExprArena::default();
            let error = apply_final_root_output_layout(
                source,
                &[column(1, target_type, false)],
                &mut arena,
                37,
            )
            .unwrap_err();
            assert!(error.to_string().contains("logical domain differs"));
            assert!(
                arena
                    .node(novarocks_execution::exec::expr::ExprId(0))
                    .is_none()
            );
        }
    }

    #[test]
    fn final_layout_refuses_runtime_domain_conflict_before_retagging() {
        for primitive in [
            common::PrimitiveType::Hll,
            common::PrimitiveType::Percentile,
        ] {
            let declared = [column(1, common::PrimitiveType::Bitmap, false)];
            let (source, _) = values(&declared);
            let (_, conflicting_input) = values(&[column(1, primitive, false)]);
            let mut arena = ExprArena::default();
            let adapted =
                apply_final_root_output_layout(source, &declared, &mut arena, 37).unwrap();
            let ExecNodeKind::Project(project) = adapted.node.kind else {
                panic!("semantic result requires runtime validation")
            };
            let factory = ProjectProcessorFactory::new(
                project.node_id,
                project.is_subordinate,
                Arc::new(arena),
                project.exprs,
                project.expr_slot_ids,
                project.expr_slot_schemas,
                project.output_indices,
                project.output_chunk_schema,
            )
            .with_final_result_input_validation(project.validate_final_result_input);
            let mut operator = factory.create(1, 0);
            let processor = operator.as_processor_mut().unwrap();
            let state = RuntimeState::default();
            let error = processor.push_chunk(&state, conflicting_input).unwrap_err();
            assert!(error.to_string().contains("logical domain"));
            assert!(processor.pull_chunk(&state).unwrap().is_none());
        }
    }

    #[test]
    fn equal_static_semantic_layout_still_materializes_actual_output_fields() {
        let declared = [column(1, common::PrimitiveType::Bitmap, false)];
        let (source, _) = values(&declared);
        let (_, plain_input) = values(&[column(1, common::PrimitiveType::Varbinary, false)]);
        let original = plain_input.columns()[0].to_data();
        let mut arena = ExprArena::default();
        let adapted = apply_final_root_output_layout(source, &declared, &mut arena, 37).unwrap();
        let ExecNodeKind::Project(project) = adapted.node.kind else {
            panic!("static semantic facts alone do not prove runtime field publication")
        };
        let factory = ProjectProcessorFactory::new(
            project.node_id,
            project.is_subordinate,
            Arc::new(arena),
            project.exprs,
            project.expr_slot_ids,
            project.expr_slot_schemas,
            project.output_indices,
            project.output_chunk_schema,
        )
        .with_final_result_input_validation(project.validate_final_result_input);
        let mut operator = factory.create(1, 0);
        let processor = operator.as_processor_mut().unwrap();
        let state = RuntimeState::default();
        processor.push_chunk(&state, plain_input).unwrap();
        let output = processor.pull_chunk(&state).unwrap().unwrap();
        assert_eq!(
            logical_type_of_field(output.batch.schema().field(0)),
            Some(LogicalType::Bitmap)
        );
        assert_eq!(
            output.chunk_schema().slots()[0]
                .field_schema()
                .logical_type(),
            Some(LogicalType::Bitmap)
        );
        assert_eq!(output.columns()[0].to_data(), original);
        for (source, target) in original
            .buffers()
            .iter()
            .zip(output.columns()[0].to_data().buffers())
        {
            assert!(source.ptr_eq(target));
        }
    }

    #[test]
    fn exact_schema_retains_the_original_node_and_schema_owner() {
        let columns = [column(1, common::PrimitiveType::Varbinary, false)];
        let (source, _) = values(&columns);
        let schema = Arc::clone(&source.output_schema);
        let mut arena = ExprArena::default();
        let adapted = apply_final_root_output_layout(source, &columns, &mut arena, 37).unwrap();
        assert!(matches!(adapted.node.kind, ExecNodeKind::Values(_)));
        assert!(Arc::ptr_eq(&schema, &adapted.output_schema));
        assert!(
            arena
                .node(novarocks_execution::exec::expr::ExprId(0))
                .is_none()
        );
    }

    #[test]
    fn final_layout_can_widen_but_cannot_narrow_nullable() {
        let source_columns = [column(1, common::PrimitiveType::Varbinary, false)];
        let (source, _) = values(&source_columns);
        let mut arena = ExprArena::default();
        let adapted = apply_final_root_output_layout(
            source,
            &[column(1, common::PrimitiveType::Bitmap, true)],
            &mut arena,
            37,
        )
        .unwrap();
        assert!(adapted.output_schema.slots()[0].nullable());

        let (source, _) = values(&[column(1, common::PrimitiveType::Varbinary, true)]);
        assert!(
            apply_final_root_output_layout(
                source,
                &[column(1, common::PrimitiveType::Bitmap, false)],
                &mut ExprArena::default(),
                37,
            )
            .unwrap_err()
            .contains("cannot narrow")
        );
    }

    #[test]
    fn missing_wrong_or_reordered_slots_are_rejected_before_expressions() {
        use common::PrimitiveType as P;
        let source_columns = [
            column(1, P::Varbinary, false),
            column(2, P::Varbinary, false),
        ];
        for declared in [
            vec![column(1, P::Bitmap, false)],
            vec![column(1, P::Bitmap, false), column(3, P::Hll, false)],
            vec![column(2, P::Bitmap, false), column(1, P::Hll, false)],
            vec![column(1, P::Bitmap, false), column(1, P::Hll, false)],
        ] {
            let (source, _) = values(&source_columns);
            let mut arena = ExprArena::default();
            assert!(apply_final_root_output_layout(source, &declared, &mut arena, 37).is_err());
            assert!(
                arena
                    .node(novarocks_execution::exec::expr::ExprId(0))
                    .is_none()
            );
        }
    }

    #[test]
    fn carrier_changes_and_missing_wire_types_are_rejected() {
        use common::PrimitiveType as P;
        let mut missing = column(1, P::Bitmap, false);
        missing.r#type = None;
        for declared in [column(1, P::Json, false), missing] {
            let (source, _) = values(&[column(1, P::Varbinary, false)]);
            let mut arena = ExprArena::default();
            assert!(apply_final_root_output_layout(source, &[declared], &mut arena, 37).is_err());
            assert!(
                arena
                    .node(novarocks_execution::exec::expr::ExprId(0))
                    .is_none()
            );
        }
    }
}
