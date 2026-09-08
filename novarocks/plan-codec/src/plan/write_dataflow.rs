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

//! Encoding the dataflow write nodes.
//!
//! The writer handle is a property of the logical write target, not of where
//! the plan runs, so the encoder can stamp it in directly from the begin
//! session. Nothing here depends on placement: the same canonical handle is
//! copied into every submission that serves its target, and copying is the
//! ordinary path rather than a rejected one.
//!
//! That is what removes the old post-placement patch step and the four
//! separate exact-cover checks that surrounded it. Completeness of a write is
//! now proved by the execution graph closing -- every sender reaches EOS, the
//! finish node emits, the frontend reads EOF -- not by a pre-enumerated
//! manifest of physical writers.

use std::collections::BTreeMap;

use novarocks_proto_codec::catalog::encode_catalog_handle;
use novarocks_proto_codec::{FieldPath, arrow_physical};
use novarocks_proto_models::{connector_write as write_dto, plan};
use novarocks_spi::connector::CatalogHandle;
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_sql::plan_read::{TableFinishNode, TableWriterNode};

use super::relational::{encode_resolved_aggregate_signature, encode_unpivot_constant};
use super::type_mapping::encode_type;
use super::write::encode_connector_write_input_binding;
use super::{NativePlanEncodeContext, encode_exprs, required_context_ref};

/// The frozen per-query write targets the encoder stamps into writer nodes.
///
/// The handles are already canonical and already charged against the query's
/// unique-handle budget, so the bytes submitted are exactly the bytes the
/// frontend accounted for.
#[derive(Clone, Debug)]
pub struct SealedWriteTargets {
    catalog_handle: CatalogHandle,
    handles: BTreeMap<u32, write_dto::ConnectorWriterHandle>,
}

impl SealedWriteTargets {
    pub fn new(
        catalog_handle: CatalogHandle,
        handles: BTreeMap<u32, write_dto::ConnectorWriterHandle>,
    ) -> Self {
        Self {
            catalog_handle,
            handles,
        }
    }

    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    fn handle_for(&self, target: WriteTargetOrdinal) -> Option<&write_dto::ConnectorWriterHandle> {
        self.handles.get(&target.get())
    }

    pub fn ordinals(&self) -> impl Iterator<Item = u32> + '_ {
        self.handles.keys().copied()
    }

    /// The one target this session sealed, for a plan shape that has exactly one
    /// writer. A single-writer plan cannot express a session with several
    /// targets, so a session that sealed more than one is refused here rather
    /// than silently having its extra targets written by nobody.
    pub fn sole_target_ordinal(&self) -> Result<WriteTargetOrdinal, String> {
        let mut ordinals = self.handles.keys().copied();
        let (Some(ordinal), None) = (ordinals.next(), ordinals.next()) else {
            return Err(format!(
                "a single-writer plan requires a write session with exactly one target, but the session sealed {}",
                self.handles.len()
            ));
        };
        WriteTargetOrdinal::try_new(ordinal).map_err(|error| error.to_string())
    }
}

pub(super) fn encode_table_writer_node<F>(
    src: &TableWriterNode,
    ctx: &NativePlanEncodeContext<'_, F>,
) -> Result<plan::TableWriterNode, String> {
    let targets = required_context_ref(ctx.write_targets, || {
        "native table writer node has no sealed write session".to_string()
    })?;
    let ordinal = src.write_target_ordinal();
    let handle = targets.handle_for(ordinal).ok_or_else(|| {
        format!(
            "native table writer node names write target {} which the begin session did not seal",
            ordinal.get()
        )
    })?;
    let target_schema = src
        .target_schema()
        .iter()
        .map(|column| {
            Ok(novarocks_proto_models::common::OutputColumn {
                column_id: column.column_id,
                name: column.name.clone(),
                r#type: Some(encode_type(&column.data_type)?),
                nullable: column.nullable,
                is_internal: column.is_internal,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(plan::TableWriterNode {
        catalog_handle: Some(encode_catalog_handle(targets.catalog_handle())),
        write_target_ordinal: ordinal.get(),
        handle: Some(handle.clone()),
        input: Some(encode_connector_write_input_binding(src.input())),
        // One writer node per fragment today, so its physical writer ordinal is
        // zero. It is carried explicitly rather than implied, because the
        // backend builds a writer's physical context from it and a silent zero
        // would be indistinguishable from a forgotten field.
        writer_ordinal: 0,
        output_exprs: encode_exprs(src.output_exprs())?,
        target_schema,
        writer_multiplex_schema: Some(encode_writer_multiplex_schema(
            src.writer_multiplex_schema(),
        )?),
        partial_aggregate_plan: Some(encode_writer_partial_aggregate_plan(
            src.partial_aggregate_plan(),
        )?),
    })
}

fn encode_writer_partial_aggregate_plan(
    src: &novarocks_sql::plan_read::WriterPartialAggregatePlan,
) -> Result<plan::WriterPartialAggregatePlan, String> {
    Ok(plan::WriterPartialAggregatePlan {
        calls: src
            .calls()
            .iter()
            .map(|call| {
                Ok(plan::WriterPartialAggregateCall {
                    input_slot_id: call.input_slot_id(),
                    function_name: call.function_name().to_string(),
                    resolved_signature: Some(encode_resolved_aggregate_signature(call.resolved())?),
                    intermediate_slot_id: call.intermediate_slot_id(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
    })
}

fn encode_writer_final_aggregate_plan(
    src: &novarocks_sql::plan_read::WriterFinalAggregatePlan,
) -> Result<plan::WriterFinalAggregatePlan, String> {
    let calls = src
        .calls()
        .iter()
        .map(|call| {
            Ok(plan::WriterFinalAggregateCall {
                function_name: call.function_name().to_string(),
                resolved_signature: Some(encode_resolved_aggregate_signature(call.resolved())?),
                intermediate_input_slot_id: call.intermediate_input_slot_id(),
                final_output_slot_id: call.final_output_slot_id(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let unpivot = src
        .unpivot()
        .map(|unpivot| {
            Ok::<plan::WriterGroupedUnpivotPlan, String>(plan::WriterGroupedUnpivotPlan {
                grouping_input_slot_id: unpivot.grouping_input_slot_id(),
                grouping_output_slot_id: unpivot.grouping_output_slot_id(),
                passthrough_output_slot_id: unpivot.passthrough_output_slot_id(),
                value_output_slot_id: unpivot.value_output_slot_id(),
                literal_output_slot_ids: unpivot.literal_output_slot_ids().to_vec(),
                mappings: unpivot
                    .mappings()
                    .iter()
                    .map(|mapping| {
                        Ok(plan::WriterGroupedUnpivotMapping {
                            grouping_key: mapping.target().get(),
                            input_value_slot_id: mapping.input_value_slot_id(),
                            constants: mapping
                                .constants()
                                .iter()
                                .map(encode_unpivot_constant)
                                .collect::<Result<Vec<_>, _>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                max_output_rows: u64::try_from(unpivot.max_output_rows())
                    .map_err(|_| "writer Unpivot row budget exceeds u64".to_string())?,
                max_output_bytes: u64::try_from(unpivot.max_output_bytes())
                    .map_err(|_| "writer Unpivot byte budget exceeds u64".to_string())?,
            })
        })
        .transpose()?;
    Ok(plan::WriterFinalAggregatePlan { calls, unpivot })
}

fn encode_writer_multiplex_schema(
    schema: &novarocks_spi::connector::write_stack::WriterMultiplexSchema,
) -> Result<plan::WriterMultiplexSchema, String> {
    let slot_ids = schema.slot_ids();
    let (columns, schema_metadata) = arrow_physical::encode_schema(
        schema.arrow_schema().as_ref(),
        &slot_ids,
        true,
        FieldPath::root("writer_multiplex_schema"),
    )
    .map_err(|error| error.to_string())?;
    Ok(plan::WriterMultiplexSchema {
        contract_version: schema.contract_version(),
        columns,
        schema_metadata,
    })
}

fn encode_root_result_schema(
    schema: &novarocks_spi::connector::write_stack::RootWriteResultSchema,
) -> Result<plan::RootWriteResultSchema, String> {
    let slot_ids = schema.slot_ids();
    let arrow_schema = schema.arrow_schema();
    let (columns, schema_metadata) = arrow_physical::encode_schema(
        arrow_schema.as_ref(),
        &slot_ids,
        true,
        FieldPath::root("root_result_schema"),
    )
    .map_err(|error| error.to_string())?;
    Ok(plan::RootWriteResultSchema {
        contract_version: schema.contract_version(),
        columns,
        schema_metadata,
    })
}

pub(super) fn encode_table_finish_node(
    src: &TableFinishNode,
) -> Result<plan::TableFinishNode, String> {
    Ok(plan::TableFinishNode {
        expected_target_ordinals: src
            .expected_target_ordinals()
            .iter()
            .map(|target| target.get())
            .collect(),
        writer_multiplex_schema: Some(encode_writer_multiplex_schema(
            src.writer_multiplex_schema(),
        )?),
        root_result_schema: Some(encode_root_result_schema(src.root_result_schema())?),
        final_aggregate_plan: Some(encode_writer_final_aggregate_plan(
            src.final_aggregate_plan(),
        )?),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields};
    use novarocks_proto_models::connector_common as common_dto;
    use novarocks_spi::connector::write_stack::{WriterAuxiliaryChannel, WriterMultiplexSchema};
    use novarocks_spi::connector::{CatalogVersion, ConnectorInstanceId};
    use novarocks_sql::plan_read::DistributedNodeKind;
    use novarocks_sql::test_support::{NativeWriteDataflowFixture, native_write_dataflow_plan};

    use super::super::scan_facts::NoScanFacts;
    use super::super::{NativePlanEncodeContext, encode_distributed_plan_with_context};
    use super::*;

    fn catalog_handle() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("write_targets_unit").expect("instance id"),
            CatalogVersion::from_bytes([5; 32]),
        )
    }

    /// A recognisable stand-in for a real recipe. The encoder must copy it
    /// verbatim; interpreting it is the provider's job, not the encoder's.
    fn handle_for(ordinal: u32) -> write_dto::ConnectorWriterHandle {
        write_dto::ConnectorWriterHandle {
            provider_payload: Some(common_dto::ConnectorEncodedPayload {
                header: Some(common_dto::ConnectorEnvelopeHeader {
                    provider_id: "test-provider".to_string(),
                    catalog: Some(novarocks_proto_codec::catalog::encode_catalog_handle(
                        &catalog_handle(),
                    )),
                    category: common_dto::ConnectorPayloadCategory::WriteHandle as i32,
                    codec_revision: 1,
                }),
                payload: format!("target-{ordinal}").into_bytes(),
            }),
        }
    }

    fn sealed(ordinals: &[u32]) -> SealedWriteTargets {
        SealedWriteTargets::new(
            catalog_handle(),
            ordinals
                .iter()
                .map(|ordinal| (*ordinal, handle_for(*ordinal)))
                .collect(),
        )
    }

    fn encoded_writer_nodes(
        fixture: NativeWriteDataflowFixture,
        targets: &SealedWriteTargets,
    ) -> Vec<plan::TableWriterNode> {
        let sealed_plan = native_write_dataflow_plan(fixture).expect("sealed dataflow write plan");
        let encoded = encode_distributed_plan_with_context(
            &sealed_plan,
            NativePlanEncodeContext::<NoScanFacts> {
                scan_facts: None,
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                write_targets: Some(targets),
            },
        )
        .expect("encode dataflow write plan");
        collect_writer_nodes(&encoded)
    }

    fn collect_writer_nodes(encoded: &plan::DistributedPlan) -> Vec<plan::TableWriterNode> {
        fn visit(node: &plan::DistributedNode, out: &mut Vec<plan::TableWriterNode>) {
            if let Some(plan::distributed_node::Payload::TableWriter(writer)) =
                node.payload.as_ref()
            {
                out.push(writer.clone());
            }
            for child in &node.children {
                visit(child, out);
            }
        }
        let mut out = Vec::new();
        for fragment in &encoded.fragments {
            if let Some(root) = fragment.root.as_ref() {
                visit(root, &mut out);
            }
        }
        out
    }

    #[test]
    fn a_writer_node_is_stamped_with_its_targets_catalog_and_recipe() {
        let targets = sealed(&[0]);
        let writers = encoded_writer_nodes(NativeWriteDataflowFixture::SingleWriter, &targets);
        assert_eq!(writers.len(), 1);
        assert_eq!(writers[0].write_target_ordinal, 0);
        assert_eq!(writers[0].handle.as_ref(), Some(&handle_for(0)));
        assert!(writers[0].catalog_handle.is_some());
        assert!(
            !writers[0].target_schema.is_empty(),
            "the sealed write target schema must reach the backend"
        );
        let relation = writers[0]
            .writer_multiplex_schema
            .as_ref()
            .expect("writer relation contract");
        assert_eq!(relation.contract_version, 1);
        assert_eq!(relation.columns.len(), 4);
        assert!(relation.columns.iter().all(|column| column.is_internal));
    }

    #[test]
    fn each_route_writer_is_stamped_with_its_own_targets_recipe() {
        let targets = sealed(&[0, 1]);
        let writers =
            encoded_writer_nodes(NativeWriteDataflowFixture::ChangeStreamTwoWriters, &targets);
        assert_eq!(writers.len(), 2);
        let mut seen = writers
            .iter()
            .map(|writer| (writer.write_target_ordinal, writer.handle.clone()))
            .collect::<Vec<_>>();
        seen.sort_by_key(|(ordinal, _)| *ordinal);
        assert_eq!(seen[0], (0, Some(handle_for(0))));
        assert_eq!(seen[1], (1, Some(handle_for(1))));
    }

    #[test]
    fn a_writer_node_without_a_sealed_session_fails_to_encode() {
        let sealed_plan = native_write_dataflow_plan(NativeWriteDataflowFixture::SingleWriter)
            .expect("sealed dataflow write plan");
        let error = encode_distributed_plan_with_context(
            &sealed_plan,
            NativePlanEncodeContext::<NoScanFacts> {
                scan_facts: None,
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                write_targets: None,
            },
        )
        .expect_err("no sealed write session");
        assert!(
            error.contains("no sealed write session"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_writer_node_naming_an_unsealed_target_fails_to_encode() {
        // The begin session sealed target 0 only; the change-stream plan needs
        // two. Submitting the second writer with no recipe would give a backend
        // a writer it cannot bind, so the encode fails instead.
        let targets = sealed(&[0]);
        let sealed_plan =
            native_write_dataflow_plan(NativeWriteDataflowFixture::ChangeStreamTwoWriters)
                .expect("sealed dataflow write plan");
        let error = encode_distributed_plan_with_context(
            &sealed_plan,
            NativePlanEncodeContext::<NoScanFacts> {
                scan_facts: None,
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                write_targets: Some(&targets),
            },
        )
        .expect_err("unsealed target");
        assert!(
            error.contains("which the begin session did not seal"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn the_finish_node_carries_target_and_exact_relation_contracts() {
        let sealed_plan =
            native_write_dataflow_plan(NativeWriteDataflowFixture::ChangeStreamTwoWriters)
                .expect("sealed dataflow write plan");
        let finish = sealed_plan
            .fragments()
            .iter()
            .find_map(|fragment| match &fragment.root.payload {
                DistributedNodeKind::TableFinish(finish) => Some(finish),
                _ => None,
            })
            .expect("one finish node");
        let encoded = encode_table_finish_node(finish).expect("encode finish");
        assert_eq!(encoded.expected_target_ordinals, vec![0, 1]);
        let writer = encoded.writer_multiplex_schema.expect("writer relation");
        assert_eq!(writer.contract_version, 1);
        assert_eq!(writer.columns.len(), 4);
        let root = encoded.root_result_schema.expect("root relation");
        assert_eq!(root.contract_version, 1);
        assert_eq!(root.columns.len(), 8);
    }

    #[test]
    fn relation_encoder_preserves_exact_arrow_physical_fields() {
        let item = Arc::new(
            Field::new("item64", DataType::LargeUtf8, false)
                .with_metadata(HashMap::from([("logical".to_string(), "tag".to_string())])),
        );
        let entries = Arc::new(Field::new(
            "entries_exact",
            DataType::Struct(Fields::from(vec![
                Field::new("key_exact", DataType::Utf8, false),
                Field::new("value_exact", DataType::LargeBinary, false),
            ])),
            false,
        ));
        let contract = WriterMultiplexSchema::try_new(vec![
            WriterAuxiliaryChannel::try_new(10, "list64", DataType::LargeList(item))
                .expect("list channel"),
            WriterAuxiliaryChannel::try_new(11, "map", DataType::Map(entries, true))
                .expect("map channel"),
            WriterAuxiliaryChannel::try_new(
                12,
                "fixed",
                DataType::FixedSizeList(
                    Arc::new(Field::new("fixed_item", DataType::Binary, false)),
                    3,
                ),
            )
            .expect("fixed channel"),
        ])
        .expect("writer schema");
        let encoded = encode_writer_multiplex_schema(&contract).expect("encode relation");
        let decoded = arrow_physical::decode_schema(
            &encoded.columns,
            &encoded.schema_metadata,
            FieldPath::root("writer_multiplex_schema"),
        )
        .expect("decode relation");
        assert_eq!(decoded.schema().as_ref(), contract.arrow_schema().as_ref());
        assert_eq!(decoded.slot_ids(), contract.slot_ids());
    }

    #[test]
    fn a_single_writer_plan_reads_its_target_ordinal_off_the_session() {
        let targets =
            SealedWriteTargets::new(catalog_handle(), BTreeMap::from([(3, handle_for(3))]));
        assert_eq!(
            targets
                .sole_target_ordinal()
                .expect("one sealed target is a sole target")
                .get(),
            3,
            "the ordinal must come from the session, not from the plan's position"
        );
    }

    #[test]
    fn a_single_writer_plan_refuses_a_session_that_sealed_several_targets() {
        let targets = SealedWriteTargets::new(
            catalog_handle(),
            BTreeMap::from([(0, handle_for(0)), (1, handle_for(1))]),
        );
        let error = targets
            .sole_target_ordinal()
            .expect_err("a two-target session cannot be served by one writer");
        assert!(
            error.contains("exactly one target") && error.contains('2'),
            "the refusal must name the disagreement it found: {error}"
        );
    }
}
