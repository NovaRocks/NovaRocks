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

//! The frontend adapter onto the plan encoder.
//!
//! Unwrapping the prepared encoding view and sealing the result back into an
//! artifact-bound attachment are frontend responsibilities; the encoding
//! between them belongs to [`novarocks_plan_codec`], which cannot name either
//! of those frontend types.

use novarocks_plan_codec::SealedWriteTargets;

pub use crate::query_execution::native_fragment::{
    NativeFragmentAttachment, NativeFragmentEncodingView,
};

/// Encode one immutable distributed plan and its exact prepared bindings into
/// the native FE-to-BE wire bundle.
pub fn encode_native_fragment_bundle(
    source: NativeFragmentEncodingView<'_>,
) -> Result<NativeFragmentAttachment, String> {
    let plan = source.distributed_plan();
    let scan_facts = source.scan_facts();
    let encoded = novarocks_plan_codec::encode_distributed_plan(plan, scan_facts)?;
    source.seal(encoded.fragments)
}

/// Encode a plan that contains write dataflow nodes.
///
/// The sealed targets come from the begin session and are stamped into every
/// writer node, so a recipe never has to be patched in after placement. A plan
/// with a writer node and no sealed targets fails to encode rather than
/// submitting a writer the backend could not bind.
pub(crate) fn encode_native_fragment_bundle_with_write_targets(
    source: NativeFragmentEncodingView<'_>,
    write_targets: &SealedWriteTargets,
) -> Result<NativeFragmentAttachment, String> {
    let plan = source.distributed_plan();
    let scan_facts = source.scan_facts();
    let encoded = novarocks_plan_codec::encode_distributed_plan_with_write_targets(
        plan,
        scan_facts,
        write_targets,
    )?;
    source.seal(encoded.fragments)
}

/// Encode a sealed plan/preparation pair, using the write session's sealed
/// recipes when the pair carries them.
///
/// The choice is read off the input rather than made by the caller: whether a
/// plan has writer nodes is a property of the plan, and a caller that picked
/// the wrong entrypoint would either drop the recipes or attach them to a plan
/// with nowhere to put them.
pub(crate) fn encode_native_fragment_bundle_for_input(
    input: &crate::query_execution::post_compile::NativeFragmentEncodingInput,
) -> Result<NativeFragmentAttachment, String> {
    match input.sealed_write_targets() {
        Some(write_targets) => {
            encode_native_fragment_bundle_with_write_targets(input.encoding_view(), write_targets)
        }
        None => encode_native_fragment_bundle(input.encoding_view()),
    }
}

#[cfg(test)]
mod tests {
    use novarocks_proto_models::plan;
    use novarocks_spi::connector::{CatalogHandle, CatalogVersion, ConnectorInstanceId};
    use novarocks_sql::test_support::{NativeWriteDataflowFixture, native_write_dataflow_plan};

    use super::*;
    use crate::query_execution::post_compile::NativeFragmentEncodingInput;

    fn sealed_targets(ordinals: &[u32]) -> SealedWriteTargets {
        SealedWriteTargets::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("bundle_write_targets").expect("instance id"),
                CatalogVersion::from_bytes([9; 32]),
            ),
            ordinals
                .iter()
                .map(|ordinal| {
                    (
                        *ordinal,
                        novarocks_proto_models::connector_write::ConnectorWriterHandle {
                            handle: Some(
                                novarocks_proto_models::connector_write::connector_writer_handle::Handle::Iceberg(
                                    novarocks_proto_models::connector_write::IcebergWriterHandle {
                                        branch: novarocks_proto_models::connector_write::IcebergWriteBranch::Data as i32,
                                        table: Some(
                                            novarocks_proto_models::connector_write::IcebergWriteTableFacts {
                                                table_uuid: format!("target-{ordinal}"),
                                                ..Default::default()
                                            },
                                        ),
                                        output: None,
                                        data: None,
                                        old_deletes: std::collections::BTreeMap::new(),
                        equality: None,
                                    },
                                ),
                            ),
                        },
                    )
                })
                .collect(),
        )
    }

    fn dataflow_encoding_input() -> NativeFragmentEncodingInput {
        let plan = native_write_dataflow_plan(NativeWriteDataflowFixture::SingleWriter)
            .expect("sealed dataflow write plan");
        let prepared =
            crate::query_execution::preparation::prepared_fragment_set_for_native_encode_test(
                &plan,
            )
            .expect("prepared fragments");
        NativeFragmentEncodingInput::new(plan, prepared)
    }

    fn writer_nodes(attachment: &NativeFragmentAttachment) -> Vec<plan::TableWriterNode> {
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
        for (_, fragment) in attachment.fragments_in_id_order() {
            if let Some(root) = fragment.root.as_ref() {
                visit(root, &mut out);
            }
        }
        out
    }

    /// The entrypoint reads the choice off the input, so a statement flow that
    /// attached its session's recipes gets them stamped into the writer node
    /// without having to pick an encoder itself.
    #[test]
    fn an_input_carrying_sealed_targets_encodes_them_into_its_writer_node() {
        let input = dataflow_encoding_input().with_sealed_write_targets(sealed_targets(&[0]));
        let attachment =
            encode_native_fragment_bundle_for_input(&input).expect("encode with sealed targets");

        let writers = writer_nodes(&attachment);
        assert_eq!(writers.len(), 1);
        assert_eq!(writers[0].write_target_ordinal, 0);
        assert!(writers[0].handle.is_some());
        assert!(writers[0].catalog_handle.is_some());
    }

    /// A dataflow write plan whose session never reached the encoder fails to
    /// encode rather than submitting a writer the backend could not bind.
    #[test]
    fn an_input_without_sealed_targets_cannot_encode_a_writer_node() {
        let input = dataflow_encoding_input();
        let error =
            encode_native_fragment_bundle_for_input(&input).expect_err("no sealed write session");
        assert!(
            error.contains("no sealed write session"),
            "unexpected error: {error}"
        );
    }
}
