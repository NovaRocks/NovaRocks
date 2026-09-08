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

//! Fragment-native decoding for the typed connector scan source.
//!
//! Structural validation of the carrier belongs to the protocol layer: this
//! decoder parses the wire message once through
//! [`ConnectorTableScanSource::parse`] and never re-checks what that parse
//! already proved. What is left here is the fragment-local work the protocol
//! layer cannot do: deciding which relation kinds this backend slice reads,
//! agreeing the scan's ordered assignments with the columns the connector
//! actually reads, resolving the installed typed provider for exactly this
//! binding generation, and assembling the execution scan node.

use std::sync::Arc;

use novarocks_execution::exec::chunk::ChunkSchemaRef;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::scan::{BoundScanRanges, ScanSource};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::connector_read::{ConnectorRelation, ConnectorRelationKind};
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{connector_read as dto, plan};
use novarocks_spi::connector::CatalogHandle;
use novarocks_spi::connector::read_stack::ConnectorReadWorkSource;
use novarocks_types::SlotId;

use crate::connector::batch_transform::ConnectorBatchTransform;
use crate::connector::typed_runtime::{
    TypedConnectorScanSource, TypedConnectorSystemTableScanSource,
};

use super::super::context::NativePlanDecodeContext;
use super::super::error::NativeFragmentLeafDecodeError;
use super::super::node::DecodedNode;
use super::common::{
    ConnectorVariantPathTransform, DecodedScanOutputColumns, lower_scan_predicate,
    parse_scan_limit, validate_variant_path_read_slots,
};
use super::variant_path::NativeVariantPathPlan;

/// Lower one `ScanSource.typed_connector_read` into an execution scan node.
pub(super) fn lower_typed_connector_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &dto::ConnectorTableScanSource,
    output_columns: &DecodedScanOutputColumns,
    variant_path_plan: &NativeVariantPathPlan,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
    // One parse, and the only one: presence, bounds, known enums, uniqueness,
    // and the carrier's cross-field rules are the protocol layer's contract.
    let scan_source = novarocks_proto_codec::connector_read::ConnectorTableScanSource::parse(
        source.clone(),
        FieldPath::root("typed_connector_read"),
    )
    .map_err(leaf_from_protocol)?;

    let table = scan_source.table();
    // Closed relation set: every kind this slice does not read is a stable
    // typed refusal naming that kind, never a `_` arm that would silently
    // accept the next relation someone adds.
    match table.relation() {
        // A system relation is read like any other: what differs is only how
        // its work reaches this backend, which the carrier states separately as
        // its work source. Change-window and table-execute relations are read
        // like any other too: their splits name their specialized work, and the
        // connector's own page source is what knows the difference.
        ConnectorRelation::Table(_)
        | ConnectorRelation::ChangeWindow(_)
        | ConnectorRelation::SystemTable(_)
        | ConnectorRelation::TableExecute(_) => {}
        ConnectorRelation::TableFunction(_) | ConnectorRelation::MergeTable(_) => {
            return Err(unsupported_relation(table.relation_kind()));
        }
    }

    // Order matters, and this block must stay above the runtime resolution
    // below. Everything the carrier and the plan node must agree on is decided
    // here, from the wire alone: a plan this backend cannot execute is then
    // refused on its own terms rather than as a side effect of which providers
    // happen to be installed. Moving any of it below `typed_scan_runtime_inputs`
    // would let "no provider is installed" mask "this plan is not executable".
    let read_slot_ids = connector_read_slot_ids(scan, output_columns, variant_path_plan);
    check_assignments_match_read_columns(&scan_source, &read_slot_ids)?;
    let layout = output_columns.layout();
    let output_schema = output_columns.output_schema();
    let output_materialization =
        output_materialization(&read_slot_ids, &output_schema, variant_path_plan)?;

    let inputs = typed_scan_runtime_inputs(ctx)?;
    let catalog_handle = catalog_handle(table);
    let execution = (inputs.catalog_read_execution)(&catalog_handle).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "table", error)
            .append_field("catalog_name")
    })?;
    let decoded_scan = novarocks_proto_codec::connector_read::DecodedConnectorReadScan::decode(
        execution.decoder().as_ref(),
        &scan_source,
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "typed_connector_read",
            error.to_string(),
        )
    })?;
    ctx.typed_scan_runtime()
        .expect("typed runtime was resolved above")
        .register_read_execution(node.node_id, execution.clone())
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::Conflict,
                "typed_connector_read",
                error,
            )
        })?;

    let predicate = lower_scan_predicate(scan, arena, &layout, ctx)?;
    // `slot_ids[i]` names page channel `i`, and a page channel exists for each
    // assignment, so both lanes are handed the connector's read column list
    // rather than the node's output schema. Whatever separates the two is the
    // materialization's job, not this list's.
    let source: Arc<dyn ScanSource> = match decoded_scan.work_source() {
        ConnectorReadWorkSource::RuntimeSplits => {
            // The provider is built per fragment instance so its footer cache
            // and delete manager cannot outlive the request that opened them.
            let page_source_provider = execution
                .provider_factory()
                .create_page_source_provider(&inputs.request, inputs.reader_policy)
                .map_err(provider_refusal)?;
            let source = TypedConnectorScanSource::new(
                scan_source,
                decoded_scan,
                page_source_provider,
                inputs.session,
                inputs.request,
                inputs.queues,
                node.node_id,
                read_slot_ids,
                inputs.runtime_filter,
            );
            match output_materialization {
                Some(transform) => Arc::new(
                    source.with_output_materialization(transform, Arc::clone(&output_schema)),
                ),
                None => Arc::new(source),
            }
        }
        ConnectorReadWorkSource::WholeRelation => {
            // One backend reads the whole relation itself, so this lane needs
            // no split queue and no runtime filter: there is nothing to divide
            // and nothing to prune between splits.
            let system_table_provider = execution
                .provider_factory()
                .create_system_table_provider(&inputs.request)
                .map_err(provider_refusal)?;
            let source = TypedConnectorSystemTableScanSource::new(
                decoded_scan,
                system_table_provider,
                inputs.session,
                inputs.request,
                node.node_id,
                read_slot_ids,
            );
            match output_materialization {
                Some(transform) => Arc::new(
                    source.with_output_materialization(transform, Arc::clone(&output_schema)),
                ),
                None => Arc::new(source),
            }
        }
    };

    // Neither lane carries a frozen range: the split-driven one receives its
    // work on the task-update queue, and the whole-relation one already knows
    // its single unit of work. The range binding is therefore empty by
    // construction for both.
    ctx.capture_scan_ranges(node.node_id, BoundScanRanges::None);
    let scan_node = novarocks_execution::exec::node::scan::ScanNode::new(source)
        .with_node_id(node.node_id)
        .with_output_chunk_schema(Arc::clone(&output_schema))
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        // A split-driven scan may legally start with zero splits, so an empty
        // morsel set must not be padded into a synthetic one.
        .with_accept_empty_scan_ranges(true);
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout,
        output_schema,
    })
}

/// The fragment-local runtime inputs a typed scan needs beyond its carrier.
struct TypedScanRuntimeInputs {
    catalog_read_execution: super::super::context::CatalogReadExecutionResolver,
    queues: Arc<
        novarocks_execution::connector::TaskAttemptSplitQueues<
            crate::fragment::ingress::ReceivedReadSplit,
        >,
    >,
    session: novarocks_spi::connector::read_stack::ConnectorSession,
    request: novarocks_spi::connector::ConnectorRequestContext,
    reader_policy: novarocks_spi::connector::read_stack::ConnectorPageSourceProviderOptions,
    /// Absent when this attempt installed no runtime filter.
    runtime_filter: crate::fragment::decode::plan::context::RuntimeFilterSessionResolver,
}

/// Resolve the typed scan's runtime inputs from the fragment decode context.
///
/// The bundle is supplied by the fragment runtime at submission time. Refusing
/// when it is absent keeps the boundary fail-closed: the alternative would be a
/// scan bound to a registry it invented, which is exactly the fallback this
/// stack removes.
fn typed_scan_runtime_inputs(
    ctx: &NativePlanDecodeContext,
) -> Result<TypedScanRuntimeInputs, NativeFragmentLeafDecodeError> {
    let runtime = ctx.typed_scan_runtime().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "typed_connector_runtime",
            "typed connector scans require the typed provider registry, the task attempt split \
             queues, and the connector session in the fragment decode context",
        )
    })?;
    let (_, query_expire) =
        novarocks_execution::runtime::query_options::query_expire_durations(ctx.query_options());
    // Budgets here bound the connector's own request accounting, not the wire:
    // the carrier was already bounded by the protocol layer before it arrived.
    let request = novarocks_spi::connector::ConnectorRequestContext::try_new(
        std::time::Instant::now() + query_expire,
        ctx.connector_cancellation()?,
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map(|request| {
        request
            .with_storage_resolver(runtime.storage_resolver())
            .with_resources(runtime.connector_resources())
    })
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "typed_connector_runtime",
            error.to_string(),
        )
    })?;
    let cache = novarocks_execution::runtime::cache::ExecutionCacheOptions::from_query_options(
        ctx.query_options(),
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "query_options",
            error,
        )
    })?;
    Ok(TypedScanRuntimeInputs {
        catalog_read_execution: Arc::new({
            let runtime = runtime.clone();
            move |handle| runtime.catalog_read_execution(handle)
        }),
        queues: runtime.queues(),
        session: runtime.session(),
        request,
        reader_policy: novarocks_spi::connector::read_stack::ConnectorPageSourceProviderOptions {
            enable_parquet_reader_page_index: ctx
                .query_options()
                .is_some_and(|options| options.enable_parquet_reader_page_index()),
            data_cache: novarocks_spi::connector::read_stack::ConnectorDataCacheOptions {
                enable_scan_datacache: cache.enable_scan_datacache,
                enable_populate_datacache: cache.enable_populate_datacache,
                enable_datacache_async_populate_mode: cache.enable_datacache_async_populate_mode,
                enable_datacache_io_adaptor: cache.enable_datacache_io_adaptor,
                enable_cache_select: cache.enable_cache_select,
                datacache_evict_probability: cache.datacache_evict_probability,
                datacache_priority: cache.datacache_priority,
                datacache_ttl_seconds: cache.datacache_ttl_seconds,
                datacache_sharing_work_period: cache.datacache_sharing_work_period,
            },
        },
        runtime_filter: runtime.runtime_filter(),
    })
}

/// The slots the typed connector actually reads, in page-channel order.
///
/// `ScanNode.columns` carries two kinds of column: the connector's physical
/// columns, and the synthetic VARIANT path columns derived from one of them
/// after the read. Only the physical ones have an assignment, so only they name
/// a page channel; the synthetic ones are dropped here and would have to be
/// materialized on top of the read.
/// The slots the connector itself produces, in page-channel order.
///
/// Derived from the node's decoded output rather than from every column the
/// scan lists: a scan node carries the whole relation's columns and the
/// required set narrows them, so reading from the full list would ask the
/// connector for columns the query never reads — including the metadata
/// pseudo-columns no data file holds. The synthetic columns are then removed,
/// because the engine builds those above the connector.
fn connector_read_slot_ids(
    scan: &plan::ScanNode,
    output_columns: &DecodedScanOutputColumns,
    variant_path_plan: &NativeVariantPathPlan,
) -> Vec<SlotId> {
    let output_slot_ids = output_columns
        .columns()
        .iter()
        .map(|column| SlotId::new(column.column_id))
        .filter(|slot_id| !variant_path_plan.output_slot_ids.contains(slot_id))
        .collect::<std::collections::HashSet<_>>();
    let variant_source_slot_ids = variant_path_plan
        .specs
        .iter()
        .map(|spec| spec.source_read_slot_id)
        .collect::<std::collections::HashSet<_>>();
    // A query can project a regular column beside a derived VARIANT path while
    // omitting the physical VARIANT source from its output. The source still
    // needs a page channel so the transform can materialize the derived slot.
    // Keep every selected physical column in the scan's declared order: that
    // is the same order in which the producer assigned page channels.
    scan.columns
        .iter()
        .map(|column| SlotId::new(column.column_id))
        .filter(|slot_id| {
            output_slot_ids.contains(slot_id) || variant_source_slot_ids.contains(slot_id)
        })
        .collect()
}

/// Agree the carrier's ordered assignments with the columns it reads.
///
/// The two orders are one contract: `assignments[i]` produces page channel `i`,
/// and read column `i` binds that channel. Count is the only part of that
/// contract this side can check: an assignment's variable is an opaque
/// expression identifier minted by the producer, deliberately not the column's
/// name, so comparing the two would reject every real scan.
fn check_assignments_match_read_columns(
    scan_source: &novarocks_proto_codec::connector_read::ConnectorTableScanSource,
    read_slot_ids: &[SlotId],
) -> Result<(), NativeFragmentLeafDecodeError> {
    let assignments = scan_source.assignments();
    if assignments.len() != read_slot_ids.len() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "assignments",
            format!(
                "typed connector scan assigns {} columns but the plan node declares {} connector read columns",
                assignments.len(),
                read_slot_ids.len()
            ),
        ));
    }
    Ok(())
}

/// How this scan turns its read columns into the node's output columns.
///
/// `None` is the ordinary scan, whose page channels already are the output.
/// A scan that projects a VARIANT path reads only the physical source column
/// and derives the synthetic one after the read, which is the same derivation
/// the opaque carrier attaches to its reader.
fn output_materialization(
    read_slot_ids: &[SlotId],
    output_schema: &ChunkSchemaRef,
    variant_path_plan: &NativeVariantPathPlan,
) -> Result<Option<Arc<dyn ConnectorBatchTransform>>, NativeFragmentLeafDecodeError> {
    if variant_path_plan.specs.is_empty() {
        // No derived column exists, so nothing can rebuild one column list into
        // the other: the connector must already read exactly the output.
        let output_slot_ids = output_schema.slot_ids();
        if read_slot_ids != output_slot_ids {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::Unsupported,
                "assignments",
                format!(
                    "typed connector scan reads slots {read_slot_ids:?} but the plan node declares \
                     output slots {output_slot_ids:?} and derives no column, so nothing can \
                     produce the difference"
                ),
            ));
        }
        return Ok(None);
    }
    validate_variant_path_read_slots(&variant_path_plan.specs, read_slot_ids, "assignments")?;
    Ok(Some(Arc::new(ConnectorVariantPathTransform::new(
        read_slot_ids.to_vec(),
        output_schema,
        variant_path_plan.specs.clone(),
    ))))
}

/// Carry a provider refusal back as the catalog name it was resolved under.
///
/// Both lanes resolve a per-fragment-instance provider from the same installed
/// binding, so both name the same wire field when that resolution fails.
fn provider_refusal(
    error: novarocks_spi::connector::ConnectorError,
) -> NativeFragmentLeafDecodeError {
    NativeFragmentLeafDecodeError::at_field(
        ProtocolErrorKind::InvalidValue,
        "table",
        error.to_string(),
    )
    .append_field("catalog_name")
}

/// The exact immutable catalog runtime identity this relation belongs to.
fn catalog_handle(
    table: &novarocks_proto_codec::connector_read::CatalogTableHandle,
) -> CatalogHandle {
    table.catalog_handle().clone()
}

/// The stable refusal for a relation kind this slice does not read yet.
fn unsupported_relation(kind: ConnectorRelationKind) -> NativeFragmentLeafDecodeError {
    let named = match kind {
        // Reached only if the supported arm above is ever narrowed; keeping it
        // in this closed match is what makes a new relation kind a compile
        // error at both sites rather than a silent acceptance here.
        ConnectorRelationKind::Table => "table",
        ConnectorRelationKind::TableFunction => "table_function",
        ConnectorRelationKind::ChangeWindow => "change_window",
        ConnectorRelationKind::SystemTable => "system_table",
        ConnectorRelationKind::TableExecute => "table_execute",
        ConnectorRelationKind::MergeTable => "merge_table",
    };
    NativeFragmentLeafDecodeError::at_field(
        ProtocolErrorKind::Unsupported,
        "table",
        format!("typed connector scan relation `{named}` is not admitted yet"),
    )
}

/// Carry a protocol refusal into the fragment decoder without rebuilding it.
///
/// The protocol error already names its own field path, and the leaf error can
/// only append static field names and indexes, so the exact path travels in the
/// detail while the kind and the carrier's own root are preserved.
fn leaf_from_protocol(error: ProtocolError) -> NativeFragmentLeafDecodeError {
    NativeFragmentLeafDecodeError::at_collection(error.kind(), error.to_string())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_proto_models::common;

    use crate::connector::typed_runtime::test_support;

    use novarocks_execution::exec::node::scan::{ScanMorsel, ScanMorsels};

    use super::super::super::node::decode_node;
    use super::*;

    /// A live attempt, so a decode reaches the binding instead of failing on a
    /// cancellation it was never given.
    struct NeverCancelled;

    impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    /// Borrow the scan out of a fixture node.
    fn scan_of(node: &plan::DistributedNode) -> &plan::ScanNode {
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
        else {
            panic!("fixture node carries a physical payload");
        };
        let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
            panic!("fixture node carries a scan");
        };
        scan
    }

    /// Mutably borrow the scan out of a fixture node.
    fn scan_of_mut(node: &mut plan::DistributedNode) -> &mut plan::ScanNode {
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
        else {
            panic!("fixture node carries a physical payload");
        };
        let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_mut() else {
            panic!("fixture node carries a scan");
        };
        scan
    }

    /// Build a distributed scan node whose source is the typed carrier.
    fn typed_scan_node(
        source: dto::ConnectorTableScanSource,
        columns: Vec<common::OutputColumn>,
    ) -> plan::DistributedNode {
        typed_scan_node_with_required(source, columns, Vec::new())
    }

    /// The same node, with `required_columns` narrowing the decoded output.
    fn typed_scan_node_with_required(
        source: dto::ConnectorTableScanSource,
        columns: Vec<common::OutputColumn>,
        required_columns: Vec<String>,
    ) -> plan::DistributedNode {
        let table_columns = columns
            .iter()
            .map(|column| typed_column_def(&column.name, &DataType::Int64))
            .collect();
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns.clone(),
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: table_columns,
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource {
                            kind: Some(plan::scan_source::Kind::TypedConnectorRead(source)),
                        }),
                    }),
                    alias: None,
                    columns,
                    predicates: Vec::new(),
                    required_columns,
                    dict_columns: Vec::new(),
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    fn output_column(column_id: u32, name: &str) -> common::OutputColumn {
        typed_output_column(column_id, name, &DataType::Int64)
    }

    fn typed_output_column(
        column_id: u32,
        name: &str,
        data_type: &DataType,
    ) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(
                crate::fragment::decode::type_decode::encode_type(data_type).expect("encode type"),
            ),
            nullable: true,
            is_internal: false,
        }
    }

    fn typed_column_def(name: &str, data_type: &DataType) -> plan::ColumnDef {
        plan::ColumnDef {
            name: name.to_string(),
            data_type: Some(
                crate::fragment::decode::type_decode::encode_type(data_type).expect("encode type"),
            ),
            nullable: true,
            write_default_json: None,
            logical_type: None,
        }
    }

    /// A typed scan whose wire columns mix physical columns with one synthetic
    /// VARIANT path column derived from the LargeBinary column `v`.
    ///
    /// `extra_physical` adds further physical columns after `v`, which is how a
    /// test moves the connector read column count without touching the carrier.
    fn typed_variant_scan_node(
        source: dto::ConnectorTableScanSource,
        extra_physical: &[&str],
    ) -> plan::DistributedNode {
        let mut table_columns = vec![
            typed_column_def("v", &DataType::LargeBinary),
            typed_column_def("__nr_var_v_0", &DataType::Int64),
        ];
        let mut columns = vec![
            typed_output_column(1, "v", &DataType::LargeBinary),
            typed_output_column(2, "__nr_var_v_0", &DataType::Int64),
        ];
        let mut required_columns = vec!["v".to_string(), "__nr_var_v_0".to_string()];
        for (index, name) in extra_physical.iter().enumerate() {
            let column_id = u32::try_from(index).expect("column index") + 3;
            table_columns.push(typed_column_def(name, &DataType::Int64));
            columns.push(typed_output_column(column_id, name, &DataType::Int64));
            required_columns.push((*name).to_string());
        }
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns.clone(),
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: table_columns,
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource {
                            kind: Some(plan::scan_source::Kind::TypedConnectorRead(source)),
                        }),
                    }),
                    alias: None,
                    columns,
                    predicates: Vec::new(),
                    required_columns,
                    dict_columns: Vec::new(),
                    variant_columns: vec![plan::ScanVariantColumn {
                        source_column_id: 1,
                        source_column: "v".to_string(),
                        synthetic_column_id: 2,
                        synthetic_column: "__nr_var_v_0".to_string(),
                        canonical_path: "$.a.b".to_string(),
                        requested_type: Some(
                            crate::fragment::decode::type_decode::encode_type(&DataType::Int64)
                                .expect("encode type"),
                        ),
                        strict: true,
                    }],
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    fn decode_node_error(node: &plan::DistributedNode) -> novarocks_proto_codec::ProtocolError {
        let error = decode_node(
            node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("typed connector decoding must refuse");
        error.protocol().expect("protocol error").clone()
    }

    fn decode_error(
        source: dto::ConnectorTableScanSource,
        columns: Vec<common::OutputColumn>,
    ) -> novarocks_proto_codec::ProtocolError {
        let node = typed_scan_node(source, columns);
        let error = decode_node(
            &node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("typed connector decoding must refuse");
        error.protocol().expect("protocol error").clone()
    }

    /// The fixture carrier with a second ordered assignment appended.
    fn scan_source_with_two_assignments() -> dto::ConnectorTableScanSource {
        let mut source = test_support::scan_source_proto();
        source.assignments.push(dto::ScanAssignment {
            variable: "v1".to_owned(),
            column: Some(test_support::column_handle(2)),
            value_type: Some(novarocks_proto_codec::connector_read::encode_value_type(
                novarocks_spi::connector::read_stack::ConnectorValueType::BigInt,
            )),
        });
        source
    }

    /// The `$files` system relation, whose reference is valid for either lane.
    fn system_table_relation() -> dto::catalog_table_handle::Relation {
        dto::catalog_table_handle::Relation::SystemTable(dto::ConnectorSystemTableReference {
            provider_payload: Some(test_support::encoded_payload(
                novarocks_spi::connector::ConnectorCodecCategory::ReadTable,
                bytes::Bytes::from_static(b"system-table"),
            )),
        })
    }

    fn change_window_relation() -> dto::catalog_table_handle::Relation {
        dto::catalog_table_handle::Relation::ChangeWindow(dto::ConnectorChangeWindowHandle {
            provider_payload: Some(test_support::encoded_payload(
                novarocks_spi::connector::ConnectorCodecCategory::ReadTable,
                bytes::Bytes::from_static(b"change-window"),
            )),
        })
    }

    /// A system relation carrier that states how its work reaches this backend.
    fn system_table_scan_source(work_source: dto::ScanWorkSource) -> dto::ConnectorTableScanSource {
        let mut source = scan_with_relation(system_table_relation());
        source.work_source = work_source as i32;
        source
    }

    /// Lower one typed scan and bind its source the way the pipeline does.
    ///
    /// Returns the bound source's profile name, which is how a test tells the
    /// two lanes apart, and the morsel set it built before any split was ever
    /// offered.
    fn lower_and_build_morsels(node: &plan::DistributedNode) -> (String, ScanMorsels) {
        let ctx = NativePlanDecodeContext::default()
            .with_connector_cancellation(std::sync::Arc::new(NeverCancelled))
            .with_typed_scan_runtime(Some(test_support::typed_scan_runtime()));
        let decoded =
            decode_node(node, &mut ExprArena::default(), &ctx).expect("lower the typed scan");
        let ExecNodeKind::Scan(scan) = decoded.node.kind else {
            panic!("a lowered typed scan is a scan node");
        };
        let profile = scan
            .source()
            .profile_name()
            .expect("a bound scan source names its profile");
        let morsels = scan
            .source()
            .bind(
                ctx.captured_ranges_for_test(node.node_id)
                    .expect("scan decode captures ranges"),
            )
            .expect("bind the lowered scan source")
            .build_morsels()
            .expect("build the lowered scan's morsels");
        (profile, morsels)
    }

    /// Replace the carrier's relation while keeping everything else valid.
    fn scan_with_relation(
        relation: dto::catalog_table_handle::Relation,
    ) -> dto::ConnectorTableScanSource {
        let mut source = test_support::scan_source_proto();
        let table = source.table.as_mut().expect("carrier table");
        table.relation = Some(relation);
        source
    }

    #[test]
    fn typed_scan_decode_without_a_runtime_fails_closed() {
        // A decode context that cannot supply the runtime must refuse rather
        // than bind the scan to a registry it invented.
        let error = decode_error(
            test_support::scan_source_proto(),
            vec![output_column(1, "id")],
        );
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert!(
            error
                .path()
                .to_string()
                .ends_with("typed_connector_read.typed_connector_runtime"),
            "unexpected path: {}",
            error.path()
        );
    }

    #[test]
    fn typed_scan_decode_binds_a_table_relation_once_the_runtime_is_supplied() {
        let node = typed_scan_node(
            test_support::scan_source_proto(),
            vec![output_column(1, "id")],
        );
        let ctx = NativePlanDecodeContext::default()
            .with_connector_cancellation(std::sync::Arc::new(NeverCancelled))
            .with_typed_scan_runtime(Some(test_support::typed_scan_runtime()));
        decode_node(&node, &mut ExprArena::default(), &ctx)
            .expect("a supplied runtime binds the typed scan");
    }

    #[test]
    fn typed_scan_runtime_carries_the_query_page_index_policy() {
        let ctx = NativePlanDecodeContext::default()
            .with_query_options(Some(
                novarocks_execution::runtime::query_options::QueryOptions {
                    enable_parquet_reader_page_index: true,
                    ..Default::default()
                },
            ))
            .with_connector_cancellation(std::sync::Arc::new(NeverCancelled))
            .with_typed_scan_runtime(Some(test_support::typed_scan_runtime()));
        let inputs = typed_scan_runtime_inputs(&ctx).expect("typed runtime inputs");
        assert!(inputs.reader_policy.enable_parquet_reader_page_index);
    }

    #[test]
    fn typed_scan_decode_rejects_an_assignment_count_mismatch() {
        let error = decode_error(
            test_support::scan_source_proto(),
            vec![output_column(1, "id"), output_column(2, "flag")],
        );
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert!(
            error
                .detail()
                .contains("assigns 1 columns but the plan node declares 2 connector read columns"),
            "unexpected detail: {}",
            error.detail()
        );
    }

    /// A synthetic VARIANT path column is derived from a physical column after
    /// the read, so it has no assignment and no page channel. Counting it as a
    /// connector read column would make every real variant scan look like an
    /// assignment count mismatch.
    #[test]
    fn typed_scan_decode_counts_assignments_against_physical_columns_not_synthetic_ones() {
        // Two physical columns (`v`, `extra`) and one synthetic column against
        // one assignment: the reported read count must be 2, not 3.
        let error = decode_node_error(&typed_variant_scan_node(
            test_support::scan_source_proto(),
            &["extra"],
        ));
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert!(
            error
                .detail()
                .contains("assigns 1 columns but the plan node declares 2 connector read columns"),
            "unexpected detail: {}",
            error.detail()
        );
    }

    /// The connector reads exactly what the node outputs.
    ///
    /// A scan node carries every column of its relation and `required_columns`
    /// narrows them, so a producer that assigned the un-narrowed list would ask
    /// the connector for columns the query never reads — including the metadata
    /// pseudo-columns no data file holds. Assignments are therefore agreed
    /// against the narrowed output, and a producer that sends the wider list is
    /// a count mismatch.
    #[test]
    fn typed_scan_decode_agrees_assignments_against_the_narrowed_output() {
        let node = typed_scan_node_with_required(
            scan_source_with_two_assignments(),
            vec![output_column(1, "id"), output_column(2, "flag")],
            vec!["id".to_string()],
        );
        let error = decode_node_error(&node);
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert!(
            error.detail().contains("assigns 2 columns")
                && error.detail().contains("1 connector read columns"),
            "unexpected detail: {}",
            error.detail()
        );
    }

    /// A scan that projects a VARIANT path reads only the physical source
    /// column — the synthetic column has no assignment and so no page channel —
    /// while the node keeps declaring both, because the synthetic one is
    /// materialized on top of the read rather than fetched by the connector.
    #[test]
    fn typed_scan_decode_reads_only_physical_slots_and_still_outputs_the_synthetic_column() {
        let node = typed_variant_scan_node(test_support::scan_source_proto(), &[]);
        let scan = scan_of(&node);
        let table = scan.table.as_ref().expect("fixture table");
        let output_columns =
            super::super::common::decode_scan_output_columns(scan, FieldPath::root("scan"))
                .expect("decode scan output columns");
        let variant_path_plan = super::super::variant_path::parse_native_scan_variant_path_columns(
            scan,
            table,
            output_columns.columns(),
        )
        .expect("parse variant path columns");

        // One assignment, one page channel, one read slot: the synthetic column
        // is not among them.
        assert_eq!(test_support::scan_source_proto().assignments.len(), 1);
        assert_eq!(
            connector_read_slot_ids(scan, &output_columns, &variant_path_plan),
            vec![SlotId::new(1)]
        );

        let ctx = NativePlanDecodeContext::default()
            .with_connector_cancellation(std::sync::Arc::new(NeverCancelled))
            .with_typed_scan_runtime(Some(test_support::typed_scan_runtime()));
        let decoded = decode_node(&node, &mut ExprArena::default(), &ctx)
            .expect("a VARIANT path scan lowers once its runtime is supplied");
        assert_eq!(
            decoded.output_schema.slot_ids(),
            [SlotId::new(1), SlotId::new(2)],
            "the node must still declare the synthetic column it materializes"
        );
    }

    /// A derived VARIANT path can require a physical source that is not part
    /// of the output projection. The source still precedes later regular
    /// columns in the connector's page-channel order.
    #[test]
    fn typed_scan_decode_keeps_an_unprojected_variant_source_in_channel_order() {
        let mut node = typed_variant_scan_node(scan_source_with_two_assignments(), &["id"]);
        let scan = scan_of_mut(&mut node);
        scan.required_columns = vec!["__nr_var_v_0".to_string(), "id".to_string()];
        let table = scan.table.as_ref().expect("fixture table");
        let output_columns =
            super::super::common::decode_scan_output_columns(scan, FieldPath::root("scan"))
                .expect("decode narrowed output columns");
        let variant_path_plan = super::super::variant_path::parse_native_scan_variant_path_columns(
            scan,
            table,
            output_columns.columns(),
        )
        .expect("parse variant path columns");

        assert_eq!(
            connector_read_slot_ids(scan, &output_columns, &variant_path_plan),
            vec![SlotId::new(1), SlotId::new(3)],
            "the connector must read the hidden VARIANT source before the later regular column"
        );
    }

    /// The producer mints opaque positional variables, never column names, so
    /// a decoder that compared the two would reject every real scan.
    #[test]
    fn typed_scan_decode_binds_an_assignment_whose_variable_is_not_the_column_name() {
        let node = typed_scan_node(
            test_support::scan_source_proto(),
            vec![output_column(1, "id")],
        );
        let ctx = NativePlanDecodeContext::default()
            .with_connector_cancellation(std::sync::Arc::new(NeverCancelled))
            .with_typed_scan_runtime(Some(test_support::typed_scan_runtime()));
        assert_ne!(
            test_support::scan_source_proto().assignments[0].variable,
            "id",
            "the fixture must not accidentally name the output column"
        );
        decode_node(&node, &mut ExprArena::default(), &ctx)
            .expect("an opaque assignment variable binds the typed scan");
    }

    /// Derived columns are a property of the plan node, not of how the scan's
    /// work arrives, so the split-free lane materializes them through the same
    /// seam the split-driven one uses.
    #[test]
    fn typed_scan_decode_materializes_variant_path_columns_on_the_split_free_lane_too() {
        let node = typed_variant_scan_node(
            system_table_scan_source(dto::ScanWorkSource::WholeRelation),
            &[],
        );
        let ctx = NativePlanDecodeContext::default()
            .with_connector_cancellation(std::sync::Arc::new(NeverCancelled))
            .with_typed_scan_runtime(Some(test_support::typed_scan_runtime()));
        let decoded = decode_node(&node, &mut ExprArena::default(), &ctx)
            .expect("a whole-relation scan with a VARIANT path column lowers");
        let ExecNodeKind::Scan(scan) = &decoded.node.kind else {
            panic!("a lowered typed scan is a scan node");
        };
        assert_eq!(
            scan.source()
                .profile_name()
                .expect("a bound scan source names its profile"),
            "TypedConnectorSystemTableScan"
        );
        assert_eq!(
            decoded.output_schema.slot_ids(),
            [SlotId::new(1), SlotId::new(2)],
            "the node must still declare the synthetic column it materializes"
        );
    }

    /// A distributed system relation is scheduled exactly like a data table:
    /// its splits arrive on the task-update queue, so it binds the split-driven
    /// source and stays alive until that queue is exhausted.
    #[test]
    fn typed_scan_decode_lowers_a_runtime_split_system_relation_to_the_split_driven_source() {
        let node = typed_scan_node(
            system_table_scan_source(dto::ScanWorkSource::RuntimeSplits),
            vec![output_column(1, "id")],
        );
        let (profile, morsels) = lower_and_build_morsels(&node);
        assert_eq!(profile, "TypedConnectorScan");
        // One morsel even before a split exists: it is the driver that drains
        // the queue, and reporting none would leave every delivered split
        // unread.
        assert_eq!(morsels.morsels.len(), 1);
        assert!(
            !morsels.has_more,
            "a split-driven scan grows its queue, not its morsel set"
        );
    }

    #[test]
    fn typed_scan_decode_lowers_a_change_window_to_the_split_driven_source() {
        let node = typed_scan_node(
            scan_with_relation(change_window_relation()),
            vec![output_column(1, "id")],
        );
        let (profile, morsels) = lower_and_build_morsels(&node);
        assert_eq!(profile, "TypedConnectorScan");
        assert_eq!(morsels.morsels.len(), 1);
        assert!(!morsels.has_more);
    }

    /// A single-backend system relation has no split at all: one backend reads
    /// the whole relation itself, so it binds the split-free source.
    #[test]
    fn typed_scan_decode_lowers_a_whole_relation_system_relation_to_the_split_free_source() {
        let node = typed_scan_node(
            system_table_scan_source(dto::ScanWorkSource::WholeRelation),
            vec![output_column(1, "id")],
        );
        let (profile, _) = lower_and_build_morsels(&node);
        assert_eq!(profile, "TypedConnectorSystemTableScan");
    }

    /// The failure the split-driven lane would have hung on: no split is ever
    /// offered for a whole-relation scan, so its work must already be complete
    /// and closed at bind time rather than waiting for one that never comes.
    #[test]
    fn typed_scan_decode_lets_a_whole_relation_system_scan_terminate_with_no_split_offered() {
        let node = typed_scan_node(
            system_table_scan_source(dto::ScanWorkSource::WholeRelation),
            vec![output_column(1, "id")],
        );
        let (_, morsels) = lower_and_build_morsels(&node);
        assert!(
            matches!(&morsels.morsels[..], [ScanMorsel::OperatorDriven]),
            "the whole relation is exactly one unit of work: {:?}",
            morsels.morsels
        );
        assert!(
            !morsels.has_more,
            "nothing can add work to a whole-relation scan, so it must not wait for a split"
        );
    }

    #[test]
    fn typed_scan_decode_refuses_every_relation_kind_it_does_not_read_yet() {
        let cases = [
            (
                dto::catalog_table_handle::Relation::TableFunction(
                    dto::ConnectorTableFunctionHandle {
                        provider_payload: Some(test_support::encoded_payload(
                            novarocks_spi::connector::ConnectorCodecCategory::ReadTable,
                            bytes::Bytes::from_static(b"table-function"),
                        )),
                    },
                ),
                "table_function",
            ),
            (
                dto::catalog_table_handle::Relation::MergeTable(dto::ConnectorMergeTableHandle {
                    provider_payload: Some(test_support::encoded_payload(
                        novarocks_spi::connector::ConnectorCodecCategory::ReadTable,
                        bytes::Bytes::from_static(b"merge-table"),
                    )),
                }),
                "merge_table",
            ),
        ];
        for (relation, expected) in cases {
            let error = decode_error(scan_with_relation(relation), vec![output_column(1, "id")]);
            assert_eq!(error.kind(), ProtocolErrorKind::Unsupported);
            assert!(
                error.detail().contains(expected),
                "expected `{expected}` in: {}",
                error.detail()
            );
            assert!(
                error
                    .path()
                    .to_string()
                    .ends_with("typed_connector_read.table"),
                "unexpected path: {}",
                error.path()
            );
        }
    }

    #[test]
    fn typed_scan_decode_reports_a_structurally_invalid_carrier_as_a_protocol_refusal() {
        let mut source = test_support::scan_source_proto();
        source.max_batch_rows = 0;
        let error = decode_error(source, vec![output_column(1, "id")]);
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert!(
            error.detail().contains("max_batch_rows"),
            "unexpected detail: {}",
            error.detail()
        );
    }
}
