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

//! Neutral write-target binding for SQL write statements.
//!
//! One statement resolves its write target exactly once, against a single
//! provider generation, and carries:
//!
//!   - the [`ConnectorTableMetadata`] that generation produced (neutral Arrow
//!     schema, bounded planning facts, opaque table handle);
//!   - the [`ConnectorControlPlanningLease`] that produced it, retained so the
//!     write lease derived from it acts on that same generation.
//!
//! Core never interprets the opaque handle. Physical write facts a writer
//! needs — staging location, sequence numbers, partition spec objects, commit
//! vocabulary, abort cleanup — are deliberately absent: they belong to Provider
//! write preparation, reached through the derived write lease.
//!
//! This is the write-path sibling of the MV refresh binding in
//! Frontend MV refresh target binding. The two are deliberately separate
//! types: the MV one additionally carries MV refresh-ledger identity
//! (refresh markers, bootstrap state, main-ancestor lineage) that has no
//! meaning for an INSERT or a row mutation.

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorControlResolver, ConnectorRequestContext,
    ConnectorRowMutationIntent, ConnectorRowMutationPreparation,
    ConnectorRowMutationPreparationOutcome, ConnectorRowMutationPreparationRequest,
    ConnectorTableColumnRole, ConnectorTableColumnVisibility, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableResolution, ConnectorWriteLease,
    ConnectorWriteOperationId, ConnectorWriteTargetRef,
};

/// One write target, resolved once against a single provider generation.
///
/// Cloning is cheap: the metadata's schema is an `Arc` and the lease is a
/// handle onto an already-resolved generation.
///
/// Deliberately not `Debug`: neither [`ConnectorTableMetadata`] nor
/// [`ConnectorControlPlanningLease`] is `Debug`, precisely so an opaque
/// provider handle and a live generation cannot end up in a log line.
#[derive(Clone)]
pub struct ConnectorWriteTargetBinding {
    metadata: ConnectorTableMetadata,
    lease: ConnectorControlPlanningLease,
}

impl ConnectorWriteTargetBinding {
    pub const fn new(
        metadata: ConnectorTableMetadata,
        lease: ConnectorControlPlanningLease,
    ) -> Self {
        Self { metadata, lease }
    }

    /// The exact generation that produced every fact in this binding.
    ///
    /// Write preparation must derive its lease from this one rather than
    /// re-resolving `latest`, otherwise a concurrent commit could split one
    /// statement across two generations.
    pub const fn lease(&self) -> &ConnectorControlPlanningLease {
        &self.lease
    }

    pub const fn metadata(&self) -> &ConnectorTableMetadata {
        &self.metadata
    }

    /// Opaque provider handle. Core passes it through and never decodes it.
    pub const fn handle(&self) -> &ConnectorTableHandle {
        &self.metadata.table
    }

    pub const fn identity(&self) -> &ConnectorTableIdentity {
        &self.metadata.identity
    }

    /// The write target's neutral Arrow schema.
    ///
    /// This replaces reading `current_schema()` off a concrete provider table:
    /// column shaping, projection and default filling all work from here.
    pub fn arrow_schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.metadata.schema
    }

    /// SQL-visible columns in the shape row DML must write.
    ///
    /// Visibility, row-lineage ownership and provider-declared write type
    /// overrides are all bounded neutral planning facts. Keeping this
    /// projection here avoids loading or decoding a concrete provider table in
    /// statement planning.
    pub fn dml_target_columns(&self) -> Vec<novarocks_types::schema::ColumnDef> {
        self.metadata
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, field)| {
                let fact = self.metadata.planning_facts.column_facts().get(ordinal);
                if matches!(
                    fact.map(|fact| fact.visibility()),
                    Some(ConnectorTableColumnVisibility::Hidden)
                ) || matches!(
                    fact.map(|fact| fact.role()),
                    Some(ConnectorTableColumnRole::RowLineageSystem)
                ) {
                    return None;
                }
                Some(novarocks_types::schema::ColumnDef {
                    name: field.name().to_string(),
                    data_type: fact
                        .and_then(|fact| fact.write_target_type())
                        .cloned()
                        .unwrap_or_else(|| field.data_type().clone()),
                    nullable: field.is_nullable(),
                    write_default: None,
                    logical_type: None,
                })
            })
            .collect()
    }

    /// Derive the write lease for this statement from the same generation.
    pub fn derive_write_lease(&self) -> Result<ConnectorWriteLease, String> {
        self.lease
            .derive_write_lease()
            .map_err(|error| error.to_string())
    }

    /// Derive the NCP-6 write-stack lease from the same generation that
    /// resolved this target.
    ///
    /// It names the generation explicitly rather than resolving whatever is
    /// active now, because between planning and commit the active generation
    /// can be replaced, and committing through the replacement would attach
    /// staged work to a runtime that never admitted it.
    pub(crate) fn derive_write_stack_lease(
        &self,
        host: &novarocks_catalog_application::ConnectorControlHost,
    ) -> Result<novarocks_catalog_application::ConnectorWriteStackLease, String> {
        derive_write_stack_lease(host, &self.lease)
    }

    /// Ask the exact generation that resolved this target to sign one row
    /// mutation. The opaque table handle is passed through unchanged.
    pub fn prepare_row_mutation(
        &self,
        target_ref: &str,
        operation_id: ConnectorWriteOperationId,
        intent: ConnectorRowMutationIntent,
        context: ConnectorRequestContext,
    ) -> Result<(ConnectorWriteLease, ConnectorRowMutationPreparation), String> {
        let lease = self
            .derive_write_lease()
            .map_err(|error| format!("derive connector row-mutation write lease: {error}"))?;
        let preparation = match lease
            .prepare_row_mutation(ConnectorRowMutationPreparationRequest {
                operation_id,
                table: self.handle().clone(),
                target_ref: ConnectorWriteTargetRef::parse(target_ref).map_err(|error| {
                    format!("validate connector row-mutation target ref: {error}")
                })?,
                intent,
                context,
            })
            .map_err(|error| format!("prepare connector row mutation: {error}"))?
        {
            ConnectorRowMutationPreparationOutcome::Prepared(preparation) => preparation,
            ConnectorRowMutationPreparationOutcome::Denied(error) => {
                return Err(format!("connector row-mutation admission denied: {error}"));
            }
        };
        Ok((lease, preparation))
    }

    /// Resolve the current head used by the existing durable DML journal.
    ///
    /// This intentionally preserves the historical RefHead observation rather
    /// than claiming it is the opaque base sealed into a write preparation.
    /// Those two facts can differ if the external ref moves; harmonizing them
    /// is a separate lifecycle change.
    pub fn journal_ref_head_snapshot_id(
        &self,
        target_ref: &str,
        context: ConnectorRequestContext,
    ) -> Result<Option<i64>, String> {
        let facts = super::metadata_read_reference_facts_with_planning_lease(
            self.lease.clone(),
            context,
            self.identity().namespace.as_ref(),
            self.identity().table.as_ref(),
        )?;
        if target_ref == "main" {
            return Ok(facts.current_snapshot_id());
        }
        facts
            .named_references()
            .iter()
            .find(|reference| reference.name.as_ref() == target_ref)
            .map(|reference| Some(reference.snapshot_id))
            .ok_or_else(|| {
                format!("iceberg ref: branch '{target_ref}' not found in table metadata")
            })
    }
}

/// Resolve a SQL write target into a neutral binding.
///
/// Mirrors `load_mv_target_binding`: acquire one planning lease, then load the
/// table metadata through that same lease, so the schema, planning facts and
/// opaque handle cannot drift apart.
pub fn load_write_target_binding(
    controls: &dyn ConnectorControlResolver,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
    context: ConnectorRequestContext,
) -> Result<ConnectorWriteTargetBinding, String> {
    let lease = super::acquire_metadata_planning_lease(controls, catalog)?;
    let metadata = super::metadata_load_connector_table_with_planning_lease(
        &lease, context, namespace, table, resolution,
    )?;
    Ok(ConnectorWriteTargetBinding::new(metadata, lease))
}

/// Derive the NCP-6 write-stack lease from a retained planning generation.
///
/// Statements that never build a [`ConnectorWriteTargetBinding`] -- the ones
/// that resolve their target through a row-mutation, rewrite, or MV binding --
/// reach the same generation through this function, so every write commits
/// through the incarnation that planned it rather than through whichever one
/// happens to be active at commit time.
pub(crate) fn derive_write_stack_lease(
    host: &novarocks_catalog_application::ConnectorControlHost,
    planning_lease: &ConnectorControlPlanningLease,
) -> Result<novarocks_catalog_application::ConnectorWriteStackLease, String> {
    host.acquire_exact_write_stack(planning_lease.control_runtime_id())
        .map_err(|error| format!("derive connector write-stack lease: {error}"))
}

/// Project a provider-signed input shape back into the request that produced
/// it.
///
/// A begin session must describe the same input the admission already signed.
/// Rebuilding the request from field names would let the two drift; projecting
/// the signed shape cannot.
pub(crate) fn write_input_request_for_shape(
    shape: &novarocks_spi::connector::ConnectorWriteInputShape,
) -> novarocks_spi::connector::ConnectorWriteInputRequest {
    use novarocks_spi::connector::{
        ConnectorWriteFieldRequest, ConnectorWriteInputRequest, ConnectorWriteInputShape,
    };

    fn requests(
        fields: &[novarocks_spi::connector::ConnectorWriteFieldBinding],
    ) -> Vec<ConnectorWriteFieldRequest> {
        fields
            .iter()
            .map(|field| ConnectorWriteFieldRequest::new(field.field().clone()))
            .collect()
    }

    match shape {
        ConnectorWriteInputShape::Data { fields } => ConnectorWriteInputRequest::Data {
            fields: requests(fields),
        },
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputRequest::RowLineage {
            data_fields: requests(data_fields),
            row_identity_fields: requests(row_identity_fields),
        },
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputRequest::PositionDelete {
            identity_fields: requests(identity_fields),
            partition_source_fields: requests(partition_source_fields),
        },
        ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputRequest::DeletionVector {
            identity_fields: requests(identity_fields),
            partition_source_fields: requests(partition_source_fields),
        },
        ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
            ConnectorWriteInputRequest::EqualityDelete {
                equality_fields: requests(equality_fields),
            }
        }
    }
}
