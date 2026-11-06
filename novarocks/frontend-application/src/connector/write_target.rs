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
//!   - the [`ConnectorControlPlanningPin`] that produced it, retained so the
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
    ConnectorControlPlanningPin, ConnectorRequestContext, ConnectorRowMutationIntent,
    ConnectorRowMutationPreparation, ConnectorRowMutationPreparationOutcome,
    ConnectorRowMutationPreparationRequest, ConnectorTableColumnRole,
    ConnectorTableColumnVisibility, ConnectorTableHandle, ConnectorTableIdentity,
    ConnectorTableMetadata, ConnectorWrite, ConnectorWriteOperationId, ConnectorWriteTargetRef,
};

use crate::dml::error::DmlExecutionError;

/// One write target, resolved once against a single provider generation.
///
/// Cloning is cheap: the metadata's schema is an `Arc` and the lease is a
/// handle onto an already-resolved generation.
///
/// Deliberately not `Debug`: neither [`ConnectorTableMetadata`] nor
/// [`ConnectorControlPlanningPin`] is `Debug`, precisely so an opaque
/// provider handle and a live generation cannot end up in a log line.
#[derive(Clone)]
pub struct ConnectorWriteTargetBinding {
    metadata: ConnectorTableMetadata,
    planning_pin: ConnectorControlPlanningPin,
}

impl ConnectorWriteTargetBinding {
    pub const fn new(
        metadata: ConnectorTableMetadata,
        planning_pin: ConnectorControlPlanningPin,
    ) -> Self {
        Self {
            metadata,
            planning_pin,
        }
    }

    /// The exact generation that produced every fact in this binding.
    ///
    /// Write preparation must derive its lease from this one rather than
    /// re-resolving `latest`, otherwise a concurrent commit could split one
    /// statement across two generations.
    pub const fn planning_pin(&self) -> &ConnectorControlPlanningPin {
        &self.planning_pin
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

    /// SQL-visible columns in the shape row DML must write.
    ///
    /// Visibility, row-lineage ownership and provider-declared write type
    /// overrides are all bounded neutral planning facts. Keeping this
    /// projection here avoids loading or decoding a concrete provider table in
    /// statement planning.
    pub fn dml_target_columns(&self) -> Result<Vec<novarocks_types::schema::ColumnDef>, String> {
        crate::connector::require_current_write_authority(&self.metadata.planning_facts)?;
        self.metadata
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, _field)| {
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
                Some(crate::connector::application::connector_write_column(
                    &self.metadata,
                    ordinal,
                    None,
                ))
            })
            .collect()
    }

    /// Derive the write lease for this statement from the same generation.
    pub fn derive_write_lease(&self) -> Result<ConnectorWrite, String> {
        self.planning_pin
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
    ) -> Result<novarocks_catalog_application::ConnectorWriteStackPin, String> {
        derive_write_stack_lease(host, &self.planning_pin)
    }

    /// Ask the exact generation that resolved this target to sign one row
    /// mutation. The opaque table handle is passed through unchanged.
    pub fn prepare_row_mutation(
        &self,
        target_ref: &str,
        operation_id: ConnectorWriteOperationId,
        intent: ConnectorRowMutationIntent,
        context: ConnectorRequestContext,
    ) -> Result<(ConnectorWrite, ConnectorRowMutationPreparation), DmlExecutionError> {
        let connector_write = self
            .derive_write_lease()
            .map_err(|error| format!("derive connector row-mutation write lease: {error}"))?;
        let preparation = row_mutation_preparation(connector_write.prepare_row_mutation(
            ConnectorRowMutationPreparationRequest {
                operation_id,
                table: self.handle().clone(),
                target_ref: ConnectorWriteTargetRef::parse(target_ref).map_err(|error| {
                    format!("validate connector row-mutation target ref: {error}")
                })?,
                intent,
                context,
            },
        ))?;
        Ok((connector_write, preparation))
    }
}

/// Provider refusals and failed calls retain their original fact through DML.
fn row_mutation_preparation(
    outcome: Result<
        ConnectorRowMutationPreparationOutcome,
        novarocks_spi::connector::ConnectorError,
    >,
) -> Result<ConnectorRowMutationPreparation, DmlExecutionError> {
    let provider_failure = |error| {
        DmlExecutionError::Application(
            novarocks_query_application::session_error::QueryServiceError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            ),
        )
    };
    match outcome.map_err(provider_failure)? {
        ConnectorRowMutationPreparationOutcome::Prepared(preparation) => Ok(preparation),
        ConnectorRowMutationPreparationOutcome::Denied(error) => Err(provider_failure(error)),
    }
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
    planning_pin: &ConnectorControlPlanningPin,
) -> Result<novarocks_catalog_application::ConnectorWriteStackPin, String> {
    host.acquire_exact_write_stack(planning_pin.control_runtime_id())
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
) -> Result<novarocks_spi::connector::ConnectorWriteInputRequest, String> {
    use novarocks_spi::connector::{
        ConnectorWriteFieldRequest, ConnectorWriteInputRequest, ConnectorWriteInputShape,
    };

    fn requests(
        fields: &[novarocks_spi::connector::ConnectorWriteFieldBinding],
    ) -> Result<Vec<ConnectorWriteFieldRequest>, String> {
        fields
            .iter()
            .map(|field| {
                let value = field.execution_type().ok_or_else(|| {
                    "provider-signed writer field has no complete execution declaration".to_string()
                })?;
                ConnectorWriteFieldRequest::try_new_typed(field.field().clone(), value.clone())
                    .map_err(|error| error.to_string())
            })
            .collect()
    }

    Ok(match shape {
        ConnectorWriteInputShape::Data { fields } => ConnectorWriteInputRequest::Data {
            fields: requests(fields)?,
        },
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputRequest::RowLineage {
            data_fields: requests(data_fields)?,
            row_identity_fields: requests(row_identity_fields)?,
        },
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputRequest::PositionDelete {
            identity_fields: requests(identity_fields)?,
            partition_source_fields: requests(partition_source_fields)?,
        },
        ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputRequest::DeletionVector {
            identity_fields: requests(identity_fields)?,
            partition_source_fields: requests(partition_source_fields)?,
        },
        ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
            ConnectorWriteInputRequest::EqualityDelete {
                equality_fields: requests(equality_fields)?,
            }
        }
    })
}

#[cfg(test)]
mod row_mutation_preparation_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

    #[test]
    fn provider_row_mutation_refusal_keeps_its_original_fact_through_dml() {
        for denied in [false, true] {
            for kind in [
                ConnectorErrorKind::InvalidRequest,
                ConnectorErrorKind::Internal,
                ConnectorErrorKind::Cancelled,
                ConnectorErrorKind::DeadlineExceeded,
                ConnectorErrorKind::ResourceExhausted,
                ConnectorErrorKind::Unavailable,
            ] {
                // Identical diagnostic text cannot give a system or control
                // failure the InvalidRequest author's user-error identity.
                let original =
                    ConnectorError::new(kind, "UPDATE and MERGE require an Iceberg v3 table");
                let fact = original.failure_fact();
                let outcome = if denied {
                    Ok(ConnectorRowMutationPreparationOutcome::Denied(original))
                } else {
                    Err(original)
                };
                let failure = match row_mutation_preparation(outcome) {
                    Ok(_) => panic!("scripted provider refusal cannot prepare a mutation"),
                    Err(failure) => failure.into_dml_error(None),
                };
                let application = failure
                    .application_error()
                    .expect("typed application error");
                let source = application
                    .failure_cause()
                    .expect("original provider cause");
                let provider = source
                    .downcast_ref::<ConnectorError>()
                    .expect("original ConnectorError remains available");
                assert_eq!(provider.kind(), kind);
                assert_eq!(source.failure_fact(), fact);
                assert_eq!(failure.execution_failure().unwrap().fact(), fact);
                let expected_class = match kind {
                    ConnectorErrorKind::InvalidRequest => FailureClass::Data,
                    ConnectorErrorKind::Internal => FailureClass::Unexpected,
                    ConnectorErrorKind::Cancelled | ConnectorErrorKind::DeadlineExceeded => {
                        FailureClass::Control
                    }
                    ConnectorErrorKind::ResourceExhausted => FailureClass::Resource,
                    ConnectorErrorKind::Unavailable => FailureClass::Dependency,
                    _ => unreachable!(),
                };
                assert_eq!(fact.class(), expected_class);
            }
        }
    }

    #[test]
    fn nonprovider_row_mutation_diagnostics_keep_the_existing_engine_route() {
        let failure = DmlExecutionError::from(
            "InvalidRequest: UPDATE and MERGE require an Iceberg v3 table".to_owned(),
        )
        .into_dml_error(None);
        assert!(failure.application_error().is_none());
        assert!(failure.execution_failure().is_none());
        assert!(failure.to_string().starts_with("Executor:"));
    }
}
