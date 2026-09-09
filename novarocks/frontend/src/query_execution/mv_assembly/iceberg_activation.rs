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

//! Provider-neutral activation for the current frontend-owned MV refresh route.
//!
//! The adapter translates application publication facts into the generic
//! managed-publication intent. The exact connector generation owns physical
//! writer registration, provenance encoding and commit/reconcile machinery.

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorManagedPublicationEmptyInputDisposition,
    ConnectorManagedPublicationIntent, ConnectorManagedPublicationTarget,
    ConnectorManagedPublicationTechnique, ConnectorRequestContext,
    ConnectorStagedPublicationBaseFact, ConnectorTableIdentity, ConnectorTableResolution,
    ConnectorWriteInputRequest, ConnectorWriteLease,
};

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::mv::domain::application::MvIncrementalWriteMode;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::storage_observation::MvLakePublishedProjection;
use crate::query_execution::kernels::QueryPreparationKernel;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvIncrementalWriteRequest, MvRefreshCommittedFacts, MvRefreshPublicationIntent,
    MvRefreshPublicationTechnique, MvStagedRefreshWriteMode, PreparedMvFirstRefreshWrite,
};
use crate::query_execution::mv_assembly::refresh_handoff::{
    PreparedMvRefreshWrite, PreparedMvRefreshWriteArtifact,
};
use crate::query_execution::mv_native_write::{
    MvRefreshProviderActivation, PreparedMvNativeWriteAssembly,
};

/// Core-side provider adapter installed into the frontend composition.
///
/// It owns only the query-preparation kernel and MV leaf ports required to
/// bind an already admitted write. It cannot recover a state aggregate or
/// create a hidden all-in-one activation path.
pub struct IcebergMvRefreshProviderActivation {
    query_kernel: QueryPreparationKernel,
    ports: IcebergMvCorePorts,
}

impl IcebergMvRefreshProviderActivation {
    pub fn new(query_kernel: QueryPreparationKernel, ports: IcebergMvCorePorts) -> Self {
        Self {
            query_kernel,
            ports,
        }
    }
}

impl MvRefreshProviderActivation for IcebergMvRefreshProviderActivation {
    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedMvNativeWriteAssembly, String> {
        match prepared.into_assembly_artifact() {
            PreparedMvRefreshWriteArtifact::FirstRefresh(prepared) => {
                super::first_refresh_staging::bind_prepared_mv_first_refresh_staging(
                    &self.query_kernel,
                    &self.ports,
                    prepared,
                    planning_lease,
                    exact_lease,
                    execution,
                )
            }
            PreparedMvRefreshWriteArtifact::Incremental(prepared) => {
                super::incremental_staging::bind_prepared_mv_incremental_staging(
                    &self.query_kernel,
                    &self.ports,
                    prepared,
                    planning_lease,
                    exact_lease,
                    execution,
                )
            }
        }
    }

    fn interpret_write_commit(
        &self,
        intent: MvRefreshPublicationIntent,
        receipt: &novarocks_spi::connector::ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, String> {
        MvRefreshCommittedFacts::from_write_receipt(intent, receipt)
    }

    fn observe_published_package(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
        expected_snapshot_id: i64,
        connector_context: &ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::MvLakePackageObservation, String> {
        if planning_lease.binding().descriptor().instance_id != table.instance_id {
            return Err(
                "MV publication observation table belongs to a different connector generation"
                    .to_string(),
            );
        }
        let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
            planning_lease,
            connector_context.clone(),
            table.namespace.as_ref(),
            table.table.as_ref(),
            ConnectorTableResolution::StrictBaseTable,
        )
        .map_err(|error| format!("reload MV publication target metadata: {error}"))?;
        if metadata.identity != *table {
            return Err(
                "MV publication observation loaded metadata for a different target table"
                    .to_string(),
            );
        }
        let package = self
            .ports
            .storage_observation()
            .observe_lake_package(planning_lease, &metadata, connector_context.clone())
            .map_err(|error| format!("observe MV publication lake package: {error}"))?
            .ok_or_else(|| "MV publication target has no lake package observation".to_string())?;
        let local = crate::mv::domain::storage_observation::lake_package_from_spi(package.clone())
            .map_err(|error| format!("validate MV publication lake package: {error}"))?;
        if local.table != *table {
            return Err(
                "MV publication observer returned a package for a different target table"
                    .to_string(),
            );
        }
        let projection = local
            .published_projection()
            .map_err(|error| format!("project MV publication lake package: {error}"))?;
        require_exact_published_projection(projection, expected_snapshot_id)?;
        Ok(package)
    }
}

fn require_exact_published_projection(
    projection: MvLakePublishedProjection,
    expected_snapshot_id: i64,
) -> Result<MvLakePublishedProjection, String> {
    match &projection {
        MvLakePublishedProjection::Published {
            last_refreshed_iceberg_snapshot_id,
            ..
        } if *last_refreshed_iceberg_snapshot_id == expected_snapshot_id => Ok(projection),
        MvLakePublishedProjection::Published {
            last_refreshed_iceberg_snapshot_id,
            ..
        } => Err(format!(
            "MV publication lake snapshot {last_refreshed_iceberg_snapshot_id} does not match committed snapshot {expected_snapshot_id}"
        )),
        MvLakePublishedProjection::NeverPublished => {
            Err("MV publication committed but its lake package is never-published".to_string())
        }
    }
}

/// Open the write session one MV first refresh publishes through.
///
/// A first refresh republishes the materialization wholesale, so it sends plain
/// data rows and the publication seals exactly one unrouted data branch --
/// `ConnectorManagedPublicationShape::Data`. The shape is declared here rather
/// than inferred because the provider cannot tell a publication's data rows from
/// ordinary DML by input alone, and the difference decides whether the commit is
/// a publication.
///
/// The publication id travels inside the managed intent and nowhere else. It
/// reaches the snapshot the commit writes -- that is what the publication fence
/// reads back -- but no writer recipe, commit fragment, or backend sees it.
pub(crate) fn begin_first_refresh_connector_write_session(
    prepared: &PreparedMvFirstRefreshWrite,
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
    planning_lease: &ConnectorControlPlanningLease,
    typed_connector_control: &std::sync::Arc<crate::connector::ConnectorControlHost>,
) -> Result<std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>, String> {
    if !exact_lease.matches_provider_binding_key(prepared.observed_binding()) {
        return Err("MV first-refresh write lease drifted from prepared binding".to_string());
    }
    if !exact_lease.matches_provider_instance(prepared.target_table().owner()) {
        return Err(
            "MV first-refresh staging table belongs to a different connector instance".to_string(),
        );
    }
    let target = crate::catalog_application::resolver::TargetBackend {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("static Iceberg provider ID"),
        catalog: prepared.target_catalog().to_string(),
        namespace: prepared.target_namespace().to_string(),
        table: prepared.target_name().to_string(),
    };
    let intent = match prepared.write_mode() {
        MvStagedRefreshWriteMode::Append => novarocks_spi::connector::ConnectorWriteIntent::Append,
        MvStagedRefreshWriteMode::FullOverwrite => {
            novarocks_spi::connector::ConnectorWriteIntent::Overwrite
        }
    };
    // What an empty result means is the publication's business, not the
    // terminal's: an append that produced nothing has nothing to publish, while
    // a full overwrite that produced nothing is a truncate and must still
    // commit. The provider applies this at finish, so the frontend commits
    // either way and reads the effect back.
    let empty_input = match prepared.write_mode() {
        MvStagedRefreshWriteMode::Append => {
            ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit
        }
        MvStagedRefreshWriteMode::FullOverwrite => {
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite
        }
    };
    // A partition replacement establishes the new default spec in the same
    // commit that publishes the rows, so it writes to main rather than to a
    // staging branch that would then be fast-forwarded.
    let target_ref = if prepared
        .publication_intent()
        .partition_spec_replacement()
        .is_some()
    {
        "main"
    } else {
        prepared.staging_branch()
    };
    let input = ConnectorWriteInputRequest::Data {
        fields: prepared
            .write_input_fields()
            .iter()
            .map(|field| novarocks_spi::connector::ConnectorWriteFieldRequest::new(field.clone()))
            .collect(),
    };
    let managed_publication =
        managed_publication_activation_intent(prepared.publication_intent(), empty_input)?;
    crate::query_execution::write_session::begin_connector_write_session(
        crate::connector::write_target::derive_write_stack_lease(
            typed_connector_control,
            planning_lease,
        )?,
        exact_lease,
        crate::query_execution::dml::iceberg_writer::connector_write_begin_request(
            &target,
            target_ref,
            intent,
            input,
            novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::ManagedPublication {
                intent: managed_publication,
                shape: novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape::Data,
            },
            connector_context,
        )?,
    )
}

/// The write intent, signed input, and branch shape one incremental refresh mode
/// declares.
///
/// The shape is the whole decision this function exists to make, and it cannot
/// be inferred from the input: a row-lineage input with a `_file`/`_pos`
/// identity is exactly what ordinary DML row mutation sends, so the provider
/// reading the input alone could not tell a publication from DML -- and that
/// difference decides whether the single commit is a publication.
fn incremental_publication_write_input(
    mode: MvIncrementalWriteMode,
    target_write_fields: &[arrow::datatypes::Field],
) -> Result<
    (
        novarocks_spi::connector::ConnectorWriteIntent,
        ConnectorWriteInputRequest,
        novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape,
    ),
    String,
> {
    use novarocks_execution::exec::row_position::{
        ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
        ICEBERG_ROW_POS_COL,
    };
    use novarocks_spi::connector::ConnectorWriteFieldRequest;
    use novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape;

    if target_write_fields.is_empty() {
        return Err("MV incremental write has no target write fields".to_string());
    }
    let field = |name: &str, data_type: arrow::datatypes::DataType, nullable: bool| {
        ConnectorWriteFieldRequest::new(arrow::datatypes::Field::new(name, data_type, nullable))
    };
    let mut data_fields = target_write_fields
        .iter()
        .map(|target_field| ConnectorWriteFieldRequest::new(target_field.clone()))
        .collect::<Vec<_>>();
    Ok(match mode {
        MvIncrementalWriteMode::FastAppend => (
            novarocks_spi::connector::ConnectorWriteIntent::Append,
            ConnectorWriteInputRequest::Data {
                fields: data_fields,
            },
            ConnectorManagedPublicationShape::InsertOnlyChangeStream,
        ),
        MvIncrementalWriteMode::RowDelta => {
            // The v3 lineage columns travel with the after-image so a replaced
            // row keeps the identity it already had instead of being re-minted
            // as a fresh row. They are nullable because an inserted row has no
            // prior identity to carry.
            data_fields.push(field(
                ICEBERG_ROW_ID_COL,
                arrow::datatypes::DataType::Int64,
                true,
            ));
            data_fields.push(field(
                ICEBERG_LAST_UPDATED_SEQ_COL,
                arrow::datatypes::DataType::Int64,
                true,
            ));
            (
                novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
                ConnectorWriteInputRequest::RowLineage {
                    data_fields,
                    row_identity_fields: vec![
                        field(
                            ICEBERG_FILE_PATH_COL,
                            arrow::datatypes::DataType::Utf8,
                            false,
                        ),
                        field(
                            ICEBERG_ROW_POS_COL,
                            arrow::datatypes::DataType::Int64,
                            false,
                        ),
                    ],
                },
                ConnectorManagedPublicationShape::RowMutation,
            )
        }
    })
}

/// Open the write session one MV incremental refresh publishes through.
///
/// An incremental refresh applies a change stream, so its rows arrive as change
/// events and SQL routes them to the branches the publication seals. Which
/// branches those are is declared here and cannot be inferred: an incremental
/// merge-on-read refresh arrives as a `RowLineage` input with a `_file`/`_pos`
/// identity, which is indistinguishable from an ordinary DML row mutation by
/// input shape alone, and the difference decides whether the commit is a
/// publication or DML.
///
/// * A fast-append refresh only ever inserts, so it supersedes nothing. It sends
///   plain data rows and declares `InsertOnlyChangeStream`, which seals one
///   *routed* data branch accepting only `Insert` and freezes no delete
///   artifact. Note what the declaration does not do: an effect no branch
///   accepts is dropped by the router rather than refused, so "no delete
///   appears" stays this mode's own precondition -- established upstream by the
///   write-mode policy -- and is not made an enforced invariant by the routing.
/// * A row-delta refresh replaces and retires rows, so it declares `RowMutation`
///   and sends row-lineage change events. The session then seals the branches a
///   row mutation needs and freezes the old delete artifacts they supersede.
///
/// The publication id travels inside the managed intent and nowhere else. It
/// reaches the snapshot the commit writes -- that is what the publication fence
/// reads back -- but no writer recipe, commit fragment, or backend sees it.
#[expect(
    clippy::too_many_arguments,
    reason = "Opening an incremental publication session needs each independently frozen request, publication, schema, and lease fact."
)]
pub(crate) fn begin_incremental_connector_write_session(
    request: &MvIncrementalWriteRequest,
    publication_intent: &MvRefreshPublicationIntent,
    mode: MvIncrementalWriteMode,
    target_write_fields: &[arrow::datatypes::Field],
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
    planning_lease: &ConnectorControlPlanningLease,
    typed_connector_control: &std::sync::Arc<crate::connector::ConnectorControlHost>,
) -> Result<std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>, String> {
    let target = crate::catalog_application::resolver::TargetBackend {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("static Iceberg provider ID"),
        catalog: request.target_catalog.clone(),
        namespace: request.target_namespace.clone(),
        table: request.target_name.clone(),
    };
    let (intent, input, shape) = incremental_publication_write_input(mode, target_write_fields)?;
    // An incremental window that materialized nothing has nothing to publish,
    // and its staging branch still points at the old, unmarked target snapshot.
    // The provider applies this at finish, so the frontend commits either way
    // and reads the effect back.
    let managed_publication = managed_publication_activation_intent(
        publication_intent,
        ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit,
    )?;
    crate::query_execution::write_session::begin_connector_write_session(
        crate::connector::write_target::derive_write_stack_lease(
            typed_connector_control,
            planning_lease,
        )?,
        exact_lease,
        crate::query_execution::dml::iceberg_writer::connector_write_begin_request(
            &target,
            &request.staging_branch,
            intent,
            input,
            novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::ManagedPublication {
                intent: managed_publication,
                shape,
            },
            connector_context,
        )?,
    )
}

/// Release a session that will never reach its commit.
///
/// Nothing external has happened -- a begin performs reads only, and a session
/// commits only through a completion this attempt never produced -- but the
/// provider is holding a session for a write that will not run, and this
/// refresh's one terminal decision is the only thing that releases it. So a
/// failed refresh leaves nothing behind. The original failure is what the
/// caller reports, so a failure to release is logged rather than substituted
/// for it.
pub(crate) fn release_mv_write_session_without_commit(
    write_session: &crate::query_execution::write_session::ConnectorWriteSession,
    connector_context: &ConnectorRequestContext,
) {
    if let Err(error) = write_session.abort(connector_context.clone()) {
        tracing::warn!(
            %error,
            "releasing an uncommitted MV first-refresh write session failed",
        );
    }
}

pub(crate) fn managed_publication_activation_intent(
    publication: &MvRefreshPublicationIntent,
    empty_input: ConnectorManagedPublicationEmptyInputDisposition,
) -> Result<ConnectorManagedPublicationIntent, String> {
    let arguments = (
        publication.publication_id(),
        ConnectorManagedPublicationTarget::try_new(
            publication.target_object_id().clone(),
            publication.expected_target_snapshot_id(),
        )
        .map_err(|error| format!("build managed MV publication target: {error}"))?,
        match publication.technique() {
            MvRefreshPublicationTechnique::Full => ConnectorManagedPublicationTechnique::Full,
            MvRefreshPublicationTechnique::Incremental => {
                ConnectorManagedPublicationTechnique::Incremental
            }
            MvRefreshPublicationTechnique::MetadataOnly => {
                return Err(
                    "metadata-only MV refresh must use the catalog staging operation".to_string(),
                );
            }
        },
        publication
            .bases()
            .iter()
            .map(|base| ConnectorStagedPublicationBaseFact {
                table: base.table_fqn().into(),
                object_id: base.table_object_id().clone(),
                from_version: base.from_snapshot(),
                to_version: base.to_snapshot(),
            })
            .collect(),
        publication.definition_fingerprint(),
        empty_input,
        publication.descriptor_properties().clone(),
    );
    match publication.partition_spec_replacement() {
        Some(replacement) => {
            ConnectorManagedPublicationIntent::try_new_with_partition_spec_replacement(
                arguments.0,
                arguments.1,
                arguments.2,
                arguments.3,
                arguments.4,
                arguments.5,
                replacement.clone(),
                publication
                    .expected_committed_partitioning()
                    .cloned()
                    .ok_or_else(|| {
                        "managed MV partition replacement is missing its exact preview partitioning"
                            .to_string()
                    })?,
                arguments.6,
            )
        }
        None => ConnectorManagedPublicationIntent::try_new(
            arguments.0,
            arguments.1,
            arguments.2,
            arguments.3,
            arguments.4,
            arguments.5,
            arguments.6,
        ),
    }
    .map_err(|error| format!("build managed MV publication activation intent: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::{DataType, Field};
    use novarocks_spi::connector::ConnectorWriteIntent;
    use novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape;

    use super::{
        ConnectorWriteInputRequest, MvIncrementalWriteMode, MvLakePublishedProjection,
        incremental_publication_write_input, require_exact_published_projection,
    };

    fn target_write_fields() -> Vec<Field> {
        vec![
            Field::new("k1", DataType::Int64, false),
            Field::new("v1", DataType::Utf8, true),
        ]
    }

    fn field_names(fields: &[novarocks_spi::connector::ConnectorWriteFieldRequest]) -> Vec<String> {
        fields
            .iter()
            .map(|field| field.field().name().to_string())
            .collect()
    }

    /// A fast-append refresh only ever inserts, so it supersedes nothing. It
    /// must declare the shape that seals ONE routed data branch accepting only
    /// `Insert`: the wholesale-republication shape seals an unrouted branch, and
    /// SQL's change-stream compile requires every branch to declare which
    /// effects it accepts, so its rows would have nowhere to be routed.
    #[test]
    fn a_fast_append_refresh_declares_the_insert_only_change_stream_shape() {
        let (intent, input, shape) = incremental_publication_write_input(
            MvIncrementalWriteMode::FastAppend,
            &target_write_fields(),
        )
        .expect("fast-append declares its publication shape");

        assert_eq!(
            shape,
            ConnectorManagedPublicationShape::InsertOnlyChangeStream
        );
        assert_eq!(intent, ConnectorWriteIntent::Append);
        // Plain data rows: nothing is superseded, so no row identity is signed
        // and the session freezes no old delete artifact.
        let ConnectorWriteInputRequest::Data { fields } = input else {
            panic!("an insert-only publication publishes data files");
        };
        assert_eq!(field_names(&fields), vec!["k1", "v1"]);
    }

    /// A row-delta refresh retires the row versions it replaces, so it must
    /// declare the shape that seals the branches a row mutation needs -- the
    /// delete branch included. That is also what makes the session freeze the
    /// old delete artifacts those branches supersede; the insert-only shape
    /// freezes none.
    #[test]
    fn a_row_delta_refresh_declares_the_row_mutation_shape_with_a_file_pos_identity() {
        let (intent, input, shape) = incremental_publication_write_input(
            MvIncrementalWriteMode::RowDelta,
            &target_write_fields(),
        )
        .expect("row-delta declares its publication shape");

        assert_eq!(shape, ConnectorManagedPublicationShape::RowMutation);
        assert_eq!(intent, ConnectorWriteIntent::RowDelta);
        let ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } = input
        else {
            panic!("a change-stream publication sends row-lineage change events");
        };
        // `_file`/`_pos` is what makes this a merge-on-read mutation. A
        // `_row_id`/`_last_updated_sequence_number` identity would be
        // copy-on-write, which a publication carries no match selection for and
        // the provider refuses outright.
        assert_eq!(field_names(&row_identity_fields), vec!["_file", "_pos"]);
        assert!(
            row_identity_fields
                .iter()
                .all(|field| !field.field().is_nullable()),
            "a row identity that could be null would name no row"
        );
        // The v3 lineage columns ride with the after-image so a replaced row
        // keeps the identity it already had. They are nullable because an
        // inserted row has no prior identity to carry.
        assert_eq!(
            field_names(&data_fields),
            vec!["k1", "v1", "_row_id", "_last_updated_sequence_number"]
        );
        assert!(
            data_fields[2..]
                .iter()
                .all(|field| field.field().is_nullable())
        );
    }

    /// A signed input with no fields would seal a branch no row could satisfy.
    #[test]
    fn an_incremental_refresh_without_target_write_fields_is_refused() {
        for mode in [
            MvIncrementalWriteMode::FastAppend,
            MvIncrementalWriteMode::RowDelta,
        ] {
            assert!(
                incremental_publication_write_input(mode, &[]).is_err(),
                "{mode:?} must not sign an empty write input"
            );
        }
    }

    fn published(snapshot_id: i64) -> MvLakePublishedProjection {
        MvLakePublishedProjection::Published {
            last_refresh_ms: 1_700_000_010_000,
            last_refresh_rows: 7,
            last_refreshed_iceberg_snapshot_id: snapshot_id,
            base_snapshots: BTreeMap::new(),
            base_table_object_ids: BTreeMap::new(),
        }
    }

    #[test]
    fn exact_published_projection_retains_the_lake_timestamp() {
        assert_eq!(
            require_exact_published_projection(published(99), 99)
                .expect("exact snapshot is accepted"),
            published(99)
        );
    }

    #[test]
    fn advanced_published_projection_fails_closed() {
        let error = require_exact_published_projection(published(100), 99)
            .expect_err("advanced lake head must not finalize an older publication");

        assert!(error.contains("does not match committed snapshot 99"));
    }
}
