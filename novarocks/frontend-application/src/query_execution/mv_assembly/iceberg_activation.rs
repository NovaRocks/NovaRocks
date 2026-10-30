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
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind,
    ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
    ConnectorRequestContext, ConnectorWriteInputRequest, ConnectorWriteLease,
};

use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::query_execution::kernels::QueryPreparationKernel;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvIncrementalWriteRequest, PreparedMvFirstRefreshWrite, PreparedMvMetadataOnlyWrite,
};
use crate::query_execution::mv_native_write::MvRefreshProviderActivation;
use novarocks_mv_application::product::MvIncrementalWriteMode;
use novarocks_mv_application::publication::{
    MvRefreshCommittedFacts, MvRefreshPublicationIntent, MvRefreshPublicationTechnique,
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
    fn prepare_write_source(
        &self,
        request: super::activation_request::OriginalMvWritePreparationRequest,
    ) -> super::activation_source::MvWritePreparationCustody {
        let super::activation_request::OriginalMvWritePreparationRequest {
            prepared,
            target,
            planning,
            execution,
            context,
            session,
            io,
            turn,
            owners,
            capacity,
            requests,
        } = request;
        super::activation_source::MvWritePreparationCustody::new(
            super::activation_preparation::MvWriteAnalysisPreparationInput {
                kernel: self.query_kernel.clone(),
                ports: self.ports.clone(),
                prepared,
                target,
                planning,
                execution,
                context,
                session,
            },
            io,
            turn,
            owners,
            capacity,
            requests,
        )
    }

    fn interpret_write_commit(
        &self,
        intent: MvRefreshPublicationIntent,
        receipt: &novarocks_spi::connector::ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, String> {
        MvRefreshCommittedFacts::from_write_receipt(intent, receipt)
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
pub(crate) async fn begin_first_refresh_connector_write_session_async_in(
    prepared: &PreparedMvFirstRefreshWrite,
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
    planning_lease: &ConnectorControlPlanningLease,
    typed_connector_control: &std::sync::Arc<novarocks_catalog_application::ConnectorControlHost>,
    receiving: &crate::query_execution::write_session::WriteBeginReceiving,
) -> Result<
    std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ConnectorError,
> {
    let request =
        prepare_first_refresh_document_write_request(prepared, connector_context, exact_lease)?;
    begin_pending_document_write_async_in(
        request,
        exact_lease,
        planning_lease,
        typed_connector_control,
        receiving,
    )
    .await
}

fn prepare_first_refresh_document_write_request(
    prepared: &PreparedMvFirstRefreshWrite,
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest, ConnectorError> {
    if !exact_lease.matches_provider_binding_key(prepared.observed_binding()) {
        return Err(invalid_activation(
            "MV first-refresh write lease drifted from prepared binding".to_string(),
        ));
    }
    if !exact_lease.matches_provider_instance(prepared.target_table().owner()) {
        return Err(invalid_activation(
            "MV first-refresh staging table belongs to a different connector instance".to_string(),
        ));
    }
    let target = crate::catalog_application::resolver::TargetBackend {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("static Iceberg provider ID"),
        catalog: prepared.target_catalog().to_string(),
        namespace: prepared.target_namespace().to_string(),
        table: prepared.target_name().to_string(),
    };
    // A document publication commits the target itself, so its write intent is
    // the one its technique implies rather than the staging mode's.
    let intent = match prepared.publication_intent().technique() {
        MvRefreshPublicationTechnique::Full => {
            novarocks_spi::connector::ConnectorWriteIntent::Overwrite
        }
        MvRefreshPublicationTechnique::Incremental => {
            novarocks_spi::connector::ConnectorWriteIntent::Append
        }
        MvRefreshPublicationTechnique::MetadataOnly => {
            return Err(invalid_activation(
                "metadata-only MV refresh publishes through its own session, not a data write"
                    .to_string(),
            ));
        }
    };
    // Every publication commits, including one that produced no rows. A full
    // refresh that produced nothing is a truncate -- a view over an empty
    // source exists, is readable, and has a publication -- and an incremental
    // window that materialized nothing still consumed the window: the output
    // version this commit mints is what carries the advanced watermark, and
    // the next refresh reads that watermark back out of P. Settling such a
    // refresh without a commit left the watermark where it was, so the next
    // refresh re-read a window it had already consumed.
    let empty_input = ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite;
    // A document publication is one commit against the target itself: there is
    // no staging branch to fast-forward from, and the provider refuses a
    // publication opened anywhere but main.
    let target_ref = "main";
    let input = ConnectorWriteInputRequest::Data {
        fields: prepared.write_input_fields().to_vec(),
    };
    let base = publication_write_base(
        exact_lease,
        prepared.target_table(),
        target_ref,
        intent,
        input.clone(),
        connector_context.clone(),
    )?;
    let declaration =
        document_publication_declaration(prepared.publication_intent(), base.clone(), empty_input)
            .map_err(invalid_activation)?;
    // The publication document cannot exist yet: it describes inputs and an
    // output this write has not produced. The declaration is the authority the
    // one later bind is checked against.
    crate::query_execution::dml::iceberg_writer::connector_write_begin_request_on_base(
            &target,
            target_ref,
            intent,
            input,
            novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            Some(base),
            novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
                declaration,
                shape: novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape::Data,
            },
            connector_context,
        )
        .map_err(invalid_activation)
}

/// Open the write session one metadata-only MV publication commits through.
///
/// It is the same session a first refresh opens, with the same shape and the
/// same single commit against `main`. Two things differ, and both follow from
/// the inputs not having moved: the intent is `Append`, because there are no
/// rows to replace, and the empty-input disposition is `CommitEmptyWrite`,
/// because the empty write *is* the publication -- an output version whose one
/// new fact is the watermark P records.
pub(crate) async fn begin_metadata_only_connector_write_session_async_in(
    prepared: &PreparedMvMetadataOnlyWrite,
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
    planning_lease: &ConnectorControlPlanningLease,
    typed_connector_control: &std::sync::Arc<novarocks_catalog_application::ConnectorControlHost>,
    receiving: &crate::query_execution::write_session::WriteBeginReceiving,
) -> Result<
    std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ConnectorError,
> {
    let request =
        prepare_metadata_only_document_write_request(prepared, connector_context, exact_lease)?;
    begin_pending_document_write_async_in(
        request,
        exact_lease,
        planning_lease,
        typed_connector_control,
        receiving,
    )
    .await
}

fn prepare_metadata_only_document_write_request(
    prepared: &PreparedMvMetadataOnlyWrite,
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest, ConnectorError> {
    let publication = prepared.publication_intent();
    if !matches!(
        publication.technique(),
        MvRefreshPublicationTechnique::MetadataOnly
    ) {
        return Err(invalid_activation(
            "MV metadata-only session opened for another technique".to_string(),
        ));
    }
    if !exact_lease.matches_provider_binding_key(prepared.observed_binding()) {
        return Err(invalid_activation(
            "MV metadata-only write lease drifted from prepared binding".to_string(),
        ));
    }
    if !exact_lease.matches_provider_instance(prepared.target_table().owner()) {
        return Err(invalid_activation(
            "MV metadata-only target belongs to a different connector instance".to_string(),
        ));
    }
    let target = crate::catalog_application::resolver::TargetBackend {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("static Iceberg provider ID"),
        catalog: publication.target_catalog().to_string(),
        namespace: publication.target_namespace().to_string(),
        table: publication.target_name().to_string(),
    };
    let target_ref = "main";
    let intent = novarocks_spi::connector::ConnectorWriteIntent::Append;
    let input = ConnectorWriteInputRequest::Data {
        fields: prepared.write_input_fields().to_vec(),
    };
    let base = publication_write_base(
        exact_lease,
        prepared.target_table(),
        target_ref,
        intent,
        input.clone(),
        connector_context.clone(),
    )?;
    let declaration = document_publication_declaration(
        publication,
        base.clone(),
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
    )
    .map_err(invalid_activation)?;
    crate::query_execution::dml::iceberg_writer::connector_write_begin_request_on_base(
            &target,
            target_ref,
            intent,
            input,
            novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            Some(base),
            novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
                declaration,
                shape: novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape::Data,
            },
            connector_context,
        )
        .map_err(invalid_activation)
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
    target_write_fields: &[novarocks_spi::connector::ConnectorWriteFieldRequest],
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
        let declared = novarocks_type_contract::FunctionValueType::new(data_type.clone(), nullable);
        ConnectorWriteFieldRequest::try_new_typed(
            arrow::datatypes::Field::new(name, data_type, nullable),
            declared,
        )
        .map_err(|error| error.to_string())
    };
    let mut data_fields = target_write_fields.to_vec();
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
            )?);
            data_fields.push(field(
                ICEBERG_LAST_UPDATED_SEQ_COL,
                arrow::datatypes::DataType::Int64,
                true,
            )?);
            (
                novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
                ConnectorWriteInputRequest::RowLineage {
                    data_fields,
                    row_identity_fields: vec![
                        field(
                            ICEBERG_FILE_PATH_COL,
                            arrow::datatypes::DataType::Utf8,
                            false,
                        )?,
                        field(
                            ICEBERG_ROW_POS_COL,
                            arrow::datatypes::DataType::Int64,
                            false,
                        )?,
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
pub(crate) async fn begin_incremental_connector_write_session_async_in(
    request: &MvIncrementalWriteRequest,
    target_table: &novarocks_spi::connector::ConnectorTableHandle,
    publication_intent: &MvRefreshPublicationIntent,
    mode: MvIncrementalWriteMode,
    target_write_fields: &[novarocks_spi::connector::ConnectorWriteFieldRequest],
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
    planning_lease: &ConnectorControlPlanningLease,
    typed_connector_control: &std::sync::Arc<novarocks_catalog_application::ConnectorControlHost>,
    receiving: &crate::query_execution::write_session::WriteBeginReceiving,
) -> Result<
    std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ConnectorError,
> {
    let request = prepare_incremental_document_write_request(
        request,
        target_table,
        publication_intent,
        mode,
        target_write_fields,
        connector_context,
        exact_lease,
    )?;
    begin_pending_document_write_async_in(
        request,
        exact_lease,
        planning_lease,
        typed_connector_control,
        receiving,
    )
    .await
}

fn prepare_incremental_document_write_request(
    request: &MvIncrementalWriteRequest,
    target_table: &novarocks_spi::connector::ConnectorTableHandle,
    publication_intent: &MvRefreshPublicationIntent,
    mode: MvIncrementalWriteMode,
    target_write_fields: &[novarocks_spi::connector::ConnectorWriteFieldRequest],
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest, ConnectorError> {
    let target = crate::catalog_application::resolver::TargetBackend {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("static Iceberg provider ID"),
        catalog: request.target_catalog.clone(),
        namespace: request.target_namespace.clone(),
        table: request.target_name.clone(),
    };
    let (intent, input, shape) = incremental_publication_write_input(mode, target_write_fields)
        .map_err(invalid_activation)?;
    let base = publication_write_base(
        exact_lease,
        target_table,
        "main",
        intent,
        input.clone(),
        connector_context.clone(),
    )?;
    // An incremental window that materialized nothing still commits: the
    // output version is what advances the watermark P records.
    let declaration = document_publication_declaration(
        publication_intent,
        base.clone(),
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
    )
    .map_err(invalid_activation)?;
    crate::query_execution::dml::iceberg_writer::connector_write_begin_request_on_base(
            &target,
            "main",
            intent,
            input,
            novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            Some(base),
            novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
                declaration,
                shape,
            },
            connector_context,
        )
        .map_err(invalid_activation)
}

/// Freeze the declaration one MV publication commits under.
///
/// The declaration names the admission, the exact target object and the exact
/// base the provider issued for this session. Everything the publication
/// *describes* — its inputs, its output and its computation — lives in the
/// publication document bound before the commit, not here.
pub(crate) fn document_publication_declaration(
    publication: &MvRefreshPublicationIntent,
    expected_base: novarocks_spi::connector::ConnectorWriteBaseVersion,
    empty_input: ConnectorManagedPublicationEmptyInputDisposition,
) -> Result<
    novarocks_spi::connector::document_storage::ConnectorDocumentPublicationDeclaration,
    String,
> {
    let technique = match publication.technique() {
        MvRefreshPublicationTechnique::Full => ConnectorManagedPublicationTechnique::Full,
        MvRefreshPublicationTechnique::Incremental => {
            ConnectorManagedPublicationTechnique::Incremental
        }
        MvRefreshPublicationTechnique::MetadataOnly => {
            ConnectorManagedPublicationTechnique::MetadataOnly
        }
    };
    novarocks_spi::connector::document_storage::ConnectorDocumentPublicationDeclaration::try_new(
        publication.publication_id(),
        publication.admission().clone(),
        publication.target_object_id().clone(),
        expected_base,
        technique,
        empty_input,
        publication.partition_spec_replacement().cloned(),
        publication.expected_committed_partitioning().cloned(),
    )
    .map_err(|error| format!("build the MV document publication declaration: {error}"))
}

/// Ask the exact write generation for the base this publication will commit
/// onto. The declaration and the session must name the same one.
pub(crate) fn publication_write_base(
    exact_lease: &ConnectorWriteLease,
    table: &novarocks_spi::connector::ConnectorTableHandle,
    target_ref: &str,
    intent: novarocks_spi::connector::ConnectorWriteIntent,
    input: ConnectorWriteInputRequest,
    connector_context: ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorWriteBaseVersion, ConnectorError> {
    let outcome =
        exact_lease.prepare_write(novarocks_spi::connector::ConnectorWritePreparationRequest {
            table: table.clone(),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::parse(target_ref)?,
            intent,
            purpose:
                novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            input,
            context: connector_context,
        })?;
    match outcome {
        novarocks_spi::connector::ConnectorWritePreparationOutcome::Prepared(preparation) => {
            Ok(preparation.base_version().clone())
        }
        novarocks_spi::connector::ConnectorWritePreparationOutcome::Denied(error) => Err(error),
    }
}

fn invalid_activation(message: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

async fn begin_pending_document_write_async_in(
    request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
    exact_lease: &ConnectorWriteLease,
    planning_lease: &ConnectorControlPlanningLease,
    typed_connector_control: &novarocks_catalog_application::ConnectorControlHost,
    receiving: &crate::query_execution::write_session::WriteBeginReceiving,
) -> Result<
    std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ConnectorError,
> {
    let lease =
        typed_connector_control.acquire_exact_write_stack(planning_lease.control_runtime_id())?;
    crate::query_execution::write_session::begin_connector_application_document_write_session_pending_async_in(
        lease, exact_lease, request, receiving,
    ).await
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::{DataType, Field};
    use novarocks_spi::connector::ConnectorWriteIntent;
    use novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape;

    use super::{
        ConnectorWriteInputRequest, MvIncrementalWriteMode, incremental_publication_write_input,
    };

    fn target_write_fields() -> Vec<novarocks_spi::connector::ConnectorWriteFieldRequest> {
        [("k1", DataType::Int64, false), ("v1", DataType::Utf8, true)]
            .into_iter()
            .map(|(name, data_type, nullable)| {
                novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                    Field::new(name, data_type.clone(), nullable),
                    novarocks_type_contract::FunctionValueType::new(data_type, nullable),
                )
                .expect("explicit target declaration")
            })
            .collect()
    }

    fn field_names(fields: &[novarocks_spi::connector::ConnectorWriteFieldRequest]) -> Vec<String> {
        fields
            .iter()
            .map(|field| {
                let declared = field.execution_type().expect("complete write declaration");
                assert_eq!(declared.data_type(), field.field().data_type());
                assert_eq!(declared.nullable(), field.field().is_nullable());
                field.field().name().to_string()
            })
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

    #[test]
    fn publication_write_base_calls_the_exact_lease_once_and_preserves_original_refusal() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_spi::connector::{
            ConnectorError, ConnectorErrorKind, ConnectorInstanceId, ConnectorProviderBindingKey,
            ConnectorRequestContext, ConnectorStopOwner, ConnectorTableHandle,
            ConnectorWriteAdmissionPurpose, ConnectorWriteControl, ConnectorWriteLease,
            ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
            ProviderBindingEpoch,
        };
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        };

        struct AdmissionControl {
            key: ConnectorProviderBindingKey,
            denied: bool,
            calls: AtomicUsize,
            request: Mutex<Option<ConnectorWritePreparationRequest>>,
            failure: Mutex<Option<ConnectorError>>,
        }
        impl ConnectorWriteControl for AdmissionControl {
            fn binding_key(&self) -> &ConnectorProviderBindingKey {
                &self.key
            }
            fn prepare_write(
                &self,
                request: ConnectorWritePreparationRequest,
            ) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
                assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
                *self.request.lock().unwrap() = Some(request);
                let original = self
                    .failure
                    .lock()
                    .unwrap()
                    .take()
                    .expect("one original refusal");
                if self.denied {
                    Ok(ConnectorWritePreparationOutcome::Denied(original))
                } else {
                    Err(original)
                }
            }
        }

        for denied in [false, true] {
            let error = ConnectorError::from_domain(
                ConnectorErrorKind::ResourceExhausted,
                novarocks_workload_control::WorkError::Capacity(
                    "actual MV write preparation refusal",
                ),
            )
            .with_cleanup_context("same original preparation cleanup");
            let original = std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<SharedFailureCause>())
                .expect("actual Connector domain cause")
                .clone();
            let fact = original.failure_fact();
            let message = error.to_string();
            let key = ConnectorProviderBindingKey {
                instance_id: ConnectorInstanceId::parse("mv_activation_test").unwrap(),
                incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
            };
            let control = Arc::new(AdmissionControl {
                key: key.clone(),
                denied,
                calls: AtomicUsize::new(0),
                request: Mutex::new(None),
                failure: Mutex::new(Some(error)),
            });
            let lease = ConnectorWriteLease::new(key.clone(), control.clone(), || {}).unwrap();
            let table = ConnectorTableHandle::try_new(
                key.instance_id.clone(),
                bytes::Bytes::from_static(b"admitted-mv-table"),
            )
            .unwrap();
            let fields = target_write_fields();
            let input = ConnectorWriteInputRequest::Data {
                fields: fields.clone(),
            };
            let stop = ConnectorStopOwner::new();
            let context = ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(60),
                stop.view(),
                1024,
                8192,
            )
            .unwrap();
            let marker = context.request_scope_extension_or_insert_with(|| AtomicUsize::new(0));
            let returned = super::publication_write_base(
                &lease,
                &table,
                "main",
                ConnectorWriteIntent::Append,
                input,
                context.clone(),
            )
            .unwrap_err();
            assert_eq!(control.calls.load(Ordering::SeqCst), 1);
            assert!(control.failure.lock().unwrap().is_none());
            let actual = control
                .request
                .lock()
                .unwrap()
                .take()
                .expect("actual Provider request");
            assert_eq!(actual.table.owner(), table.owner());
            assert_eq!(actual.table.payload(), table.payload());
            assert_eq!(actual.target_ref.as_str(), "main");
            assert_eq!(actual.intent, ConnectorWriteIntent::Append);
            assert_eq!(
                actual.purpose,
                ConnectorWriteAdmissionPurpose::MaterializedViewRefresh
            );
            let ConnectorWriteInputRequest::Data {
                fields: actual_fields,
            } = actual.input
            else {
                panic!("preserve the prepared data input");
            };
            assert_eq!(actual_fields.len(), fields.len());
            for (actual_field, prepared_field) in actual_fields.iter().zip(&fields) {
                assert_eq!(actual_field.field(), prepared_field.field());
                assert_eq!(
                    actual_field.execution_type(),
                    prepared_field.execution_type()
                );
            }
            assert_eq!(actual.context.deadline(), context.deadline());
            assert_eq!(
                actual.context.max_handle_payload_bytes(),
                context.max_handle_payload_bytes()
            );
            assert_eq!(
                actual.context.max_total_payload_bytes(),
                context.max_total_payload_bytes()
            );
            let actual_marker = actual
                .context
                .request_scope_extension_or_insert_with::<AtomicUsize>(|| {
                    panic!("must retain the original request scope")
                });
            assert!(Arc::ptr_eq(&marker, &actual_marker));
            assert!(!actual.context.is_cancelled());
            stop.request_stop();
            assert!(actual.context.is_cancelled());
            assert!(context.is_cancelled());
            assert_eq!(returned.kind(), ConnectorErrorKind::ResourceExhausted);
            assert_eq!(returned.to_string(), message);
            let source = std::error::Error::source(&returned)
                .and_then(|source| source.downcast_ref::<SharedFailureCause>())
                .expect("same original source");
            assert_eq!(source, &original);
            assert_eq!(source.failure_fact(), fact);
            assert!(
                source
                    .downcast_ref::<novarocks_workload_control::WorkError>()
                    .is_some()
            );
        }
    }
}
