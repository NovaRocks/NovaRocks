// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information regarding
// copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Frontend execution of a lake-authoritative MV publication.

use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
use novarocks_mv_application::publication::MvRefreshPublicationIntent;

/// Bind the exact publication document this write promised when it opened.
///
/// The session opened with a declaration and no payload because P states the
/// post-write logical cardinality, which requires the completed write effect
/// and the admitted prior publication.
/// Building P here and binding it before finish is what makes the document and
/// the rows it describes one commit rather than two.
pub(crate) fn bind_publication_documents(
    intent: &MvRefreshPublicationIntent,
    admitted: &crate::mv::domain::staged_create::AdmittedMvDataPublication,
    session: &crate::query_execution::write_session::ConnectorWriteSession,
    logical_result_rows: Option<u64>,
) -> Result<(), MvApplicationError> {
    let declaration = session.pending_publication_declaration().map_err(|error| {
        MvApplicationError::from_connector(MvApplicationErrorKind::InvalidRequest, error)
    })?;
    let documents = admitted
        .publication_document_set(intent.publication_id(), logical_result_rows)
        .map_err(invalid)?;
    let publication = novarocks_spi::connector::KeyedPublicationIntent {
        declaration,
        documents,
    };
    session
        .bind_keyed_document_publication(publication)
        .map_err(|error| {
            MvApplicationError::from_connector(MvApplicationErrorKind::InvalidRequest, error)
        })
}

fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}
