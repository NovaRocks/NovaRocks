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

use novarocks_mv_application::persistence::documents::MvDocumentError;
use novarocks_mv_application::persistence::keyed_documents::decode_current_keyed_documents;
use novarocks_parser::{Span, build, printer::print_create_materialized_view_definition};
use novarocks_query_application::{
    api::{CatalogTableReadBinding, QueryResult, build_string_query_result},
    session_error::QueryServiceError,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRequestContext, KeyedDocumentReadError,
};
use novarocks_sql::analyze_error::AnalyzeError;

use super::{
    iceberg_guard::is_materialized_view_documents,
    readiness::{application_error, connector_application_error, document_application_error},
};

pub(crate) async fn execute(
    binding: CatalogTableReadBinding,
    context: ConnectorRequestContext,
    source: &str,
    span: Span,
) -> Result<QueryResult, QueryServiceError> {
    let (target, planning) = binding.into_parts();
    let context = crate::connector::context_for_planning_pin_typed(&planning, context)
        .map_err(connector_application_error)?;
    let storage = planning
        .derive_document_storage_lease()
        .map_err(connector_application_error)?;
    let observation = match storage
        .observe_keyed_document_definition(target.clone(), context)
        .await
    {
        Ok(observation) => observation,
        Err(KeyedDocumentReadError::Invalid(error))
            if error.kind() == ConnectorErrorKind::NotFound =>
        {
            let error = AnalyzeError::unknown_table(
                format!(
                    "Materialized view '{}.{}.{}' does not exist",
                    target.instance_id.as_str(),
                    target.namespace,
                    target.table
                ),
                span,
            );
            return Err(QueryServiceError::from_user_error(
                error.to_user_error(Some(source)),
            ));
        }
        Err(error) => {
            return Err(document_application_error(MvDocumentError::KeyedStorage(
                error,
            )));
        }
    };
    if !is_materialized_view_documents(&observation.view) {
        return Err(connector_application_error(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "Relation '{}.{}.{}' is a table, not a materialized view",
                target.instance_id.as_str(),
                target.namespace,
                target.table,
            ),
        )));
    }
    let documents =
        decode_current_keyed_documents(observation.view, storage.catalog_handle().clone())
            .map_err(document_application_error)?;
    let name = build::object_name(
        [
            target.instance_id.as_str(),
            &target.namespace,
            &target.table,
        ]
        .into_iter()
        .map(build::ident_from_catalog)
        .collect(),
    )
    .map_err(|error| application_error(error.to_string()))?;
    let mut properties = observation.properties;
    properties.extend(
        novarocks_mv_application::engine_properties::definition_properties(
            &documents.configuration,
        ),
    );
    properties.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let properties =
        crate::catalog_application::create_table_ddl::definition_properties(properties)
            .map_err(application_error)?;
    let sql = print_create_materialized_view_definition(
        &name,
        &properties,
        &documents.definition.query.effective_sql,
    )
    .map_err(|error| application_error(error.to_string()))?;
    build_string_query_result("Create Materialized View", vec![sql]).map_err(application_error)
}
