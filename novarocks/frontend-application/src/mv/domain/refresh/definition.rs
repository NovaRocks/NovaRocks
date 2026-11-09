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

//! Persisted Iceberg MV definition lookup and SQL identity helpers.

use sha2::{Digest, Sha256};

/// Computes the stable persisted-SQL fingerprint used by refresh artifacts.
pub fn mv_definition_fingerprint(select_sql: &str) -> String {
    hex::encode(Sha256::digest(select_sql.as_bytes()))
}

/// Project the exact catalog/namespace/table identity already retained by the lake view.
pub(crate) fn stored_mv_identity(
    projection: &novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
) -> novarocks_types::naming::TableIdentity {
    let target = &projection.facts.source_revision().target;
    novarocks_types::naming::TableIdentity::new(
        target.instance_id.as_str(),
        &target.namespace,
        &target.table,
    )
}

/// Read the exact stored source. Resolution remains with the caller's frozen
/// definition context; the statement issuing a refresh supplies no defaults.
pub fn read_stored_mv_query(
    sql: &str,
    target: &novarocks_types::naming::TableIdentity,
) -> Result<novarocks_parser::ast::Query, novarocks_sql::analyze_error::AnalyzeError> {
    use novarocks_sql::analyze_error::{AnalyzeError, StoredDefinition};
    let statements = novarocks_parser::parse(sql).map_err(|error| {
        AnalyzeError::stored_definition_parse_failure(
            StoredDefinition::materialized_view(target.fqn()),
            std::sync::Arc::from(sql),
            error,
            None,
        )
    })?;
    let mut statements = statements.into_iter();
    match (statements.next(), statements.next()) {
        (Some(novarocks_parser::ast::Statement::Query(query)), None) => Ok(query),
        _ => Err(AnalyzeError::stored_definition_not_query(
            StoredDefinition::materialized_view(target.fqn()),
            std::sync::Arc::from(sql),
            None,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_fingerprint_is_stable_for_the_same_sql() {
        assert_eq!(
            mv_definition_fingerprint("SELECT * FROM ice.db.t"),
            mv_definition_fingerprint("SELECT * FROM ice.db.t")
        );
        assert_ne!(
            mv_definition_fingerprint("SELECT * FROM ice.db.t"),
            mv_definition_fingerprint("SELECT * FROM ice.db.other")
        );
    }

    #[test]
    fn parse_stored_select_rejects_non_query_statement() {
        let target = novarocks_types::naming::TableIdentity::new("ice", "db", "mv");
        let error = read_stored_mv_query("DELETE FROM ice.db.t", &target).unwrap_err();
        assert_eq!(error.code().as_str(), "sql.analyze.invalid_view");
        assert_eq!(error.stored_definition().unwrap().name(), "ice.db.mv");
        assert_eq!(
            error.stored_view_definition_source(),
            Some("DELETE FROM ice.db.t")
        );
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn stored_parser_failure_keeps_its_identity_source_and_actual_cause() {
        use novarocks_sql::analyze_error::StoredDefinitionKind;
        let target = novarocks_types::naming::TableIdentity::new("ice", "db", "mv");
        let sql = "SELECT\n (";
        let parser_error = novarocks_parser::parse(sql).unwrap_err();
        let parser_public = parser_error.to_user_error(sql);
        let error = read_stored_mv_query(sql, &target).unwrap_err();
        assert_eq!(error.code().as_str(), "sql.analyze.invalid_view");
        assert_eq!(
            error.stored_definition().unwrap().kind(),
            StoredDefinitionKind::MaterializedView
        );
        assert_eq!(error.stored_definition().unwrap().name(), "ice.db.mv");
        assert_eq!(error.stored_view_definition_source(), Some(sql));
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<novarocks_parser::ParserError>(),
            Some(&parser_error)
        );
        let public = error.to_user_error(None);
        assert!(public.location().is_none());
        assert!(public.message().contains("ice.db.mv"));
        assert!(public.message().contains(parser_public.code().as_str()));
        let location = parser_public.location().unwrap();
        assert!(
            public
                .message()
                .contains(&format!("{}:{}", location.line(), location.column()))
        );
    }
}
