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

//! SQL admission helpers over native typed parser nodes.

pub fn query_allows_throw_exception_hint(
    query: &novarocks_parser::ast::Query,
) -> Result<bool, crate::analyze_error::AnalyzeError> {
    Ok(crate::sql_mode::query_sql_semantics(
        &crate::sql_mode::SqlSemanticSettings::default(),
        query,
    )?
    .sql_mode()
    .allow_throw_exception())
}

/// Returns a positive literal memory override from the root query's facts.
pub fn query_mem_limit_hint(query: &novarocks_parser::ast::Query) -> Option<i64> {
    use novarocks_parser::ast::{Expr, LiteralKind};
    query.session_properties.iter().find_map(|property| {
        let [name] = property.name.parts.as_slice() else {
            return None;
        };
        if !name.value.eq_ignore_ascii_case("query_mem_limit") {
            return None;
        }
        let Expr::Literal(literal) = &property.value else {
            return None;
        };
        let LiteralKind::Long {
            parsed_value: value,
            ..
        } = &literal.kind
        else {
            return None;
        };
        Some(*value).filter(|value| *value > 0)
    })
}

#[cfg(test)]
mod tests {
    use super::{query_allows_throw_exception_hint, query_mem_limit_hint};

    #[test]
    fn allow_throw_exception_uses_root_session_properties() {
        let query = &mut novarocks_parser::parser::parse_query(
            "WITH SESSION sql_mode = 'ALLOW_THROW_EXCEPTION' SELECT 1",
        )
        .expect("typed session fixture parses");
        assert!(query_allows_throw_exception_hint(query).unwrap());
    }

    #[test]
    fn query_memory_limit_uses_root_session_properties() {
        let query =
            &mut novarocks_parser::parser::parse_query("WITH SESSION query_mem_limit = 1 SELECT 1")
                .expect("typed session fixture parses");
        assert_eq!(query_mem_limit_hint(query), Some(1));
    }
}
