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

//! Native typed-query table-reference extraction and catalog stripping.

use novarocks_parser::ast::{self as ast, Fold, TableFactor};

struct CatalogStripper;

impl Fold for CatalogStripper {
    fn fold_table_factor(&mut self, factor: TableFactor) -> TableFactor {
        let factor = ast::fold_table_factor(self, factor);
        match factor {
            TableFactor::Table {
                mut name,
                metadata,
                alias,
                version,
                span,
            } => {
                if name.parts.len() == 3 {
                    name.parts.remove(0);
                }
                TableFactor::Table {
                    name,
                    metadata,
                    alias,
                    version,
                    span,
                }
            }
            other => other,
        }
    }
}

/// Rewrite every catalog-qualified table factor to its two-part native form.
/// The Fold traversal includes CTEs, derived relations, JOIN ON predicates,
/// and expression-contained subqueries.
pub(crate) fn strip_catalog_from_three_part_names(query: &mut ast::Query) {
    let mut stripper = CatalogStripper;
    *query = Fold::fold_query(&mut stripper, query.clone());
}

#[cfg(test)]
mod tests {
    use novarocks_parser::ast::Statement;

    use super::*;

    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse native SQL");
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        query.clone()
    }

    #[test]
    fn strips_catalog_in_cte_and_expression_subquery() {
        let mut query = parse_query(
            "WITH x AS (SELECT * FROM cat.db.seed) \
             SELECT (SELECT count(*) FROM cat.db.t) FROM cat.db.outer_t",
        );
        strip_catalog_from_three_part_names(&mut query);
        assert_eq!(
            novarocks_parser::printer::print_query(&query),
            "WITH x AS (SELECT * FROM db.seed) SELECT (SELECT count(*) FROM db.t) FROM db.outer_t"
        );
    }
}
