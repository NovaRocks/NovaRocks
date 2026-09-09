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

pub fn query_allows_throw_exception_hint(query: &novarocks_parser::ast::Query) -> bool {
    use novarocks_parser::ast::{BinaryOperator, Expr, LiteralKind, SelectHintValue, SetExpr};

    let mut body = query.body.as_ref();
    while let SetExpr::Query(nested) = body {
        body = nested.body.as_ref();
    }
    let SetExpr::Select(select) = body else {
        return false;
    };
    select.hints.iter().any(|hint| {
        hint.name.value.eq_ignore_ascii_case("set_var")
            && matches!(&hint.value, SelectHintValue::Call { arguments } if arguments.iter().any(|argument| {
                matches!(argument,
                    Expr::Binary(binary)
                        if binary.operator == BinaryOperator::Equal
                            && matches!(binary.left.as_ref(), Expr::Identifier(name) if name.value.eq_ignore_ascii_case("sql_mode"))
                            && matches!(binary.right.as_ref(), Expr::Literal(literal) if matches!(&literal.kind, LiteralKind::String(value) if value.to_ascii_lowercase().contains("allow_throw_exception")))
                )
            }))
    })
}

/// Returns the positive per-statement execution-memory limit carried by a
/// typed `SET_VAR(query_mem_limit = N)` hint.
pub fn query_mem_limit_hint(query: &novarocks_parser::ast::Query) -> Option<i64> {
    use novarocks_parser::ast::{BinaryOperator, Expr, LiteralKind, SelectHintValue, SetExpr};

    let mut body = query.body.as_ref();
    while let SetExpr::Query(nested) = body {
        body = nested.body.as_ref();
    }
    let SetExpr::Select(select) = body else {
        return None;
    };
    select.hints.iter().find_map(|hint| {
        if !hint.name.value.eq_ignore_ascii_case("set_var") {
            return None;
        }
        let SelectHintValue::Call { arguments } = &hint.value else {
            return None;
        };
        arguments.iter().find_map(|argument| {
            let Expr::Binary(binary) = argument else {
                return None;
            };
            let Expr::Identifier(name) = binary.left.as_ref() else {
                return None;
            };
            if binary.operator != BinaryOperator::Equal
                || !name.value.eq_ignore_ascii_case("query_mem_limit")
            {
                return None;
            }
            let Expr::Literal(literal) = binary.right.as_ref() else {
                return None;
            };
            let LiteralKind::Number(value) = &literal.kind else {
                return None;
            };
            value.parse::<i64>().ok().filter(|value| *value > 0)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{query_allows_throw_exception_hint, query_mem_limit_hint};

    #[test]
    fn allow_throw_exception_uses_typed_set_var_hints() {
        let mut statements =
            novarocks_parser::parse("SELECT /*+ SET_VAR(sql_mode = 'ALLOW_THROW_EXCEPTION') */ 1")
                .expect("typed hint fixture parses");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected query");
        };
        assert!(query_allows_throw_exception_hint(query));
    }

    #[test]
    fn query_memory_limit_uses_typed_set_var_hints() {
        let mut statements =
            novarocks_parser::parse("SELECT /*+ SET_VAR(query_mem_limit = 1) */ 1")
                .expect("typed hint fixture parses");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected query");
        };
        assert_eq!(query_mem_limit_hint(query), Some(1));
    }
}
