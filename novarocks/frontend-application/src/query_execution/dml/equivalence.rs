// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Temporary migration comparison, removed together with the old SQL authors.

use novarocks_parser::ast::{self, SyntaxEq};

pub(crate) fn require(
    site: &str,
    built: &ast::Query,
    legacy: impl FnOnce() -> Result<String, String>,
) -> Result<(), String> {
    let sql = match legacy() {
        Ok(sql) => sql,
        Err(reason) => {
            tracing::warn!("TRC-5b equivalence skipped at {site}: {reason}");
            return Ok(());
        }
    };
    let old = match novarocks_parser::parse(&sql) {
        Ok(mut statements) if statements.len() == 1 => match statements.pop().unwrap() {
            ast::Statement::Query(query) => query,
            _ => return Err(format!("TRC-5b equivalence non-query at {site}: {sql}")),
        },
        Ok(_) => {
            return Err(format!(
                "TRC-5b equivalence multiple statements at {site}: {sql}"
            ));
        }
        Err(reason) => {
            tracing::warn!("TRC-5b equivalence skipped at {site}: {reason}");
            return Ok(());
        }
    };
    if built.syntax_eq(&old) {
        Ok(())
    } else {
        Err(format!(
            "TRC-5b equivalence mismatch at {site}: built={}, legacy={sql}",
            novarocks_parser::printer::print_query(built)
        ))
    }
}
