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

/// Parses the raw stored MV SELECT SQL without invoking query compilation.
pub fn parse_mv_select_query(sql: &str) -> Result<novarocks_parser::ast::Query, String> {
    let statements =
        novarocks_parser::parse(sql).map_err(|err| format!("sql parser error: {err}"))?;
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(query.clone())
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
        assert_eq!(
            parse_mv_select_query("DELETE FROM ice.db.t").unwrap_err(),
            "stored MV SQL must be a SELECT query"
        );
    }
}
