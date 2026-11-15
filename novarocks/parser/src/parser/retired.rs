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

//! Transitional retired-head rejection, until the Trino grammar replaces it.
use super::StatementParser;
use crate::{ParseError, ast::Statement};
pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("ADMIN") || parser.current_is_word("CANCEL") {
        return Err(parser.unexpected("SQL statement"));
    }
    if parser.current_is_word("DROP")
        && ["STATS", "HISTOGRAM", "MULTIPLE"]
            .iter()
            .any(|word| parser.peek_word(1, word))
    {
        parser.consume_word("DROP")?;
        return Err(parser.unexpected("TABLE, DATABASE, CATALOG, VIEW or MATERIALIZED VIEW"));
    }
    Ok(None)
}
pub(super) fn show(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if !parser.current_is_word("SHOW") || parser.peek_word(1, "CREATE") {
        return Ok(None);
    }
    parser.consume_word("SHOW")?;
    Err(parser.unexpected("CREATE, PROCESSLIST or STATS FOR"))
}
