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

//! SQL-local typed query reference helpers.

pub(crate) mod query_refs;

use novarocks_parser::{ast, printer};

// Diagnostic fragments can borrow a subtree with statement-relative
// parameters. Report a standalone rendering failure in the diagnostic itself.
pub(crate) fn expression_for_diagnostic(expression: &ast::Expr) -> String {
    printer::print_expression(expression).unwrap_or_else(|error| format!("<{error}>"))
}

pub(crate) fn query_for_diagnostic(query: &ast::Query) -> String {
    printer::print_query(query).unwrap_or_else(|error| format!("<{error}>"))
}

pub(crate) fn type_for_diagnostic(data_type: &ast::TypeName) -> String {
    printer::print_type(data_type).unwrap_or_else(|error| format!("<{error}>"))
}
