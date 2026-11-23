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

//! An application-authored SHOW binds its exact root FROM occurrence.

use std::sync::Arc;

use novarocks_parser::ast;

use crate::system_table::{StatementSystemRelation, SystemTableDeclaration};

use super::SqlCompileError;

#[derive(Clone, Debug)]
pub(crate) struct StatementSystemRelationSource {
    pub(crate) kind: StatementSystemRelation,
    pub(crate) declaration: Arc<SystemTableDeclaration>,
}

/// Resolve the owned structural position in the current AST, after any clone
/// or rewrite. The AST table spelling is only a qualifier within this query.
pub(crate) fn statement_system_relation_site(
    query: &ast::Query,
) -> Result<&ast::Table, SqlCompileError> {
    if let ast::SetExpr::Select(select) = query.body.as_ref()
        && let Some(relation) = &select.from
    {
        let relation = match relation {
            ast::Relation::Aliased(aliased) => aliased.relation.as_ref(),
            relation => relation,
        };
        if let ast::Relation::Table(table) = relation
            && table.query_period.is_none()
            && table.branch.is_none()
        {
            return Ok(table);
        }
    }
    Err(SqlCompileError::InvalidRequest(
        "a private system relation requires one plain root FROM table".to_owned(),
    ))
}
