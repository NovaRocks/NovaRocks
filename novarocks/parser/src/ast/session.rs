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

//! Session-command syntax nodes.

use crate::Span;

use super::{Expr, Ident, Literal};

/// `SHOW [FULL] PROCESSLIST`.
///
/// `FULL` is MySQL's opt-out of truncating the reported statement text; it
/// changes only how much of `Info` is returned, never which sessions are.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShowProcessList {
    pub full: bool,
    pub span: Span,
}

pub(crate) fn write_show_process_list(statement: &ShowProcessList, output: &mut String) {
    output.push_str(if statement.full {
        "SHOW FULL PROCESSLIST"
    } else {
        "SHOW PROCESSLIST"
    });
}

/// A statement that changes or controls one SQL session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionStatement {
    Set(SetStatement),
    Use(UseStatement),
    Kill(KillStatement),
    TransactionControl(TransactionControlStatement),
}

impl SessionStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Set(statement) => statement.span,
            Self::Use(statement) => statement.span,
            Self::Kill(statement) => statement.span,
            Self::TransactionControl(statement) => statement.span,
        }
    }
}

/// A transaction-control statement recognized at the session boundary.
///
/// NovaRocks does not implement a cross-statement transaction.  These nodes
/// retain the syntactic intent so admission can reject it before any catalog
/// or data mutation is dispatched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionControlStatement {
    pub kind: TransactionControlKind,
    pub span: Span,
}

/// The closed transaction-control forms owned by the parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionControlKind {
    Begin,
    StartTransaction,
    Commit,
    Rollback,
    Savepoint,
}

impl TransactionControlKind {
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Begin => "BEGIN",
            Self::StartTransaction => "START TRANSACTION",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
            Self::Savepoint => "SAVEPOINT",
        }
    }
}

/// A `SET` statement containing independently scoped assignments.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetStatement {
    pub assignments: Vec<SetAssignment>,
    pub span: Span,
}

/// One `SET` assignment. Scope belongs to the assignment and never inherits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetAssignment {
    pub scope: SetScope,
    pub name: Ident,
    pub value: Expr,
    pub span: Span,
}

/// The syntactic scope attached to one `SET` assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetScope {
    Default,
    Session,
    Local,
    Global,
}

/// `USE <database>` or `USE <catalog>.<database>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UseStatement {
    pub catalog: Option<Ident>,
    pub database: Ident,
    pub span: Span,
}

/// `KILL [QUERY | CONNECTION] <id>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KillStatement {
    pub kind: KillKind,
    pub connection_id: Literal,
    pub span: Span,
}

/// The optional target kind in `KILL` syntax.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KillKind {
    Default,
    Query,
    Connection,
}
