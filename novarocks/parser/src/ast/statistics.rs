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

//! Syntax for statement-owned ANALYZE and SHOW STATS.
use super::{Fold, Ident, ObjectName, Visit};
use crate::{Span, printer::print_object_name};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsStatement {
    AnalyzeTable(AnalyzeTable),
    ShowStats(ShowStats),
}
impl StatisticsStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::AnalyzeTable(value) => value.span,
            Self::ShowStats(value) => value.span,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeTable {
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowStats {
    pub name: ObjectName,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &StatisticsStatement, output: &mut String) {
    match statement {
        StatisticsStatement::AnalyzeTable(value) => {
            output.push_str("ANALYZE TABLE ");
            output.push_str(&print_object_name(&value.name));
            if !value.columns.is_empty() {
                output.push_str(" (");
                for (index, column) in value.columns.iter().enumerate() {
                    if index != 0 {
                        output.push_str(", ");
                    }
                    if column.quoted {
                        output.push('`');
                        output.push_str(&column.value.replace('`', "``"));
                        output.push('`');
                    } else {
                        output.push_str(&column.value);
                    }
                }
                output.push(')');
            }
        }
        StatisticsStatement::ShowStats(value) => {
            output.push_str("SHOW STATS FOR ");
            output.push_str(&print_object_name(&value.name));
        }
    }
}
pub(crate) fn walk<V: Visit + ?Sized>(visitor: &mut V, statement: &StatisticsStatement) {
    match statement {
        StatisticsStatement::AnalyzeTable(value) => {
            visitor.visit_object_name(&value.name);
            for column in &value.columns {
                visitor.visit_ident(column);
            }
        }
        StatisticsStatement::ShowStats(value) => visitor.visit_object_name(&value.name),
    }
}
pub(crate) fn fold<F: Fold + ?Sized>(
    folder: &mut F,
    statement: StatisticsStatement,
) -> StatisticsStatement {
    match statement {
        StatisticsStatement::AnalyzeTable(mut value) => {
            value.name = folder.fold_object_name(value.name);
            value.columns = value
                .columns
                .into_iter()
                .map(|column| folder.fold_ident(column))
                .collect();
            StatisticsStatement::AnalyzeTable(value)
        }
        StatisticsStatement::ShowStats(mut value) => {
            value.name = folder.fold_object_name(value.name);
            StatisticsStatement::ShowStats(value)
        }
    }
}
