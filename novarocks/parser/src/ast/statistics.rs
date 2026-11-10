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

//! Statistics-command syntax nodes.

use crate::{Span, printer::print_object_name};

use super::{Fold, Ident, ObjectName, Visit};

/// Statistics commands currently admitted by frontend statistics owners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsStatement {
    AnalyzeTable(AnalyzeTable),
    ShowAnalyzeJobs(ShowAnalyzeJobs),
    CancelAnalyze(CancelAnalyze),
    ShowTableStats(ShowTableStats),
    DropStats(DropStats),
    DropHistogram(DropHistogram),
    DropMultipleColumnsStats(DropMultipleColumnsStats),
}

impl StatisticsStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::AnalyzeTable(value) => value.span,
            Self::ShowAnalyzeJobs(value) => value.span,
            Self::CancelAnalyze(value) => value.span,
            Self::ShowTableStats(value) => value.span,
            Self::DropStats(value) => value.span,
            Self::DropHistogram(value) => value.span,
            Self::DropMultipleColumnsStats(value) => value.span,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyzeMode {
    Default,
    Full,
    Sample,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeTable {
    pub mode: AnalyzeMode,
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub with_sync_mode: bool,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShowAnalyzeJobs {
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelAnalyze {
    pub job_id: String,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowTableStats {
    pub name: ObjectName,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropStats {
    pub name: ObjectName,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropHistogram {
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropMultipleColumnsStats {
    pub name: ObjectName,
    pub span: Span,
}

pub(crate) fn write_sql(statement: &StatisticsStatement, output: &mut String) {
    match statement {
        StatisticsStatement::AnalyzeTable(value) => {
            output.push_str("ANALYZE");
            match value.mode {
                AnalyzeMode::Default => {}
                AnalyzeMode::Full => output.push_str(" FULL"),
                AnalyzeMode::Sample => output.push_str(" SAMPLE"),
            }
            output.push_str(" TABLE ");
            output.push_str(&print_object_name(&value.name));
            write_columns(output, &value.columns);
            if value.with_sync_mode {
                output.push_str(" WITH SYNC MODE");
            }
        }
        StatisticsStatement::ShowAnalyzeJobs(_) => output.push_str("SHOW ANALYZE JOBS"),
        StatisticsStatement::CancelAnalyze(value) => {
            output.push_str("CANCEL ANALYZE ");
            output.push_str(&value.job_id);
        }
        StatisticsStatement::ShowTableStats(value) => {
            output.push_str("SHOW TABLE STATS ");
            output.push_str(&print_object_name(&value.name));
        }
        StatisticsStatement::DropStats(value) => {
            output.push_str("DROP STATS ");
            output.push_str(&print_object_name(&value.name));
        }
        StatisticsStatement::DropHistogram(value) => {
            output.push_str("DROP HISTOGRAM ON ");
            output.push_str(&print_object_name(&value.name));
            write_columns(output, &value.columns);
        }
        StatisticsStatement::DropMultipleColumnsStats(value) => {
            output.push_str("DROP MULTIPLE COLUMNS STATS ");
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
        StatisticsStatement::ShowTableStats(value) => visitor.visit_object_name(&value.name),
        StatisticsStatement::DropStats(value) => visitor.visit_object_name(&value.name),
        StatisticsStatement::DropHistogram(value) => {
            visitor.visit_object_name(&value.name);
            for column in &value.columns {
                visitor.visit_ident(column);
            }
        }
        StatisticsStatement::DropMultipleColumnsStats(value) => {
            visitor.visit_object_name(&value.name)
        }
        StatisticsStatement::ShowAnalyzeJobs(_) | StatisticsStatement::CancelAnalyze(_) => {}
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
        StatisticsStatement::ShowTableStats(mut value) => {
            value.name = folder.fold_object_name(value.name);
            StatisticsStatement::ShowTableStats(value)
        }
        StatisticsStatement::DropStats(mut value) => {
            value.name = folder.fold_object_name(value.name);
            StatisticsStatement::DropStats(value)
        }
        StatisticsStatement::DropHistogram(mut value) => {
            value.name = folder.fold_object_name(value.name);
            value.columns = value
                .columns
                .into_iter()
                .map(|column| folder.fold_ident(column))
                .collect();
            StatisticsStatement::DropHistogram(value)
        }
        StatisticsStatement::DropMultipleColumnsStats(mut value) => {
            value.name = folder.fold_object_name(value.name);
            StatisticsStatement::DropMultipleColumnsStats(value)
        }
        StatisticsStatement::ShowAnalyzeJobs(value) => StatisticsStatement::ShowAnalyzeJobs(value),
        StatisticsStatement::CancelAnalyze(value) => StatisticsStatement::CancelAnalyze(value),
    }
}

fn write_columns(output: &mut String, columns: &[Ident]) {
    if columns.is_empty() {
        return;
    }
    output.push_str(" (");
    for (index, column) in columns.iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        write_ident(output, column);
    }
    output.push(')');
}

fn write_ident(output: &mut String, ident: &Ident) {
    if ident.quoted {
        output.push('`');
        output.push_str(&ident.value.replace('`', "``"));
        output.push('`');
    } else {
        output.push_str(&ident.value);
    }
}
