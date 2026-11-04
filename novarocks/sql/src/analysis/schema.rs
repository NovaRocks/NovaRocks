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

//! SQL relation fields are separate from planner value labels.

use super::{ProjectItem, TypedExpr};
use crate::column_id::ColumnId;
use crate::common::OutputColumn;
use novarocks_type_contract::FunctionValueType;

#[derive(Clone, Debug)]
pub(crate) struct AnalyzedOutputField {
    pub column_id: ColumnId,
    /// Internal value label; never grants SQL name visibility.
    pub name: String,
    pub field_name: Option<String>,
    pub value_type: FunctionValueType,
    pub is_internal: bool,
}

impl AnalyzedOutputField {
    pub(crate) fn planner_column(&self) -> OutputColumn {
        OutputColumn {
            column_id: self.column_id,
            name: self.name.clone(),
            value_type: self.value_type.clone(),
            is_internal: self.is_internal,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AnalyzedProjectItem {
    pub expr: TypedExpr,
    pub output_name: String,
    pub field_name: Option<String>,
    pub output_column_id: ColumnId,
}

impl AnalyzedProjectItem {
    pub(crate) fn into_planner(self) -> ProjectItem {
        ProjectItem {
            expr: self.expr,
            output_name: self.output_name,
            output_column_id: self.output_column_id,
        }
    }
}

pub(crate) fn planner_columns(fields: &[AnalyzedOutputField]) -> Vec<OutputColumn> {
    fields
        .iter()
        .map(AnalyzedOutputField::planner_column)
        .collect()
}

/// Apply a visible relation column list without counting lineage fields.
pub(crate) fn apply_column_aliases(
    fields: &mut [AnalyzedOutputField],
    aliases: &[novarocks_parser::ast::Ident],
    span: novarocks_parser::Span,
) -> Result<(), crate::analyze_error::AnalyzeError> {
    if aliases.is_empty() {
        return Ok(());
    }
    let visible = fields.iter().filter(|field| !field.is_internal).count();
    if visible != aliases.len() {
        return Err(
            crate::analyze_error::AnalyzeError::mismatched_column_aliases(
                format!(
                    "Column alias list has {} entries but relation has {visible} columns available",
                    aliases.len()
                ),
                span,
            ),
        );
    }
    for (field, alias) in fields
        .iter_mut()
        .filter(|field| !field.is_internal)
        .zip(aliases)
    {
        field.field_name = Some(alias.value.clone());
        field.name = alias.value.clone();
    }
    Ok(())
}

/// Definition checks follow original ordinal order, after any explicit list.
pub(crate) fn validate_definition_column_names(
    fields: &[AnalyzedOutputField],
    aliases: &[novarocks_parser::ast::Ident],
    span: novarocks_parser::Span,
) -> Result<Vec<String>, crate::analyze_error::AnalyzeError> {
    use crate::analyze_error::AnalyzeError;
    let visible = fields
        .iter()
        .filter(|field| !field.is_internal)
        .collect::<Vec<_>>();
    if !aliases.is_empty() && aliases.len() != visible.len() {
        return Err(AnalyzeError::mismatched_column_aliases(
            format!(
                "Column alias list has {} entries but relation has {} columns available",
                aliases.len(),
                visible.len()
            ),
            span,
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut names = Vec::with_capacity(visible.len());
    for (ordinal, field) in visible.into_iter().enumerate() {
        let name = if aliases.is_empty() {
            field.field_name.as_deref()
        } else {
            Some(aliases[ordinal].value.as_str())
        };
        let name = name.ok_or_else(|| {
            AnalyzeError::missing_column_name(
                format!("Column name not specified at position {}", ordinal + 1),
                span,
            )
        })?;
        if !seen.insert(name.to_lowercase()) {
            return Err(AnalyzeError::duplicate_column_name(
                format!("Column name '{name}' specified more than once"),
                span,
            ));
        }
        names.push(name.to_owned());
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze_error::AnalyzeErrorKind as K;
    use novarocks_parser::Span;

    fn field(name: Option<&str>, internal: bool) -> AnalyzedOutputField {
        AnalyzedOutputField {
            column_id: ColumnId(1),
            name: "label".into(),
            field_name: name.map(str::to_owned),
            value_type: FunctionValueType::new(arrow::datatypes::DataType::Int32, false),
            is_internal: internal,
        }
    }

    #[test]
    fn definition_names_validate_first_offending_occurrence() {
        let span = Span::new(0, 1);
        for (fields, kind) in [
            (
                vec![
                    field(Some("a"), false),
                    field(Some("A"), false),
                    field(None, false),
                ],
                K::DuplicateColumnName,
            ),
            (
                vec![
                    field(None, false),
                    field(Some("a"), false),
                    field(Some("a"), false),
                ],
                K::MissingColumnName,
            ),
        ] {
            let error = validate_definition_column_names(&fields, &[], span).unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(error.span(), Some(span));
        }
    }

    #[test]
    fn definition_lists_precede_name_checks_and_exclude_hidden_fields() {
        let span = Span::new(0, 1);
        let fields = vec![
            field(None, false),
            field(Some("lineage"), true),
            field(None, false),
        ];
        let aliases = ["a", "B"].map(|value| novarocks_parser::ast::Ident {
            value: value.into(),
            quoted: false,
            quote_style: None,
            span,
        });
        assert_eq!(
            validate_definition_column_names(&fields, &aliases, span).unwrap(),
            ["a", "B"]
        );
        assert_eq!(
            validate_definition_column_names(&fields, &aliases[..1], span)
                .unwrap_err()
                .kind(),
            K::MismatchedColumnAliases
        );
        let mut renamed = fields;
        apply_column_aliases(&mut renamed, &aliases, span).unwrap();
        assert_eq!(renamed[0].field_name.as_deref(), Some("a"));
        assert_eq!(renamed[1].field_name.as_deref(), Some("lineage"));
        assert_eq!(renamed[2].field_name.as_deref(), Some("B"));
    }
}
