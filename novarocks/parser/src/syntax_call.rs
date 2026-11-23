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

use std::fmt;

use crate::Span;
use crate::ast::{Expr, ExtractField, TrimSpecification};

/// The canonical name and original children of a dedicated call syntax node.
#[derive(Debug)]
pub struct CanonicalCall<'a> {
    pub name: &'static str,
    pub arguments: Vec<&'a Expr>,
    pub span: Span,
}

/// A form of dedicated call syntax that has no supported canonical call.
#[derive(Debug, PartialEq, Eq)]
pub struct UnsupportedCallSyntax {
    pub message: String,
    pub span: Span,
}

impl UnsupportedCallSyntax {
    fn new(message: impl Into<String>, span: Span) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }
}

impl fmt::Display for UnsupportedCallSyntax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for UnsupportedCallSyntax {}

/// Map supported dedicated syntax to the canonical call spelling. Ordinary
/// calls and expressions outside the mapped syntax return `Ok(None)`.
pub fn canonical_call(expr: &Expr) -> Result<Option<CanonicalCall<'_>>, UnsupportedCallSyntax> {
    let span = expr.span();
    let (name, arguments) = match expr {
        Expr::Row(row) => {
            if row.fields.iter().any(|field| field.name.is_some()) {
                return Err(UnsupportedCallSyntax::new(
                    "named ROW fields are not supported",
                    span,
                ));
            }
            (
                "row",
                row.fields.iter().map(|field| &field.expression).collect(),
            )
        }
        Expr::If(node) => {
            let Some(false_value) = node.false_value.as_ref() else {
                return Err(UnsupportedCallSyntax::new(
                    "two-argument IF is not supported",
                    span,
                ));
            };
            ("if", vec![&node.condition, &node.true_value, false_value])
        }
        Expr::NullIf(node) => ("nullif", vec![&node.first, &node.second]),
        Expr::Coalesce(node) => ("coalesce", node.operands.iter().collect()),
        Expr::Try(node) => ("try", vec![&node.inner]),
        Expr::Trim(node) => {
            if node.trim_character.is_some() {
                return Err(UnsupportedCallSyntax::new(
                    "explicit TRIM characters are not supported",
                    span,
                ));
            }
            let name = match node.specification {
                TrimSpecification::Both => "trim",
                TrimSpecification::Leading => "ltrim",
                TrimSpecification::Trailing => "rtrim",
            };
            (name, vec![&node.trim_source])
        }
        Expr::Extract(node) => (extract_call_name(node.field), vec![&node.expression]),
        _ => return Ok(None),
    };
    Ok(Some(CanonicalCall {
        name,
        arguments,
        span,
    }))
}

/// The scalar identity associated with the parsed EXTRACT field.
pub fn extract_call_name(field: ExtractField) -> &'static str {
    match field {
        ExtractField::Year => "year",
        ExtractField::Quarter => "quarter",
        ExtractField::Month => "month",
        ExtractField::Day => "day",
        ExtractField::DayOfMonth => "day_of_month",
        ExtractField::DayOfWeek | ExtractField::Dow => "day_of_week",
        ExtractField::DayOfYear | ExtractField::Doy => "day_of_year",
        ExtractField::Week => "week",
        ExtractField::YearOfWeek | ExtractField::Yow => "__extract_year_of_week",
        ExtractField::Hour => "hour",
        ExtractField::Minute => "minute",
        ExtractField::Second => "second",
        ExtractField::TimezoneHour => "__extract_timezone_hour",
        ExtractField::TimezoneMinute => "__extract_timezone_minute",
    }
}
