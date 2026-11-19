// SPDX-License-Identifier: Apache-2.0
//! The new Trino core printer. This first slice is not yet a complete printer.
//! Unsupported typed facts are refused rather than erased or rendered as raw SQL.

use crate::Span;
use crate::ast::*;
use std::collections::HashMap;
use std::fmt::Write;
use std::ops::Range;

mod advanced_relation;
mod branch_statement;
mod catalog_schema;
mod ctas_statement;
mod expression;
mod json;
mod literal_type;
mod merge_call;
mod metadata_session;
mod pattern;
mod permission_statement;
mod prepared_statement;
mod query;
mod routine;
mod show_statement;
mod special;
mod statement;
mod table_alter;
mod table_statement;
mod view_statement;
mod write_statement;

/// A syntax fact which cannot be faithfully represented as standalone SQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrintError {
    Unrepresentable {
        fact: &'static str,
    },
    ParameterPosition {
        span: Span,
        expected: usize,
        actual: usize,
    },
    ParameterOrder {
        span: Span,
    },
    ParameterCount {
        expected: usize,
        actual: usize,
    },
}
impl std::fmt::Display for PrintError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unrepresentable { fact } => {
                write!(formatter, "SQL cannot faithfully represent {fact}")
            }
            Self::ParameterPosition {
                expected, actual, ..
            } => write!(
                formatter,
                "parameter position {actual} cannot occupy construction slot {expected}"
            ),
            Self::ParameterOrder { .. } => formatter
                .write_str("parameter construction order is not representable by these SQL fields"),
            Self::ParameterCount { expected, actual } => write!(
                formatter,
                "SQL retains {actual} parameter events but the AST retains {expected}"
            ),
        }
    }
}
impl std::error::Error for PrintError {}
type PrintResult = Result<(), PrintError>;

pub fn print_statement(statement: &TrinoStatement) -> Result<String, PrintError> {
    let positions = parameter_positions(|visitor| visitor.visit_trino_statement(statement))?;
    let count = positions.values.len();
    let mut writer = Writer {
        parameter_ranges: positions.ranges,
        ..Writer::default()
    };
    writer.statement(statement)?;
    writer.finish(count)
}

pub fn print_path_specification(path: &TrinoPathSpecification) -> Result<String, PrintError> {
    let mut writer = Writer::default();
    writer.path_specification(path)?;
    writer.finish(0)
}

pub fn print_query(query: &Query) -> Result<String, PrintError> {
    let positions = parameter_positions(|visitor| visitor.visit_query(query))?;
    let count = positions.values.len();
    let mut writer = Writer {
        parameter_ranges: positions.ranges,
        ..Writer::default()
    };
    writer.query(query)?;
    writer.finish(count)
}

pub fn print_object_name(name: &ObjectName) -> String {
    let mut writer = Writer::default();
    writer.name(name);
    writer.output
}

pub fn print_literal(literal: &Literal) -> Result<String, PrintError> {
    let mut writer = Writer::default();
    writer.literal(literal)?;
    writer.finish(0)
}

pub fn print_expression(expression: &Expr) -> Result<String, PrintError> {
    let positions = parameter_positions(|visitor| visitor.visit_expr(expression))?;
    let count = positions.values.len();
    let mut writer = Writer {
        parameter_ranges: positions.ranges,
        ..Writer::default()
    };
    writer.expression(expression)?;
    writer.finish(count)
}
pub fn print_type(data_type: &TypeName) -> Result<String, PrintError> {
    let positions = parameter_positions(|visitor| visitor.visit_type_name(data_type))?;
    let count = positions.values.len();
    let mut writer = Writer {
        parameter_ranges: positions.ranges,
        ..Writer::default()
    };
    writer.data_type(data_type)?;
    writer.finish(count)
}
pub fn print_function_specification(
    function: &FunctionSpecification,
) -> Result<String, PrintError> {
    let positions = parameter_positions(|visitor| visitor.visit_function_specification(function))?;
    let count = positions.values.len();
    let mut writer = Writer {
        parameter_ranges: positions.ranges,
        ..Writer::default()
    };
    writer.function_specification(function)?;
    writer.finish(count)
}
pub fn print_row_pattern(pattern: &RowPattern) -> Result<String, PrintError> {
    let positions = parameter_positions(|visitor| visitor.visit_row_pattern(pattern))?;
    let count = positions.values.len();
    let mut writer = Writer {
        parameter_ranges: positions.ranges,
        ..Writer::default()
    };
    writer.row_pattern(pattern)?;
    writer.finish(count)
}

#[derive(Default)]
struct Writer {
    output: String,
    // Observations of original fields only. This ledger validates existing
    // positions; the parser remains their sole issuer.
    parameter_events: Vec<(usize, Span)>,
    parameter_ranges: HashMap<*const Expr, ParameterSummary>,
}
impl Writer {
    fn dereference_base(&mut self, expression: &Expr) -> PrintResult {
        match expression {
            Expr::Identifier(_) | Expr::Dereference(_) => self.expression(expression),
            _ => self.parenthesized_expression(expression),
        }
    }

    fn text(&mut self, text: &str) {
        self.output.push_str(text);
    }
    fn character(&mut self, character: char) {
        self.output.push(character);
    }
    fn identifier(&mut self, identifier: &Ident) {
        if identifier.quoted {
            self.character('"');
            for character in identifier.value.chars() {
                if character == '"' {
                    self.character('"');
                }
                self.character(character);
            }
            self.character('"');
        } else {
            self.text(&identifier.value);
        }
    }
    fn name(&mut self, name: &ObjectName) {
        for (index, part) in name.parts.iter().enumerate() {
            if index != 0 {
                self.character('.');
            }
            self.identifier(part);
        }
    }
    fn identifiers(&mut self, names: &[Ident]) {
        for (index, name) in names.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.identifier(name);
        }
    }
    fn string(&mut self, value: &str) {
        self.character('\'');
        for character in value.chars() {
            if character == '\'' {
                self.character('\'');
            }
            self.character(character);
        }
        self.character('\'');
    }
    fn expressions(&mut self, values: &[Expr]) -> PrintResult {
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.expression(value)?;
        }
        Ok(())
    }
    fn parenthesized_expression(&mut self, value: &Expr) -> PrintResult {
        self.character('(');
        self.expression(value)?;
        self.character(')');
        Ok(())
    }
}

fn unsupported(fact: &'static str) -> PrintError {
    PrintError::Unrepresentable { fact }
}
#[derive(Clone, Copy, Default)]
struct ParameterSummary {
    count: usize,
    minimum: Option<usize>,
    maximum: usize,
}
impl ParameterSummary {
    fn merge(self, other: Self) -> Self {
        Self {
            count: self.count + other.count,
            minimum: match (self.minimum, other.minimum) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (left, right) => left.or(right),
            },
            maximum: self.maximum.max(other.maximum),
        }
    }
}
#[derive(Default)]
struct ParameterPositions {
    values: Vec<(usize, Span)>,
    // Keys are real immutable Expr addresses in the one borrowed input. They
    // are never dereferenced, persisted or used to infer syntax provenance.
    ranges: HashMap<*const Expr, ParameterSummary>,
    active: ParameterSummary,
}
impl Visit for ParameterPositions {
    #[recursive::recursive]
    fn visit_expr(&mut self, expression: &Expr) {
        let parent = self.active;
        self.active = ParameterSummary::default();
        walk_expr(self, expression);
        let summary = self.active;
        self.ranges.insert(expression as *const Expr, summary);
        self.active = parent.merge(summary);
    }
    fn visit_parameter(&mut self, parameter: &Parameter) {
        self.values.push((parameter.position, parameter.span));
        self.active = self.active.merge(ParameterSummary {
            count: 1,
            minimum: Some(parameter.position),
            maximum: parameter.position,
        });
    }
}
fn parameter_positions(
    visit: impl FnOnce(&mut ParameterPositions),
) -> Result<ParameterPositions, PrintError> {
    let mut positions = ParameterPositions::default();
    visit(&mut positions);
    positions.values.sort_by_key(|(position, _)| *position);
    for (expected, &(actual, span)) in positions.values.iter().enumerate() {
        if expected != actual {
            return Err(PrintError::ParameterPosition {
                span,
                expected,
                actual,
            });
        }
    }
    Ok(positions)
}

impl Writer {
    fn parameter_range(&self, expression: &Expr) -> Result<Option<Range<usize>>, PrintError> {
        let summary = self
            .parameter_ranges
            .get(&(expression as *const Expr))
            .ok_or(PrintError::ParameterOrder {
                span: expression.span(),
            })?;
        let Some(start) = summary.minimum else {
            return Ok(None);
        };
        let end = summary
            .maximum
            .checked_add(1)
            .ok_or(PrintError::ParameterOrder {
                span: expression.span(),
            })?;
        if end.checked_sub(start) != Some(summary.count) {
            return Err(PrintError::ParameterOrder {
                span: expression.span(),
            });
        }
        Ok(Some(start..end))
    }
    fn finish(self, expected: usize) -> Result<String, PrintError> {
        if self.parameter_events.len() != expected {
            return Err(PrintError::ParameterCount {
                expected,
                actual: self.parameter_events.len(),
            });
        }
        for (expected, &(actual, span)) in self.parameter_events.iter().enumerate() {
            if expected != actual {
                return Err(PrintError::ParameterPosition {
                    span,
                    expected,
                    actual,
                });
            }
        }
        Ok(self.output)
    }

    /// Reorders only the observations emitted by this real node's children.
    /// Child ranges partition this invocation; nested reordering preserves length.
    fn reorder_parameter_events(
        &mut self,
        start: usize,
        groups: &[Range<usize>],
        span: Span,
    ) -> PrintResult {
        let end = self.parameter_events.len();
        if start > end {
            return Err(PrintError::ParameterOrder { span });
        }
        let mut partition = groups.to_vec();
        partition.sort_by_key(|range| (range.start, range.end));
        let mut cursor = start;
        for range in &partition {
            if range.start != cursor || range.end < range.start || range.end > end {
                return Err(PrintError::ParameterOrder { span });
            }
            cursor = range.end;
        }
        if cursor != end {
            return Err(PrintError::ParameterOrder { span });
        }
        let ordered: Vec<_> = groups
            .iter()
            .flat_map(|range| self.parameter_events[range.clone()].iter().copied())
            .collect();
        self.parameter_events[start..].copy_from_slice(&ordered);
        Ok(())
    }

    fn inverse_position(&self, call: &FunctionCall) -> Result<bool, PrintError> {
        if call.name.parts.len() != 1
            || call.name.parts[0].quoted
            || call.name.parts[0].value != "strpos"
            || call.arguments.len() != 2
            || call
                .arguments
                .iter()
                .any(|argument| argument.name.is_some())
            || call.distinct
            || call.order_by.is_some()
            || call.filter.is_some()
            || call.window.is_some()
            || call.null_treatment.is_some()
            || call.processing_mode.is_some()
        {
            return Ok(false);
        }
        let left = self.parameter_range(&call.arguments[0].value)?;
        let right = self.parameter_range(&call.arguments[1].value)?;
        match (left, right) {
            (Some(left), Some(right)) if left.end <= right.start => Ok(false),
            (Some(left), Some(right)) if right.end <= left.start => Ok(true),
            (Some(_), Some(_)) => Err(PrintError::ParameterOrder { span: call.span }),
            _ => Ok(false),
        }
    }
}
