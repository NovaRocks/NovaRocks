// SPDX-License-Identifier: Apache-2.0
//! The new Trino core printer. This first slice is not yet a complete printer.
//! Unsupported typed facts are refused rather than erased or rendered as raw SQL.

use crate::ast::*;
use std::fmt::Write;

mod expression;
mod json;
mod literal_type;
mod query;
mod special;

pub fn print_query(query: &Query) -> String {
    let mut writer = Writer::default();
    writer.query(query);
    writer.output
}
pub fn print_expression(expression: &Expr) -> String {
    let mut writer = Writer::default();
    writer.expression(expression);
    writer.output
}
pub fn print_type(data_type: &TypeName) -> String {
    let mut writer = Writer::default();
    writer.data_type(data_type);
    writer.output
}

#[derive(Default)]
struct Writer {
    output: String,
}
impl Writer {
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
    fn expressions(&mut self, values: &[Expr]) {
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.expression(value);
        }
    }
    fn parenthesized_expression(&mut self, value: &Expr) {
        self.character('(');
        self.expression(value);
        self.character(')');
    }
}

fn unsupported(fact: &str) -> ! {
    panic!("Trino printer first slice does not yet render {fact}")
}
fn legacy(fact: &str) -> ! {
    panic!("Trino core printer cannot render legacy {fact}")
}
