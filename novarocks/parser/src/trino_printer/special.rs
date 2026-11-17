// SPDX-License-Identifier: Apache-2.0
//! Typed special-expression writers and the pinned LISTAGG constructor inverse.
use super::*;

impl Writer {
    #[recursive::recursive]
    pub(super) fn special_expression(&mut self, value: &Expr) {
        match value {
            Expr::Extract(value) => {
                self.text("EXTRACT(");
                self.text(value.field.as_str());
                self.text(" FROM ");
                self.expression(&value.expression);
                self.character(')');
            }
            Expr::AtLocal(value) => {
                self.character('(');
                self.parenthesized_expression(&value.value);
                self.text(" AT LOCAL)");
            }
            Expr::AtTimeZone(value) => {
                // timeZoneSpecifier is an interval or a string, not an arbitrary
                // expression to be coerced or reclassified by the formatter.
                match &value.time_zone {
                    Expr::Literal(Literal {
                        kind: LiteralKind::String(_) | LiteralKind::Interval(_),
                        ..
                    }) => {}
                    _ => unsupported("a time zone outside the pinned string/interval grammar"),
                }
                self.character('(');
                self.parenthesized_expression(&value.value);
                self.text(" AT TIME ZONE ");
                self.expression(&value.time_zone);
                self.character(')');
            }
            Expr::GroupingOperation(value) => {
                self.text("GROUPING(");
                for (index, column) in value.grouping_columns.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    // GroupingOperation owns normalized qualified names. The
                    // underlying Expr is only the real identifier-chain carrier.
                    let chain = identifier_chain_observed(column, |_| Ok::<_, ()>(()))
                        .expect("infallible borrowed observation");
                    if chain.is_none() {
                        unsupported("a computed expression in GROUPING's qualified-name list");
                    }
                    self.grouping_name(column);
                }
                self.character(')');
            }
            Expr::Trim(value) => {
                self.text("TRIM(");
                self.text(match value.specification {
                    TrimSpecification::Both => "BOTH",
                    TrimSpecification::Leading => "LEADING",
                    TrimSpecification::Trailing => "TRAILING",
                });
                if let Some(character) = &value.trim_character {
                    self.character(' ');
                    self.expression(character);
                }
                self.text(" FROM ");
                self.expression(&value.trim_source);
                self.character(')');
            }
            Expr::Overlay(value) => {
                self.text("OVERLAY(");
                self.expression(&value.value);
                self.text(" PLACING ");
                self.expression(&value.replacement);
                self.text(" FROM ");
                self.expression(&value.start);
                if let Some(length) = &value.length {
                    self.text(" FOR ");
                    self.expression(length);
                }
                self.character(')');
            }
            Expr::IntervalValueExpression(value) => {
                // Parenthesize the whole qualified expression. In a projection
                // a bare trailing DAY could instead become an implicit alias.
                // Its subtraction is syntax for the real left/right fields,
                // not a fabricated BinaryExpr or a replacement type node.
                self.text("((");
                self.parenthesized_expression(&value.left);
                self.text(" - ");
                self.parenthesized_expression(&value.right);
                self.text(") ");
                self.qualifier(&value.data_type.qualifier);
                self.character(')');
            }
            Expr::MethodCall(value) => {
                // A parenthesized primary receiver distinguishes a method from
                // a qualified ordinary function name without an origin flag.
                self.parenthesized_expression(&value.receiver);
                self.character('.');
                self.identifier(&value.method);
                self.method_arguments(&value.arguments);
            }
            Expr::StaticMethodCall(value) => {
                self.name(&value.type_name);
                self.text("::");
                self.identifier(&value.method);
                self.method_arguments(&value.arguments);
            }
            _ => unreachable!("special-expression dispatch is an exhaustive typed subset"),
        }
    }

    #[recursive::recursive]
    fn grouping_name(&mut self, value: &Expr) {
        match value {
            Expr::Identifier(value) => self.identifier(value),
            Expr::Dereference(value) => {
                self.grouping_name(&value.base);
                self.character('.');
                self.identifier(value.field.as_ref().expect("validated pure name chain"));
            }
            _ => unreachable!("qualified name was validated before writing"),
        }
    }

    #[recursive::recursive]
    fn method_arguments(&mut self, values: &[CallArgument]) {
        self.character('(');
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            if let Some(name) = &value.name {
                self.identifier(name);
                self.text(" => ");
            }
            self.expression(&value.value);
        }
        self.character(')');
    }

    #[recursive::recursive]
    pub(super) fn listagg(&mut self, value: &FunctionCall) {
        // These are exactly the pinned AstBuilder.visitListagg output facts.
        // A different spelling, quotation, argument kind or metadata cannot be
        // silently dropped to make the syntax appear reconstructible.
        if value.name.parts.len() != 1
            || value.name.parts[0].quoted
            || value.name.parts[0].value != "LISTAGG"
            || value.arguments.len() != 5
            || value
                .arguments
                .iter()
                .any(|argument| argument.name.is_some())
            || value.null_treatment.is_some()
            || value.processing_mode.is_some()
        {
            unsupported("LISTAGG facts outside the pinned normalized constructor");
        }
        let separator = listagg_string(&value.arguments[1].value);
        let overflow_error = listagg_boolean(&value.arguments[2].value);
        let filler = listagg_string(&value.arguments[3].value);
        let show_count = listagg_boolean(&value.arguments[4].value);
        let order_by = match &value.order_by {
            Some(order_by) if !order_by.items.is_empty() => order_by,
            _ => unsupported("LISTAGG without its nonempty WITHIN GROUP ordering"),
        };
        if overflow_error && (filler != "..." || show_count) {
            unsupported("LISTAGG ERROR carrying non-default filler/count facts");
        }

        self.identifier(&value.name.parts[0]);
        self.character('(');
        if value.distinct {
            self.text("DISTINCT ");
        }
        self.expression(&value.arguments[0].value);
        self.text(", ");
        self.string(separator);
        if overflow_error {
            self.text(" ON OVERFLOW ERROR");
        } else {
            self.text(" ON OVERFLOW TRUNCATE ");
            self.string(filler);
            self.text(if show_count {
                " WITH COUNT"
            } else {
                " WITHOUT COUNT"
            });
        }
        self.text(") WITHIN GROUP (");
        self.order_by(order_by);
        self.character(')');
        if let Some(filter) = &value.filter {
            self.text(" FILTER (WHERE ");
            self.expression(filter);
            self.character(')');
        }
        if let Some(window) = &value.window {
            self.text(" OVER ");
            self.window(window);
        }
    }
}

fn listagg_string(value: &Expr) -> &str {
    match value {
        Expr::Literal(Literal {
            kind: LiteralKind::String(value),
            ..
        }) => value,
        _ => unsupported("a LISTAGG constructor string slot with another expression kind"),
    }
}
fn listagg_boolean(value: &Expr) -> bool {
    match value {
        Expr::Literal(Literal {
            kind: LiteralKind::Boolean(value),
            ..
        }) => *value,
        _ => unsupported("a LISTAGG constructor Boolean slot with another expression kind"),
    }
}
