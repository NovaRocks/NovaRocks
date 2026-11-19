// SPDX-License-Identifier: Apache-2.0
use super::*;

impl Writer {
    #[recursive::recursive]
    pub(super) fn literal(&mut self, value: &Literal) -> PrintResult {
        match &value.kind {
            LiteralKind::Null => self.text("NULL"),
            LiteralKind::Boolean(value) => self.text(if *value { "TRUE" } else { "FALSE" }),
            LiteralKind::Long { spelling, .. } => self.text(spelling),
            LiteralKind::Double(value) => {
                let value = value.get();
                if value.is_nan() {
                    // No numeric production in the pinned grammar constructs NaN.
                    // CAST or a type constructor would change the typed AST class.
                    return Err(unsupported("a hand-authored NaN DoubleLiteral"));
                }
                if value == f64::INFINITY {
                    self.text("1E9999");
                } else if value == f64::NEG_INFINITY {
                    self.text("-1E9999");
                } else {
                    // A decimal point alone produces DecimalLiteral, not DoubleLiteral.
                    // Rust's shortest round-trip representation preserves signed zero.
                    let spelling = format!("{value:?}");
                    self.text(&spelling);
                    if !spelling.contains('e') && !spelling.contains('E') {
                        self.text("E0");
                    }
                }
            }
            LiteralKind::Decimal(value) => {
                self.text("DECIMAL ");
                self.string(value);
            }
            LiteralKind::String(value) => self.string(value),
            LiteralKind::Binary(bytes) => {
                self.text("X'");
                for byte in bytes {
                    write!(&mut self.output, "{byte:02X}").expect("String formatting cannot fail");
                }
                self.character('\'');
            }
            LiteralKind::Generic(value) => {
                // The raw tag (including its identifier delimiters) is a typed
                // GenericLiteral fact, not a fallback for an unknown node.
                self.text(&value.type_name);
                self.character(' ');
                self.string(&value.value);
            }
            LiteralKind::Interval(value) => {
                self.text("INTERVAL ");
                if value.sign == IntervalSign::Negative {
                    self.text("- ");
                }
                self.string(&value.value);
                self.character(' ');
                self.qualifier(&value.qualifier)?;
            }
        }
        Ok(())
    }

    #[recursive::recursive]
    pub(super) fn data_type(&mut self, value: &TypeName) -> PrintResult {
        match &value.kind {
            TypeKind::Generic(value) => {
                self.identifier(&value.name);
                if !value.arguments.is_empty() {
                    self.character('(');
                    for (index, argument) in value.arguments.iter().enumerate() {
                        if index != 0 {
                            self.text(", ");
                        }
                        self.type_parameter(argument)?;
                    }
                    self.character(')');
                }
            }
            TypeKind::Row(value) => {
                self.text("ROW(");
                for (index, field) in value.fields.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    if let Some(name) = &field.name {
                        self.identifier(name);
                        self.character(' ');
                    }
                    self.data_type(&field.data_type)?;
                }
                self.character(')');
            }
            TypeKind::DateTime(value) => {
                self.text(match value.kind {
                    DateTimeType::Time => "TIME",
                    DateTimeType::Timestamp => "TIMESTAMP",
                });
                if let Some(precision) = &value.precision {
                    self.character('(');
                    self.type_parameter(precision)?;
                    self.character(')');
                }
                if value.with_time_zone {
                    self.text(" WITH TIME ZONE");
                }
            }
            TypeKind::Interval(value) => {
                self.text("INTERVAL ");
                self.qualifier(&value.qualifier)?;
            }
            TypeKind::ExplicitArray(value) => {
                self.data_type(&value.element)?;
                self.text(" ARRAY[");
                self.text(&value.length.spelling);
                self.character(']');
            }
        }
        Ok(())
    }

    #[recursive::recursive]
    fn type_parameter(&mut self, value: &DataTypeParameter) -> PrintResult {
        match value {
            DataTypeParameter::Numeric(value) => self.text(&value.spelling),
            DataTypeParameter::Type(value) => self.data_type(&value.data_type)?,
        }
        Ok(())
    }

    #[recursive::recursive]
    pub(super) fn qualifier(&mut self, value: &IntervalQualifier) -> PrintResult {
        match value {
            IntervalQualifier::Simple(value) => {
                self.qualifier_field(&value.field)?;
                let fraction = match &value.field {
                    IntervalQualifierField::Second {
                        fractional_precision,
                    } => fractional_precision.as_deref(),
                    _ => None,
                };
                match (value.precision.as_deref(), fraction) {
                    (None, None) => {}
                    (Some(leading), fractional) => {
                        self.character('(');
                        self.type_parameter(leading)?;
                        if let Some(fractional) = fractional {
                            self.text(", ");
                            self.type_parameter(fractional)?;
                        }
                        self.character(')');
                    }
                    (None, Some(_)) => {
                        return Err(unsupported(
                            "SECOND fractional precision without leading precision",
                        ));
                    }
                }
            }
            IntervalQualifier::Composite(value) => {
                // The start field cannot carry fractional precision in the grammar.
                if matches!(&value.from, IntervalQualifierField::Second { .. }) {
                    return Err(unsupported("SECOND as a composite interval start"));
                }
                self.qualifier_field(&value.from)?;
                if let Some(precision) = &value.precision {
                    self.character('(');
                    self.type_parameter(precision)?;
                    self.character(')');
                }
                self.text(" TO ");
                self.qualifier_field(&value.to)?;
                if let IntervalQualifierField::Second {
                    fractional_precision: Some(value),
                } = &value.to
                {
                    self.character('(');
                    self.type_parameter(value)?;
                    self.character(')');
                }
            }
        }
        Ok(())
    }
    fn qualifier_field(&mut self, value: &IntervalQualifierField) -> PrintResult {
        self.text(match value {
            IntervalQualifierField::Year => "YEAR",
            IntervalQualifierField::Month => "MONTH",
            IntervalQualifierField::Day => "DAY",
            IntervalQualifierField::Hour => "HOUR",
            IntervalQualifierField::Minute => "MINUTE",
            IntervalQualifierField::Second { .. } => "SECOND",
        });
        Ok(())
    }
}
