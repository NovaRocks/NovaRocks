// SPDX-License-Identifier: Apache-2.0
use super::*;

impl Writer {
    #[recursive::recursive]
    pub(super) fn expression(&mut self, value: &Expr) {
        match value {
            Expr::Identifier(value) => self.identifier(value),
            Expr::Literal(value) => self.literal(value),
            Expr::AllRows(_) => self.character('*'),
            // The grammar writes '?'. Its position is an AstBuilder visit fact,
            // not a printable numeric suffix. No position is changed here.
            Expr::Parameter(_) => self.character('?'),
            Expr::Unary(value) => {
                self.character('(');
                self.text(match value.operator {
                    UnaryOperator::Not => "NOT ",
                    UnaryOperator::Plus => "+ ",
                    UnaryOperator::Minus => "- ",
                    UnaryOperator::Legacy(_) => legacy("unary operator"),
                });
                // In particular, -(1) must not become a signed LongLiteral.
                self.parenthesized_expression(&value.expression);
                self.character(')');
            }
            Expr::Binary(value) => {
                self.character('(');
                self.expression(&value.left);
                self.text(match value.operator {
                    BinaryOperator::Add => " + ",
                    BinaryOperator::Subtract => " - ",
                    BinaryOperator::Multiply => " * ",
                    BinaryOperator::Divide => " / ",
                    BinaryOperator::Modulo => " % ",
                    BinaryOperator::Legacy(_) => legacy("binary operator"),
                });
                self.expression(&value.right);
                self.character(')');
            }
            Expr::Logical(value) => {
                self.character('(');
                for (index, term) in value.terms.iter().enumerate() {
                    if index != 0 {
                        self.text(match value.operator {
                            LogicalOperator::And => " AND ",
                            LogicalOperator::Or => " OR ",
                        });
                    }
                    // A child Logical keeps its own frame; never flatten it.
                    self.expression(term);
                }
                self.character(')');
            }
            Expr::Predicated(value) => {
                self.character('(');
                self.expression(&value.value);
                self.character(' ');
                self.predicate(&value.predicate);
                self.character(')');
            }
            Expr::InList(value) => {
                self.character('(');
                self.expressions(&value.values);
                self.character(')');
            }
            Expr::Cast(value) => {
                self.text(if value.safe { "TRY_CAST(" } else { "CAST(" });
                self.expression(&value.expression);
                self.text(" AS ");
                self.data_type(&value.data_type);
                self.character(')');
            }
            Expr::FunctionCall(value) => self.call(value),
            Expr::If(value) => {
                self.text("IF(");
                self.expression(&value.condition);
                self.text(", ");
                self.expression(&value.true_value);
                if let Some(value) = &value.false_value {
                    self.text(", ");
                    self.expression(value);
                }
                self.character(')');
            }
            Expr::NullIf(value) => {
                self.text("NULLIF(");
                self.expression(&value.first);
                self.text(", ");
                self.expression(&value.second);
                self.character(')');
            }
            Expr::Coalesce(value) => {
                self.text("COALESCE(");
                self.expressions(&value.operands);
                self.character(')');
            }
            Expr::Try(value) => {
                self.text("TRY(");
                self.expression(&value.as_ref().inner);
                self.character(')');
            }
            Expr::Format(value) => {
                self.text("FORMAT(");
                self.expressions(&value.arguments);
                self.character(')');
            }
            Expr::SearchedCase(value) => {
                self.text("CASE");
                self.when_clauses(&value.when_clauses);
                self.case_default(value.default_value.as_ref());
            }
            Expr::SimpleCase(value) => {
                self.text("CASE ");
                self.expression(&value.operand);
                self.when_clauses(&value.when_clauses);
                self.case_default(value.default_value.as_ref());
            }
            Expr::Subquery(value) => {
                self.character('(');
                self.query(&value.query);
                self.character(')');
            }
            Expr::Exists(value) => {
                self.text("EXISTS ");
                self.subquery_argument(&value.subquery);
            }
            Expr::Unique(value) => {
                self.text("UNIQUE ");
                self.subquery_argument(&value.subquery);
            }
            Expr::Row(value) => {
                self.text("ROW(");
                for (index, field) in value.fields.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    self.expression(&field.expression);
                    if let Some(name) = &field.name {
                        self.text(" AS ");
                        self.identifier(name);
                    }
                }
                self.character(')');
            }
            Expr::Array(value) => {
                self.text("ARRAY[");
                self.expressions(&value.values);
                self.character(']');
            }
            Expr::Lambda(value) => {
                self.character('(');
                self.character('(');
                for (index, argument) in value.arguments.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    self.identifier(&argument.name);
                }
                self.text(") -> ");
                self.expression(&value.body);
                self.character(')');
            }
            Expr::Subscript(value) => {
                self.parenthesized_expression(&value.base);
                self.character('[');
                self.expression(&value.index);
                self.character(']');
            }
            Expr::ArrayWildcardSubscript(value) => {
                self.parenthesized_expression(&value.base);
                self.text("[*]");
            }
            Expr::Dereference(value) => {
                if value.field.is_none() {
                    // Function label.* is a real primary production. A computed
                    // base cannot be silently reclassified as a qualified name.
                    match &value.base {
                        Expr::Identifier(name) => self.identifier(name),
                        _ => unsupported("a computed-base wildcard dereference"),
                    }
                    self.text(".*");
                } else {
                    self.parenthesized_expression(&value.base);
                    self.character('.');
                    self.identifier(value.field.as_ref().expect("checked field"));
                }
            }
            Expr::CurrentCatalog(_) => self.text("CURRENT_CATALOG"),
            Expr::CurrentDate(_) => self.text("CURRENT_DATE"),
            Expr::CurrentPath(_) => self.text("CURRENT_PATH"),
            Expr::CurrentSchema(_) => self.text("CURRENT_SCHEMA"),
            Expr::CurrentUser(_) => self.text("CURRENT_USER"),
            Expr::CurrentTime(value) => self.current_time("CURRENT_TIME", value.precision),
            Expr::CurrentTimestamp(value) => {
                self.current_time("CURRENT_TIMESTAMP", value.precision)
            }
            Expr::LocalTime(value) => self.current_time("LOCALTIME", value.precision),
            Expr::LocalTimestamp(value) => self.current_time("LOCALTIMESTAMP", value.precision),
            Expr::WindowOperation(value) => {
                self.identifier(&value.name);
                self.text(" OVER ");
                self.window(&value.window);
            }
            Expr::LegacyParenthesized(_) => legacy("parenthesized expression"),
            Expr::LegacyInterval(_) => legacy("interval expression"),
            Expr::JsonConstructor(_)
            | Expr::JsonExists(_)
            | Expr::JsonValue(_)
            | Expr::JsonQuery(_)
            | Expr::JsonSerialize(_)
            | Expr::JsonObject(_)
            | Expr::JsonArray(_) => self.json_expression(value),
            Expr::Extract(_)
            | Expr::AtLocal(_)
            | Expr::AtTimeZone(_)
            | Expr::GroupingOperation(_)
            | Expr::Trim(_)
            | Expr::Overlay(_)
            | Expr::IntervalValueExpression(_)
            | Expr::MethodCall(_)
            | Expr::StaticMethodCall(_) => self.special_expression(value),
        }
    }

    #[recursive::recursive]
    pub(super) fn predicate(&mut self, value: &Predicate) {
        match value {
            Predicate::Comparison(value) => {
                self.comparison(value.operator);
                self.character(' ');
                self.expression(&value.right);
            }
            Predicate::DistinctFrom(value) => {
                self.text(if value.negated {
                    "IS NOT DISTINCT FROM "
                } else {
                    "IS DISTINCT FROM "
                });
                self.expression(&value.right);
            }
            Predicate::Between(value) => {
                self.text(if value.negated {
                    "NOT BETWEEN "
                } else {
                    "BETWEEN "
                });
                if let Some(symmetry) = value.symmetry {
                    self.text(match symmetry {
                        BetweenSymmetry::Symmetric => "SYMMETRIC ",
                        BetweenSymmetry::Asymmetric => "ASYMMETRIC ",
                    });
                }
                self.expression(&value.min);
                self.text(" AND ");
                self.expression(&value.max);
            }
            Predicate::BooleanTest(value) => {
                self.text(if value.negated { "IS NOT " } else { "IS " });
                self.text(match value.truth_value {
                    TruthValue::True => "TRUE",
                    TruthValue::False => "FALSE",
                    TruthValue::Unknown => "UNKNOWN",
                });
            }
            Predicate::IsNull(value) => self.text(if value.negated {
                "IS NOT NULL"
            } else {
                "IS NULL"
            }),
            Predicate::Like(value) => {
                self.text(if value.negated { "NOT LIKE " } else { "LIKE " });
                self.expression(&value.pattern);
                if let Some(escape) = &value.escape {
                    self.text(" ESCAPE ");
                    self.expression(escape);
                }
            }
            Predicate::In(value) => {
                self.text(if value.negated { "NOT IN " } else { "IN " });
                match &value.value_list {
                    Expr::InList(_) | Expr::Subquery(_) => self.expression(&value.value_list),
                    _ => unsupported("an IN predicate without its list/subquery wrapper"),
                }
            }
            Predicate::Overlaps(value) => {
                self.text("OVERLAPS ");
                self.expression(&value.right);
            }
            Predicate::QuantifiedComparison(value) => {
                self.comparison(value.operator);
                self.character(' ');
                self.text(match value.quantifier {
                    ComparisonQuantifier::All => "ALL ",
                    ComparisonQuantifier::Any => "ANY ",
                    ComparisonQuantifier::Some => "SOME ",
                });
                self.subquery_argument(&value.subquery);
            }
            Predicate::Match(value) => {
                self.text("MATCH ");
                if value.unique {
                    self.text("UNIQUE ");
                }
                self.text(match value.match_type {
                    MatchType::Simple => "SIMPLE ",
                    MatchType::Partial => "PARTIAL ",
                    MatchType::Full => "FULL ",
                });
                self.subquery_argument(&value.subquery);
            }
        }
    }
    fn comparison(&mut self, operator: ComparisonOperator) {
        self.text(match operator {
            ComparisonOperator::Equal => "=",
            ComparisonOperator::NotEqual => "<>",
            ComparisonOperator::LessThan => "<",
            ComparisonOperator::LessThanOrEqual => "<=",
            ComparisonOperator::GreaterThan => ">",
            ComparisonOperator::GreaterThanOrEqual => ">=",
        });
    }
    #[recursive::recursive]
    fn subquery_argument(&mut self, value: &Expr) {
        match value {
            Expr::Subquery(_) => self.expression(value),
            _ => unsupported("a missing typed subquery expression wrapper"),
        }
    }
    #[recursive::recursive]
    fn when_clauses(&mut self, values: &[WhenClause]) {
        for value in values {
            self.text(" WHEN ");
            match &value.match_value {
                WhenMatch::Operand(value) => self.expression(value),
                WhenMatch::Partial(value) => self.predicate(value),
            }
            self.text(" THEN ");
            self.expression(&value.result);
        }
    }
    #[recursive::recursive]
    fn case_default(&mut self, value: Option<&Expr>) {
        if let Some(value) = value {
            self.text(" ELSE ");
            self.expression(value);
        }
        self.text(" END");
    }
    fn current_time(&mut self, name: &str, precision: Option<i32>) {
        self.text(name);
        if let Some(precision) = precision {
            write!(&mut self.output, "({precision})").expect("String formatting cannot fail");
        }
    }
    #[recursive::recursive]
    fn call(&mut self, value: &FunctionCall) {
        if value.name.parts.len() == 1
            && !value.name.parts[0].quoted
            && value.name.parts[0].value.eq_ignore_ascii_case("LISTAGG")
        {
            return self.listagg(value);
        }
        if let Some(mode) = &value.processing_mode {
            self.text(match mode.mode {
                ProcessingModeKind::Running => "RUNNING ",
                ProcessingModeKind::Final => "FINAL ",
            });
        }
        self.name(&value.name);
        self.character('(');
        if value.distinct {
            self.text("DISTINCT ");
        }
        for (index, argument) in value.arguments.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            if let Some(name) = &argument.name {
                self.identifier(name);
                self.text(" => ");
            }
            self.expression(&argument.value);
        }
        if let Some(order_by) = &value.order_by {
            self.character(' ');
            self.order_by(order_by);
        }
        self.character(')');
        if let Some(filter) = &value.filter {
            self.text(" FILTER (WHERE ");
            self.expression(filter);
            self.character(')');
        }
        if let Some(treatment) = value.null_treatment {
            if value.window.is_none() {
                unsupported("null treatment without its window");
            }
            self.text(match treatment {
                NullTreatment::IgnoreNulls => " IGNORE NULLS",
                NullTreatment::RespectNulls => " RESPECT NULLS",
            });
        }
        if let Some(window) = &value.window {
            self.text(" OVER ");
            self.window(window);
        }
    }
}
