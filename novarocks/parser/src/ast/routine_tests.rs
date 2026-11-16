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

//! Typed routine traversal, equivalence, and structural validation contracts.
//! These are programmatic AST fixtures, not proof of parser admission.

use super::*;
use crate::build;
use std::collections::BTreeSet;

fn span() -> Span {
    Span::new(3, 9)
}
fn ident(name: &str) -> Ident {
    build::ident_at(name, None, span())
}
fn expr(name: &str) -> Expr {
    Expr::Identifier(ident(name))
}
fn ty(name: &str) -> TypeName {
    build::simple_type(name)
}
fn ret(name: &str) -> ControlStatement {
    ControlStatement::Return(ReturnStatement {
        value: expr(name),
        span: span(),
    })
}
fn declaration(name: &str, type_name: &str, default: Option<&str>) -> VariableDeclaration {
    VariableDeclaration {
        names: vec![ident(name)],
        data_type: ty(type_name),
        default_value: default.map(expr),
        span: span(),
    }
}

fn controls() -> Vec<ControlStatement> {
    vec![
        ret("ret_expr"),
        ControlStatement::Assignment(AssignmentStatement {
            target: ident("assign_target"),
            value: expr("assign_value"),
            span: span(),
        }),
        ControlStatement::Case(CaseStatement {
            expression: Some(expr("case_operand")),
            when_clauses: vec![CaseStatementWhenClause {
                expression: expr("case_when"),
                statements: vec![ret("case_body")],
                span: span(),
            }],
            else_clause: Some(ElseClause {
                statements: vec![ret("case_else")],
                span: span(),
            }),
            span: span(),
        }),
        ControlStatement::If(IfStatement {
            expression: expr("if_condition"),
            statements: vec![ret("if_then")],
            else_if_clauses: vec![ElseIfClause {
                expression: expr("elseif_condition"),
                statements: vec![ret("elseif_body")],
                span: span(),
            }],
            else_clause: Some(ElseClause {
                statements: vec![ret("if_else")],
                span: span(),
            }),
            span: span(),
        }),
        ControlStatement::Iterate(IterateStatement {
            label: ident("iterate_label"),
            span: span(),
        }),
        ControlStatement::Leave(LeaveStatement {
            label: ident("leave_label"),
            span: span(),
        }),
        ControlStatement::VariableDeclaration(declaration(
            "var_name",
            "var_type",
            Some("var_default"),
        )),
        ControlStatement::Compound(CompoundStatement {
            variable_declarations: vec![declaration("subdecl_name", "subdecl_type", None)],
            statements: vec![ret("compound_return")],
            span: span(),
        }),
        ControlStatement::Loop(LoopStatement {
            label: Some(ident("loop_label")),
            statements: vec![ret("loop_return")],
            span: span(),
        }),
        ControlStatement::While(WhileStatement {
            label: Some(ident("while_label")),
            expression: expr("while_condition"),
            statements: vec![ret("while_return")],
            span: span(),
        }),
        ControlStatement::Repeat(RepeatStatement {
            label: None,
            statements: vec![ret("repeat_return")],
            condition: expr("repeat_condition"),
            span: span(),
        }),
    ]
}

fn characteristics() -> Vec<RoutineCharacteristic> {
    vec![
        RoutineCharacteristic::Language {
            language: ident("language"),
            span: span(),
        },
        RoutineCharacteristic::Deterministic {
            deterministic: true,
            span: span(),
        },
        RoutineCharacteristic::NullInput {
            called_on_null: true,
            span: span(),
        },
        RoutineCharacteristic::Security {
            security: RoutineSecurity::Definer,
            span: span(),
        },
        RoutineCharacteristic::Comment {
            comment: "opaque comment; ${not_an_identifier}".into(),
            span: span(),
        },
        RoutineCharacteristic::Properties {
            properties: vec![
                RoutineProperty {
                    name: ident("property_value"),
                    value: Some(expr("property_expr")),
                    span: span(),
                },
                RoutineProperty {
                    name: ident("property_default"),
                    value: None,
                    span: span(),
                },
            ],
            span: span(),
        },
    ]
}

fn function(body: RoutineBody) -> FunctionSpecification {
    FunctionSpecification {
        name: build::object_name(vec![ident("function_name")]).unwrap(),
        parameters: vec![
            FunctionParameter {
                name: Some(ident("parameter_name")),
                data_type: ty("parameter_type"),
                span: span(),
            },
            FunctionParameter {
                name: None,
                data_type: ty("anonymous_type"),
                span: span(),
            },
        ],
        returns: ty("return_type"),
        characteristics: characteristics(),
        body,
        span: span(),
    }
}

fn fixture() -> FunctionSpecification {
    let mut decl = declaration("decl_a", "decl_type", Some("decl_default"));
    decl.names.push(ident("decl_b"));
    function(RoutineBody::Sql(ControlStatement::Compound(
        CompoundStatement {
            variable_declarations: vec![decl],
            statements: controls(),
            span: span(),
        },
    )))
}

fn expected_identifiers() -> Vec<String> {
    [
        "function_name",
        "parameter_name",
        "parameter_type",
        "anonymous_type",
        "return_type",
        "language",
        "property_value",
        "property_expr",
        "property_default",
        "decl_a",
        "decl_b",
        "decl_type",
        "decl_default",
        "ret_expr",
        "assign_target",
        "assign_value",
        "case_operand",
        "case_when",
        "case_body",
        "case_else",
        "if_condition",
        "if_then",
        "elseif_condition",
        "elseif_body",
        "if_else",
        "iterate_label",
        "leave_label",
        "var_name",
        "var_type",
        "var_default",
        "subdecl_name",
        "subdecl_type",
        "compound_return",
        "loop_label",
        "loop_return",
        "while_label",
        "while_condition",
        "while_return",
        "repeat_return",
        "repeat_condition",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[derive(Default)]
struct Trace {
    identifiers: Vec<String>,
    hooks: BTreeSet<&'static str>,
}
impl Visit for Trace {
    fn visit_ident(&mut self, node: &Ident) {
        self.identifiers.push(node.value.clone());
    }
    fn visit_function_specification(&mut self, node: &FunctionSpecification) {
        self.hooks.insert("function_specification");
        walk_function_specification(self, node);
    }
    fn visit_function_parameter(&mut self, node: &FunctionParameter) {
        self.hooks.insert("function_parameter");
        walk_function_parameter(self, node);
    }
    fn visit_routine_property(&mut self, node: &RoutineProperty) {
        self.hooks.insert("routine_property");
        walk_routine_property(self, node);
    }
    fn visit_return_statement(&mut self, node: &ReturnStatement) {
        self.hooks.insert("return_statement");
        walk_return_statement(self, node);
    }
    fn visit_assignment_statement(&mut self, node: &AssignmentStatement) {
        self.hooks.insert("assignment_statement");
        walk_assignment_statement(self, node);
    }
    fn visit_case_statement(&mut self, node: &CaseStatement) {
        self.hooks.insert("case_statement");
        walk_case_statement(self, node);
    }
    fn visit_case_statement_when_clause(&mut self, node: &CaseStatementWhenClause) {
        self.hooks.insert("case_statement_when_clause");
        walk_case_statement_when_clause(self, node);
    }
    fn visit_if_statement(&mut self, node: &IfStatement) {
        self.hooks.insert("if_statement");
        walk_if_statement(self, node);
    }
    fn visit_else_if_clause(&mut self, node: &ElseIfClause) {
        self.hooks.insert("else_if_clause");
        walk_else_if_clause(self, node);
    }
    fn visit_else_clause(&mut self, node: &ElseClause) {
        self.hooks.insert("else_clause");
        walk_else_clause(self, node);
    }
    fn visit_iterate_statement(&mut self, node: &IterateStatement) {
        self.hooks.insert("iterate_statement");
        walk_iterate_statement(self, node);
    }
    fn visit_leave_statement(&mut self, node: &LeaveStatement) {
        self.hooks.insert("leave_statement");
        walk_leave_statement(self, node);
    }
    fn visit_variable_declaration(&mut self, node: &VariableDeclaration) {
        self.hooks.insert("variable_declaration");
        walk_variable_declaration(self, node);
    }
    fn visit_compound_statement(&mut self, node: &CompoundStatement) {
        self.hooks.insert("compound_statement");
        walk_compound_statement(self, node);
    }
    fn visit_loop_statement(&mut self, node: &LoopStatement) {
        self.hooks.insert("loop_statement");
        walk_loop_statement(self, node);
    }
    fn visit_while_statement(&mut self, node: &WhileStatement) {
        self.hooks.insert("while_statement");
        walk_while_statement(self, node);
    }
    fn visit_repeat_statement(&mut self, node: &RepeatStatement) {
        self.hooks.insert("repeat_statement");
        walk_repeat_statement(self, node);
    }
    fn visit_routine_body(&mut self, node: &RoutineBody) {
        self.hooks.insert("routine_body");
        walk_routine_body(self, node);
    }
    fn visit_routine_characteristic(&mut self, node: &RoutineCharacteristic) {
        self.hooks.insert("routine_characteristic");
        walk_routine_characteristic(self, node);
    }
    fn visit_control_statement(&mut self, node: &ControlStatement) {
        self.hooks.insert("control_statement");
        walk_control_statement(self, node);
    }
}

struct Respan;
impl Fold for Respan {
    fn fold_ident(&mut self, mut node: Ident) -> Ident {
        node.span = Span::new(100, 110);
        node
    }
    fn fold_object_name(&mut self, node: ObjectName) -> ObjectName {
        let mut node = super::super::fold_object_name(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_type_name(&mut self, node: TypeName) -> TypeName {
        let mut node = super::super::fold_type_name(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_function_specification(
        &mut self,
        node: FunctionSpecification,
    ) -> FunctionSpecification {
        let mut node = fold_function_specification(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_function_parameter(&mut self, node: FunctionParameter) -> FunctionParameter {
        let mut node = fold_function_parameter(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_routine_property(&mut self, node: RoutineProperty) -> RoutineProperty {
        let mut node = fold_routine_property(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_return_statement(&mut self, node: ReturnStatement) -> ReturnStatement {
        let mut node = fold_return_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_assignment_statement(&mut self, node: AssignmentStatement) -> AssignmentStatement {
        let mut node = fold_assignment_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_case_statement(&mut self, node: CaseStatement) -> CaseStatement {
        let mut node = fold_case_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_case_statement_when_clause(
        &mut self,
        node: CaseStatementWhenClause,
    ) -> CaseStatementWhenClause {
        let mut node = fold_case_statement_when_clause(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_if_statement(&mut self, node: IfStatement) -> IfStatement {
        let mut node = fold_if_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_else_if_clause(&mut self, node: ElseIfClause) -> ElseIfClause {
        let mut node = fold_else_if_clause(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_else_clause(&mut self, node: ElseClause) -> ElseClause {
        let mut node = fold_else_clause(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_iterate_statement(&mut self, node: IterateStatement) -> IterateStatement {
        let mut node = fold_iterate_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_leave_statement(&mut self, node: LeaveStatement) -> LeaveStatement {
        let mut node = fold_leave_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_variable_declaration(&mut self, node: VariableDeclaration) -> VariableDeclaration {
        let mut node = fold_variable_declaration(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_compound_statement(&mut self, node: CompoundStatement) -> CompoundStatement {
        let mut node = fold_compound_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_loop_statement(&mut self, node: LoopStatement) -> LoopStatement {
        let mut node = fold_loop_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_while_statement(&mut self, node: WhileStatement) -> WhileStatement {
        let mut node = fold_while_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_repeat_statement(&mut self, node: RepeatStatement) -> RepeatStatement {
        let mut node = fold_repeat_statement(self, node);
        node.span = Span::new(100, 110);
        node
    }
    fn fold_routine_body(&mut self, node: RoutineBody) -> RoutineBody {
        match fold_routine_body(self, node) {
            RoutineBody::External { definition, .. } => RoutineBody::External {
                definition,
                span: Span::new(100, 110),
            },
            other => other,
        }
    }
    fn fold_routine_characteristic(
        &mut self,
        node: RoutineCharacteristic,
    ) -> RoutineCharacteristic {
        let mut node = fold_routine_characteristic(self, node);
        match &mut node {
            RoutineCharacteristic::Language { span, .. }
            | RoutineCharacteristic::Deterministic { span, .. }
            | RoutineCharacteristic::NullInput { span, .. }
            | RoutineCharacteristic::Security { span, .. }
            | RoutineCharacteristic::Comment { span, .. }
            | RoutineCharacteristic::Properties { span, .. } => *span = Span::new(100, 110),
        }
        node
    }
}

struct Rename;
impl Fold for Rename {
    fn fold_ident(&mut self, mut node: Ident) -> Ident {
        node.value = format!("renamed_{}", node.value);
        node
    }
}

#[test]
fn routine_visit_reaches_actual_children_in_declared_source_field_order() {
    let mut trace = Trace::default();
    trace.visit_function_specification(&fixture());
    assert_eq!(trace.identifiers, expected_identifiers());
    assert!(
        trace.hooks.contains("function_specification"),
        "function_specification callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("function_parameter"),
        "function_parameter callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("routine_property"),
        "routine_property callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("return_statement"),
        "return_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("assignment_statement"),
        "assignment_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("case_statement"),
        "case_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("case_statement_when_clause"),
        "case_statement_when_clause callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("if_statement"),
        "if_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("else_if_clause"),
        "else_if_clause callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("else_clause"),
        "else_clause callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("iterate_statement"),
        "iterate_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("leave_statement"),
        "leave_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("variable_declaration"),
        "variable_declaration callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("compound_statement"),
        "compound_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("loop_statement"),
        "loop_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("while_statement"),
        "while_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("repeat_statement"),
        "repeat_statement callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("routine_body"),
        "routine_body callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("routine_characteristic"),
        "routine_characteristic callback must be dispatched"
    );
    assert!(
        trace.hooks.contains("control_statement"),
        "control_statement callback must be dispatched"
    );
}

#[test]
fn routine_fold_rebuilds_every_actual_child_and_preserves_scalar_payloads() {
    let original = fixture();
    let folded = Rename.fold_function_specification(original.clone());
    let mut trace = Trace::default();
    trace.visit_function_specification(&folded);
    assert_eq!(
        trace.identifiers,
        expected_identifiers()
            .iter()
            .map(|name| format!("renamed_{name}"))
            .collect::<Vec<_>>()
    );
    let RoutineCharacteristic::Comment { comment, .. } = &folded.characteristics[4] else {
        panic!("comment");
    };
    assert_eq!(comment, "opaque comment; ${not_an_identifier}");
    let RoutineCharacteristic::Properties { properties, .. } = &folded.characteristics[5] else {
        panic!("properties");
    };
    assert!(properties[1].value.is_none(), "DEFAULT stays None");
    assert_eq!(folded.span, original.span);
    assert!(!folded.syntax_eq(&original));
}

#[test]
fn routine_all_control_variants_ignore_spans_but_preserve_variant_identity() {
    let nodes = controls();
    for (index, node) in nodes.iter().enumerate() {
        let shifted = Respan.fold_control_statement(node.clone());
        assert_ne!(node, &shifted);
        assert!(node.syntax_eq(&shifted));
        assert_eq!(shifted.span(), Span::new(100, 110));
        for (other_index, other) in nodes.iter().enumerate() {
            if index != other_index {
                assert!(!node.syntax_eq(other));
            }
        }
    }
    let original = fixture();
    let shifted = Respan.fold_function_specification(original.clone());
    assert_ne!(original, shifted);
    assert!(original.syntax_eq(&shifted));
}

#[test]
fn routine_control_conditions_labels_options_and_statement_order_are_syntax() {
    for original in controls() {
        let mut changed = original.clone();
        match &mut changed {
            ControlStatement::Return(node) => node.value = expr("different_return"),
            ControlStatement::Assignment(node) => node.target = ident("different_target"),
            ControlStatement::Case(node) => node.expression = None,
            ControlStatement::If(node) => node.else_clause = None,
            ControlStatement::Iterate(node) => node.label = ident("different_iterate"),
            ControlStatement::Leave(node) => node.label = ident("different_leave"),
            ControlStatement::VariableDeclaration(node) => node.default_value = None,
            ControlStatement::Compound(node) => node.variable_declarations.clear(),
            ControlStatement::Loop(node) => node.label = None,
            ControlStatement::While(node) => node.expression = expr("different_while"),
            ControlStatement::Repeat(node) => node.condition = expr("different_until"),
        }
        assert!(
            !original.syntax_eq(&changed),
            "changed syntax must not be ignored"
        );
    }
    let original = CompoundStatement {
        variable_declarations: Vec::new(),
        statements: vec![ret("first"), ret("second")],
        span: span(),
    };
    let mut reversed = original.clone();
    reversed.statements.reverse();
    assert!(!original.syntax_eq(&reversed));
    let searched = CaseStatement {
        expression: None,
        when_clauses: vec![CaseStatementWhenClause {
            expression: expr("predicate"),
            statements: vec![ret("result")],
            span: span(),
        }],
        else_clause: None,
        span: span(),
    };
    let mut explicit_else = searched.clone();
    explicit_else.else_clause = Some(ElseClause {
        statements: Vec::new(),
        span: span(),
    });
    assert!(!searched.syntax_eq(&explicit_else));
}

#[test]
fn routine_characteristic_values_default_properties_order_and_duplicates_are_syntax() {
    for original in characteristics() {
        let shifted = Respan.fold_routine_characteristic(original.clone());
        assert!(original.syntax_eq(&shifted));
        let mut changed = original.clone();
        match &mut changed {
            RoutineCharacteristic::Language { language, .. } => *language = ident("other_language"),
            RoutineCharacteristic::Deterministic { deterministic, .. } => {
                *deterministic = !*deterministic
            }
            RoutineCharacteristic::NullInput { called_on_null, .. } => {
                *called_on_null = !*called_on_null
            }
            RoutineCharacteristic::Security { security, .. } => {
                *security = RoutineSecurity::Invoker
            }
            RoutineCharacteristic::Comment { comment, .. } => comment.push('!'),
            RoutineCharacteristic::Properties { properties, .. } => {
                properties[1].value = Some(expr("DEFAULT"))
            }
        }
        assert!(!original.syntax_eq(&changed));
    }
    let original = fixture();
    let mut changed = original.clone();
    changed.characteristics.reverse();
    assert!(!original.syntax_eq(&changed));
    changed = original.clone();
    changed
        .characteristics
        .push(original.characteristics[1].clone());
    assert!(
        !original.syntax_eq(&changed),
        "repeated characteristics remain visible to validation"
    );
}

#[test]
fn routine_signature_preserves_parameter_names_types_order_and_return_type() {
    let original = fixture();
    let mut changed = original.clone();
    changed.parameters[0].name = None;
    assert!(!original.syntax_eq(&changed));
    changed = original.clone();
    changed.parameters[1].data_type = ty("other_type");
    assert!(!original.syntax_eq(&changed));
    changed = original.clone();
    changed.parameters.reverse();
    assert!(!original.syntax_eq(&changed));
    changed = original.clone();
    changed.returns = ty("other_return");
    assert!(!original.syntax_eq(&changed));
    changed = original.clone();
    changed.name.parts[0] = build::ident("function_name", Some('"'));
    assert!(
        !original.syntax_eq(&changed),
        "quoted naming facts are retained"
    );
}

#[test]
fn routine_external_definition_is_exact_opaque_content_and_not_expression_children() {
    let definition = "  opaque();\r\nRETURN not_sql; ${x}\n\n";
    let original = function(RoutineBody::External {
        definition: definition.into(),
        span: span(),
    });
    let shifted = Respan.fold_function_specification(original.clone());
    assert!(original.syntax_eq(&shifted));
    let renamed = Rename.fold_function_specification(original.clone());
    let RoutineBody::External {
        definition: after, ..
    } = &renamed.body
    else {
        panic!("external");
    };
    assert_eq!(after, definition);
    let mut trace = Trace::default();
    trace.visit_function_specification(&original);
    assert!(!trace.hooks.contains("control_statement"));
    assert!(
        !trace
            .identifiers
            .iter()
            .any(|name| name == "not_sql" || name == "x")
    );
    let mut changed = original.clone();
    changed.body = RoutineBody::External {
        definition: definition.trim().into(),
        span: span(),
    };
    assert!(
        !original.syntax_eq(&changed),
        "external whitespace is content"
    );
    changed.body = RoutineBody::Sql(ret("not_sql"));
    assert!(!original.syntax_eq(&changed));
}

fn query_with_functions(functions: Vec<FunctionSpecification>) -> crate::ast::Query {
    let mut statements = crate::parse("SELECT 1").unwrap();
    let crate::ast::Statement::Query(mut query) = statements.remove(0) else {
        panic!("query");
    };
    query.functions = functions;
    query
}

#[test]
fn query_function_slot_participates_in_real_walk_fold_and_syntax_equality() {
    let original = query_with_functions(vec![fixture()]);
    let mut trace = Trace::default();
    trace.visit_query(&original);
    assert_eq!(trace.identifiers, expected_identifiers());
    let renamed = Rename.fold_query(original.clone());
    let mut trace = Trace::default();
    trace.visit_query(&renamed);
    assert_eq!(
        trace.identifiers,
        expected_identifiers()
            .iter()
            .map(|name| format!("renamed_{name}"))
            .collect::<Vec<_>>()
    );
    assert!(!original.syntax_eq(&renamed));
    let shifted = Respan.fold_query(original.clone());
    assert!(original.syntax_eq(&shifted));
    let mut changed = original.clone();
    changed.functions.clear();
    assert!(
        !original.syntax_eq(&changed),
        "an inline declaration is part of query syntax"
    );
    let mut second = fixture();
    second.name.parts[0] = ident("other_function");
    let ordered = query_with_functions(vec![fixture(), second]);
    let mut reversed = ordered.clone();
    reversed.functions.reverse();
    assert!(
        !ordered.syntax_eq(&reversed),
        "function source order is retained"
    );
}

fn malformed_case_expression() -> Expr {
    let mut expression = build::case_when(vec![(expr("condition"), expr("result"))], None).unwrap();
    let Expr::SearchedCase(case) = &mut expression else {
        panic!("CASE");
    };
    case.when_clauses.clear();
    case.span = Span::new(700, 711);
    expression
}

#[test]
fn statement_validation_reaches_nested_routine_body_and_property_expressions() {
    use crate::{StructuralViolation, ValidateError};
    let expected = ValidateError::InvalidStructure {
        violation: StructuralViolation::EmptyCaseBranches,
        span: Span::new(700, 711),
    };
    let valid = query_with_functions(vec![function(RoutineBody::Sql(ret("valid")))]);
    crate::ast::validate_statement(&crate::ast::Statement::Query(valid)).unwrap();
    let body = RoutineBody::Sql(ControlStatement::Compound(CompoundStatement {
        variable_declarations: Vec::new(),
        statements: vec![ControlStatement::While(WhileStatement {
            label: None,
            expression: expr("valid_condition"),
            statements: vec![ControlStatement::Return(ReturnStatement {
                value: malformed_case_expression(),
                span: span(),
            })],
            span: span(),
        })],
        span: span(),
    }));
    let query = query_with_functions(vec![function(body)]);
    assert_eq!(
        crate::ast::validate_statement(&crate::ast::Statement::Query(query)).unwrap_err(),
        expected,
        "the validator must traverse Query.functions, compound, while, and RETURN"
    );
    let mut function = function(RoutineBody::Sql(ret("valid")));
    function.characteristics = vec![RoutineCharacteristic::Properties {
        properties: vec![RoutineProperty {
            name: ident("value_property"),
            value: Some(malformed_case_expression()),
            span: span(),
        }],
        span: span(),
    }];
    let query = query_with_functions(vec![function]);
    assert_eq!(
        crate::ast::validate_statement(&crate::ast::Statement::Query(query)).unwrap_err(),
        expected,
        "routine property expressions are not a validation blind spot"
    );
}
