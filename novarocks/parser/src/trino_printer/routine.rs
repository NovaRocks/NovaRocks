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

use super::*;

impl Writer {
    #[recursive::recursive]
    pub(super) fn function_specification(&mut self, value: &FunctionSpecification) {
        self.text("FUNCTION ");
        self.name(&value.name);
        self.character('(');
        for (index, parameter) in value.parameters.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            if let Some(name) = &parameter.name {
                self.identifier(name);
                self.character(' ');
            } else if matches!(
                &parameter.data_type.kind,
                TypeKind::Interval(IntervalDataType {
                    qualifier: IntervalQualifier::Simple(_),
                    ..
                })
            ) {
                unsupported(
                    "an anonymous simple interval parameter ambiguous with its optional name",
                );
            }
            self.data_type(&parameter.data_type);
        }
        self.text(") RETURNS ");
        self.data_type(&value.returns);
        for characteristic in &value.characteristics {
            self.character(' ');
            self.routine_characteristic(characteristic);
        }
        self.character(' ');
        match &value.body {
            RoutineBody::Sql(statement) => {
                if !matches!(
                    statement,
                    ControlStatement::Return(_) | ControlStatement::Compound(_)
                ) {
                    unsupported("a function SQL body not beginning with RETURN or BEGIN");
                }
                self.routine_control_statement(statement);
            }
            RoutineBody::External { definition, .. } => {
                // The grammar has one fixed, non-greedy delimiter. A trailing
                // dollar would also overlap the closing delimiter's first byte.
                if definition.contains("$$") || definition.ends_with('$') {
                    unsupported(
                        "an external definition not representable between dollar delimiters",
                    );
                }
                self.text("AS $$\n");
                self.text(definition);
                self.text("$$");
            }
        }
    }

    fn routine_characteristic(&mut self, value: &RoutineCharacteristic) {
        match value {
            RoutineCharacteristic::Language { language, .. } => {
                self.text("LANGUAGE ");
                self.identifier(language);
            }
            RoutineCharacteristic::Deterministic { deterministic, .. } => {
                self.text(if *deterministic {
                    "DETERMINISTIC"
                } else {
                    "NOT DETERMINISTIC"
                });
            }
            RoutineCharacteristic::NullInput { called_on_null, .. } => {
                self.text(if *called_on_null {
                    "CALLED ON NULL INPUT"
                } else {
                    "RETURNS NULL ON NULL INPUT"
                });
            }
            RoutineCharacteristic::Security { security, .. } => {
                self.text(match security {
                    RoutineSecurity::Invoker => "SECURITY INVOKER",
                    RoutineSecurity::Definer => "SECURITY DEFINER",
                });
            }
            RoutineCharacteristic::Comment { comment, .. } => {
                self.text("COMMENT ");
                self.string(comment);
            }
            RoutineCharacteristic::Properties { properties, .. } => {
                if properties.is_empty() {
                    unsupported("empty routine properties");
                }
                self.text("WITH (");
                for (index, property) in properties.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    self.identifier(&property.name);
                    self.text(" = ");
                    match &property.value {
                        // Parentheses distinguish Some(Identifier DEFAULT)
                        // from the dedicated property DEFAULT production.
                        Some(value) => self.parenthesized_expression(value),
                        None => self.text("DEFAULT"),
                    }
                }
                self.character(')');
            }
        }
    }

    #[recursive::recursive]
    fn routine_control_statement(&mut self, value: &ControlStatement) {
        match value {
            ControlStatement::Return(value) => {
                self.text("RETURN ");
                self.expression(&value.value);
            }
            ControlStatement::Assignment(value) => {
                self.text("SET ");
                self.identifier(&value.target);
                self.text(" = ");
                self.expression(&value.value);
            }
            ControlStatement::Case(value) => {
                if value.when_clauses.is_empty() {
                    unsupported("CASE without routine WHEN clauses");
                }
                self.text("CASE ");
                if let Some(expression) = &value.expression {
                    self.expression(expression);
                    self.character(' ');
                }
                for clause in &value.when_clauses {
                    self.text("WHEN ");
                    self.expression(&clause.expression);
                    self.text(" THEN ");
                    self.routine_statement_list(&clause.statements);
                }
                if let Some(clause) = &value.else_clause {
                    self.routine_else(clause);
                }
                self.text("END CASE");
            }
            ControlStatement::If(value) => {
                self.text("IF ");
                self.expression(&value.expression);
                self.text(" THEN ");
                self.routine_statement_list(&value.statements);
                for clause in &value.else_if_clauses {
                    self.text("ELSEIF ");
                    self.expression(&clause.expression);
                    self.text(" THEN ");
                    self.routine_statement_list(&clause.statements);
                }
                if let Some(clause) = &value.else_clause {
                    self.routine_else(clause);
                }
                self.text("END IF");
            }
            ControlStatement::Iterate(value) => {
                self.text("ITERATE ");
                self.identifier(&value.label);
            }
            ControlStatement::Leave(value) => {
                self.text("LEAVE ");
                self.identifier(&value.label);
            }
            ControlStatement::VariableDeclaration(_) => {
                unsupported("a DECLARE outside a compound declaration prefix");
            }
            ControlStatement::Compound(value) => {
                self.text("BEGIN ");
                for declaration in &value.variable_declarations {
                    self.routine_variable_declaration(declaration);
                    self.text("; ");
                }
                for statement in &value.statements {
                    self.routine_control_statement(statement);
                    self.text("; ");
                }
                self.text("END");
            }
            ControlStatement::Loop(value) => {
                self.routine_label(value.label.as_ref());
                self.text("LOOP ");
                self.routine_statement_list(&value.statements);
                self.text("END LOOP");
            }
            ControlStatement::While(value) => {
                self.routine_label(value.label.as_ref());
                self.text("WHILE ");
                self.expression(&value.expression);
                self.text(" DO ");
                self.routine_statement_list(&value.statements);
                self.text("END WHILE");
            }
            ControlStatement::Repeat(value) => {
                self.routine_label(value.label.as_ref());
                self.text("REPEAT ");
                self.routine_statement_list(&value.statements);
                self.text("UNTIL ");
                self.expression(&value.condition);
                self.text(" END REPEAT");
            }
        }
    }

    fn routine_statement_list(&mut self, values: &[ControlStatement]) {
        if values.is_empty() {
            unsupported("an empty required routine statement list");
        }
        for value in values {
            self.routine_control_statement(value);
            self.text("; ");
        }
    }
    fn routine_else(&mut self, value: &ElseClause) {
        self.text("ELSE ");
        self.routine_statement_list(&value.statements);
    }
    fn routine_label(&mut self, label: Option<&Ident>) {
        if let Some(label) = label {
            self.identifier(label);
            self.text(": ");
        }
    }
    fn routine_variable_declaration(&mut self, value: &VariableDeclaration) {
        if value.names.is_empty() {
            unsupported("a DECLARE without names");
        }
        self.text("DECLARE ");
        self.identifiers(&value.names);
        self.character(' ');
        self.data_type(&value.data_type);
        if let Some(default) = &value.default_value {
            self.text(" DEFAULT ");
            self.expression(default);
        }
    }
}
