// SPDX-License-Identifier: Apache-2.0
//! SQL/JSON syntax is rendered from the actual typed children and optional facts.
use super::*;

impl Writer {
    #[recursive::recursive]
    pub(super) fn json_expression(&mut self, value: &Expr) -> PrintResult {
        match value {
            Expr::JsonConstructor(value) => {
                self.text("JSON(");
                self.expression(&value.expression)?;
                self.json_format(value.format)?;
            }
            Expr::JsonExists(value) => {
                self.text("JSON_EXISTS(");
                self.json_path_invocation(&value.path_invocation)?;
                self.text(match value.error_behavior {
                    JsonExistsErrorBehavior::False => " FALSE",
                    JsonExistsErrorBehavior::True => " TRUE",
                    JsonExistsErrorBehavior::Unknown => " UNKNOWN",
                    JsonExistsErrorBehavior::Error => " ERROR",
                });
                self.text(" ON ERROR");
            }
            Expr::JsonValue(value) => {
                value
                    .validate_constructor_invariants()
                    .map_err(|_| unsupported("JSON_VALUE behavior/default invariant"))?;
                self.text("JSON_VALUE(");
                self.json_path_invocation(&value.path_invocation)?;
                self.json_returning(value.returned_type.as_ref(), None)?;
                self.json_value_behavior(
                    value.empty_behavior,
                    value.empty_default.as_ref(),
                    "EMPTY",
                )?;
                self.json_value_behavior(
                    value.error_behavior,
                    value.error_default.as_ref(),
                    "ERROR",
                )?;
            }
            Expr::JsonQuery(value) => {
                self.text("JSON_QUERY(");
                self.json_path_invocation(&value.path_invocation)?;
                self.json_returning(value.returned_type.as_ref(), value.output_format)?;
                self.json_wrapper(value.wrapper_behavior)?;
                self.json_quotes(value.quotes_behavior)?;
                self.json_query_behavior(value.empty_behavior, "EMPTY")?;
                self.json_query_behavior(value.error_behavior, "ERROR")?;
            }
            Expr::JsonSerialize(value) => {
                self.text("JSON_SERIALIZE(");
                self.expression(&value.expression)?;
                self.json_format(value.input_format)?;
                self.json_returning(value.returned_type.as_ref(), value.output_format)?;
                self.text(match value.error_behavior {
                    JsonSerializeErrorBehavior::Error => " ERROR ON ERROR",
                    JsonSerializeErrorBehavior::Null => " NULL ON ERROR",
                });
            }
            Expr::JsonObject(value) => {
                if value.members.is_empty() && (!value.null_on_null || value.unique_keys) {
                    return Err(unsupported(
                        "empty JSON_OBJECT with a non-default member-list policy",
                    ));
                }
                self.text("JSON_OBJECT(");
                for (index, member) in value.members.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    self.text("KEY ");
                    self.expression(&member.key)?;
                    self.text(" VALUE ");
                    self.expression(&member.value)?;
                    if let Some(format) = member.format {
                        self.json_format(format)?;
                    }
                }
                // These clauses belong to the nonempty member-list alternative.
                if !value.members.is_empty() {
                    self.json_null_policy(value.null_on_null)?;
                    self.text(if value.unique_keys {
                        " WITH UNIQUE KEYS"
                    } else {
                        " WITHOUT UNIQUE KEYS"
                    });
                }
                self.json_returning(value.returned_type.as_ref(), value.output_format)?;
            }
            Expr::JsonArray(value) => {
                if value.elements.is_empty() && value.null_on_null {
                    return Err(unsupported("empty JSON_ARRAY with NULL ON NULL"));
                }
                self.text("JSON_ARRAY(");
                for (index, element) in value.elements.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    self.expression(&element.value)?;
                    if let Some(format) = element.format {
                        self.json_format(format)?;
                    }
                }
                if !value.elements.is_empty() {
                    self.json_null_policy(value.null_on_null)?;
                }
                self.json_returning(value.returned_type.as_ref(), value.output_format)?;
            }
            _ => unreachable!("JSON expression dispatch is a typed exhaustive subset"),
        }
        self.character(')');
        Ok(())
    }

    fn json_format(&mut self, format: JsonFormat) -> PrintResult {
        self.text(match format {
            JsonFormat::Json => " FORMAT JSON",
            JsonFormat::Utf8 => " FORMAT JSON ENCODING UTF8",
            JsonFormat::Utf16 => " FORMAT JSON ENCODING UTF16",
            JsonFormat::Utf32 => " FORMAT JSON ENCODING UTF32",
        });
        Ok(())
    }

    #[recursive::recursive]
    fn json_returning(
        &mut self,
        data_type: Option<&TypeName>,
        format: Option<JsonFormat>,
    ) -> PrintResult {
        match data_type {
            Some(data_type) => {
                self.text(" RETURNING ");
                self.data_type(data_type)?;
                if let Some(format) = format {
                    self.json_format(format)?;
                }
            }
            None if format.is_some() => {
                return Err(unsupported("JSON output FORMAT without RETURNING type"));
            }
            None => {}
        }
        Ok(())
    }

    #[recursive::recursive]
    fn json_path_invocation(&mut self, value: &JsonPathInvocation) -> PrintResult {
        self.expression(&value.input_expression)?;
        self.json_format(value.input_format)?;
        self.text(", ");
        self.string(&value.json_path.value);
        if let Some(name) = &value.path_name {
            self.text(" AS ");
            self.identifier(name);
        }
        if !value.path_parameters.is_empty() {
            self.text(" PASSING ");
            for (index, parameter) in value.path_parameters.iter().enumerate() {
                if index != 0 {
                    self.text(", ");
                }
                self.expression(&parameter.parameter)?;
                if let Some(format) = parameter.format {
                    self.json_format(format)?;
                }
                self.text(" AS ");
                self.identifier(&parameter.name);
            }
        }
        Ok(())
    }

    fn json_wrapper(&mut self, behavior: JsonQueryWrapperBehavior) -> PrintResult {
        self.text(match behavior {
            JsonQueryWrapperBehavior::Without => " WITHOUT ARRAY WRAPPER",
            JsonQueryWrapperBehavior::Conditional => " WITH CONDITIONAL ARRAY WRAPPER",
            JsonQueryWrapperBehavior::Unconditional => " WITH UNCONDITIONAL ARRAY WRAPPER",
        });
        Ok(())
    }

    fn json_quotes(&mut self, behavior: Option<JsonQueryQuotesBehavior>) -> PrintResult {
        match behavior {
            Some(JsonQueryQuotesBehavior::Keep) => self.text(" KEEP QUOTES ON SCALAR STRING"),
            Some(JsonQueryQuotesBehavior::Omit) => self.text(" OMIT QUOTES ON SCALAR STRING"),
            None => {}
        }
        Ok(())
    }

    #[recursive::recursive]
    fn json_value_behavior(
        &mut self,
        behavior: JsonValueBehavior,
        default: Option<&Expr>,
        on: &str,
    ) -> PrintResult {
        match (behavior, default) {
            (JsonValueBehavior::Default, Some(expression)) => {
                self.text(" DEFAULT ");
                self.expression(expression)?;
            }
            (JsonValueBehavior::Null, None) => self.text(" NULL"),
            (JsonValueBehavior::Error, None) => self.text(" ERROR"),
            _ => return Err(unsupported("JSON value behavior/default invariant")),
        }
        self.text(" ON ");
        self.text(on);
        Ok(())
    }

    fn json_query_behavior(&mut self, behavior: JsonQueryBehavior, on: &str) -> PrintResult {
        self.text(match behavior {
            JsonQueryBehavior::Null => " NULL",
            JsonQueryBehavior::Error => " ERROR",
            JsonQueryBehavior::EmptyArray => " EMPTY ARRAY",
            JsonQueryBehavior::EmptyObject => " EMPTY OBJECT",
        });
        self.text(" ON ");
        self.text(on);
        Ok(())
    }

    fn json_null_policy(&mut self, null_on_null: bool) -> PrintResult {
        self.text(if null_on_null {
            " NULL ON NULL"
        } else {
            " ABSENT ON NULL"
        });
        Ok(())
    }

    #[recursive::recursive]
    pub(super) fn json_table(&mut self, value: &JsonTable) -> PrintResult {
        value
            .validate_constructor_invariants()
            .map_err(|_| unsupported("JSON_TABLE without columns"))?;
        self.text("JSON_TABLE(");
        self.json_path_invocation(&value.path_invocation)?;
        self.text(" COLUMNS (");
        self.json_columns(&value.columns)?;
        self.character(')');
        if let Some(plan) = &value.plan {
            match plan {
                JsonTablePlan::Default(plan) => {
                    self.text(" PLAN DEFAULT (");
                    self.json_parent_child_type(plan.parent_child)?;
                    self.text(", ");
                    self.json_siblings_type(plan.siblings)?;
                }
                JsonTablePlan::Specific(plan) => {
                    self.text(" PLAN (");
                    self.json_specific_plan(plan)?;
                }
            }
            self.character(')');
        }
        if let Some(behavior) = value.error_behavior {
            self.text(match behavior {
                JsonTableErrorBehavior::Error => " ERROR ON ERROR",
                JsonTableErrorBehavior::Empty => " EMPTY ON ERROR",
            });
        }
        self.character(')');
        Ok(())
    }

    #[recursive::recursive]
    fn json_columns(&mut self, columns: &[JsonTableColumn]) -> PrintResult {
        if columns.is_empty() {
            return Err(unsupported("empty JSON_TABLE columns"));
        }
        for (index, column) in columns.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.json_column(column)?;
        }
        Ok(())
    }

    #[recursive::recursive]
    fn json_column(&mut self, column: &JsonTableColumn) -> PrintResult {
        match column {
            JsonTableColumn::Ordinality(value) => {
                self.identifier(&value.name);
                self.text(" FOR ORDINALITY");
            }
            JsonTableColumn::Value(value) => {
                value
                    .validate_constructor_invariants()
                    .map_err(|_| unsupported("JSON_TABLE value behavior/default invariant"))?;
                self.identifier(&value.name);
                self.character(' ');
                self.data_type(&value.data_type)?;
                self.json_column_path(value.json_path.as_ref())?;
                self.json_value_behavior(
                    value.empty_behavior,
                    value.empty_default.as_ref(),
                    "EMPTY",
                )?;
                if let Some(behavior) = value.error_behavior {
                    self.json_value_behavior(behavior, value.error_default.as_ref(), "ERROR")?;
                }
            }
            JsonTableColumn::Query(value) => {
                self.identifier(&value.name);
                self.character(' ');
                self.data_type(&value.data_type)?;
                self.json_format(value.format)?;
                self.json_column_path(value.json_path.as_ref())?;
                self.json_wrapper(value.wrapper_behavior)?;
                self.json_quotes(value.quotes_behavior)?;
                self.json_query_behavior(value.empty_behavior, "EMPTY")?;
                if let Some(behavior) = value.error_behavior {
                    self.json_query_behavior(behavior, "ERROR")?;
                }
            }
            JsonTableColumn::Nested(value) => {
                value
                    .validate_constructor_invariants()
                    .map_err(|_| unsupported("JSON_TABLE nested columns without children"))?;
                self.text("NESTED PATH ");
                self.string(&value.json_path.value);
                if let Some(name) = &value.path_name {
                    self.text(" AS ");
                    self.identifier(name);
                }
                self.text(" COLUMNS (");
                self.json_columns(&value.columns)?;
                self.character(')');
            }
        }
        Ok(())
    }

    fn json_column_path(&mut self, path: Option<&JsonPathString>) -> PrintResult {
        if let Some(path) = path {
            self.text(" PATH ");
            self.string(&path.value);
        }
        Ok(())
    }

    fn json_parent_child_type(&mut self, value: JsonTableParentChildPlanType) -> PrintResult {
        self.text(match value {
            JsonTableParentChildPlanType::Outer => "OUTER",
            JsonTableParentChildPlanType::Inner => "INNER",
        });
        Ok(())
    }

    fn json_siblings_type(&mut self, value: JsonTableSiblingsPlanType) -> PrintResult {
        self.text(match value {
            JsonTableSiblingsPlanType::Union => "UNION",
            JsonTableSiblingsPlanType::Cross => "CROSS",
        });
        Ok(())
    }

    #[recursive::recursive]
    fn json_specific_plan(&mut self, plan: &JsonTableSpecificPlan) -> PrintResult {
        match plan {
            JsonTableSpecificPlan::Leaf(value) => self.identifier(&value.name),
            JsonTableSpecificPlan::ParentChild(value) => {
                self.identifier(&value.parent.name);
                self.character(' ');
                self.json_parent_child_type(value.plan_type)?;
                self.text(" (");
                self.json_specific_plan(&value.child)?;
                self.character(')');
            }
            JsonTableSpecificPlan::Siblings(value) => {
                value.validate_constructor_invariants().map_err(|_| {
                    unsupported("JSON_TABLE sibling plan with fewer than two children")
                })?;
                for (index, child) in value.siblings.iter().enumerate() {
                    if index != 0 {
                        self.character(' ');
                        self.json_siblings_type(value.plan_type)?;
                        self.character(' ');
                    }
                    // Each real child stays a planPrimary; same-kind nested
                    // siblings must not be flattened by the textual rendering.
                    self.character('(');
                    self.json_specific_plan(child)?;
                    self.character(')');
                }
            }
        }
        Ok(())
    }
}
