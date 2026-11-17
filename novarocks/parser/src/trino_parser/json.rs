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

use super::{GrammarError as ParserError, Parser};
use crate::ast::*;
use crate::trino_token::{Keyword as K, Symbol as S, TokenKind as T};
use crate::{ParseError, Span};

impl Parser<'_> {
    #[recursive::recursive]
    pub(super) fn parse_json_expression(&mut self) -> Result<Expr, ParserError> {
        let first = self.current();
        if !matches!(
            first.kind,
            T::Keyword(
                K::Json
                    | K::JsonExists
                    | K::JsonValue
                    | K::JsonQuery
                    | K::JsonSerialize
                    | K::JsonObject
                    | K::JsonArray
            )
        ) {
            return Err(self.error("a JSON expression"));
        }
        self.bump();
        self.expect_symbol(S::LParen)?;
        match first.kind {
            T::Keyword(K::Json) => {
                let (expression, format) = self.parse_json_value_expression()?;
                self.expect_symbol(S::RParen)?;
                Ok(Expr::JsonConstructor(AstBox::new(JsonConstructor {
                    expression,
                    format: format.unwrap_or(JsonFormat::Json),
                    span: self.span_since(first.span),
                })))
            }
            T::Keyword(K::JsonExists) => {
                let path_invocation = self.parse_json_path_invocation()?;
                let error_behavior = match self.peek(0).kind {
                    T::Keyword(K::True | K::False | K::Unknown | K::Error) => {
                        let behavior = match self.bump().kind {
                            T::Keyword(K::True) => JsonExistsErrorBehavior::True,
                            T::Keyword(K::False) => JsonExistsErrorBehavior::False,
                            T::Keyword(K::Unknown) => JsonExistsErrorBehavior::Unknown,
                            _ => JsonExistsErrorBehavior::Error,
                        };
                        self.expect_keyword(K::On)?;
                        self.expect_keyword(K::Error)?;
                        behavior
                    }
                    _ => JsonExistsErrorBehavior::False,
                };
                self.expect_symbol(S::RParen)?;
                Ok(Expr::JsonExists(AstBox::new(JsonExists {
                    path_invocation,
                    error_behavior,
                    span: self.span_since(first.span),
                })))
            }
            T::Keyword(K::JsonValue) => {
                let path_invocation = self.parse_json_path_invocation()?;
                let returned_type = if self.eat_keyword(K::Returning).is_some() {
                    Some(self.parse_type()?)
                } else {
                    None
                };
                let (empty_behavior, empty_default, error_behavior, error_default) =
                    self.parse_json_value_behaviors()?;
                self.expect_symbol(S::RParen)?;
                let span = self.span_since(first.span);
                let node = JsonValue::new(
                    path_invocation,
                    returned_type,
                    empty_behavior,
                    empty_default,
                    error_behavior.unwrap_or(JsonValueBehavior::Null),
                    error_default,
                    span,
                )
                .map_err(|_| json_invariant(self.source, span))?;
                Ok(Expr::JsonValue(AstBox::new(node)))
            }
            T::Keyword(K::JsonQuery) => {
                let path_invocation = self.parse_json_path_invocation()?;
                let (returned_type, output_format) = self.parse_json_returning()?;
                let wrapper_behavior = self.parse_json_wrapper()?;
                let quotes_behavior = self.parse_json_quotes()?;
                let (empty_behavior, error_behavior) = self.parse_json_query_behaviors()?;
                self.expect_symbol(S::RParen)?;
                Ok(Expr::JsonQuery(AstBox::new(JsonQuery {
                    path_invocation,
                    returned_type,
                    output_format,
                    wrapper_behavior,
                    quotes_behavior,
                    empty_behavior,
                    error_behavior: error_behavior.unwrap_or(JsonQueryBehavior::Null),
                    span: self.span_since(first.span),
                })))
            }
            T::Keyword(K::JsonSerialize) => {
                let (expression, input_format) = self.parse_json_value_expression()?;
                let (returned_type, output_format) = self.parse_json_returning()?;
                let error_behavior = if self.at_keyword(K::Error) || self.at_keyword(K::Null) {
                    let behavior = if self.eat_keyword(K::Null).is_some() {
                        JsonSerializeErrorBehavior::Null
                    } else {
                        self.expect_keyword(K::Error)?;
                        JsonSerializeErrorBehavior::Error
                    };
                    self.expect_keyword(K::On)?;
                    self.expect_keyword(K::Error)?;
                    behavior
                } else {
                    JsonSerializeErrorBehavior::Error
                };
                self.expect_symbol(S::RParen)?;
                Ok(Expr::JsonSerialize(AstBox::new(JsonSerialize {
                    expression,
                    input_format: input_format.unwrap_or(JsonFormat::Json),
                    returned_type,
                    output_format,
                    error_behavior,
                    span: self.span_since(first.span),
                })))
            }
            T::Keyword(K::JsonObject) => self.parse_json_object_body(first.span),
            T::Keyword(K::JsonArray) => self.parse_json_array_body(first.span),
            _ => unreachable!("JSON expression prefix checked above"),
        }
    }

    fn parse_json_format(&mut self) -> Result<JsonFormat, ParserError> {
        self.expect_keyword(K::Json)?;
        if self.eat_keyword(K::Encoding).is_none() {
            return Ok(JsonFormat::Json);
        }
        let format = match self.peek(0).kind {
            T::Keyword(K::Utf8) => JsonFormat::Utf8,
            T::Keyword(K::Utf16) => JsonFormat::Utf16,
            T::Keyword(K::Utf32) => JsonFormat::Utf32,
            _ => return Err(self.error("UTF8, UTF16, or UTF32")),
        };
        self.bump();
        Ok(format)
    }

    #[recursive::recursive]
    fn parse_json_value_expression(&mut self) -> Result<(Expr, Option<JsonFormat>), ParserError> {
        let expression = self.parse_expression()?;
        let format = if self.eat_keyword(K::Format).is_some() {
            Some(self.parse_json_format()?)
        } else {
            None
        };
        Ok((expression, format))
    }

    fn parse_json_path_string(&mut self) -> Result<JsonPathString, ParserError> {
        let (value, span) = self.parse_string()?;
        Ok(JsonPathString::new(value, span))
    }

    #[recursive::recursive]
    fn parse_json_path_invocation(&mut self) -> Result<JsonPathInvocation, ParserError> {
        let start = self.current().span;
        let (input_expression, input_format) = self.parse_json_value_expression()?;
        self.expect_symbol(S::Comma)?;
        let json_path = self.parse_json_path_string()?;
        let path_name = if self.eat_keyword(K::As).is_some() {
            Some(self.ident()?)
        } else {
            None
        };
        let mut path_parameters = Vec::new();
        if self.eat_keyword(K::Passing).is_some() {
            loop {
                let argument_start = self.current().span;
                let (parameter, format) = self.parse_json_value_expression()?;
                self.expect_keyword(K::As)?;
                let name = self.ident()?;
                path_parameters.push(JsonPathParameter {
                    name,
                    parameter,
                    format,
                    span: self.span_since(argument_start),
                });
                if self.eat_symbol(S::Comma).is_none() {
                    break;
                }
            }
        }
        Ok(JsonPathInvocation {
            input_expression,
            input_format: input_format.unwrap_or(JsonFormat::Json),
            json_path,
            path_name,
            path_parameters,
            span: self.span_since(start),
        })
    }

    #[recursive::recursive]
    fn parse_json_returning(
        &mut self,
    ) -> Result<(Option<TypeName>, Option<JsonFormat>), ParserError> {
        if self.eat_keyword(K::Returning).is_none() {
            return Ok((None, None));
        }
        let data_type = self.parse_type()?;
        let format = if self.eat_keyword(K::Format).is_some() {
            Some(self.parse_json_format()?)
        } else {
            None
        };
        Ok((Some(data_type), format))
    }

    fn parse_json_wrapper(&mut self) -> Result<JsonQueryWrapperBehavior, ParserError> {
        let behavior = if self.eat_keyword(K::Without).is_some() {
            JsonQueryWrapperBehavior::Without
        } else if self.eat_keyword(K::With).is_some() {
            if self.eat_keyword(K::Conditional).is_some() {
                JsonQueryWrapperBehavior::Conditional
            } else {
                let _ = self.eat_keyword(K::Unconditional);
                JsonQueryWrapperBehavior::Unconditional
            }
        } else {
            return Ok(JsonQueryWrapperBehavior::Without);
        };
        let _ = self.eat_keyword(K::Array);
        self.expect_keyword(K::Wrapper)?;
        Ok(behavior)
    }

    fn parse_json_quotes(&mut self) -> Result<Option<JsonQueryQuotesBehavior>, ParserError> {
        let behavior = if self.eat_keyword(K::Keep).is_some() {
            JsonQueryQuotesBehavior::Keep
        } else if self.eat_keyword(K::Omit).is_some() {
            JsonQueryQuotesBehavior::Omit
        } else {
            return Ok(None);
        };
        self.expect_keyword(K::Quotes)?;
        if self.eat_keyword(K::On).is_some() {
            self.expect_keyword(K::Scalar)?;
            self.expect_keyword(K::TextString)?;
        }
        Ok(Some(behavior))
    }

    fn json_value_behavior_ahead(&self) -> bool {
        matches!(
            self.peek(0).kind,
            T::Keyword(K::Null | K::Error | K::Default)
        )
    }

    #[recursive::recursive]
    fn parse_json_value_behavior(
        &mut self,
    ) -> Result<(JsonValueBehavior, Option<Expr>, K), ParserError> {
        let (behavior, value) = match self.peek(0).kind {
            T::Keyword(K::Null) => {
                self.bump();
                (JsonValueBehavior::Null, None)
            }
            T::Keyword(K::Error) => {
                self.bump();
                (JsonValueBehavior::Error, None)
            }
            T::Keyword(K::Default) => {
                self.bump();
                (JsonValueBehavior::Default, Some(self.parse_expression()?))
            }
            _ => return Err(self.error("NULL, ERROR, or DEFAULT expression")),
        };
        self.expect_keyword(K::On)?;
        let target = self.parse_json_behavior_target()?;
        Ok((behavior, value, target))
    }

    fn parse_json_behavior_target(&mut self) -> Result<K, ParserError> {
        if self.eat_keyword(K::Empty).is_some() {
            Ok(K::Empty)
        } else {
            self.expect_keyword(K::Error)?;
            Ok(K::Error)
        }
    }

    #[recursive::recursive]
    fn parse_json_value_behaviors(
        &mut self,
    ) -> Result<
        (
            JsonValueBehavior,
            Option<Expr>,
            Option<JsonValueBehavior>,
            Option<Expr>,
        ),
        ParserError,
    > {
        let mut empty = JsonValueBehavior::Null;
        let mut empty_default = None;
        let mut error = None;
        let mut error_default = None;
        if self.json_value_behavior_ahead() {
            let (behavior, value, target) = self.parse_json_value_behavior()?;
            if target == K::Empty {
                empty = behavior;
                empty_default = value;
            } else {
                error = Some(behavior);
                error_default = value;
            }
        }
        if error.is_none() && self.json_value_behavior_ahead() {
            let (behavior, value, target) = self.parse_json_value_behavior()?;
            if target != K::Error {
                return Err(self.error("ON ERROR after ON EMPTY"));
            }
            error = Some(behavior);
            error_default = value;
        }
        Ok((empty, empty_default, error, error_default))
    }

    fn json_query_behavior_ahead(&self) -> bool {
        matches!(self.peek(0).kind, T::Keyword(K::Null | K::Error | K::Empty))
    }

    fn parse_json_query_behavior(&mut self) -> Result<(JsonQueryBehavior, K), ParserError> {
        let behavior = match self.peek(0).kind {
            T::Keyword(K::Null) => {
                self.bump();
                JsonQueryBehavior::Null
            }
            T::Keyword(K::Error) => {
                self.bump();
                JsonQueryBehavior::Error
            }
            T::Keyword(K::Empty) => {
                self.bump();
                if self.eat_keyword(K::Array).is_some() {
                    JsonQueryBehavior::EmptyArray
                } else {
                    self.expect_keyword(K::Object)?;
                    JsonQueryBehavior::EmptyObject
                }
            }
            _ => return Err(self.error("NULL, ERROR, EMPTY ARRAY, or EMPTY OBJECT")),
        };
        self.expect_keyword(K::On)?;
        Ok((behavior, self.parse_json_behavior_target()?))
    }

    fn parse_json_query_behaviors(
        &mut self,
    ) -> Result<(JsonQueryBehavior, Option<JsonQueryBehavior>), ParserError> {
        let mut empty = JsonQueryBehavior::Null;
        let mut error = None;
        if self.json_query_behavior_ahead() {
            let (behavior, target) = self.parse_json_query_behavior()?;
            if target == K::Empty {
                empty = behavior;
            } else {
                error = Some(behavior);
            }
        }
        if error.is_none() && self.json_query_behavior_ahead() {
            let (behavior, target) = self.parse_json_query_behavior()?;
            if target != K::Error {
                return Err(self.error("ON ERROR after ON EMPTY"));
            }
            error = Some(behavior);
        }
        Ok((empty, error))
    }

    #[recursive::recursive]
    fn parse_json_object_body(&mut self, start: Span) -> Result<Expr, ParserError> {
        // RETURNING is also a non-reserved expression identifier. An empty
        // member list's RETURNING alternative must match its full closing ')'.
        if self.at_keyword(K::Returning) {
            let checkpoint = self.checkpoint();
            let empty_returning = (|| {
                let (returned_type, output_format) = self.parse_json_returning()?;
                self.expect_symbol(S::RParen)?;
                Ok::<_, ParserError>((returned_type, output_format))
            })();
            match empty_returning {
                Ok((returned_type, output_format)) => {
                    return Ok(Expr::JsonObject(AstBox::new(JsonObject {
                        members: Vec::new(),
                        null_on_null: true,
                        unique_keys: false,
                        returned_type,
                        output_format,
                        span: self.span_since(start),
                    })));
                }
                Err(ParserError::Syntax(_)) => self.restore(checkpoint),
                Err(error @ ParserError::Construction(_)) => return Err(error),
            }
        }
        let mut members = Vec::new();
        let mut null_on_null = true;
        let mut unique_keys = false;
        if !self.at_symbol(S::RParen) {
            loop {
                members.push(self.parse_json_object_member()?);
                if self.eat_symbol(S::Comma).is_none() {
                    break;
                }
            }
            null_on_null = self.parse_json_null_policy()?.unwrap_or(true);
            if self.at_keyword(K::With) || self.at_keyword(K::Without) {
                unique_keys = self.eat_keyword(K::With).is_some();
                if !unique_keys {
                    self.expect_keyword(K::Without)?;
                }
                self.expect_keyword(K::Unique)?;
                let _ = self.eat_keyword(K::Keys);
            }
        }
        let (returned_type, output_format) = self.parse_json_returning()?;
        self.expect_symbol(S::RParen)?;
        Ok(Expr::JsonObject(AstBox::new(JsonObject {
            members,
            null_on_null,
            unique_keys,
            returned_type,
            output_format,
            span: self.span_since(start),
        })))
    }

    #[recursive::recursive]
    fn parse_json_object_member(&mut self) -> Result<JsonObjectMember, ParserError> {
        let start = self.current().span;
        if self.at_keyword(K::Key) {
            let checkpoint = self.checkpoint();
            self.bump();
            let prefixed_key = (|| {
                let key = self.parse_expression()?;
                self.expect_keyword(K::Value)?;
                Ok::<_, ParserError>(key)
            })();
            match prefixed_key {
                Ok(key) => {
                    // VALUE commits this grammar alternative. A later value or
                    // format error cannot be swallowed by retrying the key.
                    let (value, format) = self.parse_json_value_expression()?;
                    return Ok(JsonObjectMember {
                        key,
                        value,
                        format,
                        span: self.span_since(start),
                    });
                }
                Err(ParserError::Syntax(_)) => self.restore(checkpoint),
                Err(error @ ParserError::Construction(_)) => return Err(error),
            }
        }
        let key = self.parse_expression()?;
        if self.eat_keyword(K::Value).is_none() {
            self.expect_symbol(S::Colon)?;
        }
        let (value, format) = self.parse_json_value_expression()?;
        Ok(JsonObjectMember {
            key,
            value,
            format,
            span: self.span_since(start),
        })
    }

    fn parse_json_null_policy(&mut self) -> Result<Option<bool>, ParserError> {
        let value = if self.eat_keyword(K::Null).is_some() {
            true
        } else if self.eat_keyword(K::Absent).is_some() {
            false
        } else {
            return Ok(None);
        };
        self.expect_keyword(K::On)?;
        self.expect_keyword(K::Null)?;
        Ok(Some(value))
    }

    #[recursive::recursive]
    fn parse_json_array_body(&mut self, start: Span) -> Result<Expr, ParserError> {
        if self.at_keyword(K::Returning) {
            let checkpoint = self.checkpoint();
            let empty_returning = (|| {
                let (returned_type, output_format) = self.parse_json_returning()?;
                self.expect_symbol(S::RParen)?;
                Ok::<_, ParserError>((returned_type, output_format))
            })();
            match empty_returning {
                Ok((returned_type, output_format)) => {
                    return Ok(Expr::JsonArray(AstBox::new(JsonArray {
                        elements: Vec::new(),
                        null_on_null: false,
                        returned_type,
                        output_format,
                        span: self.span_since(start),
                    })));
                }
                Err(ParserError::Syntax(_)) => self.restore(checkpoint),
                Err(error @ ParserError::Construction(_)) => return Err(error),
            }
        }
        let mut elements = Vec::new();
        let mut null_on_null = false;
        if !self.at_symbol(S::RParen) {
            loop {
                let element_start = self.current().span;
                let (value, format) = self.parse_json_value_expression()?;
                elements.push(JsonArrayElement {
                    value,
                    format,
                    span: self.span_since(element_start),
                });
                if self.eat_symbol(S::Comma).is_none() {
                    break;
                }
            }
            null_on_null = self.parse_json_null_policy()?.unwrap_or(false);
        }
        let (returned_type, output_format) = self.parse_json_returning()?;
        self.expect_symbol(S::RParen)?;
        Ok(Expr::JsonArray(AstBox::new(JsonArray {
            elements,
            null_on_null,
            returned_type,
            output_format,
            span: self.span_since(start),
        })))
    }

    #[recursive::recursive]
    pub(super) fn parse_json_table(&mut self) -> Result<JsonTable, ParserError> {
        let start = self.expect_keyword(K::JsonTable)?;
        self.expect_symbol(S::LParen)?;
        let path_invocation = self.parse_json_path_invocation()?;
        self.expect_keyword(K::Columns)?;
        let columns = self.parse_json_columns()?;
        let plan = if self.eat_keyword(K::Plan).is_some() {
            let default = self.eat_keyword(K::Default).is_some();
            self.expect_symbol(S::LParen)?;
            let plan = if default {
                JsonTablePlan::Default(self.parse_json_default_plan()?)
            } else {
                JsonTablePlan::Specific(self.parse_json_specific_plan()?)
            };
            self.expect_symbol(S::RParen)?;
            Some(plan)
        } else {
            None
        };
        let error_behavior = if self.at_keyword(K::Error) || self.at_keyword(K::Empty) {
            let behavior = if self.eat_keyword(K::Error).is_some() {
                JsonTableErrorBehavior::Error
            } else {
                self.expect_keyword(K::Empty)?;
                JsonTableErrorBehavior::Empty
            };
            self.expect_keyword(K::On)?;
            self.expect_keyword(K::Error)?;
            Some(behavior)
        } else {
            None
        };
        self.expect_symbol(S::RParen)?;
        let span = self.span_since(start);
        JsonTable::new(path_invocation, columns, plan, error_behavior, span)
            .map_err(|_| json_invariant(self.source, span))
    }

    #[recursive::recursive]
    fn parse_json_columns(&mut self) -> Result<Vec<JsonTableColumn>, ParserError> {
        self.expect_symbol(S::LParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_json_column()?);
            if self.eat_symbol(S::Comma).is_none() {
                break;
            }
        }
        self.expect_symbol(S::RParen)?;
        Ok(columns)
    }

    #[recursive::recursive]
    fn parse_json_column(&mut self) -> Result<JsonTableColumn, ParserError> {
        let start = self.current().span;
        let nested = self.at_keyword(K::Nested)
            && (matches!(self.peek(1).kind, T::String | T::UnicodeString)
                || self.peek(1).kind == T::Keyword(K::Path)
                    && matches!(self.peek(2).kind, T::String | T::UnicodeString));
        if nested {
            self.bump();
            let _ = self.eat_keyword(K::Path);
            let json_path = self.parse_json_path_string()?;
            let path_name = if self.eat_keyword(K::As).is_some() {
                Some(self.ident()?)
            } else {
                None
            };
            self.expect_keyword(K::Columns)?;
            let columns = self.parse_json_columns()?;
            let span = self.span_since(start);
            return JsonTableNestedColumns::new(json_path, path_name, columns, span)
                .map(JsonTableColumn::Nested)
                .map_err(|_| json_invariant(self.source, span));
        }
        let name = self.ident()?;
        if self.eat_keyword(K::For).is_some() {
            self.expect_keyword(K::Ordinality)?;
            return Ok(JsonTableColumn::Ordinality(JsonTableOrdinalityColumn {
                name,
                span: self.span_since(start),
            }));
        }
        let data_type = self.parse_type()?;
        let format = if self.eat_keyword(K::Format).is_some() {
            Some(self.parse_json_format()?)
        } else {
            None
        };
        let json_path = if self.eat_keyword(K::Path).is_some() {
            Some(self.parse_json_path_string()?)
        } else {
            None
        };
        if let Some(format) = format {
            let wrapper_behavior = self.parse_json_wrapper()?;
            let quotes_behavior = self.parse_json_quotes()?;
            let (empty_behavior, error_behavior) = self.parse_json_query_behaviors()?;
            Ok(JsonTableColumn::Query(JsonTableQueryColumn {
                name,
                data_type,
                format,
                json_path,
                wrapper_behavior,
                quotes_behavior,
                empty_behavior,
                error_behavior,
                span: self.span_since(start),
            }))
        } else {
            let (empty_behavior, empty_default, error_behavior, error_default) =
                self.parse_json_value_behaviors()?;
            let span = self.span_since(start);
            JsonTableValueColumn::new(
                name,
                data_type,
                json_path,
                empty_behavior,
                empty_default,
                error_behavior,
                error_default,
                span,
            )
            .map(JsonTableColumn::Value)
            .map_err(|_| json_invariant(self.source, span))
        }
    }

    fn parse_json_default_plan(&mut self) -> Result<JsonTableDefaultPlan, ParserError> {
        let start = self.current().span;
        let mut parent_child = JsonTableParentChildPlanType::Outer;
        let mut siblings = JsonTableSiblingsPlanType::Union;
        if self.at_keyword(K::Outer) || self.at_keyword(K::Inner) {
            parent_child = self.parse_json_parent_child_type()?;
            if self.eat_symbol(S::Comma).is_some() {
                siblings = self.parse_json_siblings_type()?;
            }
        } else {
            siblings = self.parse_json_siblings_type()?;
            if self.eat_symbol(S::Comma).is_some() {
                parent_child = self.parse_json_parent_child_type()?;
            }
        }
        Ok(JsonTableDefaultPlan {
            parent_child,
            siblings,
            span: self.span_since(start),
        })
    }

    fn parse_json_parent_child_type(
        &mut self,
    ) -> Result<JsonTableParentChildPlanType, ParserError> {
        if self.eat_keyword(K::Outer).is_some() {
            Ok(JsonTableParentChildPlanType::Outer)
        } else {
            self.expect_keyword(K::Inner)?;
            Ok(JsonTableParentChildPlanType::Inner)
        }
    }
    fn parse_json_siblings_type(&mut self) -> Result<JsonTableSiblingsPlanType, ParserError> {
        if self.eat_keyword(K::Union).is_some() {
            Ok(JsonTableSiblingsPlanType::Union)
        } else {
            self.expect_keyword(K::Cross)?;
            Ok(JsonTableSiblingsPlanType::Cross)
        }
    }

    fn parse_json_plan_leaf(&mut self) -> Result<JsonTablePlanLeaf, ParserError> {
        let name = self.ident()?;
        Ok(JsonTablePlanLeaf {
            span: name.span,
            name,
        })
    }

    #[recursive::recursive]
    fn parse_json_plan_primary(&mut self) -> Result<JsonTableSpecificPlan, ParserError> {
        if self.eat_symbol(S::LParen).is_some() {
            let plan = self.parse_json_specific_plan()?;
            self.expect_symbol(S::RParen)?;
            Ok(plan)
        } else {
            Ok(JsonTableSpecificPlan::Leaf(self.parse_json_plan_leaf()?))
        }
    }

    #[recursive::recursive]
    fn parse_json_specific_plan(&mut self) -> Result<JsonTableSpecificPlan, ParserError> {
        let start = self.current().span;
        if self.is_identifier(0) && matches!(self.peek(1).kind, T::Keyword(K::Outer | K::Inner)) {
            let parent = self.parse_json_plan_leaf()?;
            let plan_type = self.parse_json_parent_child_type()?;
            let child = self.parse_json_plan_primary()?;
            return Ok(JsonTableSpecificPlan::ParentChild(
                JsonTablePlanParentChild {
                    plan_type,
                    parent,
                    child: AstBox::new(child),
                    span: self.span_since(start),
                },
            ));
        }
        let bare_name = self.is_identifier(0);
        let first = self.parse_json_plan_primary()?;
        if !self.at_keyword(K::Union) && !self.at_keyword(K::Cross) {
            if bare_name {
                return Ok(first);
            }
            return Err(self.error("UNION or CROSS after a parenthesized plan primary"));
        }
        let plan_type = self.parse_json_siblings_type()?;
        let keyword = if plan_type == JsonTableSiblingsPlanType::Union {
            K::Union
        } else {
            K::Cross
        };
        let mut siblings = vec![first, self.parse_json_plan_primary()?];
        while self.eat_keyword(keyword).is_some() {
            siblings.push(self.parse_json_plan_primary()?);
        }
        let span = self.span_since(start);
        JsonTablePlanSiblings::new(plan_type, siblings, span)
            .map(JsonTableSpecificPlan::Siblings)
            .map_err(|_| json_invariant(self.source, span))
    }
}

fn json_invariant(source: &str, span: Span) -> ParserError {
    ParserError::Construction(
        ParseError::UnexpectedToken {
            expected: "valid JSON node constructor facts",
            found: source[span.start()..span.end()].to_owned(),
            span,
        }
        .into(),
    )
}
