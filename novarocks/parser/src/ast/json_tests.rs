// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Cross-author programmatic syntax contracts, intended as ast::json::tests.
//! Requires the real JSON Visit/Fold hooks to be integrated by the root owner.
//! Does not fabricate Expr/TableFactor variants or prove parser/runtime support.

use super::super::visit::{fold_expr, walk_expr};
use super::super::{
    AstBox, AstVec, BinaryExpr, BinaryOperator, DataTypeParameter, GenericDataType,
    NumericParameter, TypeKind, TypeParameter, walk_type_name,
};
use super::*;
use std::collections::BTreeSet;

fn s() -> Span {
    Span::new(10, 19)
}
fn id(prefix: &str, name: &str, span: Span) -> Ident {
    Ident {
        value: format!("{prefix}{name}"),
        quoted: true,
        span,
    }
}
fn expr(prefix: &str, name: &str, span: Span) -> Expr {
    Expr::Identifier(id(prefix, name, span))
}
fn path(prefix: &str, value: &str, span: Span) -> JsonPathString {
    JsonPathString {
        value: format!("{prefix}{value}"),
        span,
    }
}
fn simple_type(prefix: &str, name: &str, span: Span) -> TypeName {
    TypeName {
        kind: TypeKind::Generic(GenericDataType {
            name: id(prefix, name, span),
            arguments: AstVec::new(),
        }),
        span,
    }
}
fn nested_type(prefix: &str, stem: &str, span: Span) -> TypeName {
    let mut leaf = simple_type(prefix, &format!("{stem}_leaf"), span);
    let TypeKind::Generic(inner) = &mut leaf.kind else {
        unreachable!()
    };
    inner
        .arguments
        .push(DataTypeParameter::Numeric(NumericParameter {
            spelling: "17".into(),
            parsed_value: 17,
            span,
        }));
    let mut outer = simple_type(prefix, stem, span);
    let TypeKind::Generic(inner) = &mut outer.kind else {
        unreachable!()
    };
    inner.arguments.push(DataTypeParameter::Type(TypeParameter {
        data_type: AstBox::new(leaf),
    }));
    outer
}
fn clear_type_arguments(node: &mut TypeName) {
    let TypeKind::Generic(value) = &mut node.kind else {
        unreachable!()
    };
    value.arguments.clear();
}
fn invocation(prefix: &str, stem: &str, span: Span) -> JsonPathInvocation {
    JsonPathInvocation {
        input_expression: expr(prefix, &format!("{stem}_input"), span),
        input_format: JsonFormat::Utf16,
        json_path: path(prefix, &format!("{stem}_path"), span),
        path_name: Some(id(prefix, &format!("{stem}_name"), span)),
        path_parameters: vec![
            JsonPathParameter {
                name: id(prefix, &format!("{stem}_a"), span),
                parameter: expr(prefix, &format!("{stem}_arg_a"), span),
                format: None,
                span,
            },
            JsonPathParameter {
                name: id(prefix, &format!("{stem}_b"), span),
                parameter: expr(prefix, &format!("{stem}_arg_b"), span),
                format: Some(JsonFormat::Utf32),
                span,
            },
        ],
        span,
    }
}
fn leaf(prefix: &str, name: &str, span: Span) -> JsonTableSpecificPlan {
    JsonTableSpecificPlan::Leaf(JsonTablePlanLeaf {
        name: id(prefix, name, span),
        span,
    })
}
fn value_column(prefix: &str, span: Span) -> JsonTableValueColumn {
    JsonTableValueColumn {
        name: id(prefix, "v_name", span),
        data_type: nested_type(prefix, "v_type", span),
        json_path: Some(path(prefix, "v_path", span)),
        empty_behavior: JsonValueBehavior::Default,
        empty_default: Some(expr(prefix, "v_empty", span)),
        error_behavior: Some(JsonValueBehavior::Default),
        error_default: Some(expr(prefix, "v_error", span)),
        span,
    }
}
fn query_column(prefix: &str, span: Span) -> JsonTableQueryColumn {
    JsonTableQueryColumn {
        name: id(prefix, "q_name", span),
        data_type: nested_type(prefix, "q_type", span),
        format: JsonFormat::Utf8,
        json_path: Some(path(prefix, "q_path", span)),
        wrapper_behavior: JsonQueryWrapperBehavior::Conditional,
        quotes_behavior: Some(JsonQueryQuotesBehavior::Omit),
        empty_behavior: JsonQueryBehavior::EmptyArray,
        error_behavior: None,
        span,
    }
}
fn table(prefix: &str, span: Span) -> JsonTable {
    JsonTable {
        path_invocation: invocation(prefix, "table", span),
        columns: vec![
            JsonTableColumn::Ordinality(JsonTableOrdinalityColumn {
                name: id(prefix, "ordinal", span),
                span,
            }),
            JsonTableColumn::Value(value_column(prefix, span)),
            JsonTableColumn::Query(query_column(prefix, span)),
            JsonTableColumn::Nested(JsonTableNestedColumns {
                json_path: path(prefix, "nested_path", span),
                path_name: Some(id(prefix, "nested_name", span)),
                columns: (vec![
                    JsonTableColumn::Value(JsonTableValueColumn {
                        name: id(prefix, "nested_value", span),
                        data_type: simple_type(prefix, "nested_type", span),
                        json_path: None,
                        empty_behavior: JsonValueBehavior::Null,
                        empty_default: None,
                        error_behavior: None,
                        error_default: None,
                        span,
                    }),
                    JsonTableColumn::Ordinality(JsonTableOrdinalityColumn {
                        name: id(prefix, "nested_ordinal", span),
                        span,
                    }),
                ])
                .into(),
                span: span,
            }),
        ],
        plan: Some(JsonTablePlan::Specific(JsonTableSpecificPlan::ParentChild(
            JsonTablePlanParentChild {
                plan_type: JsonTableParentChildPlanType::Inner,
                parent: JsonTablePlanLeaf {
                    name: id(prefix, "plan_parent", span),
                    span,
                },
                child: crate::ast::AstBox::new(JsonTableSpecificPlan::Siblings(
                    JsonTablePlanSiblings {
                        plan_type: JsonTableSiblingsPlanType::Cross,
                        siblings: (vec![
                            leaf(prefix, "plan_left", span),
                            leaf(prefix, "plan_right", span),
                        ])
                        .into(),
                        span: span,
                    },
                )),
                span: span,
            },
        ))),
        error_behavior: Some(JsonTableErrorBehavior::Empty),
        span,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Fixtures {
    constructor: JsonConstructor,
    exists: JsonExists,
    value: JsonValue,
    query: JsonQuery,
    serialize: JsonSerialize,
    object: JsonObject,
    array: JsonArray,
    table: JsonTable,
    default_plan: JsonTablePlan,
}
fn fixtures(prefix: &str, span: Span) -> Fixtures {
    Fixtures {
        constructor: JsonConstructor {
            expression: Expr::Binary(BinaryExpr {
                left: crate::ast::AstBox::new(expr(prefix, "ctor_left", span)),
                operator: BinaryOperator::Add,
                right: crate::ast::AstBox::new(expr(prefix, "ctor_right", span)),
                span: span,
            }),
            format: JsonFormat::Utf32,
            span,
        },
        exists: JsonExists {
            path_invocation: invocation(prefix, "exists", span),
            error_behavior: JsonExistsErrorBehavior::Unknown,
            span,
        },
        value: JsonValue {
            path_invocation: invocation(prefix, "value", span),
            returned_type: Some(nested_type(prefix, "value_return", span)),
            empty_behavior: JsonValueBehavior::Default,
            empty_default: Some(expr(prefix, "value_empty", span)),
            error_behavior: JsonValueBehavior::Default,
            error_default: Some(expr(prefix, "value_error", span)),
            span,
        },
        query: JsonQuery {
            path_invocation: invocation(prefix, "query", span),
            returned_type: Some(nested_type(prefix, "query_return", span)),
            output_format: Some(JsonFormat::Utf32),
            wrapper_behavior: JsonQueryWrapperBehavior::Conditional,
            quotes_behavior: Some(JsonQueryQuotesBehavior::Omit),
            empty_behavior: JsonQueryBehavior::EmptyArray,
            error_behavior: JsonQueryBehavior::EmptyObject,
            span,
        },
        serialize: JsonSerialize {
            expression: expr(prefix, "serialize_input", span),
            input_format: JsonFormat::Utf8,
            returned_type: Some(nested_type(prefix, "serialize_return", span)),
            output_format: Some(JsonFormat::Utf16),
            error_behavior: JsonSerializeErrorBehavior::Null,
            span,
        },
        object: JsonObject {
            members: vec![
                JsonObjectMember {
                    key: expr(prefix, "key_a", span),
                    value: expr(prefix, "object_a", span),
                    format: Some(JsonFormat::Json),
                    span,
                },
                JsonObjectMember {
                    key: expr(prefix, "key_b", span),
                    value: expr(prefix, "object_b", span),
                    format: None,
                    span,
                },
            ],
            null_on_null: false,
            unique_keys: true,
            returned_type: Some(nested_type(prefix, "object_return", span)),
            output_format: Some(JsonFormat::Utf8),
            span,
        },
        array: JsonArray {
            elements: vec![
                JsonArrayElement {
                    value: expr(prefix, "array_a", span),
                    format: None,
                    span,
                },
                JsonArrayElement {
                    value: expr(prefix, "array_b", span),
                    format: Some(JsonFormat::Utf16),
                    span,
                },
            ],
            null_on_null: true,
            returned_type: Some(nested_type(prefix, "array_return", span)),
            output_format: Some(JsonFormat::Utf32),
            span,
        },
        table: table(prefix, span),
        default_plan: JsonTablePlan::Default(JsonTableDefaultPlan {
            parent_child: JsonTableParentChildPlanType::Outer,
            siblings: JsonTableSiblingsPlanType::Union,
            span,
        }),
    }
}
impl Fixtures {
    fn visit(&self, visitor: &mut impl Visit) {
        visitor.visit_json_constructor(&self.constructor);
        visitor.visit_json_exists(&self.exists);
        visitor.visit_json_value(&self.value);
        visitor.visit_json_query(&self.query);
        visitor.visit_json_serialize(&self.serialize);
        visitor.visit_json_object(&self.object);
        visitor.visit_json_array(&self.array);
        visitor.visit_json_table(&self.table);
        visitor.visit_json_table_plan(&self.default_plan);
    }
    fn fold(self, folder: &mut impl Fold) -> Self {
        Self {
            constructor: folder.fold_json_constructor(self.constructor),
            exists: folder.fold_json_exists(self.exists),
            value: folder.fold_json_value(self.value),
            query: folder.fold_json_query(self.query),
            serialize: folder.fold_json_serialize(self.serialize),
            object: folder.fold_json_object(self.object),
            array: folder.fold_json_array(self.array),
            table: folder.fold_json_table(self.table),
            default_plan: folder.fold_json_table_plan(self.default_plan),
        }
    }
    fn assert_same_syntax(&self, other: &Self) {
        assert!(self.constructor.syntax_eq(&other.constructor));
        assert!(self.exists.syntax_eq(&other.exists));
        assert!(self.value.syntax_eq(&other.value));
        assert!(self.query.syntax_eq(&other.query));
        assert!(self.serialize.syntax_eq(&other.serialize));
        assert!(self.object.syntax_eq(&other.object));
        assert!(self.array.syntax_eq(&other.array));
        assert!(self.table.syntax_eq(&other.table));
        assert!(self.default_plan.syntax_eq(&other.default_plan));
    }
}

#[derive(Default)]
struct Trace {
    events: Vec<String>,
    hooks: BTreeSet<&'static str>,
}
macro_rules! visit_hook {
    ($method:ident, $walk:ident, $ty:ty, $label:literal) => {
        fn $method(&mut self, node: &$ty) {
            self.hooks.insert($label);
            $walk(self, node);
        }
    };
}
impl Visit for Trace {
    fn visit_ident(&mut self, node: &Ident) {
        self.events.push(format!("id:{}", node.value));
    }
    fn visit_type_name(&mut self, node: &TypeName) {
        self.events.push(format!(
            "type:{}",
            match &node.kind {
                TypeKind::Generic(value) => value.name.value.as_str(),
                _ => panic!("generic fixture"),
            }
        ));
        walk_type_name(self, node);
    }
    fn visit_expr(&mut self, node: &Expr) {
        walk_expr(self, node);
    }
    fn visit_numeric_parameter(&mut self, node: &NumericParameter) {
        self.events.push(format!("number:{}", node.spelling));
    }
    fn visit_json_path_string(&mut self, node: &JsonPathString) {
        self.hooks.insert("json_path_string");
        self.events.push(format!("path:{}", node.value));
    }
    visit_hook!(
        visit_json_path_invocation,
        walk_json_path_invocation,
        JsonPathInvocation,
        "json_path_invocation"
    );
    visit_hook!(
        visit_json_path_parameter,
        walk_json_path_parameter,
        JsonPathParameter,
        "json_path_parameter"
    );
    visit_hook!(
        visit_json_constructor,
        walk_json_constructor,
        JsonConstructor,
        "json_constructor"
    );
    visit_hook!(
        visit_json_exists,
        walk_json_exists,
        JsonExists,
        "json_exists"
    );
    visit_hook!(visit_json_value, walk_json_value, JsonValue, "json_value");
    visit_hook!(visit_json_query, walk_json_query, JsonQuery, "json_query");
    visit_hook!(
        visit_json_serialize,
        walk_json_serialize,
        JsonSerialize,
        "json_serialize"
    );
    visit_hook!(
        visit_json_object,
        walk_json_object,
        JsonObject,
        "json_object"
    );
    visit_hook!(
        visit_json_object_member,
        walk_json_object_member,
        JsonObjectMember,
        "json_object_member"
    );
    visit_hook!(visit_json_array, walk_json_array, JsonArray, "json_array");
    visit_hook!(
        visit_json_array_element,
        walk_json_array_element,
        JsonArrayElement,
        "json_array_element"
    );
    visit_hook!(visit_json_table, walk_json_table, JsonTable, "json_table");
    visit_hook!(
        visit_json_table_ordinality_column,
        walk_json_table_ordinality_column,
        JsonTableOrdinalityColumn,
        "json_table_ordinality_column"
    );
    visit_hook!(
        visit_json_table_value_column,
        walk_json_table_value_column,
        JsonTableValueColumn,
        "json_table_value_column"
    );
    visit_hook!(
        visit_json_table_query_column,
        walk_json_table_query_column,
        JsonTableQueryColumn,
        "json_table_query_column"
    );
    visit_hook!(
        visit_json_table_nested_columns,
        walk_json_table_nested_columns,
        JsonTableNestedColumns,
        "json_table_nested_columns"
    );
    visit_hook!(
        visit_json_table_default_plan,
        walk_json_table_default_plan,
        JsonTableDefaultPlan,
        "json_table_default_plan"
    );
    visit_hook!(
        visit_json_table_plan_leaf,
        walk_json_table_plan_leaf,
        JsonTablePlanLeaf,
        "json_table_plan_leaf"
    );
    visit_hook!(
        visit_json_table_plan_parent_child,
        walk_json_table_plan_parent_child,
        JsonTablePlanParentChild,
        "json_table_plan_parent_child"
    );
    visit_hook!(
        visit_json_table_plan_siblings,
        walk_json_table_plan_siblings,
        JsonTablePlanSiblings,
        "json_table_plan_siblings"
    );
    visit_hook!(
        visit_json_table_column,
        walk_json_table_column,
        JsonTableColumn,
        "json_table_column"
    );
    visit_hook!(
        visit_json_table_plan,
        walk_json_table_plan,
        JsonTablePlan,
        "json_table_plan"
    );
    visit_hook!(
        visit_json_table_specific_plan,
        walk_json_table_specific_plan,
        JsonTableSpecificPlan,
        "json_table_specific_plan"
    );
}

#[derive(Default)]
struct Rename {
    hooks: BTreeSet<&'static str>,
}
macro_rules! fold_hook {
    ($method:ident, $fold:ident, $ty:ty, $label:literal) => {
        fn $method(&mut self, node: $ty) -> $ty {
            self.hooks.insert($label);
            $fold(self, node)
        }
    };
}
impl Fold for Rename {
    fn fold_ident(&mut self, mut node: Ident) -> Ident {
        node.value.insert_str(0, "renamed_");
        node
    }
    fn fold_expr(&mut self, node: Expr) -> Expr {
        fold_expr(self, node)
    }
    fn fold_json_path_string(&mut self, mut node: JsonPathString) -> JsonPathString {
        self.hooks.insert("json_path_string");
        node.value.insert_str(0, "renamed_");
        node
    }
    fold_hook!(
        fold_json_path_invocation,
        fold_json_path_invocation,
        JsonPathInvocation,
        "json_path_invocation"
    );
    fold_hook!(
        fold_json_path_parameter,
        fold_json_path_parameter,
        JsonPathParameter,
        "json_path_parameter"
    );
    fold_hook!(
        fold_json_constructor,
        fold_json_constructor,
        JsonConstructor,
        "json_constructor"
    );
    fold_hook!(
        fold_json_exists,
        fold_json_exists,
        JsonExists,
        "json_exists"
    );
    fold_hook!(fold_json_value, fold_json_value, JsonValue, "json_value");
    fold_hook!(fold_json_query, fold_json_query, JsonQuery, "json_query");
    fold_hook!(
        fold_json_serialize,
        fold_json_serialize,
        JsonSerialize,
        "json_serialize"
    );
    fold_hook!(
        fold_json_object,
        fold_json_object,
        JsonObject,
        "json_object"
    );
    fold_hook!(
        fold_json_object_member,
        fold_json_object_member,
        JsonObjectMember,
        "json_object_member"
    );
    fold_hook!(fold_json_array, fold_json_array, JsonArray, "json_array");
    fold_hook!(
        fold_json_array_element,
        fold_json_array_element,
        JsonArrayElement,
        "json_array_element"
    );
    fold_hook!(fold_json_table, fold_json_table, JsonTable, "json_table");
    fold_hook!(
        fold_json_table_ordinality_column,
        fold_json_table_ordinality_column,
        JsonTableOrdinalityColumn,
        "json_table_ordinality_column"
    );
    fold_hook!(
        fold_json_table_value_column,
        fold_json_table_value_column,
        JsonTableValueColumn,
        "json_table_value_column"
    );
    fold_hook!(
        fold_json_table_query_column,
        fold_json_table_query_column,
        JsonTableQueryColumn,
        "json_table_query_column"
    );
    fold_hook!(
        fold_json_table_nested_columns,
        fold_json_table_nested_columns,
        JsonTableNestedColumns,
        "json_table_nested_columns"
    );
    fold_hook!(
        fold_json_table_default_plan,
        fold_json_table_default_plan,
        JsonTableDefaultPlan,
        "json_table_default_plan"
    );
    fold_hook!(
        fold_json_table_plan_leaf,
        fold_json_table_plan_leaf,
        JsonTablePlanLeaf,
        "json_table_plan_leaf"
    );
    fold_hook!(
        fold_json_table_plan_parent_child,
        fold_json_table_plan_parent_child,
        JsonTablePlanParentChild,
        "json_table_plan_parent_child"
    );
    fold_hook!(
        fold_json_table_plan_siblings,
        fold_json_table_plan_siblings,
        JsonTablePlanSiblings,
        "json_table_plan_siblings"
    );
    fold_hook!(
        fold_json_table_column,
        fold_json_table_column,
        JsonTableColumn,
        "json_table_column"
    );
    fold_hook!(
        fold_json_table_plan,
        fold_json_table_plan,
        JsonTablePlan,
        "json_table_plan"
    );
    fold_hook!(
        fold_json_table_specific_plan,
        fold_json_table_specific_plan,
        JsonTableSpecificPlan,
        "json_table_specific_plan"
    );
}
fn expected_hooks() -> BTreeSet<&'static str> {
    [
        "json_path_string",
        "json_path_invocation",
        "json_path_parameter",
        "json_constructor",
        "json_exists",
        "json_value",
        "json_query",
        "json_serialize",
        "json_object",
        "json_object_member",
        "json_array",
        "json_array_element",
        "json_table",
        "json_table_ordinality_column",
        "json_table_value_column",
        "json_table_query_column",
        "json_table_nested_columns",
        "json_table_default_plan",
        "json_table_plan_leaf",
        "json_table_plan_parent_child",
        "json_table_plan_siblings",
        "json_table_column",
        "json_table_plan",
        "json_table_specific_plan",
    ]
    .into_iter()
    .collect()
}

#[test]
fn every_real_hook_is_reachable_and_fold_transforms_all_owned_children() {
    let original = fixtures("", s());
    let mut visitor = Trace::default();
    original.visit(&mut visitor);
    assert_eq!(visitor.hooks, expected_hooks());
    let mut folder = Rename::default();
    let transformed = original.clone().fold(&mut folder);
    assert_eq!(folder.hooks, expected_hooks());
    // The expected tree is authored independently from the transformation. This
    // also detects erased formats/behaviors, swapped defaults and lost spans.
    assert_eq!(transformed, fixtures("renamed_", s()));
    assert_eq!(original, fixtures("", s()));
}

#[test]
fn table_visit_preserves_parameter_column_nested_and_plan_order() {
    let mut visitor = Trace::default();
    visitor.visit_json_table(&table("", s()));
    assert_eq!(
        visitor.events,
        [
            "id:table_input",
            "path:table_path",
            "id:table_name",
            "id:table_a",
            "id:table_arg_a",
            "id:table_b",
            "id:table_arg_b",
            "id:ordinal",
            "id:v_name",
            "type:v_type",
            "id:v_type",
            "type:v_type_leaf",
            "id:v_type_leaf",
            "number:17",
            "path:v_path",
            "id:v_empty",
            "id:v_error",
            "id:q_name",
            "type:q_type",
            "id:q_type",
            "type:q_type_leaf",
            "id:q_type_leaf",
            "number:17",
            "path:q_path",
            "path:nested_path",
            "id:nested_name",
            "id:nested_value",
            "type:nested_type",
            "id:nested_type",
            "id:nested_ordinal",
            "id:plan_parent",
            "id:plan_left",
            "id:plan_right",
        ]
    );
}

#[test]
fn object_keys_are_expressions_and_return_types_are_traversed() {
    let fixture = fixtures("", s());
    let mut visitor = Trace::default();
    visitor.visit_json_object(&fixture.object);
    assert_eq!(
        visitor.events,
        [
            "id:key_a",
            "id:object_a",
            "id:key_b",
            "id:object_b",
            "type:object_return",
            "id:object_return",
            "type:object_return_leaf",
            "id:object_return_leaf",
            "number:17"
        ]
    );
    let mut visitor = Trace::default();
    visitor.visit_json_constructor(&fixture.constructor);
    assert_eq!(visitor.events, ["id:ctor_left", "id:ctor_right"]);
    let mut visitor = Trace::default();
    visitor.visit_json_value(&fixture.value);
    assert_eq!(
        &visitor.events[visitor.events.len() - 2..],
        ["id:value_empty", "id:value_error"]
    );
}

#[test]
fn source_relocation_is_ignored_by_syntax_eq_at_all_json_roots() {
    let original = fixtures("", s());
    let relocated = fixtures("", Span::new(300, 309));
    assert_ne!(original, relocated);
    original.assert_same_syntax(&relocated);
    relocated.assert_same_syntax(&original);
}

fn assert_distinct<T: Clone + SyntaxEq + std::fmt::Debug>(
    original: &T,
    change: impl FnOnce(&mut T),
) {
    let mut other = original.clone();
    change(&mut other);
    assert!(
        !original.syntax_eq(&other),
        "change was ignored: {original:?} versus {other:?}"
    );
    assert!(!other.syntax_eq(original), "SyntaxEq must be symmetric");
}

#[test]
fn query_omitted_options_and_each_explicit_clause_remain_distinct() {
    let inv = JsonPathInvocation::plain(expr("", "payload", s()), path("", "strict $.a", s()), s());
    let q = JsonQuery::plain(inv, s());
    assert_distinct(&q, |n| n.output_format = Some(JsonFormat::Json));
    assert_distinct(&q, |n| {
        n.quotes_behavior = Some(JsonQueryQuotesBehavior::Keep)
    });
    assert_distinct(&q, |n| {
        n.quotes_behavior = Some(JsonQueryQuotesBehavior::Omit)
    });
    assert_distinct(&q, |n| {
        n.returned_type = Some(simple_type("", "varchar", s()))
    });
    for mode in [
        JsonQueryWrapperBehavior::Conditional,
        JsonQueryWrapperBehavior::Unconditional,
    ] {
        assert_distinct(&q, |n| n.wrapper_behavior = mode);
    }
    for mode in [
        JsonQueryBehavior::Error,
        JsonQueryBehavior::EmptyArray,
        JsonQueryBehavior::EmptyObject,
    ] {
        assert_distinct(&q, |n| n.empty_behavior = mode);
        assert_distinct(&q, |n| n.error_behavior = mode);
    }
}

#[test]
fn invocation_names_passing_formats_values_and_order_are_syntax_facts() {
    let inv = invocation("", "call", s());
    assert_distinct(&inv, |n| n.input_expression = expr("", "other", s()));
    assert_distinct(&inv, |n| n.input_format = JsonFormat::Json);
    assert_distinct(&inv, |n| n.json_path.value = "strict $.different".into());
    assert_distinct(&inv, |n| n.path_name = None);
    assert_distinct(&inv, |n| {
        n.path_name.as_mut().unwrap().value = "other_name".into()
    });
    assert_distinct(&inv, |n| n.path_parameters.swap(0, 1));
    assert_distinct(&inv, |n| {
        n.path_parameters.pop();
    });
    assert_distinct(&inv, |n| {
        n.path_parameters[0].format = Some(JsonFormat::Json)
    });
    assert_distinct(&inv, |n| {
        n.path_parameters[0].name.value = "renamed_parameter".into()
    });
    assert_distinct(&inv, |n| {
        n.path_parameters[1].parameter = expr("", "other_argument", s())
    });
}

#[test]
fn independent_empty_and_error_defaults_are_not_interchangeable() {
    let fixture = fixtures("", s());
    assert_distinct(&fixture.value, |n| {
        std::mem::swap(&mut n.empty_default, &mut n.error_default)
    });
    assert_distinct(&fixture.value, |n| {
        n.empty_default = Some(expr("", "different_empty", s()))
    });
    assert_distinct(&fixture.value, |n| {
        n.error_default = Some(expr("", "different_error", s()))
    });
    assert_distinct(&fixture.value, |n| {
        n.empty_behavior = JsonValueBehavior::Null;
        n.empty_default = None;
    });
    assert_distinct(&fixture.value, |n| {
        n.error_behavior = JsonValueBehavior::Error;
        n.error_default = None;
    });
    assert_distinct(&fixture.value, |n| n.returned_type = None);
    assert_distinct(&value_column("", s()), |n| {
        std::mem::swap(&mut n.empty_default, &mut n.error_default)
    });
}

#[test]
fn constructors_serialization_and_collection_options_are_not_dropped() {
    let f = fixtures("", s());
    assert_distinct(&f.constructor, |n| n.format = JsonFormat::Json);
    assert_distinct(&f.constructor, |n| n.expression = expr("", "other", s()));
    for mode in [
        JsonExistsErrorBehavior::False,
        JsonExistsErrorBehavior::True,
        JsonExistsErrorBehavior::Error,
    ] {
        assert_distinct(&f.exists, |n| n.error_behavior = mode);
    }
    assert_distinct(&f.serialize, |n| n.expression = expr("", "other", s()));
    assert_distinct(&f.serialize, |n| n.input_format = JsonFormat::Json);
    assert_distinct(&f.serialize, |n| n.returned_type = None);
    assert_distinct(&f.serialize, |n| n.output_format = None);
    assert_distinct(&f.serialize, |n| {
        n.error_behavior = JsonSerializeErrorBehavior::Error
    });
    assert_distinct(&f.object, |n| n.null_on_null = true);
    assert_distinct(&f.object, |n| n.unique_keys = false);
    assert_distinct(&f.object, |n| n.returned_type = None);
    assert_distinct(&f.object, |n| n.output_format = None);
    assert_distinct(&f.object, |n| n.members.swap(0, 1));
    assert_distinct(&f.object, |n| {
        let member = &mut n.members[0];
        std::mem::swap(&mut member.key, &mut member.value);
    });
    assert_distinct(&f.object, |n| n.members[1].format = Some(JsonFormat::Json));
    assert_distinct(&f.array, |n| n.elements.swap(0, 1));
    assert_distinct(&f.array, |n| n.null_on_null = false);
    assert_distinct(&f.array, |n| n.returned_type = None);
    assert_distinct(&f.array, |n| n.output_format = None);
    assert_distinct(&f.array, |n| n.elements[0].format = Some(JsonFormat::Json));
    assert_distinct(&f.array, |n| {
        n.elements[0].value = expr("", "other_array_element", s())
    });
}

#[test]
fn typed_table_columns_paths_plans_and_order_remain_observable() {
    let t = table("", s());
    assert_distinct(&t, |n| n.columns.swap(0, 1));
    assert_distinct(&t, |n| n.plan = None);
    assert_distinct(&t, |n| n.error_behavior = None);
    assert_distinct(&t, |n| {
        n.error_behavior = Some(JsonTableErrorBehavior::Error)
    });
    let v = value_column("", s());
    assert_distinct(&v, |n| n.name.value = "other".into());
    assert_distinct(&v, |n| clear_type_arguments(&mut n.data_type));
    assert_distinct(&v, |n| n.json_path = None);
    let q = query_column("", s());
    assert_distinct(&q, |n| n.name.value = "other_query_column".into());
    assert_distinct(&q, |n| clear_type_arguments(&mut n.data_type));
    assert_distinct(&q, |n| n.format = JsonFormat::Json);
    assert_distinct(&q, |n| n.json_path = None);
    assert_distinct(&q, |n| {
        n.wrapper_behavior = JsonQueryWrapperBehavior::Without
    });
    assert_distinct(&q, |n| n.quotes_behavior = None);
    assert_distinct(&q, |n| n.empty_behavior = JsonQueryBehavior::Null);
    assert_distinct(&q, |n| n.error_behavior = Some(JsonQueryBehavior::Null));
    let JsonTableColumn::Nested(nested) = &t.columns[3] else {
        panic!("nested fixture")
    };
    assert_distinct(nested, |n| n.path_name = None);
    assert_distinct(nested, |n| n.columns.swap(0, 1));
    assert_distinct(nested, |n| n.json_path.value = "strict $.other".into());
    assert_distinct(&t, |n| {
        n.columns[0] = JsonTableColumn::Value(value_column("", s()))
    });
}

#[test]
fn parent_plan_leaf_is_visited_folded_and_compared_independently_of_child() {
    let t = table("", s());
    let Some(JsonTablePlan::Specific(JsonTableSpecificPlan::ParentChild(plan))) = &t.plan else {
        panic!("parent-child fixture")
    };
    assert_distinct(plan, |n| n.parent.name.value = "different_parent".into());
    assert_distinct(plan, |n| n.plan_type = JsonTableParentChildPlanType::Outer);
    assert_distinct(plan, |n| {
        n.child = super::AstBox::new(leaf("", "other_child", s()))
    });
    let JsonTableSpecificPlan::Siblings(siblings) = plan.child.as_ref() else {
        panic!("siblings fixture")
    };
    assert_distinct(siblings, |n| n.siblings.swap(0, 1));
    assert_distinct(siblings, |n| n.plan_type = JsonTableSiblingsPlanType::Union);
    let mut visitor = Trace::default();
    visitor.visit_json_table_plan_parent_child(plan);
    assert_eq!(
        visitor.events,
        ["id:plan_parent", "id:plan_left", "id:plan_right"]
    );
    let renamed = Rename::default().fold_json_table_plan_parent_child(plan.clone());
    assert_eq!(renamed.parent.name.value, "renamed_plan_parent");
    assert_eq!(renamed.parent.span, plan.parent.span);
    assert_distinct(&fixtures("", s()).default_plan, |n| {
        let JsonTablePlan::Default(p) = n else {
            panic!("default fixture")
        };
        p.parent_child = JsonTableParentChildPlanType::Inner;
    });
    assert_distinct(&fixtures("", s()).default_plan, |n| {
        let JsonTablePlan::Default(p) = n else {
            panic!("default fixture")
        };
        p.siblings = JsonTableSiblingsPlanType::Cross;
    });
}

#[test]
fn column_none_survives_explicit_table_error_and_cannot_equal_explicit_null() {
    let original = table("", s());
    let folded = Rename::default().fold_json_table(original.clone());
    assert_eq!(folded.error_behavior, Some(JsonTableErrorBehavior::Empty));
    let JsonTableColumn::Query(query) = &folded.columns[2] else {
        panic!("query fixture")
    };
    assert_eq!(query.error_behavior, None);
    let JsonTableColumn::Nested(nested) = &folded.columns[3] else {
        panic!("nested fixture")
    };
    let JsonTableColumn::Value(value) = &nested.columns[0] else {
        panic!("value fixture")
    };
    assert_eq!(value.error_behavior, None);
    assert_eq!(value.error_default, None);
    assert_distinct(query, |n| n.error_behavior = Some(JsonQueryBehavior::Null));
    assert_distinct(value, |n| n.error_behavior = Some(JsonValueBehavior::Null));
    // This protects syntax inheritance facts, not an implemented runtime policy.
    assert_eq!(original.columns[2].span(), folded.columns[2].span());
}

#[test]
fn plain_json_query_matches_the_pinned_arrow_migration_profile_exactly() {
    let input = expr("", "payload", s());
    let p = JsonPathString::new("strict $.a[0]".into(), Span::new(21, 36));
    let invocation = JsonPathInvocation::plain(input.clone(), p.clone(), s());
    assert_eq!(invocation.input_expression, input);
    assert_eq!(invocation.json_path, p);
    assert_eq!(invocation.input_format, JsonFormat::Json);
    assert_eq!(invocation.path_name, None);
    assert_eq!(invocation.path_parameters, vec![]);
    let q = JsonQuery::plain(invocation.clone(), Span::new(1, 40));
    assert_eq!(q.path_invocation, invocation);
    assert_eq!(q.returned_type, None);
    assert_eq!(q.output_format, None);
    assert_eq!(q.wrapper_behavior, JsonQueryWrapperBehavior::Without);
    assert_eq!(q.quotes_behavior, None);
    assert_eq!(q.empty_behavior, JsonQueryBehavior::Null);
    assert_eq!(q.error_behavior, JsonQueryBehavior::Null);
    assert_eq!(q.span(), Span::new(1, 40));
}

#[test]
fn json_value_checks_both_directions_of_each_default_pair_independently() {
    use JsonAstInvariantError::{EmptyDefaultMismatch, ErrorDefaultMismatch};
    use JsonValueBehavior::{Default, Error, Null};
    let cases = [
        (Null, false, true),
        (Null, true, false),
        (Error, false, true),
        (Error, true, false),
        (Default, false, false),
        (Default, true, true),
    ];
    for (behavior, supplied, accepted) in cases {
        let empty = JsonValue::new(
            invocation("", "value", s()),
            None,
            behavior,
            supplied.then(|| expr("", "empty", s())),
            Null,
            None,
            s(),
        );
        assert_eq!(
            empty.map(|_| ()),
            if accepted {
                Ok(())
            } else {
                Err(EmptyDefaultMismatch)
            }
        );
        let error = JsonValue::new(
            invocation("", "value", s()),
            None,
            Null,
            None,
            behavior,
            supplied.then(|| expr("", "error", s())),
            s(),
        );
        assert_eq!(
            error.map(|_| ()),
            if accepted {
                Ok(())
            } else {
                Err(ErrorDefaultMismatch)
            }
        );
    }
    let both = JsonValue::new(
        invocation("", "value", s()),
        Some(simple_type("", "bigint", s())),
        Default,
        Some(expr("", "empty", s())),
        Default,
        Some(expr("", "error", s())),
        s(),
    )
    .unwrap();
    assert_eq!(both.empty_default, Some(expr("", "empty", s())));
    assert_eq!(both.error_default, Some(expr("", "error", s())));
}

#[test]
fn value_column_accepts_inherited_none_and_checks_each_default_pair() {
    use JsonAstInvariantError::{EmptyDefaultMismatch, ErrorDefaultMismatch};
    use JsonValueBehavior::{Default, Error, Null};
    let empty_cases = [
        (Null, false, true),
        (Null, true, false),
        (Error, false, true),
        (Error, true, false),
        (Default, false, false),
        (Default, true, true),
    ];
    for (behavior, supplied, accepted) in empty_cases {
        let result = JsonTableValueColumn::new(
            id("", "c", s()),
            simple_type("", "bigint", s()),
            None,
            behavior,
            supplied.then(|| expr("", "empty", s())),
            None,
            None,
            s(),
        );
        assert_eq!(
            result.map(|_| ()),
            if accepted {
                Ok(())
            } else {
                Err(EmptyDefaultMismatch)
            }
        );
    }
    let error_cases = [
        (None, false, true),
        (None, true, false),
        (Some(Null), false, true),
        (Some(Null), true, false),
        (Some(Error), false, true),
        (Some(Error), true, false),
        (Some(Default), false, false),
        (Some(Default), true, true),
    ];
    for (behavior, supplied, accepted) in error_cases {
        let result = JsonTableValueColumn::new(
            id("", "c", s()),
            simple_type("", "bigint", s()),
            None,
            Null,
            None,
            behavior,
            supplied.then(|| expr("", "error", s())),
            s(),
        );
        assert_eq!(
            result.map(|_| ()),
            if accepted {
                Ok(())
            } else {
                Err(ErrorDefaultMismatch)
            }
        );
    }
}

#[test]
fn table_constructor_rejects_empty_columns_but_keeps_omitted_plan_and_error() {
    let inv = invocation("", "table", s());
    assert_eq!(
        JsonTable::new(inv.clone(), vec![], None, None, s()),
        Err(JsonAstInvariantError::EmptyTableColumns)
    );
    let ordinal = JsonTableColumn::Ordinality(JsonTableOrdinalityColumn {
        name: id("", "o", s()),
        span: s(),
    });
    let accepted = JsonTable::new(inv.clone(), vec![ordinal.clone()], None, None, s()).unwrap();
    assert_eq!(accepted.path_invocation, inv);
    assert_eq!(accepted.columns, [ordinal]);
    assert_eq!(accepted.plan, None);
    assert_eq!(accepted.error_behavior, None);
}

#[test]
fn nested_constructor_requires_child_columns_without_inventing_a_path_name() {
    let p = path("", "lax $.items[*]", s());
    assert_eq!(
        JsonTableNestedColumns::new(p.clone(), None, vec![], s()),
        Err(JsonAstInvariantError::EmptyNestedColumns)
    );
    let child = JsonTableColumn::Query(query_column("", s()));
    let nested = JsonTableNestedColumns::new(p.clone(), None, vec![child.clone()], s()).unwrap();
    assert_eq!(nested.json_path, p);
    assert_eq!(nested.path_name, None);
    assert_eq!(nested.columns.as_slice(), &[child]);
}

#[test]
fn sibling_constructor_checks_cardinality_without_sorting_or_deduplicating() {
    for size in [0, 1] {
        let children = (0..size).map(|_| leaf("", "same", s())).collect();
        assert_eq!(
            JsonTablePlanSiblings::new(JsonTableSiblingsPlanType::Union, children, s()),
            Err(JsonAstInvariantError::TooFewPlanSiblings)
        );
    }
    let children = vec![leaf("", "z", s()), leaf("", "a", s()), leaf("", "z", s())];
    let accepted =
        JsonTablePlanSiblings::new(JsonTableSiblingsPlanType::Cross, children.clone(), s())
            .unwrap();
    assert_eq!(accepted.siblings.as_slice(), children.as_slice());
    assert_eq!(accepted.plan_type, JsonTableSiblingsPlanType::Cross);
    assert!(
        JsonTablePlanSiblings::new(
            JsonTableSiblingsPlanType::Union,
            vec![leaf("", "same", s()), leaf("", "same", s())],
            s()
        )
        .is_ok()
    );
}

#[test]
fn local_constructor_checks_are_not_a_recursive_validation_certificate() {
    let invalid = JsonTableColumn::Value(JsonTableValueColumn {
        empty_behavior: JsonValueBehavior::Default,
        empty_default: None,
        ..value_column("", s())
    });
    // JsonTable's Java constructor only owns nonempty columns. A separate
    // recursive validation entry must still visit the malformed child.
    let t = JsonTable::new(invocation("", "table", s()), vec![invalid], None, None, s()).unwrap();
    assert_eq!(t.validate_constructor_invariants(), Ok(()));
    let JsonTableColumn::Value(column) = &t.columns[0] else {
        panic!("value fixture")
    };
    assert_eq!(
        column.validate_constructor_invariants(),
        Err(JsonAstInvariantError::EmptyDefaultMismatch)
    );
}

#[test]
fn default_and_specific_plans_are_distinct_even_for_a_single_named_leaf() {
    let default = fixtures("", s()).default_plan;
    assert_distinct(&default, |n| {
        *n = JsonTablePlan::Specific(leaf("", "root", s()))
    });
}
