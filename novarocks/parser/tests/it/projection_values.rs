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

//! Source-shaped constructors use fixed SQL as the oracle; domain-authored
//! values preserve their original AST facts without source lexing.
//! These pure tests need no fixture or process and run with the parser package.

use novarocks_parser::{SYNTHETIC, Span, ast::*, build, printer::print_query};

fn query(source: &str) -> Query {
    novarocks_parser::parser::parse_query(source).unwrap()
}
fn selected(query: &Query) -> &Select {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("SELECT")
    };
    select
}
fn row_query(rows: Vec<Expr>) -> TrinoStatement {
    TrinoStatement::Query(build::query(SetExpr::Values(Values {
        rows: rows.into(),
        span: Span::new(0, 1),
    })))
}

#[test]
fn projection_producers_preserve_distinct_alias_names_targets_and_source_spans() {
    let implicit = query("SELECT ALL 1 \"label\", \"catalog\".t.*, *");
    let explicit = query("SELECT 1 AS \"label\", \"catalog\".t.*, *");
    assert!(implicit.syntax_eq(&explicit));
    let select = selected(&implicit);
    assert!(!select.distinct);
    let SelectItem::SingleColumn(column) = &select.projection[0] else {
        panic!("scalar projection")
    };
    assert_eq!(column.expression.span(), Span::new(11, 12));
    let alias = column.alias.as_ref().unwrap();
    assert_eq!(alias.value, "label");
    assert!(alias.quoted);
    assert_eq!(alias.span, Span::new(13, 20));
    assert_eq!(column.span, Span::new(11, 20));
    let SelectItem::AllColumns(all) = &select.projection[1] else {
        panic!("qualified wildcard")
    };
    assert!(all.aliases.is_empty());
    assert_eq!(all.span, Span::new(22, 35));
    let Expr::Dereference(target) = all.target.as_ref().unwrap() else {
        panic!("actual qualifier expression")
    };
    assert_eq!(target.field.as_ref().unwrap().value, "t");
    let Expr::Identifier(first) = &target.base else {
        panic!("actual qualifier root")
    };
    assert_eq!(first.value, "catalog");
    assert!(first.quoted);
    assert_eq!(first.span, Span::new(22, 31));
    assert!(
        matches!(&select.projection[2],SelectItem::AllColumns(all) if all.target.is_none()&&all.aliases.is_empty())
    );
    let distinct = query("SELECT DISTINCT 1 AS x");
    assert!(selected(&distinct).distinct);
    assert!(!implicit.syntax_eq(&distinct));
    let rendered = print_query(&implicit).unwrap();
    assert_eq!(rendered, "SELECT 1 AS \"label\", \"catalog\".t.*, *");
    assert!(implicit.syntax_eq(&query(&rendered)));
    assert!(
        matches!(&selected(&query("SELECT 1")).projection[0],SelectItem::SingleColumn(column) if column.alias.is_none())
    );
}

#[test]
fn values_rows_keep_outer_envelopes_inner_rows_and_original_borrowed_cells() {
    let source = "VALUES ROW(1), ROW(ROW(2, 3))";
    let parsed = query(source);
    let SetExpr::Values(values) = parsed.body.as_ref() else {
        panic!("VALUES")
    };
    assert_eq!(values.rows.len(), 2);
    for (index, text) in ["ROW(1)", "ROW(ROW(2, 3))"].into_iter().enumerate() {
        let Expr::Row(row) = &values.rows[index] else {
            panic!("true outer row")
        };
        let start = source.find(text).unwrap();
        assert_eq!(row.span, Span::new(start, start + text.len()));
        assert_eq!(row.fields.len(), 1);
        assert!(row.fields[0].name.is_none());
        let view = ValuesRowCells::new(&values.rows[index]);
        assert_eq!(view.len(), 1);
        assert!(!view.has_named_fields());
        assert!(!view.is_empty());
        assert!(std::ptr::eq(
            view.get(0).unwrap(),
            &row.fields[0].expression
        ));
        assert!(view.get(1).is_none());
        assert!(std::ptr::eq(
            view.iter().next().unwrap(),
            &row.fields[0].expression
        ));
        assert_eq!(row.fields[0].span, row.fields[0].expression.span());
    }
    let inner = ValuesRowCells::new(&values.rows[1]).get(0).unwrap();
    let Expr::Row(inner) = inner else {
        panic!("inner row is one cell")
    };
    assert_eq!(inner.fields.len(), 2);
    // Named fields are public AST facts; the old grammar has no producer for them.
    let named = Expr::Row(RowExpr {
        fields: vec![RowField {
            name: Some(build::ident("x", false)),
            expression: build::integer(4),
            span: SYNTHETIC,
        }]
        .into(),
        span: SYNTHETIC,
    });
    let Expr::Row(named_row) = &named else {
        unreachable!()
    };
    assert_eq!(named_row.fields[0].name.as_ref().unwrap().value, "x");
    assert!(ValuesRowCells::new(&named).has_named_fields());
    assert!(std::ptr::eq(
        ValuesRowCells::new(&named).get(0).unwrap(),
        &named_row.fields[0].expression
    ));
    let scalar = build::integer(9);
    let view = ValuesRowCells::new(&scalar);
    assert!(std::ptr::eq(view.get(0).unwrap(), &scalar));
    assert_eq!(view.iter().count(), 1);
    assert!(!view.has_named_fields());
    let generated = build::values(vec![vec![build::integer(1)]]).unwrap();
    let SetExpr::Values(generated) = generated else {
        unreachable!()
    };
    assert!(matches!(&generated.rows[0],Expr::Row(row) if row.fields.len()==1));
    assert!(build::values(Vec::new()).is_err());
    assert!(build::values(vec![Vec::new()]).is_err());
    validate_statement(&row_query(Vec::new())).unwrap();
    validate_statement(&row_query(vec![Expr::Row(RowExpr {
        fields: AstVec::new(),
        span: Span::new(0, 2),
    })]))
    .unwrap();
    assert!(novarocks_parser::parser::parse_query("VALUES ()").is_err());
    assert!(parsed.syntax_eq(&query(&print_query(&parsed).unwrap())));
}

#[test]
fn projection_hooks_keep_real_children_and_strict_target_alias_facts() {
    let original = build::integer(7);
    let span = Span::new(4, 19);
    let target = Expr::Row(RowExpr {
        fields: vec![RowField {
            name: None,
            expression: original,
            span,
        }]
        .into(),
        span,
    });
    let item = SelectItem::AllColumns(AstBox::new(AllColumns {
        target: Some(target),
        aliases: vec![build::ident("A", true), build::ident("b", false)],
        span,
    }));
    let SelectItem::AllColumns(all) = &item else {
        unreachable!()
    };
    let target = all.target.as_ref().unwrap();
    struct Observe {
        expressions: Vec<*const Expr>,
        names: Vec<(String, bool)>,
    }
    impl Visit for Observe {
        fn visit_expr(&mut self, node: &Expr) {
            self.expressions.push(node as *const Expr);
            walk_expr(self, node);
        }
        fn visit_ident(&mut self, node: &Ident) {
            self.names.push((node.value.clone(), node.quoted));
        }
    }
    let mut observed = Observe {
        expressions: Vec::new(),
        names: Vec::new(),
    };
    observed.visit_select_item(&item);
    assert_eq!(observed.expressions[0], target as *const Expr);
    assert_eq!(observed.names, [("A".into(), true), ("b".into(), false)]);
    struct Rename;
    impl Fold for Rename {
        fn fold_ident(&mut self, mut node: Ident) -> Ident {
            node.value.push_str("_next");
            node
        }
    }
    let renamed = Rename.fold_select_item(item.clone());
    let SelectItem::AllColumns(renamed) = &renamed else {
        unreachable!()
    };
    assert_eq!(renamed.aliases[0].value, "A_next");
    assert_eq!(renamed.aliases[1].value, "b_next");
    assert!(renamed.aliases[0].quoted);
    assert!(renamed.target.as_ref().unwrap().syntax_eq(target));
    let mut moved = item.clone();
    let SelectItem::AllColumns(node) = &mut moved else {
        unreachable!()
    };
    node.span = Span::new(40, 70);
    assert!(item.syntax_eq(&moved));
    let SelectItem::AllColumns(node) = &mut moved else {
        unreachable!()
    };
    node.aliases.swap(0, 1);
    assert!(!item.syntax_eq(&moved));
    let mut select = build::select(vec![item], None, None, None).unwrap();
    validate_statement(&TrinoStatement::Query(build::query(SetExpr::Select(
        AstBox::new(select.clone()),
    ))))
    .unwrap();
    select.projection.clear();
    assert!(
        validate_statement(&TrinoStatement::Query(build::query(SetExpr::Select(
            AstBox::new(select)
        ))))
        .is_err()
    );
    assert!(build::select(Vec::new(), None, None, None).is_err());
}
