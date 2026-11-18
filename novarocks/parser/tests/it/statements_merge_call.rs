// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{
    self, Expr, Relation, SyntaxEq, TrinoMergeCase, TrinoStatement, Visit,
};
use novarocks_parser::{trino_parser, trino_printer};

fn round_trip(sql: &str, statement: &TrinoStatement) -> String {
    let rendered = trino_printer::print_statement(statement).unwrap();
    let reparsed = trino_parser::parse_statement(&rendered).unwrap();
    assert!(statement.syntax_eq(&reparsed), "{sql} became {rendered}");
    rendered
}

#[test]
fn merge_retains_target_source_actions_and_reference_parameter_order() {
    let sql = r#"MERGE INTO c.s.dst@"dev branch" AS "target"
USING (SELECT ? AS id) AS src ON "target".id = ?
WHEN MATCHED AND ? THEN UPDATE SET id = ?, "value" = ?
WHEN MATCHED AND ? THEN DELETE
WHEN NOT MATCHED AND ? THEN INSERT (id, "value") VALUES (?, ?)"#;
    let statement = trino_parser::parse_statement(sql).unwrap();
    let TrinoStatement::Merge(node) = &statement else {
        panic!("expected MERGE");
    };
    let Relation::Aliased(alias) = &node.target else {
        panic!("expected the target alias wrapper");
    };
    let Relation::Table(target) = alias.relation.as_ref() else {
        panic!("expected the target table");
    };
    assert_eq!(
        (alias.alias.value.as_str(), alias.alias.quoted),
        ("target", true)
    );
    assert!(alias.column_names.is_none());
    let branch = target.branch.as_ref().unwrap();
    assert_eq!((branch.value.as_str(), branch.quoted), ("dev branch", true));
    assert_eq!(
        &sql[branch.span.start()..branch.span.end()],
        r#""dev branch""#
    );
    assert!(target.query_period.is_none());
    let Relation::Aliased(source) = &node.source else {
        panic!("expected the source alias");
    };
    assert!(matches!(
        source.relation.as_ref(),
        Relation::TableSubquery(_)
    ));
    let [
        TrinoMergeCase::Update {
            condition: Some(Expr::Parameter(condition)),
            assignments,
            ..
        },
        TrinoMergeCase::Delete {
            condition: Some(Expr::Parameter(delete_condition)),
            ..
        },
        TrinoMergeCase::Insert {
            condition: Some(Expr::Parameter(insert_condition)),
            columns,
            values,
            ..
        },
    ] = node.cases.as_slice()
    else {
        panic!("expected the original three case actions");
    };
    assert_eq!(condition.position, 4);
    assert_eq!(delete_condition.position, 5);
    assert_eq!(insert_condition.position, 6);
    assert_eq!(assignments[0].target.value, "id");
    assert_eq!(assignments[1].target.value, "value");
    assert!(assignments[1].target.quoted);
    assert_eq!(columns.len(), 2);
    assert_eq!(values.len(), 2);
    assert_eq!(parameters_in_text(&statement), [0, 1, 4, 2, 3, 5, 6, 7, 8]);
    let printed = round_trip(sql, &statement);
    assert!(printed.contains(r#"@"dev branch" AS "target""#));
    assert!(printed.contains(r#"UPDATE SET id = ?, "value" = ?"#));
}

#[test]
fn merge_leaves_conditions_and_column_arity_to_the_original_syntax() {
    let sql = "MERGE INTO dst d USING s ON CASE WHEN s.id IS NULL THEN FALSE ELSE TRUE END \
               WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT VALUES (1)";
    let statement = trino_parser::parse_statement(sql).unwrap();
    let TrinoStatement::Merge(node) = &statement else {
        panic!("expected MERGE");
    };
    assert!(matches!(node.predicate, Expr::SearchedCase(_)));
    assert!(matches!(
        node.cases.as_slice(),
        [TrinoMergeCase::Delete { condition: None, .. },
         TrinoMergeCase::Insert { condition: None, columns, values, .. }]
        if columns.is_empty() && values.len() == 1
    ));
    round_trip(sql, &statement);

    let sql = "MERGE INTO dst USING src ON TRUE WHEN NOT MATCHED THEN INSERT (x, y) VALUES (1)";
    let statement = trino_parser::parse_statement(sql).unwrap();
    let TrinoStatement::Merge(node) = &statement else {
        panic!("expected MERGE");
    };
    assert!(matches!(
        node.cases.as_slice(),
        [TrinoMergeCase::Insert { columns, values, .. }]
        if columns.len() == 2 && values.len() == 1
    ));
    round_trip(sql, &statement);
}

#[test]
fn call_keeps_named_positional_and_empty_arguments_with_exact_ordinals() {
    let sql = r#"CALL c.system."proc"("where" => (WITH q AS (SELECT ?) SELECT ?), ?, x => 1)"#;
    let statement = trino_parser::parse_statement(sql).unwrap();
    let TrinoStatement::Call(call) = &statement else {
        panic!("expected CALL");
    };
    assert_eq!(call.name.parts.last().unwrap().value, "proc");
    assert!(call.name.parts.last().unwrap().quoted);
    let [named, positional, final_named] = call.arguments.as_slice() else {
        panic!("expected three arguments");
    };
    assert_eq!(named.name.as_ref().unwrap().value, "where");
    assert!(named.name.as_ref().unwrap().quoted);
    assert!(matches!(named.value, Expr::Subquery(_)));
    assert!(positional.name.is_none());
    assert_eq!(final_named.name.as_ref().unwrap().value, "x");
    assert_eq!(parameters_in_text(&statement), [1, 0, 2]);
    round_trip(sql, &statement);

    let sql = "CALL c.system.proc()";
    let statement = trino_parser::parse_statement(sql).unwrap();
    let TrinoStatement::Call(call) = &statement else {
        panic!("expected CALL");
    };
    assert!(call.arguments.is_empty());
    round_trip(sql, &statement);
}

#[test]
fn merge_and_call_reject_forms_absent_from_the_reference_grammar() {
    for sql in [
        "MERGE INTO dst USING src ON TRUE",
        "MERGE INTO dst USING src ON TRUE WHEN NOT MATCHED BY SOURCE THEN DELETE",
        "MERGE INTO dst USING src ON TRUE WHEN MATCHED THEN INSERT VALUES (1)",
        "MERGE INTO dst USING src ON TRUE WHEN NOT MATCHED THEN DELETE",
        "MERGE INTO dst AS d(x) USING src ON TRUE WHEN MATCHED THEN DELETE",
        "MERGE INTO dst USING src ON TRUE WHEN MATCHED THEN UPDATE SET d.x = 1",
        "MERGE INTO dst USING src ON TRUE WHEN NOT MATCHED THEN INSERT () VALUES (1)",
        "MERGE INTO dst USING src ON TRUE WHEN NOT MATCHED THEN INSERT VALUES ()",
        "CALL p",
        "CALL p(x =>)",
        "CALL p(?,)",
    ] {
        assert!(trino_parser::parse_statement(sql).is_err(), "{sql}");
    }
}

fn parameters_in_text(statement: &TrinoStatement) -> Vec<usize> {
    #[derive(Default)]
    struct Parameters(Vec<(usize, usize)>);
    impl Visit for Parameters {
        fn visit_expr(&mut self, node: &Expr) {
            if let Expr::Parameter(parameter) = node {
                self.0.push((parameter.span.start(), parameter.position));
            }
            ast::walk_expr(self, node);
        }
    }
    let mut parameters = Parameters::default();
    parameters.visit_trino_statement(statement);
    parameters.0.sort_unstable();
    parameters
        .0
        .into_iter()
        .map(|(_, position)| position)
        .collect()
}
