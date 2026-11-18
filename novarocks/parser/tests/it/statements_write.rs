// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{
    self, Expr, SetExpr, SyntaxEq, TrinoInsertMode, TrinoStatement, Visit,
};
use novarocks_parser::{Span, trino_parser, trino_printer};

fn source_text(sql: &str, span: Span) -> &str {
    &sql[span.start()..span.end()]
}

fn assert_round_trip(sql: &str, statement: &TrinoStatement) -> String {
    let printed = trino_printer::print_statement(statement).unwrap();
    let reparsed = trino_parser::parse_statement(&printed).unwrap();
    assert!(statement.syntax_eq(&reparsed), "{sql} became {printed}");
    printed
}

#[test]
fn insert_target_columns_and_query_use_their_own_source_ranges() {
    let sql = r#" /* preceding */ INSERT INTO ice.s.t@"audit-branch" ("ID", link)
        /* query starts after this comment */ WITH q AS (SELECT ?)
        SELECT ? FROM q /* following */ "#;
    let statement = trino_parser::parse_statement(sql).unwrap();
    let TrinoStatement::Insert(node) = &statement else {
        panic!("expected INSERT");
    };
    assert_eq!(node.mode, TrinoInsertMode::Into);
    let branch = node.table.branch.as_ref().unwrap();
    assert!(branch.quoted);
    assert_eq!(branch.value, "audit-branch");
    assert_eq!(source_text(sql, branch.span), r#""audit-branch""#);
    assert_eq!(source_text(sql, node.table.name.span), "ice.s.t");
    assert_eq!(
        source_text(sql, node.table.span),
        r#"INSERT INTO ice.s.t@"audit-branch""#
    );
    assert!(node.table.query_period.is_none());
    let columns = node.columns.as_ref().unwrap();
    assert_eq!((columns[0].value.as_str(), columns[0].quoted), ("ID", true));
    assert_eq!(
        (columns[1].value.as_str(), columns[1].quoted),
        ("link", false)
    );
    assert_eq!(
        source_text(sql, node.query.span),
        "WITH q AS (SELECT ?)\n        SELECT ? FROM q"
    );
    assert_eq!(node.span.end(), node.query.span.end());
    assert_positions_by_source(&statement, sql, &[1, 0]);
    let printed = assert_round_trip(sql, &statement);
    assert!(printed.contains(r#"ice.s.t@"audit-branch" ("ID", link)"#));

    let sql = "INSERT INTO t (SELECT ?) UNION ALL SELECT ?";
    let statement = trino_parser::parse_statement(sql).unwrap();
    let TrinoStatement::Insert(node) = &statement else {
        panic!("expected INSERT");
    };
    assert!(node.columns.is_none());
    assert!(matches!(node.query.body.as_ref(), SetExpr::SetOperation(_)));
    assert_eq!(
        source_text(sql, node.query.span),
        "(SELECT ?) UNION ALL SELECT ?"
    );
    assert_positions_by_source(&statement, sql, &[0, 1]);
    assert_round_trip(sql, &statement);
}

#[test]
fn overwrite_is_contextual_and_keeps_its_two_distinct_modes() {
    for (sql, mode, target) in [
        (
            "INSERT OVERWRITE t SELECT 1",
            TrinoInsertMode::Overwrite,
            "t",
        ),
        (
            "INSERT OVERWRITE INTO t (id) VALUES 1",
            TrinoInsertMode::Overwrite,
            "t",
        ),
        (
            "INSERT OVERWRITE PARTITIONS t SELECT 1",
            TrinoInsertMode::OverwritePartitions,
            "t",
        ),
        (
            "INSERT OVERWRITE PARTITIONS INTO t (id) VALUES 1",
            TrinoInsertMode::OverwritePartitions,
            "t",
        ),
        (
            "INSERT OVERWRITE partitions SELECT 1",
            TrinoInsertMode::Overwrite,
            "partitions",
        ),
        (
            "INSERT OVERWRITE partitions.s SELECT 1",
            TrinoInsertMode::Overwrite,
            "partitions.s",
        ),
        (
            "INSERT INTO overwrite SELECT 1",
            TrinoInsertMode::Into,
            "overwrite",
        ),
    ] {
        let statement = trino_parser::parse_statement(sql).unwrap();
        let TrinoStatement::Insert(node) = &statement else {
            panic!("expected INSERT");
        };
        assert_eq!(node.mode, mode, "{sql}");
        assert_eq!(source_text(sql, node.table.name.span), target);
        assert_round_trip(sql, &statement);
    }
    for (sql, mode, target, branch_name, quoted) in [
        (
            r#"INSERT OVERWRITE c.s.t@"retained" (id) SELECT ?"#,
            TrinoInsertMode::Overwrite,
            "c.s.t",
            "retained",
            true,
        ),
        (
            "INSERT OVERWRITE PARTITIONS s.t_branch@dev VALUES (99, 'us')",
            TrinoInsertMode::OverwritePartitions,
            "s.t_branch",
            "dev",
            false,
        ),
    ] {
        let statement = trino_parser::parse_statement(sql).unwrap();
        let TrinoStatement::Insert(node) = &statement else {
            panic!("expected INSERT");
        };
        assert_eq!(node.mode, mode);
        assert_eq!(source_text(sql, node.table.name.span), target);
        let branch = node.table.branch.as_ref().unwrap();
        assert_eq!(
            (branch.value.as_str(), branch.quoted),
            (branch_name, quoted)
        );
        assert_round_trip(sql, &statement);
    }

    let whole = trino_parser::parse_statement("INSERT OVERWRITE t SELECT 1").unwrap();
    let partitions =
        trino_parser::parse_statement("INSERT OVERWRITE PARTITIONS t SELECT 1").unwrap();
    assert!(!whole.syntax_eq(&partitions));
}

#[test]
fn update_assignments_and_delete_keep_branch_and_parameter_facts() {
    let sql = r#"UPDATE c.s.t@"dev branch" SET "where" = ?, b = (WITH q AS (SELECT ?) SELECT ?) WHERE id = ?"#;
    let statement = trino_parser::parse_statement(sql).unwrap();
    let TrinoStatement::Update(node) = &statement else {
        panic!("expected UPDATE");
    };
    assert_eq!(node.table.branch.as_ref().unwrap().value, "dev branch");
    assert_eq!(node.assignments.len(), 2);
    assert!(node.assignments[0].name.quoted);
    assert_eq!(node.assignments[0].name.value, "where");
    assert_eq!(source_text(sql, node.assignments[0].span), r#""where" = ?"#);
    assert_eq!(
        source_text(sql, node.assignments[1].span),
        "b = (WITH q AS (SELECT ?) SELECT ?)"
    );
    assert_eq!(
        source_text(sql, node.predicate.as_ref().unwrap().span()),
        "id = ?"
    );
    assert_positions_by_source(&statement, sql, &[0, 2, 1, 3]);
    let printed = assert_round_trip(sql, &statement);
    assert!(printed.contains(r#"@"dev branch" SET "where" = ?"#));

    let sql = "DELETE FROM c.s.t@dev WHERE id IN (WITH q AS (SELECT ?) SELECT ?)";
    let statement = trino_parser::parse_statement(sql).unwrap();
    let TrinoStatement::Delete(node) = &statement else {
        panic!("expected DELETE");
    };
    let branch = node.table.branch.as_ref().unwrap();
    assert_eq!(source_text(sql, branch.span), "dev");
    assert!(!branch.quoted);
    assert_positions_by_source(&statement, sql, &[1, 0]);
    assert_round_trip(sql, &statement);

    for sql in ["DELETE FROM c.s.t", "UPDATE c.s.t SET id = 1"] {
        let statement = trino_parser::parse_statement(sql).unwrap();
        match &statement {
            TrinoStatement::Delete(delete) => assert!(delete.predicate.is_none()),
            TrinoStatement::Update(update) => assert!(update.predicate.is_none()),
            _ => panic!("expected DELETE or UPDATE"),
        }
        assert_round_trip(sql, &statement);
    }
}

#[test]
fn write_leaf_rejects_forms_outside_the_grammar_and_two_extensions() {
    for sql in [
        "INSERT t SELECT 1",
        "INSERT INTO TABLE t VALUES 1",
        "INSERT INTO t() SELECT 1",
        "INSERT INTO t(id,) SELECT 1",
        "INSERT INTO t@ SELECT 1",
        "INSERT INTO t@a.b SELECT 1",
        "INSERT INTO t WITH SESSION x = 1 SELECT 1",
        "INSERT OVERWRITE TABLE t SELECT 1",
        "INSERT OVERWRITE t PARTITION (p = 1) VALUES 1",
        "INSERT OVERWRITE t@dev.extra SELECT 1",
        "INSERT OVERWRITE PARTITIONS t@ VALUES 1",
        r#"INSERT "OVERWRITE" t SELECT 1"#,
        "DELETE FROM t FOR VERSION AS OF 1",
        "DELETE FROM t AS x WHERE x.id = 1",
        "DELETE FROM t WHERE",
        "UPDATE t SET",
        "UPDATE t SET t.id = 1",
        "UPDATE t SET id = 1, WHERE id = 2",
        "UPDATE t AS a SET id = 1",
        "UPDATE t SET id = 1 FROM s",
    ] {
        assert!(trino_parser::parse_statement(sql).is_err(), "{sql}");
    }
}

fn assert_positions_by_source(statement: &TrinoStatement, sql: &str, expected: &[usize]) {
    #[derive(Default)]
    struct RecordParameters(Vec<(Span, usize)>);
    impl Visit for RecordParameters {
        fn visit_expr(&mut self, node: &Expr) {
            if let Expr::Parameter(parameter) = node {
                self.0.push((parameter.span, parameter.position));
            }
            ast::walk_expr(self, node);
        }
    }
    let mut parameters = RecordParameters::default();
    parameters.visit_trino_statement(statement);
    parameters.0.sort_by_key(|(span, _)| span.start());
    let actual: Vec<_> = parameters
        .0
        .iter()
        .map(|(span, position)| {
            assert_eq!(source_text(sql, *span), "?");
            *position
        })
        .collect();
    assert_eq!(actual, expected);
}
