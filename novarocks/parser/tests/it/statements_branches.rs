// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{Expr, StatementSaveMode, SyntaxEq, TrinoStatement};
use novarocks_parser::{ParseError, ParserError, trino_parser, trino_printer};

fn roundtrip(source: &str) -> TrinoStatement {
    let original =
        trino_parser::parse_statement(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    let output = trino_printer::print_statement(&original).unwrap();
    let reparsed = trino_parser::parse_statement(&output).unwrap();
    assert!(original.syntax_eq(&reparsed), "{source}\n{output}");
    original
}

#[test]
fn create_branch_keeps_its_save_mode_original_names_and_typed_property() {
    let source = "  CREATE BRANCH IF NOT EXISTS \"New.Name\" WITH (snapshot_id = ?, reset = DEFAULT) IN TABLE lake.ns.\"table.name\" FROM \"Base.Name\"";
    let TrinoStatement::CreateBranch(branch) = roundtrip(source) else {
        panic!("expected CREATE BRANCH");
    };
    assert_eq!(branch.save_mode, StatementSaveMode::Ignore);
    assert_eq!(branch.span.start(), 2);
    assert_eq!(branch.span.end(), source.len());
    assert_eq!(branch.branch_name.value, "New.Name");
    assert!(branch.branch_name.quoted);
    assert_eq!(branch.table_name.parts.len(), 3);
    assert_eq!(branch.table_name.parts[2].value, "table.name");
    let from = branch.from_branch.as_ref().unwrap();
    assert!(from.quoted);
    assert_eq!(from.value, "Base.Name");
    assert_eq!(&source[from.span.start()..from.span.end()], "\"Base.Name\"");
    assert_eq!(branch.properties[0].name.value, "snapshot_id");
    assert!(matches!(&branch.properties[0].value,
        Some(Expr::Parameter(value)) if value.position == 0 && &source[value.span.start()..value.span.end()] == "?"));
    assert_eq!(branch.properties[1].name.value, "reset");
    assert!(branch.properties[1].value.is_none());

    assert!(matches!(
        roundtrip("CREATE OR REPLACE BRANCH b IN TABLE t"),
        TrinoStatement::CreateBranch(node)
            if node.save_mode == StatementSaveMode::Replace && node.from_branch.is_none(),
    ));
    assert!(matches!(
        roundtrip("CREATE BRANCH if IN TABLE t"),
        TrinoStatement::CreateBranch(node)
            if node.save_mode == StatementSaveMode::Fail && node.branch_name.value == "if",
    ));
}

#[test]
fn branch_drop_fast_forward_and_show_keep_their_distinct_facts() {
    let TrinoStatement::DropBranch(drop) =
        roundtrip("DROP BRANCH IF EXISTS \"drop.me\" IN TABLE lake.s.t")
    else {
        panic!("expected DROP BRANCH");
    };
    assert!(drop.exists && drop.branch_name.quoted);
    assert_eq!(drop.branch_name.value, "drop.me");
    assert_eq!(drop.table_name.parts[2].value, "t");

    let source = "ALTER BRANCH \"from.me\" IN TABLE lake.s.t FAST FORWARD TO main";
    let TrinoStatement::FastForwardBranch(forward) = roundtrip(source) else {
        panic!("expected FAST FORWARD");
    };
    assert_eq!(forward.source_branch_name.value, "from.me");
    assert!(forward.source_branch_name.quoted);
    assert_eq!(forward.target_branch_name.value, "main");
    assert_eq!(forward.table_name.parts.len(), 3);

    let from = roundtrip("SHOW BRANCHES FROM TABLE lake.s.t");
    let inside = roundtrip("SHOW BRANCHES IN TABLE lake.s.t");
    assert!(from.syntax_eq(&inside));
    assert!(matches!(from, TrinoStatement::ShowBranches(node) if node.table_name.parts.len() == 3));
}

#[test]
fn branch_mode_collision_precedes_property_construction_and_old_forms_are_syntax_errors() {
    let source = "  CREATE OR REPLACE BRANCH IF NOT EXISTS b WITH (snapshot_id = 9223372036854775808) IN TABLE t";
    let error = trino_parser::parse_statement(source).unwrap_err();
    let ParserError::Parse(ParseError::UnexpectedToken { span, found, .. }) = error else {
        panic!("expected save-mode syntax error");
    };
    assert_eq!(found, "OR REPLACE and IF NOT EXISTS");
    assert_eq!(&source[span.start()..span.end()], "CREATE");

    for source in [
        "ALTER TABLE t CREATE BRANCH b",
        "CREATE BRANCH b AS OF VERSION 1 IN TABLE t",
        "CREATE BRANCH b IN TABLE t WITH (snapshot_id = 1)",
        "CREATE BRANCH b WITH () IN TABLE t",
        "CREATE BRANCH s.b IN TABLE t",
        "CREATE BRANCH b IN TABLE t FROM s.b",
        "DROP BRANCH t.b",
        "ALTER BRANCH b IN TABLE t FAST FORWARD main",
        "SHOW BRANCHES TABLE t",
        "SHOW BRANCHES IN TABLE t;",
    ] {
        assert!(
            matches!(
                trino_parser::parse_statement(source),
                Err(ParserError::Parse(_))
            ),
            "{source}",
        );
    }
}
