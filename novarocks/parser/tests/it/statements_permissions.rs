// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{
    StatementPrincipalKind, SyntaxEq, TrinoGrantor, TrinoRoleSelection, TrinoStatement,
};
use novarocks_parser::{ParseError, ParserError, trino_parser, trino_printer};

fn round_trip(sql: &str) -> TrinoStatement {
    let statement =
        trino_parser::parse_statement(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    let printed = trino_printer::print_statement(&statement).unwrap();
    let reparse = trino_parser::parse_statement(&printed).unwrap();
    assert!(statement.syntax_eq(&reparse), "{sql} became {printed}");
    statement
}

#[test]
fn role_membership_keeps_catalog_grantor_and_reference_sets() {
    let original = round_trip(
        r#"GRANT r, "r", "SELECT" TO USER u, USER "u", ROLE u
           WITH ADMIN OPTION GRANTED BY CURRENT_ROLE IN "catalog""#,
    );
    let TrinoStatement::GrantRoles(node) = &original else {
        panic!("expected role grant");
    };
    assert_eq!(node.roles.len(), 2);
    assert!(!node.roles[0].quoted);
    assert!(node.roles[1].quoted);
    assert_eq!(node.grantees.len(), 2);
    assert_eq!(node.grantees[0].kind, StatementPrincipalKind::User);
    assert_eq!(node.grantees[1].kind, StatementPrincipalKind::Role);
    assert!(node.admin_option);
    assert!(matches!(node.grantor, Some(TrinoGrantor::CurrentRole)));
    assert!(node.catalog.as_ref().unwrap().quoted);
    let reordered = round_trip(
        r#"GRANT "SELECT", r TO ROLE u, USER u WITH ADMIN OPTION
           GRANTED BY CURRENT_ROLE IN "catalog""#,
    );
    assert!(original.syntax_eq(&reordered));

    let TrinoStatement::RevokeRoles(revoke) =
        round_trip("REVOKE ADMIN OPTION FOR r FROM ROLE group_a, bob GRANTED BY USER owner IN cat")
    else {
        panic!("expected role revoke");
    };
    assert!(revoke.admin_option);
    assert!(matches!(revoke.grantor, Some(TrinoGrantor::Principal(_))));
    assert_eq!(revoke.grantees[1].kind, StatementPrincipalKind::Unspecified);

    let TrinoStatement::CreateRole(create) =
        round_trip("CREATE ROLE reader WITH ADMIN CURRENT_USER IN cat")
    else {
        panic!("expected CREATE ROLE");
    };
    assert!(matches!(create.grantor, Some(TrinoGrantor::CurrentUser)));
    let TrinoStatement::DropRole(drop) = round_trip("DROP ROLE IF EXISTS reader IN cat") else {
        panic!("expected DROP ROLE");
    };
    assert!(drop.if_exists);
}

#[test]
fn object_privileges_preserve_raw_atoms_optional_kinds_and_branch() {
    let TrinoStatement::Grant(grant) = round_trip(
        r#"GRANT SeLeCt, create /* trivia */ branch, "VIP"
           ON BRANCH "dev branch" IN table cat.sch.t TO ROLE reader WITH GRANT OPTION"#,
    ) else {
        panic!("expected privilege grant");
    };
    assert_eq!(
        grant.privileges.unwrap(),
        ["SeLeCt", "create branch", r#""VIP""#]
    );
    assert_eq!(grant.object.entity_kind.as_deref(), Some("table"));
    assert_eq!(grant.object.branch.unwrap().value, "dev branch");
    assert!(grant.grant_option);

    // TO is nonreserved. A tentative object kind is valid only when
    // the complete object, delimiter, principal and tail parse.
    let TrinoStatement::Grant(no_kind) = round_trip("GRANT SELECT ON t TO TO") else {
        panic!("expected privilege grant without a kind");
    };
    assert!(no_kind.object.entity_kind.is_none());
    assert_eq!(no_kind.object.name.parts[0].value, "t");
    assert_eq!(no_kind.grantee.name.value, "TO");
    let TrinoStatement::Grant(with_kind) = round_trip("GRANT SELECT ON custom TO TO TO") else {
        panic!("expected a custom kind and a target named TO");
    };
    assert_eq!(with_kind.object.entity_kind.as_deref(), Some("custom"));
    assert_eq!(with_kind.object.name.parts[0].value, "TO");
    assert_eq!(with_kind.grantee.name.value, "TO");

    let TrinoStatement::Revoke(revoke) =
        round_trip("REVOKE GRANT OPTION FOR ALL PRIVILEGES ON SCHEMA cat.sch FROM USER u")
    else {
        panic!("expected privilege revoke");
    };
    assert!(revoke.privileges.is_none());
    assert!(revoke.grant_option_for);
    let TrinoStatement::Deny(deny) = round_trip("DENY CREATE BRANCH, DELETE ON TABLE t TO reader")
    else {
        panic!("expected DENY");
    };
    assert_eq!(deny.privileges.unwrap(), ["CREATE BRANCH", "DELETE"]);
}

#[test]
fn role_selection_and_inspection_keep_typed_forms() {
    for (sql, expected) in [
        ("SET ROLE ALL IN catalog", TrinoRoleSelection::All),
        ("SET ROLE NONE", TrinoRoleSelection::None),
    ] {
        let TrinoStatement::SetRole(node) = round_trip(sql) else {
            panic!("expected SET ROLE");
        };
        assert!(node.selection.syntax_eq(&expected));
    }
    let TrinoStatement::SetRole(named) = round_trip(r#"SET ROLE "ALL" IN cat"#) else {
        panic!("expected named role");
    };
    assert!(matches!(named.selection, TrinoRoleSelection::Named(name) if name.quoted));

    assert!(
        matches!(round_trip("SHOW GRANTS"), TrinoStatement::ShowGrants(node) if node.object.is_none())
    );
    assert!(matches!(
        round_trip("SHOW GRANTS ON BRANCH d IN TABLE c.s.t"),
        TrinoStatement::ShowGrants(node) if node.object.as_ref().and_then(|object| object.branch.as_ref()).is_some_and(|branch| branch.value == "d")
    ));
    assert!(
        matches!(round_trip("SHOW CURRENT ROLES IN c"), TrinoStatement::ShowRoles(node) if node.current && node.catalog.is_some())
    );
    assert!(
        matches!(round_trip("SHOW ROLE GRANTS FROM c"), TrinoStatement::ShowRoleGrants(node) if node.catalog.is_some())
    );
}

#[test]
fn generic_authorization_preserves_canonical_kind_without_claiming_other_alter_forms() {
    for (sql, expected_kind, expected_object) in [
        (
            "alter materialized view sch.v set authorization user owner",
            "MATERIALIZED VIEW",
            "v",
        ),
        (
            "alter materialized view set authorization user owner",
            "MATERIALIZED",
            "view",
        ),
        (
            "alter schema cat.sch set authorization role reader",
            "SCHEMA",
            "sch",
        ),
        (
            r#"alter "Mixed Kind" sch.obj set authorization reviewer"#,
            r#""MIXED KIND""#,
            "obj",
        ),
    ] {
        let TrinoStatement::SetAuthorization(node) = round_trip(sql) else {
            panic!("expected ALTER SET AUTHORIZATION");
        };
        assert_eq!(node.owned_entity_kind, expected_kind);
        assert_eq!(node.source.parts.last().unwrap().value, expected_object);
    }
}

#[test]
fn permission_grammar_rejects_wrong_family_options_and_reserved_role_atoms() {
    for sql in [
        "GRANT ALL PRIVILEGES TO USER u",
        "GRANT r TO u WITH GRANT OPTION",
        "GRANT SELECT ON TABLE t TO u WITH ADMIN OPTION",
        "REVOKE ADMIN OPTION FOR SELECT ON t FROM u",
        "REVOKE GRANT OPTION FOR r FROM u",
        "DENY SELECT ON TABLE TO u",
        "ALTER TABLE t SET AUTHORIZATION",
    ] {
        assert!(trino_parser::parse_statement(sql).is_err(), "{sql}");
    }
    let sql = "GRANT CREATE BRANCH TO USER u";
    let error = trino_parser::parse_statement(sql).unwrap_err();
    let ParserError::Parse(ParseError::UnexpectedToken { span, .. }) = error else {
        panic!("expected a construction parse error")
    };
    assert_eq!(&sql[span.start()..span.end()], "CREATE BRANCH");
}
