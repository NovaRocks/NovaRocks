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

//! Validate parsed Iceberg reference actions against table metadata; produce a
//! `RefActionPlan` that the lower stage forwards to the executor.

#![allow(dead_code)]

use crate::analyze_error::AnalyzeError;
use crate::analyzer::iceberg_ref::{IcebergRefKind, SqlIcebergRefMetadata};
use novarocks_parser::ast::{
    AlterIcebergTable, IcebergReferenceAction, IcebergReferenceKind, IcebergTableAction,
    LiteralKind, ReferenceAnchor,
};

#[derive(Clone, Debug, PartialEq)]
pub struct RefActionPlan {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub action: RefAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RefAction {
    CreateBranch {
        name: String,
        snapshot_id: i64,
        replace: bool,
        if_not_exists: bool,
    },
    CreateTag {
        name: String,
        snapshot_id: i64,
        replace: bool,
        if_not_exists: bool,
    },
    DropBranch {
        name: String,
        if_exists: bool,
    },
    DropTag {
        name: String,
        if_exists: bool,
    },
}

/// Resolve the table, validate the action against current refs/snapshots,
/// and produce a `RefActionPlan`. Errors here are analyzer-time
/// (deterministic, fail-fast).
pub fn analyze_alter_iceberg_ref(
    stmt: &AlterIcebergTable,
    catalog: &str,
    namespace: &str,
    table: &str,
    table_metadata: &SqlIcebergRefMetadata,
) -> Result<RefActionPlan, AnalyzeError> {
    let IcebergTableAction::Reference(action) = &stmt.action else {
        return Err(AnalyzeError::invalid_query_shape(
            "iceberg ref: expected a branch or tag action",
            stmt.span,
        ));
    };
    let name = action_name(action);
    if name == "main" {
        return Err(AnalyzeError::invalid_argument(
            "iceberg ref: 'main' is reserved",
            action_name_ident(action).span,
        ));
    }

    let action = match action {
        IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Branch,
            name,
            anchor,
            if_not_exists,
            or_replace,
            options,
        } => {
            let _ = options;
            check_kind(table_metadata, name, IcebergRefKind::Branch)?;
            let snapshot_id = resolve_anchor(anchor, table_metadata, name)?;
            RefAction::CreateBranch {
                name: name.value.clone(),
                snapshot_id,
                replace: *or_replace,
                if_not_exists: *if_not_exists,
            }
        }
        IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Tag,
            name,
            anchor,
            if_not_exists,
            or_replace,
            options,
        } => {
            let _ = options;
            check_kind(table_metadata, name, IcebergRefKind::Tag)?;
            let snapshot_id = resolve_anchor(anchor, table_metadata, name)?;
            RefAction::CreateTag {
                name: name.value.clone(),
                snapshot_id,
                replace: *or_replace,
                if_not_exists: *if_not_exists,
            }
        }
        IcebergReferenceAction::Drop {
            kind: IcebergReferenceKind::Branch,
            name,
            if_exists,
        } => {
            check_kind(table_metadata, name, IcebergRefKind::Branch)?;
            RefAction::DropBranch {
                name: name.value.clone(),
                if_exists: *if_exists,
            }
        }
        IcebergReferenceAction::Drop {
            kind: IcebergReferenceKind::Tag,
            name,
            if_exists,
        } => {
            check_kind(table_metadata, name, IcebergRefKind::Tag)?;
            RefAction::DropTag {
                name: name.value.clone(),
                if_exists: *if_exists,
            }
        }
    };

    Ok(RefActionPlan {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
        action,
    })
}

fn action_name(a: &IcebergReferenceAction) -> &str {
    &action_name_ident(a).value
}

fn action_name_ident(a: &IcebergReferenceAction) -> &novarocks_parser::ast::Ident {
    match a {
        IcebergReferenceAction::Create { name, .. } | IcebergReferenceAction::Drop { name, .. } => {
            name
        }
    }
}

fn resolve_anchor(
    anchor: &ReferenceAnchor,
    metadata: &SqlIcebergRefMetadata,
    ref_name: &novarocks_parser::ast::Ident,
) -> Result<i64, AnalyzeError> {
    match anchor {
        ReferenceAnchor::Version(literal) => {
            let LiteralKind::Long {
                parsed_value: value,
                ..
            } = &literal.kind
            else {
                return Err(AnalyzeError::invalid_literal(
                    "iceberg ref: snapshot version must be a numeric literal",
                    literal.span,
                ));
            };
            let n = *value;
            if !metadata.has_snapshot(n) {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "iceberg ref: snapshot {n} not found; cannot anchor '{}'",
                        ref_name.value
                    ),
                    literal.span,
                ));
            }
            Ok(n)
        }
        ReferenceAnchor::CurrentMain => match metadata.current_snapshot_id() {
            Some(snapshot_id) => Ok(snapshot_id),
            None => Err(AnalyzeError::invalid_argument(
                "iceberg ref: cannot create branch on table without a current snapshot",
                ref_name.span,
            )),
        },
    }
}

/// If a ref of the given name exists, ensure its kind matches the expected
/// kind (branch vs tag). Mismatches are rejected.
fn check_kind(
    metadata: &SqlIcebergRefMetadata,
    name: &novarocks_parser::ast::Ident,
    expected: IcebergRefKind,
) -> Result<(), AnalyzeError> {
    if let Some(existing) = metadata.named_ref(&name.value) {
        let existing_kind = existing.kind.clone();
        if existing_kind != expected {
            let actual = match existing_kind {
                IcebergRefKind::Branch => "branch",
                IcebergRefKind::Tag => "tag",
            };
            let exp = match expected {
                IcebergRefKind::Branch => "branch",
                IcebergRefKind::Tag => "tag",
            };
            return Err(AnalyzeError::invalid_argument(
                format!("iceberg ref: '{}' is a {actual}, not a {exp}", name.value),
                name.span,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::analyzer::iceberg_ref::SqlIcebergNamedRef;
    use novarocks_parser::{
        Span,
        ast::{Ident, Literal, ObjectName},
    };

    fn metadata_empty() -> SqlIcebergRefMetadata {
        SqlIcebergRefMetadata::default()
    }

    fn metadata_with_branch(branch_name: &str) -> SqlIcebergRefMetadata {
        SqlIcebergRefMetadata::new(
            [1],
            vec![],
            BTreeMap::from([(
                branch_name.to_string(),
                SqlIcebergNamedRef {
                    snapshot_id: 1,
                    kind: IcebergRefKind::Branch,
                },
            )]),
            Some(1),
        )
    }

    fn ident(value: &str) -> Ident {
        Ident {
            value: value.to_string(),
            quoted: false,
            quote_style: None,
            span: Span::new(0, 0),
        }
    }

    fn make_stmt(action: IcebergReferenceAction) -> AlterIcebergTable {
        AlterIcebergTable {
            table: ObjectName {
                parts: vec![ident("c"), ident("s"), ident("t")],
                span: Span::new(0, 0),
            },
            action: IcebergTableAction::Reference(action),
            span: Span::new(0, 0),
        }
    }

    #[test]
    fn create_branch_main_rejected() {
        let md = metadata_empty();
        let stmt = make_stmt(IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Branch,
            name: ident("main"),
            anchor: ReferenceAnchor::CurrentMain,
            if_not_exists: false,
            or_replace: false,
            options: None,
        });
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(
            err.message().contains("'main' is reserved"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn create_branch_unknown_anchor_rejected() {
        let md = metadata_empty();
        let stmt = make_stmt(IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Branch,
            name: ident("dev"),
            anchor: ReferenceAnchor::Version(Literal {
                kind: LiteralKind::Long {
                    spelling: "99999".to_string(),
                    parsed_value: 99999,
                },
                span: Span::new(0, 0),
            }),
            if_not_exists: false,
            or_replace: false,
            options: None,
        });
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(
            err.message().contains("snapshot 99999 not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn create_tag_kind_mismatch_when_branch_exists() {
        let md = metadata_with_branch("dev");
        let stmt = make_stmt(IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Tag,
            name: ident("dev"),
            anchor: ReferenceAnchor::CurrentMain,
            if_not_exists: false,
            or_replace: false,
            options: None,
        });
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(
            err.message().contains("'dev' is a branch, not a tag"),
            "unexpected error: {err}"
        );
    }
}
