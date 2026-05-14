// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.

//! Encode FE-level `DeletePredicateTerms` into the wire `DeletePredicatePb`
//! that gets persisted into rowset metadata for DUP/UNIQUE/AGG managed-lake
//! tables. `sub_predicates` is left empty: it is the legacy hybrid
//! key/value string format used by the StarRocks shared-nothing path, and
//! lake mode reads only the structured `binary_predicates` / `in_predicates`
//! / `is_null_predicates` fields.

use crate::engine::delete_predicate_translate::{
    BinaryTerm, CmpOp, DeletePredicateTerms, InTerm, IsNullTerm,
};
use crate::service::grpc_client::proto::starrocks::{
    BinaryPredicatePb, DeletePredicatePb, InPredicatePb, IsNullPredicatePb,
};

pub fn build_delete_predicate_pb(
    terms: &DeletePredicateTerms,
    version: i32,
) -> DeletePredicatePb {
    DeletePredicatePb {
        version,
        sub_predicates: Vec::new(),
        in_predicates: terms.in_list.iter().map(in_to_pb).collect(),
        binary_predicates: terms.binary.iter().map(binary_to_pb).collect(),
        is_null_predicates: terms.is_null.iter().map(isnull_to_pb).collect(),
    }
}

fn binary_to_pb(term: &BinaryTerm) -> BinaryPredicatePb {
    // StarRocks BE's delete-predicate reader (see `parse_delete_binary_op` in
    // src/formats/starrocks/plan.rs) accepts the symbolic forms only. The
    // textual `EQ`/`NE`/... names from the proto enum are intentionally NOT
    // recognized — they exist only for legacy hybrid sub_predicates strings.
    BinaryPredicatePb {
        column_name: Some(term.column.clone()),
        op: Some(
            match term.op {
                CmpOp::Eq => "=",
                CmpOp::Ne => "!=",
                CmpOp::Lt => "<",
                CmpOp::Le => "<=",
                CmpOp::Gt => ">",
                CmpOp::Ge => ">=",
            }
            .to_string(),
        ),
        value: Some(term.value.clone()),
    }
}

fn in_to_pb(term: &InTerm) -> InPredicatePb {
    InPredicatePb {
        column_name: Some(term.column.clone()),
        is_not_in: Some(term.is_not_in),
        values: term.values.clone(),
    }
}

fn isnull_to_pb(term: &IsNullTerm) -> IsNullPredicatePb {
    IsNullPredicatePb {
        column_name: Some(term.column.clone()),
        is_not_null: Some(term.is_not_null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_binary_and_in_and_isnull() {
        let mut terms = DeletePredicateTerms::default();
        terms.binary.push(BinaryTerm {
            column: "id".into(),
            op: CmpOp::Eq,
            value: "42".into(),
        });
        terms.in_list.push(InTerm {
            column: "name".into(),
            is_not_in: false,
            values: vec!["a".into(), "b".into()],
        });
        terms.is_null.push(IsNullTerm {
            column: "deleted_at".into(),
            is_not_null: false,
        });

        let pb = build_delete_predicate_pb(&terms, 7);
        assert_eq!(pb.version, 7);
        assert!(
            pb.sub_predicates.is_empty(),
            "sub_predicates must stay empty (lake mode)"
        );
        assert_eq!(pb.binary_predicates.len(), 1);
        assert_eq!(pb.binary_predicates[0].column_name.as_deref(), Some("id"));
        assert_eq!(pb.binary_predicates[0].op.as_deref(), Some("="));
        assert_eq!(pb.binary_predicates[0].value.as_deref(), Some("42"));
        assert_eq!(pb.in_predicates.len(), 1);
        assert_eq!(pb.in_predicates[0].column_name.as_deref(), Some("name"));
        assert_eq!(pb.in_predicates[0].is_not_in, Some(false));
        assert_eq!(pb.in_predicates[0].values, vec!["a", "b"]);
        assert_eq!(pb.is_null_predicates.len(), 1);
        assert_eq!(
            pb.is_null_predicates[0].column_name.as_deref(),
            Some("deleted_at")
        );
        assert_eq!(pb.is_null_predicates[0].is_not_null, Some(false));
    }

    #[test]
    fn cmp_op_mapped_with_flipped_comparators() {
        let mut terms = DeletePredicateTerms::default();
        for op in [CmpOp::Lt, CmpOp::Le, CmpOp::Gt, CmpOp::Ge, CmpOp::Ne] {
            terms.binary.push(BinaryTerm {
                column: "c".into(),
                op,
                value: "1".into(),
            });
        }
        let pb = build_delete_predicate_pb(&terms, 1);
        let ops: Vec<&str> = pb
            .binary_predicates
            .iter()
            .map(|p| p.op.as_deref().expect("op"))
            .collect();
        assert_eq!(ops, vec!["<", "<=", ">", ">=", "!="]);
    }
}
