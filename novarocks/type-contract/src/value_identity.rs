// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed semantic identity and traversal under caller-owned observation.

use crate::{
    ValueTypeError,
    schema::{
        COMPARISON_HEADER_WORK, TypeComparisonStep, comparison_bytes_work, comparison_work_add,
    },
    value_type::{ValueField, ValueKind, ValueTypeRef},
};

/// Compare the same semantic fields as `ValueTypeRef::eq`, observing real work.
///
/// Storage ownership and cached depth/node counts are not identity. Shared
/// references do not skip observation. Names are compared byte by byte; scalar
/// parameters use the existing observed Arrow comparison. Validated immutable
/// constructors already bound recursive depth, so no shadow tree or scratch
/// allocation is required. A refusal returns immediately without later callbacks.
pub fn value_types_equal_observed<E: From<ValueTypeError>>(
    left: &ValueTypeRef,
    right: &ValueTypeRef,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    value_types_equal_weighted_observed(left, right, |_| observe())
}

pub fn value_types_equal_weighted_observed<E: From<ValueTypeError>>(
    left: &ValueTypeRef,
    right: &ValueTypeRef,
    mut observe: impl FnMut(TypeComparisonStep) -> Result<(), E>,
) -> Result<bool, E> {
    equal(left, right, &mut observe)
}

pub(crate) fn semantic_comparison_node_work(
    kind: &ValueKind,
) -> Result<usize, crate::ExecutionValueTypeError> {
    comparison_work_add(
        COMPARISON_HEADER_WORK,
        match kind {
            ValueKind::Scalar(ty) => crate::schema::type_comparison_node_work(ty)?,
            ValueKind::Nominal(_)
            | ValueKind::FixedSizeList { .. }
            | ValueKind::Struct(_)
            | ValueKind::Union(_) => COMPARISON_HEADER_WORK,
            _ => 0,
        },
    )
}
pub(crate) fn semantic_named_comparison_work(
    name: &str,
) -> Result<usize, crate::ExecutionValueTypeError> {
    comparison_work_add(
        2 * COMPARISON_HEADER_WORK,
        comparison_bytes_work(name.len())?,
    )
}

fn equal<E: From<ValueTypeError>>(
    left: &ValueTypeRef,
    right: &ValueTypeRef,
    observe: &mut impl FnMut(TypeComparisonStep) -> Result<(), E>,
) -> Result<bool, E> {
    observe(TypeComparisonStep::Header)?;
    if left.nullable() != right.nullable() {
        return Ok(false);
    }
    Ok(match (left.kind(), right.kind()) {
        (ValueKind::Scalar(left), ValueKind::Scalar(right)) => {
            crate::schema::arrow_validated_data_types_exact_weighted_observed(left, right, observe)?
        }
        (ValueKind::String, ValueKind::String) | (ValueKind::Binary, ValueKind::Binary) => true,
        (ValueKind::BoundedCharacter(left), ValueKind::BoundedCharacter(right)) => left == right,
        (ValueKind::Nominal(left), ValueKind::Nominal(right)) => {
            observe(TypeComparisonStep::Header)?;
            left == right
        }
        (ValueKind::List(left), ValueKind::List(right)) => equal(left, right, observe)?,
        (
            ValueKind::FixedSizeList {
                item: left,
                length: left_length,
            },
            ValueKind::FixedSizeList {
                item: right,
                length: right_length,
            },
        ) => {
            observe(TypeComparisonStep::Header)?;
            left_length == right_length && equal(left, right, observe)?
        }
        (ValueKind::Struct(left), ValueKind::Struct(right)) => {
            observe(TypeComparisonStep::Header)?;
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(right) {
                if !named_fields_equal(left, right, observe)? {
                    return Ok(false);
                }
            }
            true
        }
        (
            ValueKind::Map {
                key: left_key,
                value: left_value,
            },
            ValueKind::Map {
                key: right_key,
                value: right_value,
            },
        ) => equal(left_key, right_key, observe)? && equal(left_value, right_value, observe)?,
        (ValueKind::Union(left), ValueKind::Union(right)) => {
            observe(TypeComparisonStep::Header)?;
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(right) {
                observe(TypeComparisonStep::Header)?;
                if left.type_id != right.type_id
                    || !named_fields_equal(&left.field, &right.field, observe)?
                {
                    return Ok(false);
                }
            }
            true
        }
        _ => false,
    })
}

fn named_fields_equal<E: From<ValueTypeError>>(
    left: &ValueField,
    right: &ValueField,
    observe: &mut impl FnMut(TypeComparisonStep) -> Result<(), E>,
) -> Result<bool, E> {
    observe(TypeComparisonStep::Header)?;
    let left_name = left.name().as_bytes();
    let right_name = right.name().as_bytes();
    observe(TypeComparisonStep::Header)?;
    if left_name.len() != right_name.len() {
        return Ok(false);
    }
    for (left, right) in left_name.iter().zip(right_name) {
        observe(TypeComparisonStep::Bytes { inspected_bytes: 2 })?;
        if left != right {
            return Ok(false);
        }
    }
    equal(left.value_type(), right.value_type(), observe)
}

/// Borrowed source events for the actual owner's construction/resource visitor.
/// Events grant no capacity and report no allocation facts or estimates.
#[derive(Clone, Copy, Debug)]
pub enum ValueSemanticVisit<'a> {
    Node(&'a ValueTypeRef),
    NamedField(&'a ValueField),
}

/// Walk existing immutable semantics in order, without copying any source facts.
///
/// Named fields are visited before their children. List/Map edges use structural
/// roles and introduce no synthetic field names. A caller borrows node parameters,
/// child collections and original name bytes from the event, and owns any work or
/// allocation admission associated with its use of them.
pub fn visit_value_type_observed<'a, E>(
    root: &'a ValueTypeRef,
    mut observe: impl FnMut(ValueSemanticVisit<'a>) -> Result<(), E>,
) -> Result<(), E> {
    visit(root, &mut observe)
}

fn visit<'a, E>(
    root: &'a ValueTypeRef,
    observe: &mut impl FnMut(ValueSemanticVisit<'a>) -> Result<(), E>,
) -> Result<(), E> {
    observe(ValueSemanticVisit::Node(root))?;
    match root.kind() {
        ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } => visit(item, observe)?,
        ValueKind::Struct(fields) => {
            for field in fields {
                observe(ValueSemanticVisit::NamedField(field))?;
                visit(field.value_type(), observe)?;
            }
        }
        ValueKind::Map { key, value } => {
            visit(key, observe)?;
            visit(value, observe)?;
        }
        ValueKind::Union(fields) => {
            for field in fields {
                observe(ValueSemanticVisit::NamedField(&field.field))?;
                visit(field.field.value_type(), observe)?;
            }
        }
        ValueKind::Scalar(_)
        | ValueKind::String
        | ValueKind::BoundedCharacter(_)
        | ValueKind::Binary
        | ValueKind::Nominal(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, TimeUnit};

    use super::*;
    use crate::{
        ValueLogicalType,
        value_type::{ValueTypeStorageHold, ValueTypeStorageLifetime, ValueUnionField},
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Refusal {
        Cancelled,
        Deadline,
        Resource,
        Type(ValueTypeError),
    }
    impl From<ValueTypeError> for Refusal {
        fn from(value: ValueTypeError) -> Self {
            Self::Type(value)
        }
    }

    fn value(kind: ValueKind, nullable: bool) -> ValueTypeRef {
        ValueTypeRef::try_new(kind, nullable, ValueTypeStorageLifetime::pure()).unwrap()
    }

    fn named(name: &str, kind: ValueKind) -> ValueField {
        ValueField::new(name, value(kind, true), ValueTypeStorageLifetime::pure())
    }

    fn fixture() -> ValueTypeRef {
        value(
            ValueKind::Struct(
                vec![
                    named("payload", ValueKind::List(value(ValueKind::String, true))),
                    named(
                        "lookup",
                        ValueKind::Map {
                            key: value(ValueKind::Scalar(DataType::Int64), false),
                            value: value(ValueKind::Nominal(ValueLogicalType::Json), true),
                        },
                    ),
                    named(
                        "choice",
                        ValueKind::Union(
                            vec![
                                ValueUnionField {
                                    type_id: 1,
                                    field: named("text", ValueKind::String),
                                },
                                ValueUnionField {
                                    type_id: 3,
                                    field: named("bytes", ValueKind::Binary),
                                },
                            ]
                            .into(),
                        ),
                    ),
                ]
                .into(),
            ),
            true,
        )
    }

    #[test]
    fn observed_identity_agrees_with_ordinary_equality_for_all_semantic_fields() {
        let values = [
            fixture(),
            fixture(),
            value(ValueKind::String, true),
            value(ValueKind::String, false),
            value(ValueKind::Binary, true),
            value(ValueKind::Nominal(ValueLogicalType::Json), true),
            value(ValueKind::Nominal(ValueLogicalType::Uuid), true),
            value(ValueKind::Nominal(ValueLogicalType::LargeInt), true),
            value(ValueKind::Scalar(DataType::Decimal128(12, 2)), true),
            value(ValueKind::Scalar(DataType::Decimal128(12, 3)), true),
            value(
                ValueKind::Scalar(DataType::Timestamp(TimeUnit::Microsecond, None)),
                true,
            ),
            value(
                ValueKind::Scalar(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                )),
                true,
            ),
            value(
                ValueKind::FixedSizeList {
                    item: value(ValueKind::String, true),
                    length: 2,
                },
                true,
            ),
            value(
                ValueKind::FixedSizeList {
                    item: value(ValueKind::String, true),
                    length: 3,
                },
                true,
            ),
            value(
                ValueKind::Struct(vec![named("a", ValueKind::String)].into()),
                true,
            ),
            value(
                ValueKind::Struct(vec![named("b", ValueKind::String)].into()),
                true,
            ),
            value(ValueKind::List(value(ValueKind::String, true)), true),
            value(ValueKind::List(value(ValueKind::String, false)), true),
            value(
                ValueKind::Struct(
                    vec![named("a", ValueKind::String), named("b", ValueKind::Binary)].into(),
                ),
                true,
            ),
            value(
                ValueKind::Struct(
                    vec![named("b", ValueKind::Binary), named("a", ValueKind::String)].into(),
                ),
                true,
            ),
            value(
                ValueKind::Map {
                    key: value(ValueKind::String, false),
                    value: value(ValueKind::Binary, true),
                },
                true,
            ),
            value(
                ValueKind::Map {
                    key: value(ValueKind::Binary, false),
                    value: value(ValueKind::String, true),
                },
                true,
            ),
        ];
        for left in &values {
            for right in &values {
                assert_eq!(
                    value_types_equal_observed::<Refusal>(left, right, || Ok(())).unwrap(),
                    left == right
                );
            }
        }
        let union = |ids: [i8; 2], names: [&str; 2]| {
            value(
                ValueKind::Union(
                    ids.into_iter()
                        .zip(names)
                        .map(|(type_id, name)| ValueUnionField {
                            type_id,
                            field: named(name, ValueKind::String),
                        })
                        .collect(),
                ),
                true,
            )
        };
        let original = union([1, 3], ["a", "b"]);
        for other in [
            union([1, 4], ["a", "b"]),
            union([3, 1], ["b", "a"]),
            union([1, 3], ["a", "c"]),
        ] {
            assert_ne!(original, other);
            assert!(!value_types_equal_observed::<Refusal>(&original, &other, || Ok(())).unwrap());
        }
    }

    struct Hold;
    impl ValueTypeStorageHold for Hold {}

    #[test]
    fn independently_owned_storage_does_not_change_identity_or_observation() {
        let create = || {
            ValueTypeRef::try_new(
                ValueKind::List(value(ValueKind::String, false)),
                true,
                ValueTypeStorageLifetime::held(Arc::new(Hold)),
            )
            .unwrap()
        };
        let left = create();
        let right = create();
        assert!(!left.shares_storage(&right));
        let count = |right: &ValueTypeRef| {
            let mut callbacks = 0;
            assert!(
                value_types_equal_observed::<Refusal>(&left, right, || {
                    callbacks += 1;
                    Ok(())
                })
                .unwrap()
            );
            callbacks
        };
        assert_eq!(left, right);
        assert_eq!(count(&right), count(&left));
        assert_eq!(count(&left), 2);
    }

    #[test]
    fn every_actual_comparison_callback_preserves_the_primary_refusal() {
        let left = fixture();
        let right = fixture();
        let mut total = 0;
        assert!(
            value_types_equal_observed::<Refusal>(&left, &right, || {
                total += 1;
                Ok(())
            })
            .unwrap()
        );
        for cause in [Refusal::Cancelled, Refusal::Deadline, Refusal::Resource] {
            for stop in 1..=total {
                let mut callbacks = 0;
                let result = value_types_equal_observed(&left, &right, || {
                    callbacks += 1;
                    if callbacks == stop {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(result, Err(cause.clone()));
                assert_eq!(callbacks, stop);
            }
        }
    }

    #[test]
    fn named_field_comparison_observes_actual_utf8_bytes_and_stops_at_the_mismatch() {
        let name = "界".repeat(700);
        let structure = |name: &str| {
            value(
                ValueKind::Struct(vec![named(name, ValueKind::String)].into()),
                true,
            )
        };
        let left = structure(&name);
        let right = structure(&name);
        let mut equal_callbacks = 0;
        assert!(
            value_types_equal_observed::<Refusal>(&left, &right, || {
                equal_callbacks += 1;
                Ok(())
            })
            .unwrap()
        );
        // Root node, Struct arity, field, name length, child node, and the
        // bytes actually compared, including all bytes of multibyte characters.
        assert_eq!(equal_callbacks, name.len() + 5);

        let other_name = format!("{}海{}", "界".repeat(3), "界".repeat(696));
        assert_eq!(name.len(), other_name.len());
        let other = structure(&other_name);
        let first_mismatch = name
            .bytes()
            .zip(other_name.bytes())
            .position(|(left, right)| left != right)
            .unwrap();
        let mut callbacks = 0;
        assert!(
            !value_types_equal_observed::<Refusal>(&left, &other, || {
                callbacks += 1;
                Ok(())
            })
            .unwrap()
        );
        // The child is never visited after a name mismatch.
        assert_eq!(callbacks, 4 + first_mismatch + 1);
        let mut callbacks = 0;
        assert_eq!(
            value_types_equal_observed(&left, &right, || {
                callbacks += 1;
                if callbacks == name.len() {
                    Err(Refusal::Deadline)
                } else {
                    Ok(())
                }
            }),
            Err(Refusal::Deadline)
        );
        assert_eq!(callbacks, name.len());
    }

    #[test]
    fn source_visits_borrow_original_nodes_and_named_fields_in_order() {
        let root = fixture();
        let mut names = Vec::new();
        let mut nodes = 0;
        visit_value_type_observed(&root, |event| {
            match event {
                ValueSemanticVisit::Node(node) => {
                    nodes += 1;
                    assert!(node.depth() <= root.depth());
                }
                ValueSemanticVisit::NamedField(field) => names.push(field.name()),
            }
            Ok::<_, Refusal>(())
        })
        .unwrap();
        assert_eq!(nodes, root.expanded_nodes());
        assert_eq!(names, ["payload", "lookup", "choice", "text", "bytes"]);
        let ValueKind::Struct(fields) = root.kind() else {
            panic!("expected Struct");
        };
        assert!(std::ptr::eq(names[0].as_ptr(), fields[0].name().as_ptr()));
    }

    #[test]
    fn source_visit_refusal_has_no_later_events_or_completion_callback() {
        let root = fixture();
        let mut total = 0;
        visit_value_type_observed(&root, |_| {
            total += 1;
            Ok::<_, Refusal>(())
        })
        .unwrap();
        for stop in 1..=total {
            let mut callbacks = 0;
            let result = visit_value_type_observed(&root, |_| {
                callbacks += 1;
                if callbacks == stop {
                    Err(Refusal::Resource)
                } else {
                    Ok(())
                }
            });
            assert_eq!(result, Err(Refusal::Resource));
            assert_eq!(callbacks, stop);
        }
    }
}
