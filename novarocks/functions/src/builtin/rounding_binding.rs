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

//! Observed actual ROUND/TRUNCATE cast capability and result recipes.
//! This is the exact binder prerequisite, not an encoded runtime kernel.

#[cfg(test)]
use super::binding_control;
use crate::{FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionValueType};
use arrow_schema::{DataType, Field, UnionFields};
use novarocks_type_contract::{
    CompileCheckpoints, MAX_VALUE_TYPE_DEPTH, ValueLogicalType, ValueTypeRef,
};

/// A cast can select a nominal path even though its carrier is castable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CastCapability {
    Unavailable,
    Physical,
    NonPhysical,
}

/// Checked scalar target types are deliberately restricted to non-nested
/// primitives, so neither equality nor Arrow's leaf probe recurses.
#[derive(Clone, Copy, Debug)]
pub(super) enum CastTarget {
    Float64,
    Int64,
}
impl CastTarget {
    fn data_type(self) -> DataType {
        match self {
            Self::Float64 => DataType::Float64,
            Self::Int64 => DataType::Int64,
        }
    }
    fn exact(self, source: &DataType) -> bool {
        matches!(
            (self, source),
            (Self::Float64, DataType::Float64) | (Self::Int64, DataType::Int64)
        )
    }
    fn same_family(self, source: &DataType) -> bool {
        matches!(
            (self, source),
            (
                Self::Float64,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) | (
                Self::Int64,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        )
    }
}

fn mark(capability: CastCapability, semantic: &ValueTypeRef) -> CastCapability {
    if capability != CastCapability::Unavailable && semantic.domain().is_some() {
        CastCapability::NonPhysical
    } else {
        capability
    }
}
fn field_probe(
    field: &Field,
    semantic: &ValueTypeRef,
    target: CastTarget,
    depth: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CastCapability, FunctionBindingError> {
    work.step()?;
    cast_capability_typed(field.data_type(), semantic, target, depth, work)
}

/// The single Union-path author for binding and immutable cast recipes.
/// `child_depth` is the depth of each field, including nested encoded fields.
/// The result borrows the selected field and reports nominal identity after
/// Arrow's carrier choice; nominal identity never chooses another sibling.
pub(super) fn select_union_cast_field_typed<'a, 's>(
    fields: &'a UnionFields,
    semantic: &'s ValueTypeRef,
    target: CastTarget,
    child_depth: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<(i8, &'a Field, &'s ValueTypeRef, CastCapability)>, FunctionBindingError> {
    // Arrow iterates declaration order, including non-monotonic type IDs.
    for pass in 0..3 {
        work.step()?;
        for (index, (type_id, field)) in fields.iter().enumerate() {
            let child = semantic.child_type_at(index).ok_or_else(|| {
                FunctionBindingError::InvalidBinding("union semantic child is absent".into())
            })?;
            work.step()?;
            let capability = match pass {
                0 if target.exact(field.data_type()) => {
                    field_probe(field, child, target, child_depth, work)?
                }
                1 if target.same_family(field.data_type()) => {
                    field_probe(field, child, target, child_depth, work)?
                }
                2 => field_probe(field, child, target, child_depth, work)?,
                _ => CastCapability::Unavailable,
            };
            if capability != CastCapability::Unavailable {
                return Ok(Some((type_id, field, child, capability)));
            }
        }
    }
    Ok(None)
}

/// Preserve the original primitive capability and deferred-refusal boundary.
/// These two interval profiles were admitted for Int64 digits but never had a
/// successful runtime conversion; later SDK capability cleanup must not move
/// their actual nonempty-call failure into preparation.
pub(super) fn primitive_cast_capability(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (
            DataType::Interval(
                arrow_schema::IntervalUnit::YearMonth | arrow_schema::IntervalUnit::DayTime
            ),
            DataType::Int64
        )
    ) || arrow_cast::can_cast_types(source, target)
}

/// Mirrors the original scalar cast path, retaining the chosen logical identity.
/// The caller must first run the shared observed full-type preflight.
pub(super) fn cast_capability_typed(
    source: &DataType,
    semantic: &ValueTypeRef,
    target: CastTarget,
    depth: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CastCapability, FunctionBindingError> {
    work.step()?;
    if depth > MAX_VALUE_TYPE_DEPTH {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let capability = match source {
        DataType::Dictionary(_, value) => {
            cast_capability_typed(value, semantic, target, depth + 1, work)?
        }
        DataType::RunEndEncoded(_, value) => field_probe(value, semantic, target, depth + 1, work)?,
        DataType::FixedSizeList(value, 1) => field_probe(
            value,
            semantic
                .child_type_at(0)
                .ok_or(FunctionBindingError::NoMatchingOverload)?,
            target,
            depth + 1,
            work,
        )?,
        DataType::Union(fields, _) => {
            select_union_cast_field_typed(fields, semantic, target, depth + 1, work)?
                .map_or(CastCapability::Unavailable, |(_, _, _, capability)| {
                    capability
                })
        }
        // No recursive Arrow probe or nominal carrier inference for containers.
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => CastCapability::Unavailable,
        // Only primitive leaf inputs reach Arrow's existing capability owner.
        _ => {
            work.step()?;
            if primitive_cast_capability(source, &target.data_type()) {
                CastCapability::Physical
            } else {
                CastCapability::Unavailable
            }
        }
    };
    Ok(mark(capability, semantic))
}

#[cfg(test)]
fn cast_capability(
    source: &DataType,
    logical: ValueLogicalType,
    target: CastTarget,
    depth: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CastCapability, FunctionBindingError> {
    let value =
        FunctionValueType::try_from_legacy_declared_projection(source.clone(), true, logical)
            .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
    cast_capability_typed(source, value.value_type(), target, depth, work)
}
#[cfg(test)]
fn select_union_cast_field<'a>(
    fields: &'a UnionFields,
    target: CastTarget,
    depth: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<(i8, &'a Field, CastCapability)>, FunctionBindingError> {
    let value = FunctionValueType::try_from_legacy_declared_projection(
        DataType::Union(fields.clone(), arrow_schema::UnionMode::Sparse),
        true,
        ValueLogicalType::Physical,
    )
    .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
    Ok(
        select_union_cast_field_typed(fields, value.value_type(), target, depth, work)?
            .map(|(id, field, _, capability)| (id, field, capability)),
    )
}

fn truncate_input(source: &DataType) -> bool {
    matches!(
        source,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Null
    )
}

#[cfg(test)]
pub(super) fn bind_result(
    name: &str,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, FunctionBindingError> {
    bind_result_with_creation::<std::convert::Infallible>(name, request, work, None)
        .map_err(crate::FunctionBindingCreationFailure::into_pure)
}
pub(super) fn bind_result_with_creation<H>(
    name: &str,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Result<FunctionValueType, crate::FunctionBindingCreationFailure<H>> {
    if !matches!(name, "round" | "truncate") {
        return Err(FunctionBindingError::UnknownFunction.into());
    }
    if !(1..=2).contains(&request.arguments.len())
        || request.logical_argument_count != request.arguments.len()
        || request
            .arguments
            .iter()
            .any(|arg| !matches!(arg, FunctionArgument::Value { .. }))
    {
        return Err(FunctionBindingError::NoMatchingOverload.into());
    }
    super::catalogue::request_types_with_constants(request, work)?;
    let value = |index: usize| match &request.arguments[index] {
        FunctionArgument::Value { value_type, .. } => value_type,
        FunctionArgument::Lambda { .. } => unreachable!("value arguments checked above"),
    };
    for (index, argument) in request.arguments.iter().enumerate() {
        work.step()?;
        let FunctionArgument::Value { value_type, .. } = argument else {
            return Err(FunctionBindingError::NoMatchingOverload.into());
        };
        if value_type.logical_type() != ValueLogicalType::Physical {
            return Err(FunctionBindingError::NoMatchingOverload.into());
        }
        let accepted = if name == "truncate" {
            truncate_input(value_type.data_type())
        } else if index == 0 && matches!(value_type.data_type(), DataType::Decimal128(_, _)) {
            true
        } else {
            cast_capability_typed(
                value_type.data_type(),
                value_type.value_type(),
                if index == 0 {
                    CastTarget::Float64
                } else {
                    CastTarget::Int64
                },
                1,
                work,
            )? == CastCapability::Physical
        };
        if !accepted {
            return Err(FunctionBindingError::NoMatchingOverload.into());
        }
    }
    let result = match &value(0).data_type() {
        DataType::Decimal128(_, scale) => {
            let scale = if request.arguments.len() == 2 {
                let digits = if let Some(value) =
                    super::catalogue::constant_source(request.arguments.get(1), work)?
                {
                    work.flush()?;
                    value
                        .signed_integer_observed(
                            novarocks_type_contract::CompilePhase::FunctionSpecialization,
                            work.control(),
                        )
                        .map_err(FunctionBindingError::from)?
                } else {
                    None
                };
                digits.map_or(*scale, |digits| (digits as i8).max(0).min(*scale))
            } else {
                *scale
            };
            DataType::Decimal128(38, scale)
        }
        _ if request.arguments.len() == 2 => DataType::Float64,
        _ => DataType::Int64,
    };
    work.step()?;
    super::scalar_construction::with_binding_creation(creation, work, |cx| {
        cx.value(result, true, ValueLogicalType::Physical)
    })
}

#[cfg(test)]
mod tests {
    use super::super::catalogue::constant_binding_tests as cv;
    use super::*;
    use arrow_schema::{IntervalUnit, TimeUnit, UnionFields, UnionMode};
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::FunctionSpecialization);
            assert!(units <= 256);
            let mut calls = self.calls.lock().unwrap();
            let index = calls.len();
            calls.push(units);
            if let Some((at, error)) = self.refusal.clone()
                && at == index
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn arg(ty: DataType) -> FunctionArgument {
        FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(ty, true),
            constant: None,
        }
    }
    fn request(args: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: args,
            logical_argument_count: args.len(),
            expected_result_type: None,
        }
    }
    fn bind(
        name: &str,
        args: &[FunctionArgument],
    ) -> Result<FunctionValueType, FunctionBindingError> {
        binding_control::scope(crate::binding_test_control(), |work| {
            bind_result(name, request(args), work)
        })
    }
    fn union(fields: Vec<Field>) -> DataType {
        DataType::Union(
            UnionFields::try_new([7, 1], fields).unwrap(),
            UnionMode::Sparse,
        )
    }
    fn json_field() -> Field {
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap()
            .try_to_field("json")
            .unwrap()
    }
    fn probe(source: &DataType, target: CastTarget) -> CastCapability {
        binding_control::scope(crate::binding_test_control(), |work| {
            binding_control::value_type(
                &FunctionValueType::from_legacy_declared_projection(source.clone(), true),
                work,
            )?;
            cast_capability(source, ValueLogicalType::Physical, target, 1, work)
        })
        .unwrap()
    }
    #[test]
    fn shared_union_path_returns_the_original_tag_and_borrowed_field() {
        for (target, types, expected_tag) in [
            (
                CastTarget::Float64,
                [DataType::Float32, DataType::Float64],
                1,
            ),
            (CastTarget::Int64, [DataType::Int32, DataType::Int64], 1),
            (CastTarget::Float64, [DataType::Utf8, DataType::Float32], 1),
            (
                CastTarget::Float64,
                [DataType::Boolean, DataType::UInt64],
                7,
            ),
        ] {
            let source = union(
                types
                    .into_iter()
                    .enumerate()
                    .map(|(i, ty)| Field::new(format!("child{i}"), ty, true))
                    .collect(),
            );
            let DataType::Union(fields, _) = &source else {
                unreachable!()
            };
            let (tag, chosen, capability) =
                binding_control::scope(crate::binding_test_control(), |work| {
                    binding_control::value_type(
                        &FunctionValueType::from_legacy_declared_projection(source.clone(), true),
                        work,
                    )?;
                    select_union_cast_field(fields, target, 2, work)
                })
                .unwrap()
                .unwrap();
            assert_eq!(tag, expected_tag);
            assert_eq!(capability, CastCapability::Physical);
            let original = fields.iter().find(|(id, _)| *id == tag).unwrap().1;
            assert!(std::ptr::eq(chosen, original.as_ref()));
            assert_eq!(probe(&source, target), capability);
        }
    }
    #[test]
    fn shared_union_path_keeps_chosen_nominal_identity_and_no_match() {
        let source = union(vec![json_field(), Field::new("text", DataType::Utf8, true)]);
        let DataType::Union(fields, _) = &source else {
            unreachable!()
        };
        let (tag, chosen, capability) =
            binding_control::scope(crate::binding_test_control(), |work| {
                binding_control::value_type(
                    &FunctionValueType::from_legacy_declared_projection(source.clone(), true),
                    work,
                )?;
                select_union_cast_field(fields, CastTarget::Float64, 2, work)
            })
            .unwrap()
            .unwrap();
        assert_eq!(tag, 7);
        assert!(std::ptr::eq(
            chosen,
            fields.iter().next().unwrap().1.as_ref()
        ));
        assert_eq!(capability, CastCapability::NonPhysical);
        assert_eq!(probe(&source, CastTarget::Float64), capability);

        let source = union(vec![
            Field::new("bytes", DataType::Binary, true),
            Field::new("more_bytes", DataType::LargeBinary, true),
        ]);
        let DataType::Union(fields, _) = &source else {
            unreachable!()
        };
        let choice = binding_control::scope(crate::binding_test_control(), |work| {
            binding_control::value_type(
                &FunctionValueType::from_legacy_declared_projection(source.clone(), true),
                work,
            )?;
            select_union_cast_field(fields, CastTarget::Int64, 2, work)
        })
        .unwrap();
        assert!(choice.is_none());
        assert_eq!(
            probe(&source, CastTarget::Int64),
            CastCapability::Unavailable
        );
    }
    #[test]
    fn actual_primitive_round_capability_matches_arrow_for_each_target() {
        let types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(16),
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 2),
            DataType::Decimal128(38, 2),
            DataType::Decimal256(76, 2),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
        ];
        for ty in types {
            for target in [CastTarget::Float64, CastTarget::Int64] {
                assert_eq!(
                    probe(&ty, target) == CastCapability::Physical,
                    arrow_cast::can_cast_types(&ty, &target.data_type())
                        || matches!(
                            (&ty, target),
                            (
                                DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime),
                                CastTarget::Int64
                            )
                        ),
                    "{ty:?}"
                );
            }
            let result = bind("round", &[arg(ty.clone())]);
            assert_eq!(
                result.is_ok(),
                matches!(ty, DataType::Decimal128(_, _))
                    || arrow_cast::can_cast_types(&ty, &DataType::Float64),
                "{ty:?}"
            );
            if let Ok(result) = result {
                assert!(result.nullable());
            }
        }
    }
    #[test]
    fn truncate_does_not_inherit_round_cast_permissions() {
        for ty in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(18, 3),
            DataType::Null,
        ] {
            assert!(bind("truncate", &[arg(ty.clone())]).is_ok());
            assert!(bind("truncate", &[arg(DataType::Float64), arg(ty)]).is_ok());
        }
        for ty in [
            DataType::Boolean,
            DataType::UInt64,
            DataType::Float16,
            DataType::Utf8,
            DataType::Decimal256(76, 3),
        ] {
            assert!(bind("truncate", &[arg(ty.clone())]).is_err());
            assert!(bind("truncate", &[arg(DataType::Float64), arg(ty)]).is_err());
        }
        let encoded_projection =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float64));
        assert!(bind("truncate", &[arg(encoded_projection)]).is_ok());
    }
    #[test]
    fn decimal_result_literal_scale_is_original_recipe_not_expected_override() {
        for name in ["round", "truncate"] {
            for (digits, scale) in [(2, 2), (257, 1), (128, 0), (-1, 0), (99, 5)] {
                let args = [
                    arg(DataType::Decimal128(12, 5)),
                    FunctionArgument::Value {
                        value_type: FunctionValueType::new(DataType::Int64, false),
                        constant: Some(cv::i64(digits, false)),
                    },
                ];
                let expected = FunctionValueType::new(DataType::Float64, false);
                let result = binding_control::scope(crate::binding_test_control(), |work| {
                    bind_result(
                        name,
                        FunctionBindingRequest {
                            expected_result_type: Some(&expected),
                            ..request(&args)
                        },
                        work,
                    )
                })
                .unwrap();
                assert_eq!(
                    result,
                    FunctionValueType::new(DataType::Decimal128(38, scale), true)
                );
            }
            assert_eq!(
                bind(name, &[arg(DataType::Decimal128(12, -2))])
                    .unwrap()
                    .data_type()
                    .clone(),
                DataType::Decimal128(38, -2)
            );
            assert_eq!(
                bind(
                    name,
                    &[arg(DataType::Decimal128(12, 5)), arg(DataType::Int64)]
                )
                .unwrap()
                .data_type()
                .clone(),
                DataType::Decimal128(38, 5)
            );
            assert_eq!(
                bind(name, &[arg(DataType::Int32)]).unwrap(),
                FunctionValueType::new(DataType::Int64, true)
            );
            assert_eq!(
                bind(name, &[arg(DataType::Int32), arg(DataType::Int32)]).unwrap(),
                FunctionValueType::new(DataType::Float64, true)
            );
        }
    }
    #[test]
    fn arity_argument_kind_and_logical_count_are_exact() {
        for args in [
            vec![],
            vec![arg(DataType::Int64); 3],
            vec![FunctionArgument::Lambda {
                parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    Box::default(),
                ),
                result_type: FunctionValueType::new(DataType::Float64, true),
            }],
        ] {
            assert!(matches!(
                bind("round", &args),
                Err(FunctionBindingError::NoMatchingOverload)
            ));
        }
        let args = [arg(DataType::Int64)];
        for count in [0, 2] {
            assert!(matches!(
                binding_control::scope(crate::binding_test_control(), |work| bind_result(
                    "round",
                    FunctionBindingRequest {
                        logical_argument_count: count,
                        ..request(&args)
                    },
                    work
                )),
                Err(FunctionBindingError::NoMatchingOverload)
            ));
        }
    }
    #[test]
    fn encoded_cast_paths_are_recursive_without_changing_outer_result_recipe() {
        for (ty, expected) in [
            (
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Decimal128(18, 2)),
                ),
                DataType::Decimal128(38, 2),
            ),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Float64, true)),
                ),
                DataType::Int64,
            ),
            (
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 1),
                DataType::Int64,
            ),
        ] {
            assert_eq!(
                bind("round", &[arg(ty)]).unwrap(),
                FunctionValueType::new(expected, true)
            );
        }
        let unsupported =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2);
        assert!(bind("round", &[arg(unsupported)]).is_err());
    }
    #[test]
    fn union_exact_and_family_passes_precede_castable_nominal_first_child() {
        for ty in [DataType::Float64, DataType::Float32] {
            let source = union(vec![json_field(), Field::new("numeric", ty, true)]);
            assert_eq!(
                probe(&source, CastTarget::Float64),
                CastCapability::Physical
            );
            assert!(bind("round", &[arg(source)]).is_ok());
        }
        let source = union(vec![
            json_field(),
            Field::new("numeric", DataType::Boolean, true),
        ]);
        assert_eq!(
            probe(&source, CastTarget::Float64),
            CastCapability::NonPhysical
        );
        assert!(bind("round", &[arg(source)]).is_err());
        let source = union(vec![
            Field::new("numeric", DataType::Boolean, true),
            json_field(),
        ]);
        assert_eq!(
            probe(&source, CastTarget::Float64),
            CastCapability::Physical
        );
        assert!(bind("round", &[arg(source)]).is_ok());
        // Tag 7 remains first; choosing by sorted tag 1 would lose its JSON identity.
        let source = union(vec![json_field(), Field::new("text", DataType::Utf8, true)]);
        assert_eq!(
            probe(&source, CastTarget::Int64),
            CastCapability::NonPhysical
        );
    }
    #[test]
    fn selected_nested_nominal_paths_and_nonphysical_roots_never_fallback() {
        let nominal =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let args = [FunctionArgument::Value {
            value_type: nominal.clone(),
            constant: Some(cv::utf8_typed("12", nominal)),
        }];
        assert!(bind("round", &args).is_err());
        for ty in [
            DataType::FixedSizeList(Arc::new(json_field()), 1),
            DataType::RunEndEncoded(
                Arc::new(Field::new("ends", DataType::Int32, false)),
                Arc::new(json_field()),
            ),
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::FixedSizeList(Arc::new(json_field()), 1)),
            ),
        ] {
            assert_eq!(probe(&ty, CastTarget::Float64), CastCapability::NonPhysical);
            assert!(bind("round", &[arg(ty)]).is_err());
        }
    }
    #[test]
    fn invalid_source_and_expected_type_metadata_are_shared_preflight_errors() {
        let field = Field::new("bad", DataType::Float64, true)
            .with_metadata(HashMap::from([("nr_logical_type".into(), "Json".into())]));
        let source = DataType::FixedSizeList(Arc::new(field), 1);
        assert!(
            FunctionValueType::try_from_legacy_declared_projection(
                source,
                true,
                ValueLogicalType::Physical
            )
            .is_err()
        );
        assert!(
            FunctionValueType::try_with_logical_type(
                DataType::Float64,
                true,
                ValueLogicalType::Json,
            )
            .is_err()
        );
    }
    #[test]
    fn original_control_is_observed_at_entry_each_256_and_actual_tail() {
        let fields = UnionFields::try_new(
            0_i8..=127,
            (0..128).map(|index| Field::new(format!("field{index}"), DataType::Float32, true)),
        )
        .unwrap();
        let args = [arg(DataType::Union(fields, UnionMode::Sparse))];
        let baseline = Control::default();
        let result =
            binding_control::scope(&baseline, |work| bind_result("round", request(&args), work))
                .unwrap();
        assert_eq!(result, FunctionValueType::new(DataType::Int64, true));
        let calls = baseline.calls.lock().unwrap().clone();
        assert_eq!(calls[0], 0);
        assert!(calls.contains(&256));
        assert!(*calls.last().unwrap() < 256);
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..calls.len() {
                let control = Control {
                    calls: Mutex::new(Vec::new()),
                    refusal: Some((at, error.clone())),
                };
                let actual = binding_control::scope(&control, |work| {
                    bind_result("round", request(&args), work)
                })
                .unwrap_err();
                assert_eq!(actual.control_error(), Some(error.clone()));
                assert_eq!(control.calls.lock().unwrap().len(), at + 1);
            }
        }
    }
}
