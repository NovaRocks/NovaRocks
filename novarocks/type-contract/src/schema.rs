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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_schema::{DataType, Field, Schema};

/// Real comparison work before a closed operation or byte inspection.
/// This reports no storage facts and grants no capacity. The existing unit
/// observers adapt one event to one callback without changing their sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypeComparisonStep {
    Header,
    Bytes { inspected_bytes: usize },
}
impl TypeComparisonStep {
    pub const fn work_units_upper(self) -> usize {
        match self {
            Self::Header => COMPARISON_HEADER_WORK,
            Self::Bytes { inspected_bytes } => inspected_bytes,
        }
    }
}
// Includes dispatch, tags, arity/length/NULL/parameter checks and the loop's
// fixed bookkeeping. Dynamic byte inspection always has a separate event.
pub(crate) const COMPARISON_HEADER_WORK: usize = 16;

pub(crate) fn comparison_work_add(
    a: usize,
    b: usize,
) -> Result<usize, crate::ExecutionValueTypeError> {
    a.checked_add(b)
        .ok_or(crate::ExecutionValueTypeError::Invalid(
            "type comparison work overflow",
        ))
}
pub(crate) fn comparison_bytes_work(bytes: usize) -> Result<usize, crate::ExecutionValueTypeError> {
    bytes
        .checked_mul(2)
        .ok_or(crate::ExecutionValueTypeError::Invalid(
            "type comparison work overflow",
        ))
}
// The sole Arrow comparator owns these local contributions. Constructors fold
// them in their already required canonical traversal, never parse another tree.
pub(crate) fn type_comparison_node_work(
    ty: &DataType,
) -> Result<usize, crate::ExecutionValueTypeError> {
    comparison_work_add(
        COMPARISON_HEADER_WORK,
        match ty {
            DataType::Timestamp(_, Some(zone)) => comparison_bytes_work(zone.len())?,
            _ => 0,
        },
    )
}
pub(crate) fn field_comparison_work(name: &str) -> Result<usize, crate::ExecutionValueTypeError> {
    comparison_work_add(COMPARISON_HEADER_WORK, comparison_bytes_work(name.len())?)
}

/// Compare every Arrow physical field attribute recursively. Arrow's built-in
/// `Field::eq` deliberately ignores dictionary ids and dictionary ordering,
/// which is appropriate for logical schema compatibility but not for a frozen
/// internal-relation contract.
pub fn arrow_schemas_exact(left: &Schema, right: &Schema) -> bool {
    left.metadata() == right.metadata()
        && left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| arrow_fields_exact(left, right))
}

pub fn arrow_fields_exact(left: &Field, right: &Field) -> bool {
    #[allow(deprecated)]
    let dictionary_ids_equal = left.dict_id() == right.dict_id();
    left.name() == right.name()
        && left.is_nullable() == right.is_nullable()
        && left.metadata() == right.metadata()
        && dictionary_ids_equal
        && left.dict_is_ordered() == right.dict_is_ordered()
        && arrow_data_types_exact(left.data_type(), right.data_type())
}

pub fn arrow_data_types_exact(left: &DataType, right: &DataType) -> bool {
    compare_types::<std::convert::Infallible>(
        left,
        right,
        FieldMode::Exact,
        || Ok(()),
        |_, _, _| Ok(()),
    )
    .unwrap_or(false)
}

/// Compare complete carrier attributes under an explicitly selected type
/// profile. Validation borrows the same structure grammar as recipe admission;
/// the original comparison remains the sole attribute/metadata comparator.
pub fn arrow_data_types_exact_with_profile_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    profile: crate::ArrowTypeProfile,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types_with_profile(left, right, profile, FieldMode::Exact, observe)
}
/// Exact expected-first comparison for an already prepared runtime port.
/// Permissive permits only an expected required Field to have an actual nullable
/// physical declaration. Names, metadata, encoding tags, dictionary attributes
/// and every type parameter remain exact; this never peels Dict/REE wrappers.
/// Map entries/key and REE ends keep their required format declarations.
pub fn arrow_data_types_exact_with_profile_and_nullability_observed<
    E: From<crate::ValueTypeError>,
>(
    expected: &DataType,
    actual: &DataType,
    profile: crate::ArrowTypeProfile,
    nullability: crate::CarrierNullabilityPolicy,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    let mut weighted = |_: TypeComparisonStep| observe();
    Walk {
        observe: &mut weighted,
        validate: &mut |depth, left: &DataType, right: &DataType| {
            validate_comparison_nodes(depth, profile, left, right)
        },
        mode: FieldMode::Exact,

        nullability,
        ignore_field_nullability: false,
        allow_fixed_bytes: false,
    }
    .ty(expected, actual, crate::ArrowTypeDepth::ROOT)
}

/// The same expected-first physical NULL policy on an exact Field. As with the
/// original observed Field author, schema/type admission belongs to its caller.
pub fn arrow_fields_exact_with_nullability_observed<E>(
    expected: &Field,
    actual: &Field,
    nullability: crate::CarrierNullabilityPolicy,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    let mut weighted = |_: TypeComparisonStep| observe();
    Walk {
        observe: &mut weighted,
        validate: &mut |_, _: &DataType, _: &DataType| Ok(()),
        mode: FieldMode::Exact,

        nullability,
        ignore_field_nullability: false,
        allow_fixed_bytes: false,
    }
    .field(expected, actual, crate::ArrowTypeDepth::ROOT)
}

pub fn arrow_data_types_equal_with_profile_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    profile: crate::ArrowTypeProfile,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types_with_profile(left, right, profile, FieldMode::Compatibility, observe)
}
pub fn arrow_data_types_exact_borrowed_with_profile_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    profile: crate::ArrowTypeProfile,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types_with_profile(left, right, profile, FieldMode::Exact, observe)
}
pub fn arrow_data_types_equal_borrowed_with_profile_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    profile: crate::ArrowTypeProfile,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types_with_profile(left, right, profile, FieldMode::Compatibility, observe)
}
fn compare_types_with_profile<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    profile: crate::ArrowTypeProfile,
    mode: FieldMode,

    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    let mut weighted = |_: TypeComparisonStep| observe();
    Walk {
        observe: &mut weighted,
        validate: &mut |depth, left: &DataType, right: &DataType| {
            validate_comparison_nodes(depth, profile, left, right)
        },
        mode,

        nullability: crate::CarrierNullabilityPolicy::Exact,
        ignore_field_nullability: false,
        allow_fixed_bytes: false,
    }
    .ty(left, right, crate::ArrowTypeDepth::ROOT)
}

/// Callback-only comparison for the private, already validated immutable value
/// vocabulary. Keep this crate-private: raw provider or wire types must pass
/// their original admission checks before they enter a typed holder.
pub(crate) fn arrow_validated_data_types_exact_observed<E>(
    left: &DataType,
    right: &DataType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types(left, right, FieldMode::Exact, observe, |_, _, _| Ok(()))
}

/// The same exact comparison with bounded depth and an observer
/// at every visited type and field. Frozen metadata bounds are validated by
/// the type owner; this comparison does not allocate a recursive schema copy.
pub fn arrow_data_types_exact_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types(
        left,
        right,
        FieldMode::Exact,
        observe,
        |depth, left, right| {
            validate_comparison_nodes(depth, crate::ArrowTypeProfile::Physical, left, right)
        },
    )
}

/// Compare exact types with the shared depth and carrier checks. Metadata
/// comparisons borrow the original sorted entries and observe bytes in chunks.
pub fn arrow_data_types_exact_borrowed_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    arrow_data_types_exact_observed(left, right, observe)
}

/// Observe Arrow's original PartialEq rule using the sole traversal and
/// borrowed metadata. Dictionary Field ids/order remain compatibility facts;
/// child NULL, names, metadata and all carrier structure are still compared.
/// Metadata uses the same borrowed lookup as the exact port.
pub fn arrow_data_types_equal_borrowed_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    arrow_data_types_equal_observed(left, right, observe)
}

/// Exact Field comparison using borrowed metadata and the sole type traversal.
/// Like the existing observed Field port, this relies on caller-owned schema
/// bounds and does not introduce a new depth, node, or metadata limit.
pub fn arrow_fields_exact_borrowed_observed<E>(
    left: &Field,
    right: &Field,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    arrow_fields_exact_observed(left, right, observe)
}

/// Observe Arrow schema compatibility equality. Unlike the frozen exact
/// comparison, this deliberately ignores Field dictionary ids/order exactly
/// as Arrow's PartialEq does; all other attributes use the same comparison
/// engine. This is not a frozen internal-relation identity proof.
pub fn arrow_data_types_equal_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types(
        left,
        right,
        FieldMode::Compatibility,
        observe,
        |depth, left, right| {
            validate_comparison_nodes(depth, crate::ArrowTypeProfile::Physical, left, right)
        },
    )
}

/// Observe exact field comparison within the caller's already validated schema
/// domain. This adds no type-node admission bound: owners such as writer
/// relations have their own depth and aggregate schema-byte limits.
/// Callers must validate those limits before entering this borrowed traversal.
pub fn arrow_fields_exact_observed<E>(
    left: &Field,
    right: &Field,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    let mut weighted = |_: TypeComparisonStep| observe();
    Walk {
        observe: &mut weighted,
        validate: &mut |_, _: &DataType, _: &DataType| Ok(()),
        mode: FieldMode::Exact,

        nullability: crate::CarrierNullabilityPolicy::Exact,
        ignore_field_nullability: false,
        allow_fixed_bytes: false,
    }
    .field(left, right, crate::ArrowTypeDepth::ROOT)
}

fn validate_comparison_nodes<E: From<crate::ValueTypeError>>(
    depth: crate::ArrowTypeDepth,
    profile: crate::ArrowTypeProfile,
    left: &DataType,
    right: &DataType,
) -> Result<(), E> {
    depth.check(profile)?;
    crate::carrier_parameters::validate_arrow_node_shape(left)
        .map_err(crate::ValueTypeError::InvalidType)?;
    crate::carrier_parameters::validate_arrow_node_shape(right)
        .map_err(crate::ValueTypeError::InvalidType)?;
    Ok(())
}

fn compare_types<E>(
    left: &DataType,
    right: &DataType,
    mode: FieldMode,
    mut observe: impl FnMut() -> Result<(), E>,
    validate: impl FnMut(crate::ArrowTypeDepth, &DataType, &DataType) -> Result<(), E>,
) -> Result<bool, E> {
    compare_types_weighted(left, right, mode, false, |_| observe(), validate)
}

fn compare_types_weighted<E>(
    left: &DataType,
    right: &DataType,
    mode: FieldMode,
    ignore_field_nullability: bool,
    mut observe: impl FnMut(TypeComparisonStep) -> Result<(), E>,
    mut validate: impl FnMut(crate::ArrowTypeDepth, &DataType, &DataType) -> Result<(), E>,
) -> Result<bool, E> {
    Walk {
        observe: &mut observe,
        validate: &mut validate,
        mode,

        nullability: crate::CarrierNullabilityPolicy::Exact,
        ignore_field_nullability,
        allow_fixed_bytes: false,
    }
    .ty(left, right, crate::ArrowTypeDepth::ROOT)
}

pub(crate) fn arrow_validated_data_types_exact_weighted_observed<E>(
    left: &DataType,
    right: &DataType,
    observe: impl FnMut(TypeComparisonStep) -> Result<(), E>,
) -> Result<bool, E> {
    compare_types_weighted(left, right, FieldMode::Exact, false, observe, |_, _, _| {
        Ok(())
    })
}

/// Compare already validated plain layouts while ignoring only Field NULL
/// flags. This uses the same exact traversal, never encoding/carrier matching.
/// The complete logical binding is compared separately by the binding owner.
pub(crate) fn arrow_validated_data_types_ignoring_nullability_observed<E>(
    left: &DataType,
    right: &DataType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types_weighted(
        left,
        right,
        FieldMode::Exact,
        true,
        |_| observe(),
        |_, _, _| Ok(()),
    )
}

/// The exact original layout walk with the write author's narrow plain-byte
/// exception. Licensing each changed occurrence against storage belongs to the
/// checked three-binding proof; this private helper alone is no authorization.
pub(crate) fn arrow_validated_write_projection_observed<E>(
    left: &DataType,
    right: &DataType,
    ignore_field_nullability: bool,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    let mut weighted = |_: TypeComparisonStep| observe();
    Walk {
        observe: &mut weighted,
        validate: &mut |_, _: &DataType, _: &DataType| Ok(()),
        mode: FieldMode::Exact,

        nullability: crate::CarrierNullabilityPolicy::Exact,
        ignore_field_nullability,
        allow_fixed_bytes: true,
    }
    .ty(left, right, crate::ArrowTypeDepth::ROOT)
}

#[derive(Clone, Copy)]
enum FieldMode {
    Exact,
    Compatibility,
}

struct Walk<'a, F, B> {
    observe: &'a mut F,
    validate: &'a mut B,
    mode: FieldMode,

    nullability: crate::CarrierNullabilityPolicy,
    ignore_field_nullability: bool,
    allow_fixed_bytes: bool,
}
impl<
    E,
    F: FnMut(TypeComparisonStep) -> Result<(), E>,
    B: FnMut(crate::ArrowTypeDepth, &DataType, &DataType) -> Result<(), E>,
> Walk<'_, F, B>
{
    fn bytes(&mut self, left: &[u8], right: &[u8]) -> Result<bool, E> {
        if left.len() != right.len() {
            return Ok(false);
        }
        for (left, right) in left.chunks(1024).zip(right.chunks(1024)) {
            (self.observe)(TypeComparisonStep::Bytes {
                inspected_bytes: left.len() + right.len(),
            })?;
            if left != right {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn metadata(&mut self, left: &Field, right: &Field) -> Result<bool, E> {
        if left.metadata().len() != right.metadata().len() {
            return Ok(false);
        }
        // Arrow Metadata iterates in key order. The original maps provide
        // exact, incremental comparison without a second index or key copies.
        for ((key, value), (other_key, other_value)) in
            left.metadata().iter().zip(right.metadata().iter())
        {
            (self.observe)(TypeComparisonStep::Header)?;
            if !self.bytes(key.as_bytes(), other_key.as_bytes())?
                || !self.bytes(value.as_bytes(), other_value.as_bytes())?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn field(
        &mut self,
        left: &Field,
        right: &Field,
        depth: crate::ArrowTypeDepth,
    ) -> Result<bool, E> {
        (self.observe)(TypeComparisonStep::Header)?;
        #[allow(deprecated)]
        let dictionary_equal = match self.mode {
            FieldMode::Exact => {
                left.dict_id() == right.dict_id()
                    && left.dict_is_ordered() == right.dict_is_ordered()
            }
            FieldMode::Compatibility => true,
        };
        if !self.bytes(left.name().as_bytes(), right.name().as_bytes())?
            || (!self.ignore_field_nullability
                && left.is_nullable() != right.is_nullable()
                && !(self.nullability == crate::CarrierNullabilityPolicy::Permissive
                    && right.is_nullable()))
            || !self.metadata(left, right)?
            || !dictionary_equal
        {
            return Ok(false);
        }
        self.ty(left.data_type(), right.data_type(), depth)
    }
    fn ty(
        &mut self,
        left: &DataType,
        right: &DataType,
        depth: crate::ArrowTypeDepth,
    ) -> Result<bool, E> {
        (self.observe)(TypeComparisonStep::Header)?;
        // Keep the original comparison occurrence count, including the
        // entries Struct reached through a Map edge. Profile selection only
        // changes depth coordinates, never this inherited node policy.
        (self.validate)(depth, left, right)?;
        match (left, right) {
            (DataType::FixedSizeBinary(_), DataType::Binary) if self.allow_fixed_bytes => Ok(true),
            (DataType::List(left), DataType::List(right))
            | (DataType::ListView(left), DataType::ListView(right))
            | (DataType::LargeList(left), DataType::LargeList(right))
            | (DataType::LargeListView(left), DataType::LargeListView(right)) => {
                self.field(left, right, depth.comparison_child(true, false))
            }
            (DataType::FixedSizeList(left, ls), DataType::FixedSizeList(right, rs)) => {
                if ls != rs {
                    Ok(false)
                } else {
                    self.field(left, right, depth.comparison_child(true, false))
                }
            }
            (DataType::Struct(left), DataType::Struct(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for (left, right) in left.iter().zip(right) {
                    if !self.field(left, right, depth.comparison_child(true, false))? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::Union(left, lm), DataType::Union(right, rm)) => {
                if lm != rm || left.len() != right.len() {
                    return Ok(false);
                }
                for ((li, left), (ri, right)) in left.iter().zip(right.iter()) {
                    if li != ri || !self.field(left, right, depth.comparison_child(true, false))? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::Dictionary(lk, lv), DataType::Dictionary(rk, rv)) => {
                Ok(self.ty(lk, rk, depth.comparison_child(false, true))?
                    && self.ty(lv, rv, depth.comparison_child(false, true))?)
            }
            (DataType::Map(left, ls), DataType::Map(right, rs)) => {
                if self.nullability == crate::CarrierNullabilityPolicy::Permissive {
                    let (DataType::Struct(expected), DataType::Struct(actual)) =
                        (left.data_type(), right.data_type())
                    else {
                        return Ok(false);
                    };
                    if left.is_nullable()
                        || right.is_nullable()
                        || expected.len() != 2
                        || actual.len() != 2
                        || expected[0].is_nullable()
                        || actual[0].is_nullable()
                    {
                        return Ok(false);
                    }
                }
                if ls != rs {
                    Ok(false)
                } else {
                    self.field(left, right, depth.comparison_child(false, false))
                }
            }
            (DataType::RunEndEncoded(lr, lv), DataType::RunEndEncoded(rr, rv)) => {
                if self.nullability == crate::CarrierNullabilityPolicy::Permissive
                    && (lr.is_nullable() || rr.is_nullable())
                {
                    return Ok(false);
                }
                Ok(self.field(lr, rr, depth.comparison_child(false, true))?
                    && self.field(lv, rv, depth.comparison_child(false, true))?)
            }
            (DataType::Timestamp(lu, lt), DataType::Timestamp(ru, rt)) => {
                if lu != ru {
                    return Ok(false);
                }
                match (lt, rt) {
                    (Some(left), Some(right)) => self.bytes(left.as_bytes(), right.as_bytes()),
                    (None, None) => Ok(true),
                    _ => Ok(false),
                }
            }
            _ => Ok(left == right),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueTypeError,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Eq, PartialEq)]
    enum Failure {
        Type(ValueTypeError),
        Control(CompileControlError),
    }
    impl From<ValueTypeError> for Failure {
        fn from(value: ValueTypeError) -> Self {
            Self::Type(value)
        }
    }
    struct Control {
        failure: Option<CompileControlError>,
        work: Mutex<Vec<u32>>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.work.lock().unwrap().push(units);
            if units > 0
                && let Some(failure) = self.failure.clone()
            {
                Err(failure)
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn wide_exact_comparison_observes_each_edge_without_copying_the_schema() {
        let ty = DataType::Struct(
            (0..300)
                .map(|i| Field::new(i.to_string(), DataType::Int64, false))
                .collect(),
        );
        for failure in [
            None,
            Some(CompileControlError::Cancelled),
            Some(CompileControlError::DeadlineExceeded),
            Some(CompileControlError::ResourceExhausted),
        ] {
            let control = Control {
                failure: failure.clone(),
                work: Mutex::default(),
            };
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            let result = arrow_data_types_exact_observed::<Failure>(&ty, &ty, || {
                work.step().map_err(Failure::Control)
            });
            if let Some(failure) = failure {
                assert_eq!(result, Err(Failure::Control(failure)));
                assert_eq!(*control.work.lock().unwrap(), [0, 256]);
            } else {
                assert_eq!(result, Ok(true));
                work.finish().unwrap();
                assert_eq!(*control.work.lock().unwrap(), [0, 256, 256, 256, 133]);
            }
        }
    }
    #[test]
    fn observed_exact_comparison_bounds_types_without_breaking_plain_equality() {
        let mut ty = DataType::Int64;
        for _ in 1..crate::MAX_VALUE_TYPE_DEPTH {
            ty = DataType::List(Arc::new(Field::new("item", ty, true)));
        }
        assert_eq!(
            arrow_data_types_exact_observed::<ValueTypeError>(&ty, &ty, || Ok(())),
            Ok(true)
        );
        let over = DataType::List(Arc::new(Field::new("item", ty, true)));
        assert_eq!(
            arrow_data_types_exact_observed::<ValueTypeError>(&over, &over, || Ok(())),
            Err(ValueTypeError::TooDeep)
        );
        assert!(arrow_data_types_exact(&over, &over));
        let fields: Vec<_> = (0..4096)
            .map(|_| Field::new("x", DataType::Int64, false))
            .collect();
        let over = DataType::Struct(fields.into());
        assert_eq!(
            arrow_data_types_exact_observed::<ValueTypeError>(&over, &over, || Ok(())),
            Ok(true)
        );
        assert!(arrow_data_types_exact(&over, &over));
    }
    #[test]
    fn observed_comparison_preserves_dictionary_and_field_metadata_identity() {
        #[allow(deprecated)]
        let make = |id, label: &str| {
            DataType::Struct(
                vec![
                    Field::new_dict(
                        "v",
                        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                        true,
                        id,
                        false,
                    )
                    .with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "annotation".into(),
                            label.into(),
                        )]),
                    ),
                ]
                .into(),
            )
        };
        let first = make(7, "first");
        for second in [make(8, "first"), make(7, "second")] {
            assert_eq!(
                arrow_data_types_exact_observed::<ValueTypeError>(&first, &second, || Ok(())),
                Ok(false)
            );
            assert!(!arrow_data_types_exact(&first, &second));
        }
    }
    #[test]
    fn owner_validated_field_comparison_does_not_add_default_type_admission_bounds() {
        let field = Field::new(
            "writer",
            DataType::Struct(
                (0..5000)
                    .map(|i| Field::new(i.to_string(), DataType::Int32, false))
                    .collect(),
            ),
            false,
        );
        let mut visits = 0;
        assert_eq!(
            arrow_fields_exact_observed::<ValueTypeError>(&field, &field, || {
                visits += 1;
                Ok(())
            }),
            Ok(true)
        );
        assert!(visits > 15000);
        assert_eq!(
            arrow_data_types_exact_observed::<ValueTypeError>(
                field.data_type(),
                field.data_type(),
                || Ok(())
            ),
            Ok(true)
        );
    }

    #[test]
    fn owner_bounded_long_timezone_comparison_observes_bytes_before_completion() {
        let field = Field::new(
            "writer_time",
            DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("x".repeat(300 * 1024).into()),
            ),
            true,
        );
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                failure: Some(failure.clone()),
                work: Mutex::default(),
            };
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
            assert_eq!(
                arrow_fields_exact_observed::<Failure>(&field, &field, || work
                    .step()
                    .map_err(Failure::Control)),
                Err(Failure::Control(failure))
            );
            assert_eq!(*control.work.lock().unwrap(), [0, 256]);
        }
    }
    #[test]
    fn actual_long_metadata_key_comparison_preserves_control_at_first_quantum() {
        let key = "k".repeat(320 * 1024);
        let left = Field::new("x", DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([(key.clone(), "v".to_owned())]),
        );
        let right = Field::new("x", DataType::Int64, false)
            .with_metadata(std::collections::HashMap::from([(key, "v".to_owned())]));
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                failure: Some(failure.clone()),
                work: Mutex::default(),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
            let result = arrow_fields_exact_observed(&left, &right, || {
                work.step().map_err(Failure::Control)
            });
            assert_eq!(result, Err(Failure::Control(failure)));
            assert_eq!(*control.work.lock().unwrap(), [0, 256]);
        }
        assert_eq!(
            arrow_fields_exact_observed::<std::convert::Infallible>(&left, &right, || Ok(())),
            Ok(true)
        );
    }

    #[test]
    fn borrowed_metadata_preserves_unordered_exact_equality() {
        let field = |entries: &[(&str, &str)]| {
            Field::new("x", DataType::Int64, false).with_metadata(
                entries
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<std::collections::HashMap<String, String>>(),
            )
        };
        let left = field(&[("a", "first"), ("b", "second"), ("c", "third")]);
        let reordered = field(&[("c", "third"), ("b", "second"), ("a", "first")]);
        let missing = field(&[("c", "third"), ("b", "second"), ("d", "first")]);
        let changed = field(&[("c", "third"), ("b", "different"), ("a", "first")]);
        for (right, expected) in [(&reordered, true), (&missing, false), (&changed, false)] {
            assert_eq!(
                arrow_fields_exact_observed::<std::convert::Infallible>(&left, right, || Ok(())),
                Ok(expected)
            );
        }
        let empty = field(&[]);
        let mut observed = 0;
        let mut observe = |_: TypeComparisonStep| {
            observed += 1;
            Ok::<_, std::convert::Infallible>(())
        };
        assert_eq!(
            Walk {
                observe: &mut observe,
                validate: &mut |_, _: &DataType, _: &DataType| Ok(()),
                mode: FieldMode::Exact,

                nullability: crate::CarrierNullabilityPolicy::Exact,
                ignore_field_nullability: false,
                allow_fixed_bytes: false,
            }
            .metadata(&empty, &empty),
            Ok(true)
        );
        assert_eq!(observed, 0, "empty metadata needs no key work");
    }

    #[test]
    fn observed_compatibility_equals_arrow_for_dictionary_bits_and_all_other_field_attributes() {
        use arrow_schema::{UnionFields, UnionMode};
        #[allow(deprecated)]
        let field = |id, ordered| {
            Arc::new(
                Field::new_dict(
                    "item",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                    id,
                    ordered,
                )
                .with_metadata(std::collections::HashMap::<String, String>::from(
                    [("provider.id".into(), "7".into())],
                )),
            )
        };
        let wrap = |f: Arc<Field>| {
            vec![
                DataType::List(f.clone()),
                DataType::LargeList(f.clone()),
                DataType::ListView(f.clone()),
                DataType::LargeListView(f.clone()),
                DataType::FixedSizeList(f.clone(), 2),
                DataType::Struct(vec![f.clone()].into()),
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Arc::new(Field::new("key", DataType::Int64, false)),
                                f.clone(),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                DataType::Union(
                    UnionFields::try_new(vec![41], vec![f.clone()]).unwrap(),
                    UnionMode::Dense,
                ),
                DataType::Dictionary(
                    Box::new(DataType::Int16),
                    Box::new(DataType::List(f.clone())),
                ),
                DataType::RunEndEncoded(
                    Arc::new(Field::new("ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::List(f), true)),
                ),
            ]
        };
        let left = field(0, false);
        let compatible = field(i64::MAX, true);
        let variants = [
            compatible.clone(),
            Arc::new(compatible.as_ref().clone().with_name("different")),
            Arc::new(compatible.as_ref().clone().with_nullable(false)),
            Arc::new(
                compatible
                    .as_ref()
                    .clone()
                    .with_metadata(std::collections::HashMap::<String, String>::from([(
                        "provider.id".into(),
                        "8".into(),
                    )])),
            ),
            Arc::new(
                compatible
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Dictionary(
                        Box::new(DataType::Int16),
                        Box::new(DataType::Utf8),
                    )),
            ),
        ];
        for right in variants {
            for (left, right) in wrap(left.clone()).iter().zip(wrap(right).iter()) {
                assert_eq!(
                    arrow_data_types_equal_observed::<ValueTypeError>(left, right, || Ok(())),
                    Ok(left == right)
                );
                assert_eq!(
                    arrow_data_types_equal_borrowed_observed::<ValueTypeError>(left, right, || Ok(
                        ()
                    )),
                    Ok(left == right)
                );
                if left == right {
                    assert_eq!(
                        arrow_data_types_exact_observed::<ValueTypeError>(left, right, || Ok(())),
                        Ok(false)
                    );
                }
            }
        }
    }

    #[test]
    fn observed_compatibility_retains_union_ids_mode_and_timestamp_timezone_equality() {
        use arrow_schema::{TimeUnit, UnionFields, UnionMode};
        let union = |id, mode| {
            DataType::Union(
                UnionFields::try_new(vec![id], vec![Field::new("v", DataType::Int64, false)])
                    .unwrap(),
                mode,
            )
        };
        let corpus = [
            union(7, UnionMode::Dense),
            union(41, UnionMode::Dense),
            union(7, UnionMode::Sparse),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        ];
        for left in &corpus {
            for right in &corpus {
                assert_eq!(
                    arrow_data_types_equal_observed::<ValueTypeError>(left, right, || Ok(())),
                    Ok(left == right)
                );
                assert_eq!(
                    arrow_data_types_equal_borrowed_observed::<ValueTypeError>(left, right, || Ok(
                        ()
                    )),
                    Ok(left == right)
                );
            }
        }
    }
}

#[cfg(test)]
mod physical_nullability_exact_tests {
    use super::*;
    use crate::{ArrowTypeProfile, CarrierNullabilityPolicy, ValueTypeError};
    use std::sync::Arc;
    fn comparison(expected: &DataType, actual: &DataType) -> Result<bool, ValueTypeError> {
        arrow_data_types_exact_with_profile_and_nullability_observed::<ValueTypeError>(
            expected,
            actual,
            ArrowTypeProfile::Value,
            CarrierNullabilityPolicy::Permissive,
            || Ok(()),
        )
    }
    fn compare(expected: &DataType, actual: &DataType) -> bool {
        comparison(expected, actual).unwrap()
    }
    fn structure(field: Field) -> DataType {
        DataType::Struct(vec![Arc::new(field)].into())
    }
    #[test]
    fn nullable_policy_is_expected_first_and_preserves_every_other_attribute() {
        let expected = structure(Field::new("n", DataType::Int32, false));
        let wider = structure(Field::new("n", DataType::Int32, true));
        assert!(compare(&expected, &wider));
        assert!(
            !compare(&wider, &expected),
            "physical NULL declarations cannot narrow a nullable expected field"
        );
        assert!(
            !arrow_data_types_exact(&expected, &wider),
            "the original raw exact API is unchanged"
        );
        for actual in [
            structure(Field::new("renamed", DataType::Int32, true)),
            structure(Field::new("n", DataType::Int64, true)),
            structure(Field::new("n", DataType::Int32, true).with_metadata(
                std::collections::HashMap::from([("foreign".into(), "annotation".into())]),
            )),
            structure(Field::new(
                "n",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
                true,
            )),
            structure(Field::new(
                "n",
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Int32, true)),
                ),
                true,
            )),
        ] {
            assert!(
                !compare(&expected, &actual),
                "only physical NULL flags may differ"
            );
        }
        assert!(!compare(
            &structure(Field::new("d", DataType::Decimal128(18, 2), false)),
            &structure(Field::new("d", DataType::Decimal128(19, 2), true))
        ));
    }
    #[test]
    fn permissive_exact_map_and_run_end_formats_keep_required_scaffolding() {
        let map = |entries_nullable, key_nullable, value_nullable| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Int32, key_nullable)),
                            Arc::new(Field::new("value", DataType::Int64, value_nullable)),
                        ]
                        .into(),
                    ),
                    entries_nullable,
                )),
                false,
            )
        };
        let expected = map(false, false, false);
        assert!(compare(&expected, &map(false, false, true)));
        assert!(matches!(
            comparison(&expected, &map(true, false, true)),
            Err(ValueTypeError::InvalidType(_))
        ));
        assert!(!compare(&expected, &map(false, true, true)));
        let invalid = map(true, true, true);
        assert!(
            arrow_data_types_exact(&invalid, &invalid),
            "raw comparison is not newly promoted into format admission"
        );
        let expected = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int64, false)),
        );
        let actual = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, true)),
            Arc::new(Field::new("values", DataType::Int64, true)),
        );
        assert!(matches!(
            comparison(&expected, &actual),
            Err(ValueTypeError::InvalidType(_))
        ));
    }
    #[test]
    fn permissive_exact_fields_keep_dictionary_id_and_order() {
        #[expect(
            deprecated,
            reason = "Frozen field comparison still protects pinned Arrow dictionary attributes"
        )]
        let field = |id, ordered| {
            Field::new_dict(
                "d",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
                id,
                ordered,
            )
        };
        for actual in [field(8, false), field(7, true)] {
            assert!(
                !arrow_fields_exact_with_nullability_observed::<std::convert::Infallible>(
                    &field(7, false),
                    &actual,
                    CarrierNullabilityPolicy::Permissive,
                    || Ok(())
                )
                .unwrap()
            );
        }
    }
}
