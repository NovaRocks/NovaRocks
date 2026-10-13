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

use arrow_schema::{DataType, Field};
use std::fmt;

/// Semantic identity is explicit; an Arrow carrier never establishes it.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ValueLogicalType {
    #[default]
    Physical,
    Json,
    Variant,
    Hll,
    Bitmap,
    Object,
    Percentile,
    LargeInt,
    Uuid,
}

pub const NR_LOGICAL_TYPE_KEY: &str = "nr_logical_type";
pub const MAX_VALUE_TYPE_DEPTH: usize = 64;
pub const MAX_VALUE_TYPE_NODES: usize = 4096;

impl ValueLogicalType {
    pub const fn metadata_value(self) -> Option<&'static str> {
        match self {
            Self::Physical => None,
            Self::Json => Some("json"),
            Self::Variant => Some("variant"),
            Self::Hll => Some("hll"),
            Self::Bitmap => Some("bitmap"),
            Self::Object => Some("object"),
            Self::Percentile => Some("percentile"),
            Self::LargeInt => Some("largeint"),
            Self::Uuid => Some("uuid"),
        }
    }

    pub fn from_metadata_value(value: &str) -> Result<Self, ValueTypeError> {
        match value {
            "json" => Ok(Self::Json),
            "variant" => Ok(Self::Variant),
            "hll" => Ok(Self::Hll),
            "bitmap" => Ok(Self::Bitmap),
            "object" => Ok(Self::Object),
            "percentile" => Ok(Self::Percentile),
            "largeint" => Ok(Self::LargeInt),
            "uuid" => Ok(Self::Uuid),
            _ => Err(ValueTypeError::UnknownLogicalMetadata),
        }
    }

    /// Borrow the complete marker grammar while preserving the original
    /// final-result spelling contract for the five legacy domains only.
    /// Carrier qualification remains with the caller's exact boundary.
    pub fn from_legacy_compatible_metadata_value(value: &str) -> Result<Self, ValueTypeError> {
        if let Ok(logical) = Self::from_metadata_value(value) {
            return Ok(logical);
        }
        let value = value.trim();
        for logical in [
            Self::Json,
            Self::Hll,
            Self::Bitmap,
            Self::Object,
            Self::Percentile,
        ] {
            if logical
                .metadata_value()
                .is_some_and(|name| value.eq_ignore_ascii_case(name))
            {
                return Ok(logical);
            }
        }
        Err(ValueTypeError::UnknownLogicalMetadata)
    }

    pub fn validate_carrier(self, data_type: &DataType) -> Result<(), ValueTypeError> {
        let valid = match self {
            Self::Physical => true,
            Self::Json => matches!(data_type, DataType::Utf8),
            Self::Variant => matches!(data_type, DataType::LargeBinary),
            Self::Hll | Self::Bitmap | Self::Object | Self::Percentile => {
                matches!(data_type, DataType::Binary | DataType::LargeBinary)
            }
            Self::LargeInt | Self::Uuid => matches!(data_type, DataType::FixedSizeBinary(16)),
        };
        if valid {
            Ok(())
        } else {
            Err(ValueTypeError::InvalidLogicalCarrier(self))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueTypeError {
    UnknownLogicalMetadata,
    InvalidLogicalCarrier(ValueLogicalType),
    TooDeep,
    TooManyNodes,
}
impl fmt::Display for ValueTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownLogicalMetadata => f.write_str("unknown logical type metadata"),
            Self::InvalidLogicalCarrier(logical) => {
                write!(f, "invalid Arrow carrier for {logical:?}")
            }
            Self::TooDeep => f.write_str("value type exceeds the nesting depth limit"),
            Self::TooManyNodes => f.write_str("value type exceeds the node count limit"),
        }
    }
}
impl std::error::Error for ValueTypeError {}

pub fn field_logical_type(field: &Field) -> Result<ValueLogicalType, ValueTypeError> {
    let logical = match field.metadata().get(NR_LOGICAL_TYPE_KEY) {
        None => ValueLogicalType::Physical,
        Some(value) => ValueLogicalType::from_metadata_value(value)?,
    };
    logical.validate_carrier(field.data_type())?;
    Ok(logical)
}

/// This bound protects direct pure-catalog callers too, before recursive
/// domain comparisons. The carrier's own structural/resource rules remain
/// with its plan/codec owner.
pub fn validate_nested_logical_types(root: &DataType) -> Result<(), ValueTypeError> {
    validate_nested_logical_types_observed(root, || Ok(()))
}

/// Same bounded structural validation with a caller-owned work/control
/// observer. Every type node and child edge is observed without a shadow
/// validator or recursive type copy.
pub fn validate_nested_logical_types_observed<E>(
    root: &DataType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<(), E>
where
    E: From<ValueTypeError>,
{
    validate_value_type_structure_observed(root, |visit| match visit {
        ValueTypeVisit::Field(_) => Ok(()),
        _ => observe(),
    })
}

/// One walk exposes exact borrowed attributes to the caller's carrier-resource
/// checks; logical validation and node/depth bounds remain defined here.
#[derive(Clone, Copy, Debug)]
pub enum ValueTypeVisit<'a> {
    TypeNode(&'a DataType),
    ChildEdge(&'a DataType),
    Field(&'a Field),
}

trait TypePending<'a> {
    fn len(&self) -> usize;
    fn push(&mut self, item: (&'a DataType, usize));
    fn pop(&mut self) -> Option<(&'a DataType, usize)>;
}
impl<'a> TypePending<'a> for Vec<(&'a DataType, usize)> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn push(&mut self, item: (&'a DataType, usize)) {
        Vec::push(self, item);
    }
    fn pop(&mut self) -> Option<(&'a DataType, usize)> {
        Vec::pop(self)
    }
}
struct BorrowedTypePending<'a, 's> {
    slots: &'s mut [Option<(&'a DataType, usize)>; MAX_VALUE_TYPE_NODES],
    len: usize,
}
impl<'a> TypePending<'a> for BorrowedTypePending<'a, '_> {
    fn len(&self) -> usize {
        self.len
    }
    fn push(&mut self, item: (&'a DataType, usize)) {
        // The sole walk's visited+pending gate proves this slot exists.
        self.slots[self.len] = Some(item);
        self.len += 1;
    }
    fn pop(&mut self) -> Option<(&'a DataType, usize)> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        self.slots[self.len].take()
    }
}

pub fn validate_value_type_structure_observed<'a, E: From<ValueTypeError>>(
    root: &'a DataType,
    observe: impl FnMut(ValueTypeVisit<'a>) -> Result<(), E>,
) -> Result<(), E> {
    let mut pending = vec![(root, 1usize)];
    validate_value_type_structure_with_pending(&mut pending, observe)
}

/// The same grammar, bounds, borrowed events and primary-error order, using
/// caller-owned fixed scratch. No heap request is made by this walk. Scratch
/// initialization and metadata/probe work remain the caller's admitted work;
/// a refusal may leave borrowed pending entries in the caller's scratch.
pub fn validate_value_type_structure_with_scratch_observed<'a, E: From<ValueTypeError>>(
    root: &'a DataType,
    scratch: &mut [Option<(&'a DataType, usize)>; MAX_VALUE_TYPE_NODES],
    observe: impl FnMut(ValueTypeVisit<'a>) -> Result<(), E>,
) -> Result<(), E> {
    let mut pending = BorrowedTypePending {
        slots: scratch,
        len: 0,
    };
    pending.push((root, 1));
    validate_value_type_structure_with_pending(&mut pending, observe)
}

fn validate_value_type_structure_with_pending<'a, E: From<ValueTypeError>>(
    pending: &mut impl TypePending<'a>,
    observe: impl FnMut(ValueTypeVisit<'a>) -> Result<(), E>,
) -> Result<(), E> {
    visit_value_type_with_pending(pending, false, observe)
}

/// Borrow the owned edges of the original derived DataType::clone. All FieldRef,
/// Fields, UnionFields and timezone Arc occurrences are shared; only Dictionary
/// Box children recurse. The same pending scratch and source matcher below own
/// the traversal. This numerical loan performs no clone or allocation grant.
/// The caller owns scratch initialization before borrowing this entrypoint.
pub fn visit_original_data_type_clone_with_scratch_observed<'a, E: From<ValueTypeError>>(
    root: &'a DataType,
    scratch: &mut [Option<(&'a DataType, usize)>; MAX_VALUE_TYPE_NODES],
    observe: impl FnMut(ValueTypeVisit<'a>) -> Result<(), E>,
) -> Result<(), E> {
    let mut pending = BorrowedTypePending {
        slots: scratch,
        len: 0,
    };
    pending.push((root, 1));
    visit_value_type_with_pending(&mut pending, true, observe)
}

fn visit_value_type_with_pending<'a, E: From<ValueTypeError>>(
    pending: &mut impl TypePending<'a>,
    original_owned_clone: bool,
    mut observe: impl FnMut(ValueTypeVisit<'a>) -> Result<(), E>,
) -> Result<(), E> {
    let mut visited = 0usize;
    while let Some((ty, depth)) = pending.pop() {
        observe(ValueTypeVisit::TypeNode(ty))?;
        visited += 1;
        if depth > MAX_VALUE_TYPE_DEPTH {
            return Err(ValueTypeError::TooDeep.into());
        }
        if visited > MAX_VALUE_TYPE_NODES {
            return Err(ValueTypeError::TooManyNodes.into());
        }
        let mut push = |child: &'a DataType, field: Option<&'a Field>| -> Result<(), E> {
            if let Some(field) = field {
                observe(ValueTypeVisit::Field(field))?;
                field_logical_type(field)?;
            }
            observe(ValueTypeVisit::ChildEdge(child))?;
            if visited + pending.len() >= MAX_VALUE_TYPE_NODES {
                return Err(ValueTypeError::TooManyNodes.into());
            }
            pending.push((child, depth + 1));
            Ok(())
        };
        if original_owned_clone {
            // Derived clone copies the Dictionary's two Box<DataType> values.
            // Every other carrier's descendants are behind shared Arc owners.
            if let DataType::Dictionary(key, value) = ty {
                push(key, None)?;
                push(value, None)?;
            }
            continue;
        }
        match ty {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => {
                push(field.data_type(), Some(field))?;
            }
            DataType::Struct(fields) => {
                for field in fields {
                    push(field.data_type(), Some(field))?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    push(field.data_type(), Some(field))?;
                }
            }
            DataType::Dictionary(key, value) => {
                push(key, None)?;
                push(value, None)?;
            }
            DataType::RunEndEncoded(ends, values) => {
                push(ends.data_type(), Some(ends))?;
                push(values.data_type(), Some(values))?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// A carrier cast has no authority to establish or erase semantic identities.
/// Ordinary unlabelled structures may change through the existing cast rules.
pub fn preserves_nested_logical_identity(actual: &DataType, expected: &DataType) -> bool {
    preserves_nested_logical_identity_observed::<ValueTypeError>(actual, expected, || Ok(()))
        .unwrap_or(false)
}

/// The same identity rule with a caller-owned observer. The original logical
/// validation establishes depth/node bounds before recursive matching. This
/// rule does not establish cast capability or authorize an allocation.
pub fn preserves_nested_logical_identity_observed<E: From<ValueTypeError>>(
    actual: &DataType,
    expected: &DataType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    validate_nested_logical_types_observed(actual, &mut observe)?;
    validate_nested_logical_types_observed(expected, &mut observe)?;

    fn field_has<E: From<ValueTypeError>>(
        field: &Field,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        let logical = field_logical_type(field)?;
        observe()?;
        if logical != ValueLogicalType::Physical {
            Ok(true)
        } else {
            has(field.data_type(), observe)
        }
    }
    fn has<E: From<ValueTypeError>>(
        ty: &DataType,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        match ty {
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => field_has(f, observe),
            DataType::Struct(fields) => {
                for field in fields {
                    observe()?;
                    if field_has(field, observe)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    observe()?;
                    if field_has(field, observe)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            DataType::Dictionary(key, value) => {
                if has(key, observe)? {
                    Ok(true)
                } else {
                    has(value, observe)
                }
            }
            DataType::RunEndEncoded(ends, values) => {
                if field_has(ends, observe)? {
                    Ok(true)
                } else {
                    field_has(values, observe)
                }
            }
            _ => Ok(false),
        }
    }
    fn field_preserves<E: From<ValueTypeError>>(
        a: &Field,
        e: &Field,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        let equal = field_logical_type(a)? == field_logical_type(e)?;
        observe()?;
        if equal {
            preserves(a.data_type(), e.data_type(), observe)
        } else {
            Ok(false)
        }
    }
    fn preserves<E: From<ValueTypeError>>(
        a: &DataType,
        e: &DataType,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        match (a, e) {
            (DataType::List(a), DataType::List(e))
            | (DataType::LargeList(a), DataType::LargeList(e))
            | (DataType::ListView(a), DataType::ListView(e))
            | (DataType::LargeListView(a), DataType::LargeListView(e))
            | (DataType::FixedSizeList(a, _), DataType::FixedSizeList(e, _))
            | (DataType::Map(a, _), DataType::Map(e, _)) => field_preserves(a, e, observe),
            (DataType::Struct(a), DataType::Struct(e)) if a.len() == e.len() => {
                for (a, e) in a.iter().zip(e.iter()) {
                    observe()?;
                    if !field_preserves(a, e, observe)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::Union(a, _), DataType::Union(e, _)) if a.len() == e.len() => {
                for ((ai, af), (ei, ef)) in a.iter().zip(e.iter()) {
                    let same_id = ai == ei;
                    observe()?;
                    if !same_id || !field_preserves(af, ef, observe)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (DataType::Dictionary(ak, av), DataType::Dictionary(ek, ev)) => {
                if preserves(ak, ek, observe)? {
                    preserves(av, ev, observe)
                } else {
                    Ok(false)
                }
            }
            (DataType::RunEndEncoded(ar, av), DataType::RunEndEncoded(er, ev)) => {
                if field_preserves(ar, er, observe)? {
                    field_preserves(av, ev, observe)
                } else {
                    Ok(false)
                }
            }
            _ => {
                if has(a, observe)? {
                    Ok(false)
                } else {
                    Ok(!has(e, observe)?)
                }
            }
        }
    }
    preserves(actual, expected, &mut observe)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FunctionValueType, fits_nested_nullability};
    use std::sync::Arc;

    #[test]
    fn complete_marker_grammar_preserves_only_original_legacy_spellings() {
        for logical in [
            ValueLogicalType::Json,
            ValueLogicalType::Variant,
            ValueLogicalType::Hll,
            ValueLogicalType::Bitmap,
            ValueLogicalType::Object,
            ValueLogicalType::Percentile,
            ValueLogicalType::LargeInt,
            ValueLogicalType::Uuid,
        ] {
            let canonical = logical.metadata_value().unwrap();
            assert_eq!(
                ValueLogicalType::from_legacy_compatible_metadata_value(canonical),
                Ok(logical)
            );
            let decorated = format!(" {} ", canonical.to_ascii_uppercase());
            let expected = if matches!(
                logical,
                ValueLogicalType::LargeInt | ValueLogicalType::Variant | ValueLogicalType::Uuid
            ) {
                Err(ValueTypeError::UnknownLogicalMetadata)
            } else {
                Ok(logical)
            };
            assert_eq!(
                ValueLogicalType::from_legacy_compatible_metadata_value(&decorated),
                expected
            );
            assert_eq!(
                ValueLogicalType::from_metadata_value(&decorated),
                Err(ValueTypeError::UnknownLogicalMetadata)
            );
        }
        for value in ["", "   ", "unknown", "physical"] {
            assert_eq!(
                ValueLogicalType::from_legacy_compatible_metadata_value(value),
                Err(ValueTypeError::UnknownLogicalMetadata)
            );
        }
    }

    #[test]
    fn logical_domains_require_explicit_identity_and_exact_carriers() {
        for (logical, carrier) in [
            (ValueLogicalType::Json, DataType::Utf8),
            (ValueLogicalType::Variant, DataType::LargeBinary),
            (ValueLogicalType::Hll, DataType::Binary),
            (ValueLogicalType::Bitmap, DataType::Binary),
            (ValueLogicalType::Object, DataType::LargeBinary),
            (ValueLogicalType::Percentile, DataType::Binary),
            (ValueLogicalType::LargeInt, DataType::FixedSizeBinary(16)),
            (ValueLogicalType::Uuid, DataType::FixedSizeBinary(16)),
        ] {
            let typed =
                FunctionValueType::try_with_logical_type(carrier.clone(), false, logical).unwrap();
            let plain = FunctionValueType::new(carrier, false);
            assert!(!typed.same_value_domain(&plain));
            assert!(!typed.fits_value_type(&plain));
            assert!(!plain.fits_value_type(&typed));
            assert!(
                FunctionValueType::try_with_logical_type(DataType::Int64, false, logical).is_err()
            );
        }
        let variant = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Variant,
        )
        .unwrap();
        let object = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Object,
        )
        .unwrap();
        assert!(!variant.same_value_domain(&object));
        assert!(
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                false,
                ValueLogicalType::Variant
            )
            .is_err()
        );
    }

    #[test]
    fn nested_logical_identity_survives_nullable_widening() {
        let field = |nullable, label: Option<&str>| {
            let field = Field::new("item", DataType::Utf8, nullable);
            match label {
                None => field,
                Some(label) => {
                    field.with_metadata([(NR_LOGICAL_TYPE_KEY.into(), label.into())].into())
                }
            }
        };
        let actual = DataType::List(Arc::new(field(false, Some("json"))));
        let expected = DataType::List(Arc::new(field(true, Some("json"))));
        assert!(fits_nested_nullability(&actual, &expected));
        assert!(!fits_nested_nullability(&expected, &actual));
        assert!(!fits_nested_nullability(
            &actual,
            &DataType::List(Arc::new(field(true, None)))
        ));
        let unknown = DataType::List(Arc::new(field(true, Some("unknown"))));
        assert_eq!(
            validate_nested_logical_types(&unknown),
            Err(ValueTypeError::UnknownLogicalMetadata)
        );
        assert!(!fits_nested_nullability(&unknown, &unknown));
        assert!(preserves_nested_logical_identity(&actual, &expected));
        let plain = DataType::List(Arc::new(field(true, None)));
        assert!(!preserves_nested_logical_identity(&actual, &plain));
        assert!(!preserves_nested_logical_identity(&plain, &actual));
        assert!(!preserves_nested_logical_identity(&actual, &DataType::Utf8));
        assert!(preserves_nested_logical_identity(&plain, &DataType::Utf8));
    }

    #[test]
    fn direct_catalog_types_have_near_and_over_bounds() {
        let mut ty = DataType::Int64;
        for _ in 1..MAX_VALUE_TYPE_DEPTH {
            ty = DataType::List(Arc::new(Field::new("item", ty, true)));
        }
        validate_nested_logical_types(&ty).unwrap();
        let over = DataType::List(Arc::new(Field::new("item", ty, true)));
        assert_eq!(
            validate_nested_logical_types(&over),
            Err(ValueTypeError::TooDeep)
        );
        let fields = (0..MAX_VALUE_TYPE_NODES - 1)
            .map(|i| Field::new(format!("v{i}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        validate_nested_logical_types(&DataType::Struct(fields.clone().into())).unwrap();
        let mut over = fields;
        over.push(Field::new("over", DataType::Int64, false));
        assert_eq!(
            validate_nested_logical_types(&DataType::Struct(over.into())),
            Err(ValueTypeError::TooManyNodes)
        );
    }

    #[test]
    fn typed_equality_keeps_nested_dictionary_ids() {
        #[allow(deprecated)]
        let value = |id| {
            FunctionValueType::new(
                DataType::Struct(
                    vec![Field::new_dict(
                        "dict",
                        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                        false,
                        id,
                        false,
                    )]
                    .into(),
                ),
                false,
            )
        };
        assert_ne!(value(1), value(2));
        assert!(!value(1).same_value_domain(&value(2)));
    }

    #[test]
    fn observed_validation_checks_child_edges_and_nodes_with_bounded_work() {
        use crate::{CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl};
        use std::sync::Mutex;
        struct Control {
            units: Mutex<Vec<u32>>,
            cancel: bool,
        }
        impl PureCompileControl for Control {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                self.units.lock().unwrap().push(units);
                if self.cancel && units > 0 {
                    Err(CompileControlError::Cancelled)
                } else {
                    Ok(())
                }
            }
        }
        #[derive(Debug, PartialEq)]
        enum Failure {
            Type(ValueTypeError),
            Control(CompileControlError),
        }
        impl From<ValueTypeError> for Failure {
            fn from(error: ValueTypeError) -> Self {
                Self::Type(error)
            }
        }
        let data_type = DataType::Struct(
            (0..300)
                .map(|n| std::sync::Arc::new(Field::new(n.to_string(), DataType::Int64, false)))
                .collect(),
        );
        for cancel in [false, true] {
            let control = Control {
                units: Mutex::default(),
                cancel,
            };
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            let result = validate_nested_logical_types_observed::<Failure>(&data_type, || {
                work.step().map_err(Failure::Control)
            });
            if cancel {
                assert_eq!(
                    result,
                    Err(Failure::Control(CompileControlError::Cancelled))
                );
                assert_eq!(control.units.lock().unwrap().iter().sum::<u32>(), 256);
            } else {
                result.unwrap();
                work.finish().unwrap();
                let units = control.units.lock().unwrap();
                assert_eq!(units.iter().sum::<u32>(), 601);
                assert!(units.iter().all(|n| *n <= 256));
            }
        }
    }

    fn identity_field(label: Option<&str>) -> Arc<Field> {
        let field = Field::new("item", DataType::Utf8, true)
            .with_metadata([("provider.field.id".into(), "73".into())].into());
        Arc::new(match label {
            Some(label) => {
                let mut metadata = field.metadata().clone();
                metadata.insert(NR_LOGICAL_TYPE_KEY.into(), label.into());
                field.with_metadata(metadata)
            }
            None => field,
        })
    }

    fn identity_wrappers(field: Arc<Field>) -> Vec<DataType> {
        use arrow_schema::{UnionFields, UnionMode};
        vec![
            DataType::List(field.clone()),
            DataType::LargeList(field.clone()),
            DataType::ListView(field.clone()),
            DataType::LargeListView(field.clone()),
            DataType::FixedSizeList(field.clone(), 2),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int64, false).into(),
                            field.clone(),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            DataType::Struct(vec![field.clone()].into()),
            DataType::Union(
                UnionFields::try_new(vec![7], vec![field.clone()]).unwrap(),
                UnionMode::Dense,
            ),
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::List(field.clone())),
            ),
            DataType::RunEndEncoded(Arc::new(Field::new("ends", DataType::Int32, false)), field),
        ]
    }

    #[test]
    fn observed_identity_preserves_labels_in_every_original_carrier_branch() {
        let labelled = identity_wrappers(identity_field(Some("json")));
        let plain = identity_wrappers(identity_field(None));
        for (labelled, plain) in labelled.iter().zip(plain.iter()) {
            for (a, e, expected) in [
                (labelled, labelled, true),
                (labelled, plain, false),
                (plain, labelled, false),
                (plain, &DataType::Int64, true),
                (labelled, &DataType::Int64, false),
            ] {
                let mut visits = 0;
                assert_eq!(
                    preserves_nested_logical_identity_observed::<ValueTypeError>(a, e, || {
                        visits += 1;
                        Ok(())
                    }),
                    Ok(expected),
                );
                assert_eq!(preserves_nested_logical_identity(a, e), expected);
                assert!(visits > 0);
            }
        }
        // Dictionary keys participate even though carrier grammar is a separate
        // author. This helper preserves the original logical-identity rule.
        let labelled_key = DataType::Dictionary(
            Box::new(DataType::List(identity_field(Some("json")))),
            Box::new(DataType::Int64),
        );
        let plain_key = DataType::Dictionary(
            Box::new(DataType::List(identity_field(None))),
            Box::new(DataType::Int64),
        );
        assert!(!preserves_nested_logical_identity(
            &labelled_key,
            &plain_key
        ));
        let nominal = |label: &str| {
            DataType::List(Arc::new(
                Field::new("item", DataType::FixedSizeBinary(16), false)
                    .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), label.into())].into()),
            ))
        };
        assert!(!preserves_nested_logical_identity(
            &nominal("uuid"),
            &nominal("largeint")
        ));
        assert!(!preserves_nested_logical_identity(
            &nominal("largeint"),
            &nominal("uuid")
        ));
    }

    #[test]
    fn observed_identity_keeps_original_shape_fallback_and_union_id_order() {
        use arrow_schema::{UnionFields, UnionMode};
        let labelled = identity_field(Some("json"));
        let list = DataType::List(labelled.clone());
        let large = DataType::LargeList(labelled.clone());
        assert!(!preserves_nested_logical_identity(&list, &large));
        assert!(preserves_nested_logical_identity(
            &DataType::FixedSizeList(labelled.clone(), 1),
            &DataType::FixedSizeList(labelled.clone(), 8),
        ));
        let map = identity_wrappers(labelled.clone()).remove(5);
        let DataType::Map(entries, _) = map else {
            unreachable!()
        };
        assert!(preserves_nested_logical_identity(
            &DataType::Map(entries.clone(), false),
            &DataType::Map(entries, true),
        ));
        let union = |ids: Vec<i8>, mode| {
            DataType::Union(
                UnionFields::try_new(ids, vec![labelled.clone(), identity_field(None)]).unwrap(),
                mode,
            )
        };
        assert!(preserves_nested_logical_identity(
            &union(vec![7, 41], UnionMode::Dense),
            &union(vec![7, 41], UnionMode::Sparse),
        ));
        assert!(!preserves_nested_logical_identity(
            &union(vec![7, 41], UnionMode::Dense),
            &union(vec![41, 7], UnionMode::Dense),
        ));
        let short = DataType::Struct(vec![labelled].into());
        let longer =
            DataType::Struct(vec![identity_field(Some("json")), identity_field(None)].into());
        assert!(!preserves_nested_logical_identity(&short, &longer));
        assert!(preserves_nested_logical_identity(
            &DataType::Struct(vec![identity_field(None)].into()),
            &DataType::Struct(Vec::<Arc<Field>>::new().into()),
        ));
        let invalid = DataType::List(identity_field(Some("unknown")));
        assert_eq!(
            preserves_nested_logical_identity_observed::<ValueTypeError>(
                &invalid,
                &invalid,
                || Ok(())
            ),
            Err(ValueTypeError::UnknownLogicalMetadata),
        );
        assert!(!preserves_nested_logical_identity(&invalid, &invalid));
    }

    #[derive(Debug, PartialEq)]
    enum IdentityFailure {
        Type(ValueTypeError),
        Control(crate::CompileControlError),
    }
    impl From<ValueTypeError> for IdentityFailure {
        fn from(error: ValueTypeError) -> Self {
            Self::Type(error)
        }
    }
    impl From<crate::CompileControlError> for IdentityFailure {
        fn from(error: crate::CompileControlError) -> Self {
            Self::Control(error)
        }
    }
    struct IdentityControl {
        calls: std::sync::Mutex<Vec<(crate::CompilePhase, u32)>>,
        refusal: Option<(usize, crate::CompileControlError)>,
    }
    impl crate::PureCompileControl for IdentityControl {
        fn checkpoint(
            &self,
            phase: crate::CompilePhase,
            units: u32,
        ) -> Result<(), crate::CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            let ordinal = calls.len();
            calls.push((phase, units));
            match self.refusal {
                Some((at, error)) if at == ordinal => Err(error),
                _ => Ok(()),
            }
        }
    }
    fn run_identity(
        a: &DataType,
        e: &DataType,
        control: &IdentityControl,
    ) -> Result<bool, IdentityFailure> {
        let mut work = crate::CompileCheckpoints::try_new(control, crate::CompilePhase::Validate)?;
        let result = preserves_nested_logical_identity_observed(a, e, || {
            work.step().map_err(IdentityFailure::Control)
        });
        // The generic observer has no ownership of the caller's tail. A typed
        // refusal returns immediately; ordinary false still completes the tail.
        if matches!(result, Err(IdentityFailure::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    #[test]
    fn observed_identity_wide_sparse_labels_preserve_every_control_callback_prefix() {
        let wide = |erase_last: bool| {
            DataType::Struct(
                (0..320)
                    .map(|n| {
                        let label = if n % 37 == 0 || n == 319 {
                            Some("json")
                        } else {
                            None
                        };
                        identity_field(if erase_last && n == 319 { None } else { label })
                    })
                    .collect(),
            )
        };
        let actual = wide(false);
        for (expected, expected_result) in [(wide(false), true), (wide(true), false)] {
            let success = IdentityControl {
                calls: Default::default(),
                refusal: None,
            };
            assert_eq!(
                run_identity(&actual, &expected, &success),
                Ok(expected_result)
            );
            let trace = success.calls.into_inner().unwrap();
            assert_eq!(trace[0], (crate::CompilePhase::Validate, 0));
            assert_eq!(trace[1], (crate::CompilePhase::Validate, 256));
            assert!(trace.len() > 3);
            assert!(trace.last().unwrap().1 < 256);
            for cause in [
                crate::CompileControlError::Cancelled,
                crate::CompileControlError::DeadlineExceeded,
                crate::CompileControlError::ResourceExhausted,
            ] {
                for at in 0..trace.len() {
                    let control = IdentityControl {
                        calls: Default::default(),
                        refusal: Some((at, cause)),
                    };
                    assert_eq!(
                        run_identity(&actual, &expected, &control),
                        Err(IdentityFailure::Control(cause))
                    );
                    assert_eq!(control.calls.into_inner().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "logical_scratch_tests.rs"]
mod scratch_tests;
