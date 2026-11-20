// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Logical-domain preservation for an explicitly selected carrier cast.

use crate::{FunctionValueType, ValueKind, ValueLogicalType, ValueTypeRef};
use arrow_schema::DataType;

/// Check nominal domains along structural positions. This grants no cast
/// capability and does not replace exact input/output binding validation.
/// Ordinary scalar parameters, names, layout widths and NULL rules remain with
/// the declared cast owner. Nominal children cannot be erased by shape changes.
pub fn preserves_value_domains_observed<E>(
    actual: &ValueTypeRef,
    expected: &ValueTypeRef,
    mut observe: impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    preserves(actual, expected, &mut observe)
}
fn has_domain<E>(
    value: &ValueTypeRef,
    observe: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    observe(1)?;
    if value.domain().is_some() {
        return Ok(true);
    }
    for index in 0..value.child_count() {
        if has_domain(
            value.child_type_at(index).expect("checked child position"),
            observe,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}
fn preserves<E>(
    actual: &ValueTypeRef,
    expected: &ValueTypeRef,
    observe: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    observe(1)?;
    if actual.domain().is_some() || expected.domain().is_some() {
        return Ok(actual.domain() == expected.domain());
    }
    let paired = match (actual.kind(), expected.kind()) {
        (ValueKind::List(_), ValueKind::List(_))
        | (ValueKind::FixedSizeList { .. }, ValueKind::FixedSizeList { .. })
        | (ValueKind::Map { .. }, ValueKind::Map { .. }) => true,
        (ValueKind::Struct(a), ValueKind::Struct(b)) => a.len() == b.len(),
        (ValueKind::Union(a), ValueKind::Union(b)) if a.len() == b.len() => {
            for (a, b) in a.iter().zip(b) {
                observe(1)?;
                if a.type_id != b.type_id {
                    return Ok(false);
                }
            }
            true
        }
        _ => false,
    };
    if paired {
        for index in 0..actual.child_count() {
            if !preserves(
                actual.child_type_at(index).expect("checked child position"),
                expected
                    .child_type_at(index)
                    .expect("matching child position"),
                observe,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    } else if has_domain(actual, observe)? {
        Ok(false)
    } else {
        Ok(!has_domain(expected, observe)?)
    }
}

/// The original default JSON-text parser CAST dispatch. This is a carrier
/// capability, not a value-domain authorization or a second conversion rule.
/// The existing runtime owns parsing, positional/named projection and NULLs.
pub fn default_json_structure_carrier_cast_supported(source: &DataType, target: &DataType) -> bool {
    source == &DataType::Utf8
        && matches!(
            target,
            DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
        )
}

/// Complete typed source/target admission for that existing intrinsic. Both
/// authored JSON text and ordinary UTF8 text used the original parser CAST;
/// target child domains are supplied by the explicit target, never inferred
/// from observed values. Non-text nominal domains cannot enter this port.
pub fn is_default_json_structure_cast(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> bool {
    (!source.nullable() || target.nullable())
        && matches!(
            source.logical_type(),
            ValueLogicalType::Json | ValueLogicalType::Physical
        )
        && target.logical_type() == ValueLogicalType::Physical
        && default_json_structure_carrier_cast_supported(source.data_type(), target.data_type())
}

/// Describe the explicit lifting of a physical NULL element into a nullable
/// JSON list element. This does not preserve a value domain, prepare a runtime
/// cast, or infer nominal identity from the UTF8 carrier.
pub fn is_null_list_to_json_cast(source: &FunctionValueType, target: &FunctionValueType) -> bool {
    if source.logical_type() != ValueLogicalType::Physical
        || target.logical_type() != ValueLogicalType::Physical
        || (source.nullable() && !target.nullable())
    {
        return false;
    }
    let (ValueKind::List(source_item), ValueKind::List(target_item)) =
        (source.value_type().kind(), target.value_type().kind())
    else {
        return false;
    };
    let (DataType::List(source_field), DataType::List(target_field)) =
        (source.data_type(), target.data_type())
    else {
        return false;
    };
    matches!(source_item.kind(), ValueKind::Scalar(DataType::Null))
        && source_field.data_type() == &DataType::Null
        && matches!(
            target_item.kind(),
            ValueKind::Nominal(ValueLogicalType::Json)
        )
        && target_item.nullable()
        && target_field.is_nullable()
        && target_field.data_type() == &DataType::Utf8
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ValueLogicalType, ValueTypeStorageLifetime};
    use arrow_schema::DataType;
    fn value(kind: ValueKind) -> ValueTypeRef {
        ValueTypeRef::try_new(kind, true, ValueTypeStorageLifetime::pure()).unwrap()
    }
    fn check(a: &ValueTypeRef, b: &ValueTypeRef) -> bool {
        preserves_value_domains_observed::<std::convert::Infallible>(a, b, |_| Ok(())).unwrap()
    }
    #[test]
    fn root_and_children_domains_need_explicit_conversion_owner() {
        let json = value(ValueKind::Nominal(ValueLogicalType::Json));
        let string = value(ValueKind::String);
        assert!(!check(&json, &string));
        assert!(!check(&string, &json));
        assert!(check(&json, &json));
        let nested = value(ValueKind::List(json));
        assert!(!check(&nested, &string));
        assert!(!check(&nested, &value(ValueKind::List(string.clone()))));
        assert!(check(&value(ValueKind::Scalar(DataType::Int32)), &string));
    }
    #[test]
    fn original_callback_refusal_stops_before_later_nodes() {
        let value = value(ValueKind::List(value(ValueKind::List(value(
            ValueKind::String,
        )))));
        let mut count = 0;
        preserves_value_domains_observed::<()>(&value, &value, |units| {
            count += units;
            Ok(())
        })
        .unwrap();
        for stop in 1..=count {
            let mut seen = 0;
            let result = preserves_value_domains_observed(&value, &value, |units| {
                seen += units;
                if seen >= stop { Err(stop) } else { Ok(()) }
            });
            assert_eq!(result, Err(stop));
            assert_eq!(seen, stop);
        }
    }
    fn list(item: ValueTypeRef, carrier: DataType, nullable: bool) -> FunctionValueType {
        let storage = ValueTypeStorageLifetime::pure();
        let layout = crate::ValueLayout::try_new(
            &DataType::List(std::sync::Arc::new(arrow_schema::Field::new(
                "item",
                carrier,
                item.nullable(),
            ))),
            storage.clone(),
        )
        .unwrap();
        FunctionValueType::try_from_value(
            ValueTypeRef::try_new(ValueKind::List(item), nullable, storage).unwrap(),
            layout,
        )
        .unwrap()
    }

    #[test]
    fn null_list_to_json_requires_the_exact_nominal_child_and_nullability_shape() {
        let source = list(
            value(ValueKind::Scalar(DataType::Null)),
            DataType::Null,
            true,
        );
        let json = value(ValueKind::Nominal(ValueLogicalType::Json));
        let target = list(json.clone(), DataType::Utf8, true);
        assert!(is_null_list_to_json_cast(&source, &target));
        assert!(
            !check(source.value_type(), target.value_type()),
            "NULL lifting must not become domain preservation"
        );
        let required_root = target.with_nullable(false).unwrap();
        assert!(!is_null_list_to_json_cast(&source, &required_root));
        assert!(is_null_list_to_json_cast(
            &source.with_nullable(false).unwrap(),
            &required_root
        ));
        let required_json = json
            .with_nullable(false, ValueTypeStorageLifetime::pure())
            .unwrap();
        assert!(!is_null_list_to_json_cast(
            &source,
            &list(required_json, DataType::Utf8, true)
        ));
        for ordinary in [
            list(value(ValueKind::String), DataType::Utf8, true),
            list(
                value(ValueKind::Scalar(DataType::Int32)),
                DataType::Int32,
                true,
            ),
        ] {
            assert!(!is_null_list_to_json_cast(&source, &ordinary));
        }
        let text_source = list(value(ValueKind::String), DataType::Utf8, true);
        assert!(!is_null_list_to_json_cast(&text_source, &target));
        assert!(!is_null_list_to_json_cast(
            &source,
            &FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap()
        ));
    }
}
