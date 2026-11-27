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

//! Canonical aggregate output and intermediate Arrow type contracts.

use std::sync::Arc;

use arrow_schema::DataType;
use novarocks_type_contract::canonical_agg_decimal_type;

use novarocks_type_contract as largeint;

/// Borrow the actual argument declarations without a carrier-copy vector.
#[derive(Clone, Copy)]
pub enum AggregateTypeArguments<'a> {
    Arrow(&'a [DataType]),
    Values(&'a [crate::FunctionArgument]),
}
impl<'a> AggregateTypeArguments<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Arrow(values) => values.len(),
            Self::Values(values) => values.len(),
        }
    }
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
    pub fn data_type(self, index: usize) -> Option<&'a DataType> {
        match self {
            Self::Arrow(values) => values.get(index),
            Self::Values(values) => match values.get(index)? {
                crate::FunctionArgument::Value { value_type, .. } => Some(value_type.data_type()),
                crate::FunctionArgument::Lambda { .. }
                | crate::FunctionArgument::CandidateGroup { .. } => None,
            },
        }
    }
    pub fn value_type(self, index: usize) -> Option<&'a crate::FunctionValueType> {
        match self {
            Self::Values(values) => match values.get(index)? {
                crate::FunctionArgument::Value { value_type, .. } => Some(value_type),
                crate::FunctionArgument::Lambda { .. }
                | crate::FunctionArgument::CandidateGroup { .. } => None,
            },
            Self::Arrow(_) => None,
        }
    }
}

/// The original aggregate type rule's lazy construction decision. It borrows
/// complete arguments; no expected tree, Field, name or argument Vec is built
/// while deciding or replaying the selected signature.
#[derive(Clone)]
pub enum AggregateTypeRecipe<'a> {
    Scalar(DataType),
    Argument(AggregateTypeArguments<'a>, usize),
    List(AggregateTypeArguments<'a>, usize),
    FloatList,
    OrderedLists(AggregateTypeArguments<'a>),
    Map(AggregateTypeArguments<'a>, usize, usize),
    NullMap,
    TopK(AggregateTypeArguments<'a>, usize),
}

impl<'a> AggregateTypeRecipe<'a> {
    /// Replay this exact complete decision against an immutable selected port.
    /// Borrowed proof children avoid Field/type holders and repeat raw grammar.
    pub(crate) fn matches_value_observed(
        &self,
        actual: novarocks_type_contract::BorrowedTypedValueBinding<'_>,
        nullable: bool,
        domain: novarocks_type_contract::ValueLogicalType,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<bool, crate::FunctionBindingError> {
        self.matches_value_from_sources_observed(actual, nullable, domain, None, work)
    }

    fn matches_value_from_sources_observed(
        &self,
        actual: novarocks_type_contract::BorrowedTypedValueBinding<'_>,
        nullable: bool,
        domain: novarocks_type_contract::ValueLogicalType,
        selected: Option<&[crate::FunctionArgumentType]>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<bool, crate::FunctionBindingError> {
        use novarocks_type_contract::{ValueKind, ValueLogicalType as L};
        work.step()?;
        if actual.value_type().nullable() != nullable {
            return Ok(false);
        }
        let same_source =
            |source: AggregateTypeArguments<'a>,
             index,
             actual: novarocks_type_contract::BorrowedTypedValueBinding<'_>,
             work: &mut novarocks_type_contract::CompileCheckpoints<'_>| {
                let source = match selected {
                    Some(arguments) => match arguments.get(index) {
                        Some(crate::FunctionArgumentType::Value(value)) => Some(value),
                        _ => None,
                    },
                    None => source.value_type(index),
                }
                .ok_or(crate::FunctionBindingError::NoMatchingOverload)?;
                source
                    .typed_binding()
                    .borrowed()
                    .same_physical_binding_ignoring_root_nullability_observed(actual, || {
                        work.step().map_err(crate::FunctionBindingError::from)
                    })
            };
        let name = |actual: &str,
                    expected: &str,
                    work: &mut novarocks_type_contract::CompileCheckpoints<'_>|
         -> Result<bool, crate::FunctionBindingError> {
            work.step()?;
            if actual.len() != expected.len() {
                return Ok(false);
            }
            for (a, b) in actual.bytes().zip(expected.bytes()) {
                work.step()?;
                if a != b {
                    return Ok(false);
                }
            }
            Ok(true)
        };
        let scalar = |actual: novarocks_type_contract::BorrowedTypedValueBinding<'_>,
                      expected: &DataType,
                      domain: L,
                      work: &mut novarocks_type_contract::CompileCheckpoints<'_>|
         -> Result<bool, crate::FunctionBindingError> {
            work.step()?;
            if actual.data_type() != expected {
                return Ok(false);
            }
            Ok(if domain != L::Physical {
                matches!(actual.value_type().kind(), ValueKind::Nominal(value) if *value == domain)
            } else {
                match expected {
                    DataType::Utf8 => matches!(actual.value_type().kind(), ValueKind::String),
                    DataType::Binary => matches!(actual.value_type().kind(), ValueKind::Binary),
                    _ => {
                        matches!(actual.value_type().kind(), ValueKind::Scalar(value) if value == expected)
                    }
                }
            })
        };
        Ok(match self {
            Self::Scalar(expected) => scalar(actual, expected, domain, work)?,
            Self::Argument(source, index) => same_source(*source, *index, actual, work)?,
            Self::List(source, index) => match recipe_list_child(actual, work)? {
                Some(child) => same_source(*source, *index, child, work)?,
                None => false,
            },
            Self::FloatList => match recipe_list_child(actual, work)? {
                Some(child) => scalar(child, &DataType::Float64, L::Physical, work)?,
                None => false,
            },
            Self::OrderedLists(source) => {
                let (ValueKind::Struct(semantic), DataType::Struct(fields)) =
                    (actual.value_type().kind(), actual.data_type())
                else {
                    return Ok(false);
                };
                let channels = selected.map_or_else(|| source.len(), |arguments| arguments.len());
                if fields.len() != channels || semantic.len() != channels {
                    return Ok(false);
                }
                for index in 0..channels {
                    work.step()?;
                    if !fields[index].is_nullable()
                        || !ordinal_field_name_matches(semantic[index].name(), index, work)?
                        || !ordinal_field_name_matches(fields[index].name(), index, work)?
                    {
                        return Ok(false);
                    }
                    let Some(field) = actual
                        .child_at(index)
                        .filter(|field| field.value_type().nullable())
                    else {
                        return Ok(false);
                    };
                    let Some(child) = recipe_list_child(field, work)? else {
                        return Ok(false);
                    };
                    if !same_source(*source, index, child, work)? {
                        return Ok(false);
                    }
                }
                true
            }
            Self::Map(source, key, val) => {
                let DataType::Map(entries, sorted) = actual.data_type() else {
                    return Ok(false);
                };
                let DataType::Struct(fields) = entries.data_type() else {
                    return Ok(false);
                };
                if *sorted
                    || entries.is_nullable()
                    || !name(entries.name(), "entries", work)?
                    || fields.len() != 2
                    || fields[0].is_nullable()
                    || !fields[1].is_nullable()
                    || !name(fields[0].name(), "key", work)?
                    || !name(fields[1].name(), "value", work)?
                {
                    return Ok(false);
                }
                let (Some(k), Some(v)) = (actual.child_at(0), actual.child_at(1)) else {
                    return Ok(false);
                };
                matches!(actual.value_type().kind(), ValueKind::Map { .. })
                    && !k.value_type().nullable()
                    && v.value_type().nullable()
                    && same_source(*source, *key, k, work)?
                    && same_source(*source, *val, v, work)?
            }
            Self::NullMap => {
                let DataType::Map(entries, sorted) = actual.data_type() else {
                    return Ok(false);
                };
                let DataType::Struct(fields) = entries.data_type() else {
                    return Ok(false);
                };
                if *sorted
                    || entries.is_nullable()
                    || !name(entries.name(), "entries", work)?
                    || fields.len() != 2
                    || fields[0].is_nullable()
                    || !fields[1].is_nullable()
                    || !name(fields[0].name(), "key", work)?
                    || !name(fields[1].name(), "value", work)?
                {
                    return Ok(false);
                }
                let (Some(k), Some(v)) = (actual.child_at(0), actual.child_at(1)) else {
                    return Ok(false);
                };
                matches!(actual.value_type().kind(), ValueKind::Map { .. })
                    && !k.value_type().nullable()
                    && v.value_type().nullable()
                    && scalar(k, &DataType::Null, L::Physical, work)?
                    && scalar(v, &DataType::Null, L::Physical, work)?
            }
            Self::TopK(source, index) => {
                let Some(child) = recipe_list_child(actual, work)? else {
                    return Ok(false);
                };
                let (ValueKind::Struct(semantic), DataType::Struct(fields)) =
                    (child.value_type().kind(), child.data_type())
                else {
                    return Ok(false);
                };
                if semantic.len() != 2 || fields.len() != 2 {
                    return Ok(false);
                }
                for (index, expected) in ["item", "count"].into_iter().enumerate() {
                    if !semantic[index].value_type().nullable()
                        || !fields[index].is_nullable()
                        || !name(semantic[index].name(), expected, work)?
                        || !name(fields[index].name(), expected, work)?
                    {
                        return Ok(false);
                    }
                }
                let (Some(item), Some(count)) = (child.child_at(0), child.child_at(1)) else {
                    return Ok(false);
                };
                same_source(*source, *index, item, work)?
                    && scalar(count, &DataType::Int64, L::Physical, work)?
            }
        })
    }

    /// Pure first author of this same complete decision. Generic runtime
    /// creation is supplied by the guarded-selection adapter, never by replay.
    pub(crate) fn materialize_value_pure(
        &self,
        nullable: bool,
        domain: novarocks_type_contract::ValueLogicalType,
    ) -> Result<crate::FunctionValueType, crate::FunctionBindingError> {
        use arrow_schema::Field;
        use novarocks_type_contract::{
            ValueField, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
        };
        let value = |source: AggregateTypeArguments<'a>, index| {
            source
                .value_type(index)
                .ok_or(crate::FunctionBindingError::NoMatchingOverload)
        };
        let mut storage = ValueTypeStorageLifetime::pure();
        let mut retain =
            |source: &crate::FunctionValueType| -> Result<(), crate::FunctionBindingError> {
                storage = storage.retaining_observed(
                &source.typed_binding().layout().storage_lifetime(),
                |_| Ok::<_, novarocks_type_contract::owned_resources::layout::LayoutResourceError>(()),
            ).map_err(|_| crate::FunctionBindingError::Control(novarocks_type_contract::CompileControlError::ResourceExhausted))?;
                Ok(())
            };
        let (kind, layout) = match self {
            Self::Scalar(layout) => {
                return crate::FunctionValueType::try_with_logical_type(
                    layout.clone(),
                    nullable,
                    domain,
                )
                .map_err(Into::into);
            }
            Self::Argument(source, index) => {
                return value(*source, *index)?
                    .with_nullable(nullable)
                    .map_err(Into::into);
            }
            Self::List(source, index) => {
                let source = value(*source, *index)?;
                retain(source)?;
                let child = source.with_nullable(true)?;
                (
                    ValueKind::List(child.value_type().clone()),
                    DataType::List(Arc::new(Field::new(
                        "item",
                        child.data_type().clone(),
                        true,
                    ))),
                )
            }
            Self::FloatList => {
                let child = crate::FunctionValueType::new(DataType::Float64, true);
                (
                    ValueKind::List(child.value_type().clone()),
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                )
            }
            Self::OrderedLists(source) => {
                let mut semantics = Vec::with_capacity(source.len());
                let mut fields = Vec::with_capacity(source.len());
                for index in 0..source.len() {
                    let source = value(*source, index)?;
                    retain(source)?;
                    let list = crate::builtin::signature::declared_list(source, true)?;
                    let name = format!("c{index}");
                    fields.push(Arc::new(Field::new(&name, list.data_type().clone(), true)));
                    semantics.push(ValueField::new(
                        name,
                        list.value_type().clone(),
                        ValueTypeStorageLifetime::pure(),
                    ));
                }
                (
                    ValueKind::Struct(semantics.into_boxed_slice()),
                    DataType::Struct(fields.into()),
                )
            }
            Self::Map(source, key, val) => {
                let key_source = value(*source, *key)?;
                let value_source = value(*source, *val)?;
                retain(key_source)?;
                retain(value_source)?;
                let key = key_source.with_nullable(false)?;
                let val = value_source.with_nullable(true)?;
                (
                    ValueKind::Map {
                        key: key.value_type().clone(),
                        value: val.value_type().clone(),
                    },
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Arc::new(Field::new("key", key.data_type().clone(), false)),
                                    Arc::new(Field::new("value", val.data_type().clone(), true)),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                )
            }
            Self::NullMap => {
                let key = crate::FunctionValueType::new(DataType::Null, false);
                let val = crate::FunctionValueType::new(DataType::Null, true);
                (
                    ValueKind::Map {
                        key: key.value_type().clone(),
                        value: val.value_type().clone(),
                    },
                    self.materialize_arrow(),
                )
            }
            Self::TopK(source, index) => {
                let source = value(*source, *index)?;
                retain(source)?;
                let child = source.with_nullable(true)?;
                let count = crate::FunctionValueType::new(DataType::Int64, true);
                let item = ValueTypeRef::try_new(
                    ValueKind::Struct(
                        vec![
                            ValueField::new(
                                "item",
                                child.value_type().clone(),
                                ValueTypeStorageLifetime::pure(),
                            ),
                            ValueField::new(
                                "count",
                                count.value_type().clone(),
                                ValueTypeStorageLifetime::pure(),
                            ),
                        ]
                        .into_boxed_slice(),
                    ),
                    true,
                    ValueTypeStorageLifetime::pure(),
                )
                .map_err(novarocks_type_contract::ValueTypeError::from)?;
                (
                    ValueKind::List(item),
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(
                            vec![
                                Arc::new(Field::new("item", child.data_type().clone(), true)),
                                Arc::new(Field::new("count", DataType::Int64, true)),
                            ]
                            .into(),
                        ),
                        true,
                    ))),
                )
            }
        };
        let semantic = ValueTypeRef::try_new(kind, nullable, ValueTypeStorageLifetime::pure())
            .map_err(novarocks_type_contract::ValueTypeError::from)?;
        crate::FunctionValueType::try_from_value(
            semantic,
            ValueLayout::try_from_normalized_arrow(layout, storage)
                .map_err(novarocks_type_contract::ValueTypeError::from)?,
        )
        .map_err(Into::into)
    }

    /// Explicit pure compatibility projection of this same decision. Execution
    /// binding authors retain the complete semantic children separately.
    pub fn materialize_arrow(&self) -> DataType {
        use arrow_schema::{Field, Fields};
        let argument = |source: AggregateTypeArguments<'a>, index| {
            source.data_type(index).cloned().unwrap_or(DataType::Null)
        };
        let list = |item| DataType::List(Arc::new(Field::new("item", item, true)));
        let map = |key, value| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", key, false)),
                            Arc::new(Field::new("value", value, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        match self {
            Self::Scalar(ty) => ty.clone(),
            Self::Argument(source, index) => argument(*source, *index),
            Self::List(source, index) => list(argument(*source, *index)),
            Self::FloatList => list(DataType::Float64),
            Self::OrderedLists(source) => DataType::Struct(Fields::from(
                (0..source.len())
                    .map(|index| {
                        Arc::new(Field::new(
                            format!("c{index}"),
                            list(argument(*source, index)),
                            true,
                        ))
                    })
                    .collect::<Vec<_>>(),
            )),
            Self::Map(source, key, value) => {
                map(argument(*source, *key), argument(*source, *value))
            }
            Self::NullMap => map(DataType::Null, DataType::Null),
            Self::TopK(source, index) => list(DataType::Struct(
                vec![
                    Arc::new(Field::new("item", argument(*source, *index), true)),
                    Arc::new(Field::new("count", DataType::Int64, true)),
                ]
                .into(),
            )),
        }
    }
}

/// Replay the original TopK decision with the installed complete arguments.
/// The recipe decision only reads its first carrier; its source-child proof
/// borrows the selected declaration, without reconstructing an expected tree.
pub(crate) fn matches_selected_top_k_types_observed(
    arguments: &[crate::FunctionArgumentType],
    output: &crate::FunctionValueType,
    state: &crate::FunctionValueType,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, crate::FunctionBindingError> {
    let Some(crate::FunctionArgumentType::Value(first)) = arguments.first() else {
        return Err(crate::FunctionBindingError::NoMatchingOverload);
    };
    let (output_recipe, state_recipe) = aggregate_type_recipes(
        "approx_top_k",
        AggregateTypeArguments::Arrow(std::slice::from_ref(first.data_type())),
    )
    .map_err(|_| crate::FunctionBindingError::NoMatchingOverload)?;
    Ok(output_recipe.matches_value_from_sources_observed(
        output.typed_binding().borrowed(),
        true,
        novarocks_type_contract::ValueLogicalType::Physical,
        Some(arguments),
        work,
    )? && state_recipe.matches_value_from_sources_observed(
        state.typed_binding().borrowed(),
        true,
        novarocks_type_contract::ValueLogicalType::Physical,
        Some(arguments),
        work,
    )?)
}

/// Borrow the installed ARRAY source channels through the same complete
/// recipe matcher, without rebuilding fields or copying carrier declarations.
pub(crate) fn matches_selected_array_types_observed(
    name: &str,
    arguments: &[crate::FunctionArgumentType],
    output: &crate::FunctionValueType,
    state: &crate::FunctionValueType,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, crate::FunctionBindingError> {
    if !matches!(name, "array_agg" | "array_agg_distinct") {
        return Err(crate::FunctionBindingError::NoMatchingOverload);
    }
    let Some(crate::FunctionArgumentType::Value(first)) = arguments.first() else {
        return Err(crate::FunctionBindingError::NoMatchingOverload);
    };
    // The original decision reads the first carrier and the actual selected
    // channel count. Each source proof reads its complete selected channel;
    // ORDER children never come from an output/state projection.
    let (output_recipe, state_recipe) = aggregate_type_recipes_core(
        name,
        AggregateTypeArguments::Arrow(std::slice::from_ref(first.data_type())),
        arguments.len(),
    )
    .map_err(|_| crate::FunctionBindingError::NoMatchingOverload)?;
    Ok(output_recipe.matches_value_from_sources_observed(
        output.typed_binding().borrowed(),
        true,
        novarocks_type_contract::ValueLogicalType::Physical,
        Some(arguments),
        work,
    )? && state_recipe.matches_value_from_sources_observed(
        state.typed_binding().borrowed(),
        true,
        novarocks_type_contract::ValueLogicalType::Physical,
        Some(arguments),
        work,
    )?)
}

/// Decide output/state layouts through the sole original aggregate rule.
/// NULL promises and semantic domains belong to the actual binding author.
pub fn aggregate_type_recipes<'a>(
    name: &str,
    arguments: AggregateTypeArguments<'a>,
) -> Result<(AggregateTypeRecipe<'a>, AggregateTypeRecipe<'a>), String> {
    aggregate_type_recipes_core(name, arguments, arguments.len())
}

fn aggregate_type_recipes_core<'a>(
    name: &str,
    arguments: AggregateTypeArguments<'a>,
    argument_count: usize,
) -> Result<(AggregateTypeRecipe<'a>, AggregateTypeRecipe<'a>), String> {
    use AggregateTypeRecipe as R;
    let first = arguments.data_type(0).unwrap_or(&DataType::Null);
    let scalar = R::Scalar;
    let existing = || R::Argument(arguments, 0);
    let binary = || scalar(DataType::Binary);
    Ok(match name {
        name if is_state_combinator_aggregate_function(name) => (binary(), binary()),
        "count" | "count_if" => (scalar(DataType::Int64), scalar(DataType::Int64)),
        "sum" => match first {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => (scalar(DataType::Int64), scalar(DataType::Decimal128(38, 0))),
            DataType::Float32 | DataType::Float64 => {
                (scalar(DataType::Float64), scalar(DataType::Float64))
            }
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                (existing(), scalar(DataType::Decimal256(76, 0)))
            }
            DataType::Decimal128(..) => {
                let out =
                    canonical_agg_decimal_type("sum", first).expect("sum decimal canonical type");
                let DataType::Decimal128(_, scale) = out else {
                    unreachable!("canonical SUM decimal is a 128-bit decimal")
                };
                (scalar(out), scalar(DataType::Decimal256(76, scale)))
            }
            _ => (scalar(DataType::Float64), scalar(DataType::Float64)),
        },
        "avg" | "multi_distinct_avg" => {
            if name == "multi_distinct_avg"
                && !matches!(
                    first,
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Decimal128(..)
                        | DataType::Decimal256(..)
                )
            {
                return Err(format!(
                    "multi_distinct_avg unsupported input type: {first:?}"
                ));
            }
            let out = match first {
                DataType::Decimal128(..) => scalar(
                    canonical_agg_decimal_type("avg", first).expect("avg decimal canonical type"),
                ),
                DataType::Decimal256(..) => existing(),
                _ => scalar(DataType::Float64),
            };
            (
                out,
                scalar(if name == "multi_distinct_avg" {
                    DataType::Binary
                } else {
                    DataType::Utf8
                }),
            )
        }
        "min" | "max" | "any_value" => (existing(), existing()),
        "group_concat" | "string_agg" | "listagg" => {
            (scalar(DataType::Utf8), R::OrderedLists(arguments))
        }
        "bool_or" | "bool_and" | "boolor_agg" | "booland_agg" | "every" => {
            (scalar(DataType::Boolean), scalar(DataType::Boolean))
        }
        "array_agg" | "array_agg_distinct" => {
            let list = R::List(arguments, 0);
            (
                list.clone(),
                if argument_count <= 1 {
                    list
                } else {
                    R::OrderedLists(arguments)
                },
            )
        }
        "map_agg" => (R::Map(arguments, 0, 1), R::Map(arguments, 0, 1)),
        "approx_count_distinct" | "ndv" | "multi_distinct_count" => {
            (scalar(DataType::Int64), binary())
        }
        "multi_distinct_sum" => {
            let out = match first {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    scalar(DataType::Int64)
                }
                DataType::Float32 | DataType::Float64 => scalar(DataType::Float64),
                DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                    existing()
                }
                DataType::Decimal128(..) => scalar(
                    canonical_agg_decimal_type("multi_distinct_sum", first)
                        .expect("multi_distinct_sum decimal canonical type"),
                ),
                _ => scalar(DataType::Float64),
            };
            (out, binary())
        }
        "max_by" | "min_by" => (existing(), binary()),
        "covar_pop" | "covar_samp" | "corr" | "var_pop" | "var_samp" | "variance"
        | "variance_pop" | "variance_samp" | "stddev" | "stddev_pop" | "stddev_samp" | "std" => {
            (scalar(DataType::Float64), binary())
        }
        "percentile_approx" | "percentile_approx_weighted" => {
            let index = if name == "percentile_approx" { 1 } else { 2 };
            (
                if matches!(arguments.data_type(index), Some(DataType::List(_))) {
                    R::FloatList
                } else {
                    scalar(DataType::Float64)
                },
                binary(),
            )
        }
        "approx_top_k" => (R::TopK(arguments, 0), binary()),
        "min_n" | "max_n" => (R::List(arguments, 0), binary()),
        _ => return Err(format!("unknown aggregate function `{name}`")),
    })
}

/// Pure legacy Arrow projection. Selection and replay share the decision above;
/// this adapter does not own a second aggregate inference table.
pub fn infer_agg_function_types(
    name: &str,
    arg_types: &[DataType],
    _is_distinct: bool,
) -> Result<(DataType, Option<DataType>), String> {
    let (output, state) = aggregate_type_recipes(name, AggregateTypeArguments::Arrow(arg_types))?;
    Ok((output.materialize_arrow(), Some(state.materialize_arrow())))
}

fn is_state_combinator_aggregate_function(name: &str) -> bool {
    matches!(
        name,
        "count_state"
            | "count_state_signed"
            | "sum_state"
            | "sum_state_merge"
            | "sum_state_signed"
            | "avg_state"
            | "avg_state_merge"
            | "avg_state_signed"
            | "count_state_merge"
            | "min_state"
            | "min_state_merge"
            | "min_state_signed"
            | "max_state"
            | "max_state_merge"
            | "max_state_signed"
            | "bool_or_state"
            | "bool_or_state_merge"
            | "bool_or_state_signed"
            | "bool_and_state"
            | "bool_and_state_merge"
            | "bool_and_state_signed"
            | "count_distinct_state"
            | "count_distinct_state_merge"
            | "count_distinct_state_signed"
            | "approx_count_distinct_state"
            | "approx_count_distinct_state_merge"
            | "approx_count_distinct_state_signed"
    )
}

/// Internal aggregate selected by SQL for an admitted DISTINCT modifier.
#[derive(Clone, Copy, Debug)]
pub enum DistinctAggregate {
    Count,
    Sum,
    Avg,
}

impl crate::EngineFunctionCatalog {
    pub fn resolve_distinct_aggregate(
        &self,
        aggregate: DistinctAggregate,
        request: crate::FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<crate::ResolvedFunctionBinding, crate::FunctionBindingError> {
        let (function, overload) = match aggregate {
            DistinctAggregate::Count => (
                "builtin.aggregate/multi_distinct_count/v1",
                "builtin.aggregate/multi_distinct_count/derived-v1",
            ),
            DistinctAggregate::Sum => (
                "builtin.aggregate/multi_distinct_sum/v1",
                "builtin.aggregate/multi_distinct_sum/derived-v1",
            ),
            DistinctAggregate::Avg => (
                "builtin.aggregate/multi_distinct_avg/v1",
                "builtin.aggregate/multi_distinct_avg/derived-v1",
            ),
        };
        self.resolve_bound_trusted_at_overload(
            &crate::FunctionId::try_new(function).expect("valid DISTINCT aggregate identity"),
            crate::FunctionKind::Aggregate,
            &crate::FunctionOverloadId::try_new(overload)
                .expect("valid DISTINCT overload identity"),
            request,
            control,
        )
    }
}

fn recipe_list_child<'a>(
    actual: novarocks_type_contract::BorrowedTypedValueBinding<'a>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<
    Option<novarocks_type_contract::BorrowedTypedValueBinding<'a>>,
    crate::FunctionBindingError,
> {
    work.step()?;
    let DataType::List(field) = actual.data_type() else {
        return Ok(None);
    };
    if !matches!(
        actual.value_type().kind(),
        novarocks_type_contract::ValueKind::List(_)
    ) || !field.is_nullable()
        || field.name() != "item"
    {
        return Ok(None);
    }
    Ok(actual
        .child_at(0)
        .filter(|child| child.value_type().nullable()))
}
fn ordinal_field_name_matches(
    actual: &str,
    index: usize,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, crate::FunctionBindingError> {
    let mut digits = [0u8; std::mem::size_of::<usize>() * 3];
    let mut at = digits.len();
    let mut value = index;
    loop {
        work.step()?;
        at -= 1;
        digits[at] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    work.step()?;
    if actual.len() != digits.len() - at + 1 {
        return Ok(false);
    }
    for (a, b) in actual
        .bytes()
        .zip(std::iter::once(b'c').chain(digits[at..].iter().copied()))
    {
        work.step()?;
        if a != b {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Fields};
    use novarocks_type_contract::canonical_agg_decimal_type;

    use super::infer_agg_function_types;

    #[test]
    fn distinct_avg_has_its_own_binary_state_and_canonical_decimal_result() {
        assert_eq!(
            infer_agg_function_types("multi_distinct_avg", &[DataType::Decimal256(50, 2)], true)
                .unwrap(),
            (DataType::Decimal256(50, 2), Some(DataType::Binary))
        );
        assert_eq!(
            infer_agg_function_types("multi_distinct_avg", &[DataType::Int64], true).unwrap(),
            (DataType::Float64, Some(DataType::Binary))
        );
        assert_eq!(
            infer_agg_function_types("multi_distinct_avg", &[DataType::Decimal128(10, 2)], true)
                .unwrap(),
            (DataType::Decimal128(38, 8), Some(DataType::Binary))
        );
        assert_eq!(
            infer_agg_function_types("avg", &[DataType::Int64], false).unwrap(),
            (DataType::Float64, Some(DataType::Utf8))
        );
    }

    #[test]
    fn ordinary_decimal256_avg_retains_bound_precision_scale_and_utf8_state() {
        for input in [DataType::Decimal256(50, 10), DataType::Decimal256(76, 0)] {
            assert_eq!(
                infer_agg_function_types("avg", std::slice::from_ref(&input), false).unwrap(),
                (input, Some(DataType::Utf8))
            );
        }
    }

    #[test]
    fn infers_core_numeric_aggregate_types() {
        assert_eq!(
            infer_agg_function_types("count", &[], false).unwrap(),
            (DataType::Int64, Some(DataType::Int64))
        );
        assert_eq!(
            infer_agg_function_types("sum", &[DataType::Int32], false).unwrap(),
            (DataType::Int64, Some(DataType::Decimal128(38, 0)))
        );
        assert_eq!(
            infer_agg_function_types("sum", &[DataType::FixedSizeBinary(16)], false).unwrap(),
            (
                DataType::FixedSizeBinary(16),
                Some(DataType::Decimal256(76, 0))
            )
        );
        assert_eq!(
            infer_agg_function_types("sum", &[DataType::Float32], false).unwrap(),
            (DataType::Float64, Some(DataType::Float64))
        );
        assert_eq!(
            infer_agg_function_types("avg", &[DataType::Float64], false).unwrap(),
            (DataType::Float64, Some(DataType::Utf8))
        );
    }

    #[test]
    fn infers_decimal_and_distinct_sum_contracts() {
        let input = DataType::Decimal128(20, 2);
        let sum = canonical_agg_decimal_type("sum", &input).unwrap();
        let distinct = canonical_agg_decimal_type("multi_distinct_sum", &input).unwrap();
        assert_eq!(sum, DataType::Decimal128(38, 2));
        assert_eq!(
            infer_agg_function_types("sum", std::slice::from_ref(&input), false).unwrap(),
            (sum, Some(DataType::Decimal256(76, 2)))
        );
        assert_eq!(
            infer_agg_function_types("multi_distinct_sum", &[input], true).unwrap(),
            (distinct, Some(DataType::Binary))
        );
    }

    #[test]
    fn infers_state_collection_and_rejects_unknown_aggregates() {
        assert_eq!(
            infer_agg_function_types("sum_state_merge", &[DataType::Int64], false).unwrap(),
            (DataType::Binary, Some(DataType::Binary))
        );
        let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(
            infer_agg_function_types("array_agg", &[DataType::Utf8], false).unwrap(),
            (list.clone(), Some(list))
        );
        assert_eq!(
            infer_agg_function_types("unknown_zero_arg", &[], false).unwrap_err(),
            "unknown aggregate function `unknown_zero_arg`"
        );
    }

    #[test]
    fn preserves_nested_collection_type_structure() {
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Int32, true)),
                ])),
                false,
            )),
            false,
        );
        assert_eq!(
            infer_agg_function_types("map_agg", &[DataType::Utf8, DataType::Int32], false,)
                .unwrap(),
            (map.clone(), Some(map))
        );

        let top_k = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("item", DataType::Utf8, true)),
                Arc::new(Field::new("count", DataType::Int64, true)),
            ])),
            true,
        )));
        assert_eq!(
            infer_agg_function_types("approx_top_k", &[DataType::Utf8], false).unwrap(),
            (top_k, Some(DataType::Binary))
        );

        let output = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let intermediate = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new(
                "c0",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            )),
            Arc::new(Field::new(
                "c1",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            )),
        ]));
        assert_eq!(
            infer_agg_function_types("array_agg", &[DataType::Utf8, DataType::Int64], false,)
                .unwrap(),
            (output, Some(intermediate))
        );
    }
}
