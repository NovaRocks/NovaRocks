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
//! The original TypeSpec realization decision, consumed by either its pure
//! constructor or exact selected-type comparison. Binding/pass authorization
//! remains with the original resolver; this value is not a selection proof.
use super::scalar_construction::{Failure, ScalarConstruction, pure_value};
use super::signature::{Bindings, TypeSpec, realize};
use crate::{FunctionBindingError, FunctionValueType};
use arrow_schema::DataType;
use novarocks_type_contract::{
    BorrowedTypedValueBinding, CharacterKind, CharacterType, CompileCheckpoints, ValueKind,
    ValueLogicalType, ValueTypeError,
};

#[derive(Clone, Copy)]
pub(super) struct ScalarTypeRecipe<'a> {
    spec: &'a TypeSpec,
    bindings: &'a Bindings,
    nullable: bool,
}

enum RealizationRule<'a> {
    Source(&'a FunctionValueType),
    Scalar(DataType, ValueLogicalType),
    Character(CharacterType),
    List(&'a TypeSpec),
    Map(&'a TypeSpec, &'a TypeSpec),
}
impl<'a> ScalarTypeRecipe<'a> {
    pub(super) fn new(spec: &'a TypeSpec, bindings: &'a Bindings, nullable: bool) -> Self {
        Self {
            spec,
            bindings,
            nullable,
        }
    }
    fn child(self, spec: &'a TypeSpec, nullable: bool) -> Self {
        Self::new(spec, self.bindings, nullable)
    }
    fn rule<E>(self) -> Result<RealizationRule<'a>, Failure<E>> {
        Ok(match self.spec {
            TypeSpec::Any(name)
            | TypeSpec::Decimal128Of(name)
            | TypeSpec::Decimal256Of(name)
            | TypeSpec::MapWithUninhabitedKey { map: name, .. } => {
                RealizationRule::Source(self.bindings.lookup_value_borrowed(name).ok_or_else(
                    || Failure::Rule(format!("value type variable {name} is unbound")),
                )?)
            }
            TypeSpec::AnyType | TypeSpec::AnyDecimal128 => {
                return Err(Failure::Rule(
                    "wildcard has no intrinsic result type".into(),
                ));
            }
            TypeSpec::Character(kind, width) => {
                let width = self
                    .bindings
                    .character_width(*width)
                    .map_err(Failure::Rule)?;
                match (kind, width) {
                    (CharacterKind::Varchar, None) => {
                        RealizationRule::Scalar(DataType::Utf8, ValueLogicalType::Physical)
                    }
                    (CharacterKind::Varchar, Some(width))
                        if width > u64::from(CharacterType::MAX_VARCHAR_LENGTH) =>
                    {
                        // Trino's width ceiling denotes unbounded VARCHAR.
                        RealizationRule::Scalar(DataType::Utf8, ValueLogicalType::Physical)
                    }
                    (kind, Some(width)) => {
                        let character = u32::try_from(width)
                            .ok()
                            .and_then(|width| CharacterType::new(*kind, width))
                            .ok_or(ValueTypeError::InvalidCharacterWidth {
                                kind: *kind,
                                length: width,
                            })?;
                        RealizationRule::Character(character)
                    }
                    (CharacterKind::Char, None) => {
                        return Err(Failure::Rule("CHAR requires a finite width".into()));
                    }
                }
            }
            TypeSpec::List(item) => RealizationRule::List(item),
            TypeSpec::Map(key, value) => RealizationRule::Map(key, value),
            spec => RealizationRule::Scalar(
                realize(spec, self.bindings).map_err(Failure::Rule)?,
                match spec {
                    TypeSpec::LargeInt => ValueLogicalType::LargeInt,
                    TypeSpec::Bitmap => ValueLogicalType::Bitmap,
                    TypeSpec::Json => ValueLogicalType::Json,
                    TypeSpec::Uuid => ValueLogicalType::Uuid,
                    _ => ValueLogicalType::Physical,
                },
            ),
        })
    }
    fn binding_rule(self) -> Result<RealizationRule<'a>, FunctionBindingError> {
        self.rule().map_err(|error| match error {
            Failure::Rule(message) => FunctionBindingError::InvalidBinding(message.into()),
            error => pure_value(error).into(),
        })
    }

    pub(super) fn materialize_with_construction<E>(
        self,
        creation: &mut ScalarConstruction<'_, E>,
    ) -> Result<FunctionValueType, Failure<E>> {
        let result = match self.rule()? {
            RealizationRule::Source(source) => source.clone(),
            RealizationRule::Scalar(carrier, domain) => {
                creation.value(carrier, self.nullable, domain)?
            }
            RealizationRule::Character(character) => {
                creation.character(character, self.nullable)?
            }
            RealizationRule::List(item) => super::signature::declared_list_with_construction(
                &self
                    .child(item, true)
                    .materialize_with_construction(creation)?,
                self.nullable,
                creation,
            )?,
            RealizationRule::Map(key, value) => super::signature::declared_map_with_construction(
                &self
                    .child(key, false)
                    .materialize_with_construction(creation)?,
                &self
                    .child(value, true)
                    .materialize_with_construction(creation)?,
                self.nullable,
                creation,
            )?,
        };
        let result = creation.nullable(&result, self.nullable)?;
        result.validate()?;
        Ok(result)
    }
    pub(super) fn matches_value_observed(
        self,
        actual: BorrowedTypedValueBinding<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, FunctionBindingError> {
        work.step()?;
        if actual.value_type().nullable() != self.nullable {
            return Ok(false);
        }
        match self.binding_rule()? {
            RealizationRule::Source(source) => source
                .typed_binding()
                .borrowed()
                .same_physical_binding_ignoring_root_nullability_observed(actual, || {
                    work.step().map_err(FunctionBindingError::from)
                }),
            RealizationRule::Scalar(carrier, domain) => {
                Self::matches_scalar(actual, &carrier, domain, work)
            }
            RealizationRule::Character(character) => Ok(actual.value_type().kind()
                == &ValueKind::BoundedCharacter(character)
                && actual.data_type() == &DataType::Utf8),
            RealizationRule::List(item) => {
                if !matches!(actual.value_type().kind(), ValueKind::List(_))
                    || !matches!(actual.data_type(), DataType::List(_))
                {
                    return Ok(false);
                }
                let Some(child) = actual.child_at(0) else {
                    return Ok(false);
                };
                self.child(item, true).matches_value_observed(child, work)
            }
            RealizationRule::Map(key, value) => {
                if !matches!(actual.value_type().kind(), ValueKind::Map { .. })
                    || !matches!(actual.data_type(), DataType::Map(_, false))
                {
                    return Ok(false);
                }
                let (Some(a), Some(b)) = (actual.child_at(0), actual.child_at(1)) else {
                    return Ok(false);
                };
                if !self.child(key, false).matches_value_observed(a, work)? {
                    return Ok(false);
                }
                self.child(value, true).matches_value_observed(b, work)
            }
        }
    }
    fn matches_scalar(
        actual: BorrowedTypedValueBinding<'_>,
        carrier: &DataType,
        domain: ValueLogicalType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, FunctionBindingError> {
        work.step()?;
        let kind = if domain != ValueLogicalType::Physical {
            ValueKind::Nominal(domain)
        } else {
            match carrier {
                DataType::Utf8 | DataType::LargeUtf8 => ValueKind::String,
                DataType::Binary | DataType::LargeBinary => ValueKind::Binary,
                other => ValueKind::Scalar(other.clone()),
            }
        };
        if actual.value_type().kind() != &kind {
            return Ok(false);
        }
        novarocks_type_contract::arrow_data_types_exact_observed::<crate::KernelFailure>(
            actual.data_type(),
            carrier,
            || work.step().map_err(crate::kernel_control::compile_failure),
        )
        .map_err(super::binding_control::type_error)
    }
    /// Exact builtin result-domain overrides apply only to the original scalar
    /// carrier rule, never an inferred domain from an observed Arrow array.
    pub(super) fn matches_nominal_result_observed(
        self,
        actual: BorrowedTypedValueBinding<'_>,
        domain: ValueLogicalType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, FunctionBindingError> {
        work.step()?;
        if actual.value_type().nullable() != self.nullable {
            return Ok(false);
        }
        let RealizationRule::Scalar(carrier, _) = self.binding_rule()? else {
            return Err(FunctionBindingError::InvalidBinding(
                "fixed scalar result-domain override has no original scalar carrier".into(),
            ));
        };
        Self::matches_scalar(actual, &carrier, domain, work)
    }
}
