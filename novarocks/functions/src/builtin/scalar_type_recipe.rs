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
use super::signature::{Bindings, TypeSpec, realize};
use crate::{FunctionBindingError, FunctionValueType};
use arrow_schema::DataType;
use novarocks_type_contract::{
    BorrowedTypedValueBinding, CompileCheckpoints, ValueKind, ValueLogicalType,
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
    fn rule(self) -> Result<RealizationRule<'a>, String> {
        Ok(match self.spec {
            TypeSpec::Any(name)
            | TypeSpec::Decimal128Of(name)
            | TypeSpec::MapWithUninhabitedKey { map: name, .. } => RealizationRule::Source(
                self.bindings
                    .lookup_value_borrowed(name)
                    .ok_or_else(|| format!("value type variable {name} is unbound"))?,
            ),
            TypeSpec::AnyType | TypeSpec::AnyDecimal128 => {
                return Err("wildcard has no intrinsic result type".into());
            }
            TypeSpec::List(item) => RealizationRule::List(item),
            TypeSpec::Map(key, value) => RealizationRule::Map(key, value),
            spec => RealizationRule::Scalar(
                realize(spec, self.bindings)?,
                match spec {
                    TypeSpec::LargeInt => ValueLogicalType::LargeInt,
                    TypeSpec::Bitmap => ValueLogicalType::Bitmap,
                    TypeSpec::Json => ValueLogicalType::Json,
                    _ => ValueLogicalType::Physical,
                },
            ),
        })
    }
    pub(super) fn materialize_pure(self) -> Result<FunctionValueType, String> {
        self.materialize_with_construction(&mut super::scalar_construction::ScalarConstruction::<
            std::convert::Infallible,
        >::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ))
        .map_err(super::scalar_construction::pure_string)
    }
    pub(super) fn materialize_with_construction<E>(
        self,
        creation: &mut super::scalar_construction::ScalarConstruction<'_, E>,
    ) -> Result<FunctionValueType, super::scalar_construction::Failure<E>> {
        let result = match self
            .rule()
            .map_err(super::scalar_construction::Failure::Rule)?
        {
            RealizationRule::Source(source) => source.clone(),
            RealizationRule::Scalar(carrier, domain) => {
                creation.value(carrier, self.nullable, domain)?
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
        match self
            .rule()
            .map_err(|error| FunctionBindingError::InvalidBinding(error.into()))?
        {
            RealizationRule::Source(source) => source
                .typed_binding()
                .borrowed()
                .same_physical_binding_ignoring_root_nullability_observed(actual, || {
                    work.step().map_err(FunctionBindingError::from)
                }),
            RealizationRule::Scalar(carrier, domain) => {
                Self::matches_scalar(actual, &carrier, domain, work)
            }
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
        let RealizationRule::Scalar(carrier, _) = self
            .rule()
            .map_err(|error| FunctionBindingError::InvalidBinding(error.into()))?
        else {
            return Err(FunctionBindingError::InvalidBinding(
                "fixed scalar result-domain override has no original scalar carrier".into(),
            ));
        };
        Self::matches_scalar(actual, &carrier, domain, work)
    }
}
