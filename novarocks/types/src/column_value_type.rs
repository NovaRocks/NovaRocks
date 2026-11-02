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

//! Catalog declaration authoring, independent of execution annotations.

use crate::schema::{ColumnDef, SqlType};
use crate::{
    ExecutionColumnTypeError, execution_value_type_from_declaration, sql_type_from_execution_value,
    validate_execution_declaration_observed,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    FunctionValueType, ValueLogicalType, ValueTypeError, ValueTypeStorageLifetime,
    validate_arrow_layout_structure_observed,
};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnValueTypeError {
    Type(ValueTypeError),
    DeclarationMismatch,
    UnrepresentableRootDomain(ValueLogicalType),
    UnrepresentableCarrier(DataType),
}
impl From<ValueTypeError> for ColumnValueTypeError {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}
impl From<ExecutionColumnTypeError> for ColumnValueTypeError {
    fn from(error: ExecutionColumnTypeError) -> Self {
        match error {
            ExecutionColumnTypeError::DeclarationMismatch => Self::DeclarationMismatch,
            ExecutionColumnTypeError::Type(error) => Self::Type(error.into()),
            ExecutionColumnTypeError::UnrepresentableValue(reason) => {
                Self::Type(ValueTypeError::InvalidType(reason))
            }
            ExecutionColumnTypeError::UnrepresentableCarrier(carrier) => {
                Self::UnrepresentableCarrier(carrier)
            }
        }
    }
}
impl fmt::Display for ColumnValueTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Type(error) => error.fmt(f),
            Self::DeclarationMismatch => {
                f.write_str("column declaration conflicts with its execution value layout")
            }
            Self::UnrepresentableCarrier(carrier) => {
                write!(f, "column schema cannot represent the {carrier:?} carrier")
            }
            Self::UnrepresentableRootDomain(domain) => write!(
                f,
                "column schema cannot represent the {domain:?} root domain"
            ),
        }
    }
}
impl std::error::Error for ColumnValueTypeError {}

pub fn sql_type_from_value_type(
    value: &FunctionValueType,
) -> Result<SqlType, ColumnValueTypeError> {
    sql_type_from_execution_value(value.typed_binding()).map_err(Into::into)
}

impl ColumnDef {
    /// Persist the complete recursive declaration, including nominal children.
    /// Catalog facts own their ordinary schema representation independently;
    /// runtime frozen ports retain the original complete FunctionValueType.
    pub fn from_value_type(
        name: String,
        value_type: FunctionValueType,
        write_default: Option<crate::schema::ColumnDefault>,
    ) -> Result<Self, ColumnValueTypeError> {
        let logical_type = Some(sql_type_from_value_type(&value_type)?);
        Ok(Self {
            name,
            data_type: value_type.data_type().clone(),
            nullable: value_type.nullable(),
            logical_type,
            write_default,
        })
    }

    pub fn declared_value_type(&self) -> Result<FunctionValueType, ColumnValueTypeError> {
        self.declared_value_type_observed(|| Ok(()))
    }

    /// Read the explicit SQL declaration and borrowed carrier NULL/layout facts.
    /// This is a catalog authoring boundary. Nested Arrow annotations contribute
    /// no domains; an absent declaration explicitly authors ordinary values.
    pub fn declared_value_type_observed<E: From<ColumnValueTypeError> + From<ValueTypeError>>(
        &self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<FunctionValueType, E> {
        validate_arrow_layout_structure_observed(&self.data_type, |_| observe())?;
        let value = match &self.logical_type {
            None => FunctionValueType::try_with_logical_type(
                self.data_type.clone(),
                self.nullable,
                ValueLogicalType::Physical,
            )
            .map_err(ColumnValueTypeError::from)
            .map_err(E::from)?,
            Some(declaration) => {
                validate_execution_declaration_observed(declaration, || {
                    observe().map_err(DeclarationWalkError::Caller)
                })
                .map_err(|error| match error {
                    DeclarationWalkError::Caller(error) => error,
                    DeclarationWalkError::Source(error) => {
                        E::from(ColumnValueTypeError::from(error))
                    }
                })?;
                observe()?;
                let binding = execution_value_type_from_declaration(
                    declaration,
                    &self.data_type,
                    self.nullable,
                    ValueTypeStorageLifetime::pure(),
                )
                .map_err(ColumnValueTypeError::from)
                .map_err(E::from)?;
                FunctionValueType::try_from_typed_binding(binding)
                    .map_err(ColumnValueTypeError::from)
                    .map_err(E::from)?
            }
        };
        // Retained construction work remains under this caller's observation.
        novarocks_type_contract::visit_value_type_observed(value.value_type(), |_| observe())?;
        observe()?;
        Ok(value)
    }
}

enum DeclarationWalkError<E> {
    Caller(E),
    Source(ExecutionColumnTypeError),
}
impl<E> From<ExecutionColumnTypeError> for DeclarationWalkError<E> {
    fn from(error: ExecutionColumnTypeError) -> Self {
        Self::Source(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
    use std::sync::Arc;
    #[test]
    fn unrepresentable_scalar_retains_exact_carrier_class_at_nested_occurrences() {
        for carrier in [DataType::Null, DataType::UInt64] {
            let value = FunctionValueType::new(carrier.clone(), true);
            assert_eq!(
                sql_type_from_value_type(&value),
                Err(ColumnValueTypeError::UnrepresentableCarrier(
                    carrier.clone()
                ))
            );
            let nested = FunctionValueType::new(
                DataType::List(Arc::new(Field::new("item", carrier.clone(), true))),
                false,
            );
            assert_eq!(
                sql_type_from_value_type(&nested),
                Err(ColumnValueTypeError::UnrepresentableCarrier(carrier))
            );
        }
    }
    fn column(data_type: DataType, declaration: Option<SqlType>) -> ColumnDef {
        ColumnDef {
            name: "source".into(),
            data_type,
            nullable: true,
            logical_type: declaration,
            write_default: None,
        }
    }
    #[test]
    fn explicit_roots_are_authored_without_carrier_inference() {
        for (carrier, declaration, expected) in [
            (DataType::Utf8, SqlType::Json, ValueLogicalType::Json),
            (
                DataType::LargeBinary,
                SqlType::Variant,
                ValueLogicalType::Variant,
            ),
            (DataType::Binary, SqlType::Hll, ValueLogicalType::Hll),
            (DataType::Binary, SqlType::Bitmap, ValueLogicalType::Bitmap),
            (
                DataType::FixedSizeBinary(16),
                SqlType::LargeInt,
                ValueLogicalType::LargeInt,
            ),
        ] {
            let physical = column(carrier.clone(), None).declared_value_type().unwrap();
            let declared = column(carrier, Some(declaration))
                .declared_value_type()
                .unwrap();
            assert_eq!(physical.logical_type(), ValueLogicalType::Physical);
            assert_eq!(declared.logical_type(), expected);
            assert_ne!(physical, declared);
        }
        assert!(
            column(DataType::Int64, Some(SqlType::Json))
                .declared_value_type()
                .is_err()
        );
    }
    #[test]
    fn frozen_value_domain_roundtrips_without_carrier_inference() {
        for (carrier, domain) in [
            (DataType::Utf8, ValueLogicalType::Physical),
            (DataType::Utf8, ValueLogicalType::Json),
            (DataType::LargeBinary, ValueLogicalType::Variant),
            (DataType::Binary, ValueLogicalType::Hll),
            (DataType::Binary, ValueLogicalType::Bitmap),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
            (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
        ] {
            let original =
                FunctionValueType::try_from_legacy_declared_projection(carrier, false, domain)
                    .expect("valid temporary legacy value projection");
            let frozen =
                ColumnDef::from_value_type("value".into(), original.clone(), None).unwrap();
            assert_eq!(frozen.declared_value_type().unwrap(), original);
        }
        let child = Field::new("source_item", DataType::Utf8, false).with_metadata(
            std::collections::HashMap::<String, String>::from([
                (NR_LOGICAL_TYPE_KEY.into(), "json".into()),
                ("provider.id".into(), "73".into()),
            ]),
        );
        let original = FunctionValueType::from_legacy_declared_projection(
            DataType::LargeList(Arc::new(child)),
            true,
        );
        let frozen = ColumnDef::from_value_type("nested".into(), original.clone(), None).unwrap();
        assert_eq!(frozen.declared_value_type().unwrap(), original);
        let uuid = FunctionValueType::try_from_legacy_declared_projection(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .expect("valid temporary legacy value projection");
        let column = ColumnDef::from_value_type("uuid".into(), uuid.clone(), None).unwrap();
        assert_eq!(column.logical_type, Some(SqlType::Uuid));
        assert_eq!(column.declared_value_type().unwrap(), uuid);
    }
    #[test]
    fn declared_children_separate_semantics_layout_and_provider_facts() {
        let item = Field::new("provider_element", DataType::Utf8, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider.id".into(),
                "37".into(),
            )]),
        );
        let source = column(
            DataType::LargeList(Arc::new(item)),
            Some(SqlType::Array(Box::new(SqlType::Json))),
        );
        let ty = source.declared_value_type().unwrap();
        let DataType::LargeList(item) = ty.data_type() else {
            panic!("layout changed");
        };
        assert_eq!(item.name(), "item");
        assert!(!item.is_nullable());
        assert!(item.metadata().is_empty());
        assert_eq!(
            ty.value_type().child_type_at(0).unwrap().domain(),
            Some(ValueLogicalType::Json)
        );
        let DataType::LargeList(original) = &source.data_type else {
            panic!("source changed");
        };
        assert_eq!(original.name(), "provider_element");
        assert_eq!(original.metadata()["provider.id"], "37");
    }
    #[test]
    fn execution_domains_do_not_come_from_nested_annotations() {
        for tag in ["json", "not_a_domain"] {
            let item = Field::new("provider_item", DataType::Utf8, true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    NR_LOGICAL_TYPE_KEY.into(),
                    tag.into(),
                )]),
            );
            let explicit = column(
                DataType::List(Arc::new(item.clone())),
                Some(SqlType::Array(Box::new(SqlType::String))),
            )
            .declared_value_type()
            .unwrap();
            let absent = column(DataType::List(Arc::new(item)), None)
                .declared_value_type()
                .unwrap();
            for ty in [explicit, absent] {
                assert_eq!(ty.value_type().child_type_at(0).unwrap().domain(), None);
                let DataType::List(item) = ty.data_type() else {
                    panic!("layout changed");
                };
                assert!(item.metadata().is_empty());
                assert_eq!(item.name(), "item");
            }
        }
    }
    #[test]
    fn malformed_declaration_shapes_fail_before_a_value_is_returned() {
        for source in [
            column(
                DataType::Int64,
                Some(SqlType::Array(Box::new(SqlType::BigInt))),
            ),
            column(
                DataType::Struct(
                    vec![Arc::new(Field::new("actual", DataType::Int64, true))].into(),
                ),
                Some(SqlType::Struct(vec![("other".into(), SqlType::BigInt)])),
            ),
        ] {
            assert!(source.declared_value_type().is_err());
        }
    }
    #[test]
    fn wide_declaration_uses_the_callers_observer_through_finish() {
        #[derive(Debug, PartialEq)]
        enum Error {
            Source,
            Stop,
        }
        impl From<ColumnValueTypeError> for Error {
            fn from(_: ColumnValueTypeError) -> Self {
                Self::Source
            }
        }
        impl From<ValueTypeError> for Error {
            fn from(_: ValueTypeError) -> Self {
                Self::Source
            }
        }
        let fields = (0..320)
            .map(|i| Arc::new(Field::new(format!("c{i}"), DataType::Utf8, true)))
            .collect::<Vec<_>>();
        let declarations = (0..320)
            .map(|i| (format!("c{i}"), SqlType::Json))
            .collect::<Vec<_>>();
        let source = column(
            DataType::Struct(fields.into()),
            Some(SqlType::Struct(declarations)),
        );
        let mut calls = 0;
        source
            .declared_value_type_observed::<Error>(|| {
                calls += 1;
                Ok(())
            })
            .unwrap();
        assert!(calls > 640);
        for stop in [1, 256, calls] {
            let mut seen = 0;
            let error = source
                .declared_value_type_observed::<Error>(|| {
                    seen += 1;
                    if seen == stop {
                        Err(Error::Stop)
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            assert_eq!(error, Error::Stop);
            assert_eq!(seen, stop);
        }
    }
}
