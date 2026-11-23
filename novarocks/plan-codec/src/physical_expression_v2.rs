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

//! Borrowed expression definition projection in both directions. Receiving
//! retains the original namespaces without inventing legacy effect metadata.
//! This component does not authenticate kernels or validate a full package.

use crate::{
    physical_binding_v2::BindingCodecError, physical_semantics_v2::SemanticsCodecError,
    physical_type_v2::TypeCodecError, physical_value_v2::ValueCodecError,
};
use novarocks_type_contract::CompileControlError;
use std::fmt;

mod kind;
pub(crate) use kind::window_frame as encode_window_frame;
mod materialize;
mod namespace;
mod owner_admission;
mod read;
mod receiving_grammar;
pub use materialize::{
    MaterializedExpressions, PreparedExpressionMaterialization, materialize_expressions,
    materialize_expressions_in, prepare_expression_materialization,
    prepare_expression_materialization_in,
};
pub(crate) use receiving_grammar::decode_window_frame;

pub use namespace::{
    EncodedExpressions, ExpressionDefinitionSource, ExpressionNamespaceWriteFacts,
    ExpressionProjectionLimits, ExpressionTypeIds, PreparedExpressionNamespaceWrite,
    encode_expression_definitions, prepare_expression_definitions,
    prepare_expression_definitions_in,
};

pub(crate) use namespace::tree_lookup_work as expression_tree_lookup_work;
pub use read::{
    DecodedExpressions, ExpressionDefinitionInput, ExpressionNamespaceReadFacts,
    decode_expression_definitions, decode_expression_definitions_in,
};

fn encode_trino_cast_mode(mode: novarocks_type_contract::TrinoCastMode) -> Option<i32> {
    use novarocks_proto_models::physical_package_v2::TrinoCastMode as Wire;
    use novarocks_type_contract::TrinoCastMode as Mode;
    Some(match mode {
        Mode::Explicit => Wire::Explicit,
        Mode::Try => Wire::Try,
        Mode::Implicit => Wire::Implicit,
        Mode::StoreAssignment => Wire::StoreAssignment,
    } as i32)
}

fn decode_trino_cast_mode(
    mode: Option<i32>,
) -> Result<novarocks_type_contract::TrinoCastMode, ExpressionCodecError> {
    use novarocks_proto_models::physical_package_v2::TrinoCastMode as Wire;
    use novarocks_type_contract::TrinoCastMode as Mode;
    match mode.and_then(|value| Wire::try_from(value).ok()) {
        Some(Wire::Explicit) => Ok(Mode::Explicit),
        Some(Wire::Try) => Ok(Mode::Try),
        Some(Wire::Implicit) => Ok(Mode::Implicit),
        Some(Wire::StoreAssignment) => Ok(Mode::StoreAssignment),
        _ => Err(ExpressionCodecError::InvalidShape(
            "CAST mode is absent or invalid",
        )),
    }
}

#[derive(Debug)]
pub enum ExpressionCodecError {
    InvalidShape(&'static str),
    Control(CompileControlError),
    Type(TypeCodecError),
    Semantics(SemanticsCodecError),
    Binding(BindingCodecError),
    Value(ValueCodecError),
    Constant(novarocks_physical_plan::ConstantReferenceError),
    Unit(novarocks_expression_contract::UnitError),
    ConstructedUnit(novarocks_expression_contract::ConstructedUnitError),
    UnitResource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
}
impl fmt::Display for ExpressionCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShape(message) => f.write_str(message),
            Self::Control(error) => error.fmt(f),
            Self::Type(error) => error.fmt(f),
            Self::Semantics(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
            Self::Value(error) => error.fmt(f),
            Self::Constant(error) => error.fmt(f),
            Self::Unit(error) => error.fmt(f),
            Self::ConstructedUnit(error) => error.fmt(f),
            Self::UnitResource(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ExpressionCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Semantics(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::Value(source) => Some(source),
            Self::Constant(source) => Some(source),
            Self::Unit(source) => Some(source),
            Self::ConstructedUnit(source) => Some(source),
            Self::UnitResource(source) => Some(source),
            _ => None,
        }
    }
}
impl From<novarocks_expression_contract::UnitError> for ExpressionCodecError {
    fn from(value: novarocks_expression_contract::UnitError) -> Self {
        match value {
            novarocks_expression_contract::UnitError::Control(error) => Self::Control(error),
            value => Self::Unit(value),
        }
    }
}
impl From<CompileControlError> for ExpressionCodecError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<TypeCodecError> for ExpressionCodecError {
    fn from(value: TypeCodecError) -> Self {
        match value {
            TypeCodecError::Control(error) => Self::Control(error),
            value => Self::Type(value),
        }
    }
}
impl From<SemanticsCodecError> for ExpressionCodecError {
    fn from(value: SemanticsCodecError) -> Self {
        match value {
            SemanticsCodecError::Control(error) => Self::Control(error),
            value => Self::Semantics(value),
        }
    }
}
impl From<BindingCodecError> for ExpressionCodecError {
    fn from(value: BindingCodecError) -> Self {
        match value {
            BindingCodecError::Control(error) => Self::Control(error),
            value => Self::Binding(value),
        }
    }
}

impl From<ValueCodecError> for ExpressionCodecError {
    fn from(value: ValueCodecError) -> Self {
        match value {
            ValueCodecError::Control(error) => Self::Control(error),
            value => Self::Value(value),
        }
    }
}
impl From<novarocks_physical_plan::ConstantReferenceError> for ExpressionCodecError {
    fn from(value: novarocks_physical_plan::ConstantReferenceError) -> Self {
        use novarocks_constant_contract::ConstantError;
        use novarocks_physical_plan::ConstantReferenceError;
        match value {
            ConstantReferenceError::Control(error)
            | ConstantReferenceError::Constant(ConstantError::Control(error)) => {
                Self::Control(error)
            }
            ConstantReferenceError::Constant(ConstantError::Limit(_)) => {
                Self::Control(CompileControlError::ResourceExhausted)
            }
            value => Self::Constant(value),
        }
    }
}

/// Internal IDs already bound to this definition's original immutable owners.
/// The kind leaf never chooses these IDs or authors another type namespace.
struct PreparedExpressionIds<'a> {
    root_carrier_type_id: Option<u32>,
    lambda_parameter_type_ids: &'a [u32],
    function_binding_id: Option<u32>,
}

#[cfg(test)]
mod error_tests;

impl From<novarocks_constant_contract::ConstantTableError> for ExpressionCodecError {
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(novarocks_physical_plan::ConstantReferenceError::from(error))
    }
}
