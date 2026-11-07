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

//! Required constant preparation, independent of optional optimizer folding.

use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_functions::{ConstantPolicy, ConstantValue};
use novarocks_parser::{Span, ast};
use novarocks_physical_plan::PlanVersionId;
use novarocks_type_contract::FunctionValueType;

use super::{
    SqlAuthoredPhysicalPlan, SqlCatalogSnapshot, SqlCompileControl, SqlCompileError,
    SqlFunctionCatalog,
};

/// The admitted SQL targets of a required session-property computation.
/// This vocabulary carries no application registry or setter authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredConstantTarget {
    Boolean,
    Integer,
    Bigint,
    Double,
    Varchar,
}
impl RequiredConstantTarget {
    pub const fn data_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Integer => DataType::Int32,
            Self::Bigint => DataType::Int64,
            Self::Double => DataType::Float64,
            Self::Varchar => DataType::Utf8,
        }
    }
}

/// Materialized inputs keep their original checked pool and selected ordinal.
pub enum RequiredConstantInput<'a> {
    Expression(&'a ast::Expr),
    Materialized(ConstantValue),
}

/// One statement's immutable services. No service is reacquired by evaluation.
pub struct RequiredConstantRequest<'a> {
    pub input: RequiredConstantInput<'a>,
    pub target: RequiredConstantTarget,
    pub catalog: &'a dyn SqlCatalogSnapshot,
    pub current_database: &'a str,
    pub functions: Arc<dyn SqlFunctionCatalog>,
    pub sql_semantics: &'a crate::sql_mode::SqlSemanticSettings,
    pub constant_policy: ConstantPolicy,
    pub control: &'a SqlCompileControl,
    pub version: PlanVersionId,
}

/// SQL's exact one-cell computation. The application cannot rewrite its tree,
/// replace a selected binding, or reconstruct an expression from display text.
pub struct PreparedRequiredConstant {
    pub(crate) source: SqlAuthoredPhysicalPlan,
    pub(crate) value_type: FunctionValueType,
    pub(crate) span: Span,
    pub(crate) policy: ConstantPolicy,
    pub(crate) control: SqlCompileControl,
    pub(crate) original_value: Option<ConstantValue>,
}
impl std::fmt::Debug for PreparedRequiredConstant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedRequiredConstant")
            .field("value_type", &self.value_type)
            .field("span", &self.span)
            .finish_non_exhaustive()
    }
}
impl PreparedRequiredConstant {
    pub const fn source(&self) -> &SqlAuthoredPhysicalPlan {
        &self.source
    }
    pub const fn value_type(&self) -> &FunctionValueType {
        &self.value_type
    }
    pub const fn span(&self) -> Span {
        self.span
    }
    pub const fn constant_policy(&self) -> ConstantPolicy {
        self.policy
    }
    pub const fn control(&self) -> &SqlCompileControl {
        &self.control
    }
    /// Only an unchanged, already admitted CV qualifies. A conversion keeps
    /// its actual compiled computation instead of relabelling the old value.
    pub const fn original_value(&self) -> Option<&ConstantValue> {
        self.original_value.as_ref()
    }
}

// Design: ADR-0222 (docs/adr/ADR-0222-session-property-declarations-and-required-constant-evaluation.md)
pub fn prepare_required_constant(
    request: RequiredConstantRequest<'_>,
) -> Result<PreparedRequiredConstant, SqlCompileError> {
    crate::analyzer::constant_expression::prepare(request)
}
