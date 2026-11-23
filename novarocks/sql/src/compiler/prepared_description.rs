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

//! Prepared statement descriptors from the original SQL analyzer.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, NullArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_functions::{ConstantPolicy, ConstantPool};
use novarocks_type_contract::{
    CompilePhase, FunctionValueType, TemporalType, ValueKind, ValueLogicalType,
};
use novarocks_types::naming::TableIdentity;

use super::{SqlCompileControl, SqlCompileError};
use crate::analysis::{ExprKind, TypedExpr};
use crate::analyze_error::AnalyzeError;
use crate::column_id::ColumnId;

#[derive(Debug)]
pub struct SqlPreparedDescription {
    /// Parameters are ordered by their original zero-based source ordinal.
    pub parameters: Vec<FunctionValueType>,
    /// The original visible fields, in output occurrence order.
    pub columns: Vec<SqlDescribedColumn>,
}

#[derive(Clone, Debug)]
pub struct SqlDescribedColumn {
    pub name: String,
    pub origin: Option<TableIdentity>,
    pub value_type: FunctionValueType,
    pub aliased: bool,
}

impl SqlDescribedColumn {
    /// The Trino descriptor uses fixed semantic width independently of Arrow.
    pub fn trino_type_size(&self) -> i32 {
        use DataType as D;

        match self.value_type.value_type().kind() {
            ValueKind::Scalar(D::Null | D::Boolean | D::Int8) => 1,
            ValueKind::Scalar(D::Int16) => 2,
            ValueKind::Scalar(D::Int32 | D::Float32 | D::Date32) => 4,
            ValueKind::Scalar(D::Int64 | D::Float64 | D::Time32(..) | D::Time64(..)) => 8,
            ValueKind::Scalar(D::Timestamp(unit, zone)) => {
                if *unit == TimeUnit::Nanosecond
                    || (*unit == TimeUnit::Microsecond && zone.is_some())
                {
                    12
                } else {
                    8
                }
            }
            ValueKind::Scalar(D::Decimal128(precision, _) | D::Decimal256(precision, _)) => {
                if *precision <= 18 {
                    8
                } else {
                    16
                }
            }
            ValueKind::Nominal(ValueLogicalType::Uuid) => 16,
            ValueKind::Temporal(TemporalType::Time(_)) => 8,
            ValueKind::Temporal(TemporalType::TimeWithTimeZone(precision)) => {
                if precision.digits() <= 9 { 8 } else { 12 }
            }
            ValueKind::Temporal(TemporalType::Timestamp(precision)) => {
                if precision.digits() <= 6 {
                    8
                } else {
                    12
                }
            }
            ValueKind::Temporal(TemporalType::TimestampWithTimeZone(precision)) => {
                if precision.digits() <= 3 { 8 } else { 12 }
            }
            ValueKind::Temporal(TemporalType::Interval(interval)) => {
                if interval.is_year_month() || interval.fractional_precision().digits() <= 6 {
                    8
                } else {
                    12
                }
            }
            _ => 0,
        }
    }
}

/// Only description analysis creates this state. A selected UNKNOWN value
/// keeps its source ordinal through cloning without introducing a column,
/// treating a parameter as a syntactic literal, or adding a planner operator.
#[derive(Clone, Debug)]
pub(crate) struct DescriptionAnalysis {
    parameter_pools: Vec<ConstantPool>,
    parameters_per_pool: usize,
    parameter_types: Vec<FunctionValueType>,
    origins: HashMap<ColumnId, Arc<TableIdentity>>,
}

impl DescriptionAnalysis {
    pub(crate) fn new(
        count: usize,
        policy: ConstantPolicy,
        control: &SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        let unknown = FunctionValueType::new(DataType::Null, true);
        let parameters_per_pool = policy
            .max_rows
            .min(policy.max_logical_elements)
            .min(u32::MAX as u64) as usize;
        if count != 0 && parameters_per_pool == 0 {
            return Err(SqlCompileError::ResourceExhausted);
        }
        let mut parameter_pools = Vec::new();
        let mut remaining = count;
        if remaining != 0 {
            let field = Arc::new(Field::new("parameter", DataType::Null, true));
            while remaining != 0 {
                control.check()?;
                let rows = remaining.min(parameters_per_pool);
                parameter_pools.push(ConstantPool::try_new(
                    Arc::clone(&field),
                    unknown.clone(),
                    NullArray::new(rows).to_data(),
                    policy,
                    CompilePhase::Validate,
                    control,
                )?);
                remaining -= rows;
            }
        }
        Ok(Self {
            parameter_pools,
            parameters_per_pool,
            parameter_types: vec![unknown; count],
            origins: HashMap::new(),
        })
    }

    pub(crate) fn parameter(&self, ordinal: usize) -> Result<TypedExpr, AnalyzeError> {
        if ordinal >= self.parameter_types.len() {
            return Err(AnalyzeError::internal(
                "Description lost its parameter occurrence",
            ));
        }
        let value = self
            .parameter_pools
            .get(ordinal / self.parameters_per_pool)
            .ok_or_else(|| AnalyzeError::internal("Description lost its parameter occurrence"))?
            .value((ordinal % self.parameters_per_pool) as u32)
            .map_err(|error| AnalyzeError::internal(error.to_string()))?;
        Ok(TypedExpr {
            value_type: value.value_type().clone(),
            kind: ExprKind::Constant(value),
        })
    }

    pub(crate) fn parameter_ordinal(&self, mut expression: &TypedExpr) -> Option<usize> {
        while let ExprKind::Nested(inner) = &expression.kind {
            expression = inner;
        }
        let ExprKind::Constant(value) = &expression.kind else {
            return None;
        };
        self.parameter_pools
            .iter()
            .position(|pool| pool.backing_identity() == value.pool().backing_identity())
            .map(|pool| pool * self.parameters_per_pool + value.ordinal() as usize)
    }

    pub(crate) fn coerce_parameter(
        &mut self,
        ordinal: usize,
        target: &FunctionValueType,
    ) -> Result<(), AnalyzeError> {
        if self.parameter_types[ordinal].data_type() == &DataType::Null {
            self.parameter_types[ordinal] = target.clone();
        } else if !self.parameter_types[ordinal].same_value_domain(target)
            && target.data_type() != &DataType::Null
        {
            return Err(AnalyzeError::internal(
                "Description assigned conflicting implicit types to one parameter occurrence",
            ));
        }
        Ok(())
    }

    pub(crate) fn set_origin(&mut self, column: ColumnId, table: Arc<TableIdentity>) {
        self.origins.insert(column, table);
    }

    pub(crate) fn copy_origin(&mut self, original: ColumnId, replacement: ColumnId) {
        if let Some(origin) = self.origins.get(&original).cloned() {
            self.set_origin(replacement, origin);
        }
    }

    pub(crate) fn into_description(
        self,
        fields: Vec<crate::analysis::AnalyzedOutputField>,
    ) -> SqlPreparedDescription {
        let columns = fields
            .into_iter()
            .filter(|field| !field.is_internal)
            .enumerate()
            .map(|(ordinal, field)| SqlDescribedColumn {
                name: field.field_name.unwrap_or_else(|| format!("_col{ordinal}")),
                origin: self
                    .origins
                    .get(&field.column_id)
                    .map(|origin| (**origin).clone()),
                value_type: field.value_type,
                aliased: field.aliased,
            })
            .collect();
        SqlPreparedDescription {
            parameters: self.parameter_types,
            columns,
        }
    }
}
