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

use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, FunctionValueType};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SqlInlineColumn {
    pub name: Arc<str>,
    pub value_type: FunctionValueType,
}

impl SqlInlineColumn {
    pub fn from_catalog_column(
        column: &novarocks_types::schema::ColumnDef,
        control: &crate::compiler::SqlCompileControl,
    ) -> Result<Self, crate::compiler::SqlCompileError> {
        Ok(Self {
            name: Arc::from(column.name.as_str()),
            value_type: crate::analyzer::inline_column_value_type(column, control)?,
        })
    }
}

/// A schema-declared relation of already-owned values. It has no stored-view
/// identity, parser source, provider handle, scan token, or lookup authority.
#[derive(Debug)]
pub struct SqlInlineRelation {
    pub(crate) columns: Box<[SqlInlineColumn]>,
    pub(crate) batches: Box<[Box<[ConstantPool]>]>,
    pub(crate) rows: usize,
    retained_bytes: u64,
}
impl SqlInlineRelation {
    /// Retain the original checked pools and their exact full declarations.
    /// Zero rows retain their schema without fabricating parser VALUES cells.
    pub fn try_new(
        columns: Box<[SqlInlineColumn]>,
        batches: Box<[Box<[ConstantPool]>]>,
        control: &crate::compiler::SqlCompileControl,
    ) -> Result<Self, crate::compiler::SqlCompileError> {
        use crate::compiler::SqlCompileError;
        let invalid = |message: &str| SqlCompileError::InvalidRequest(message.into());
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        if columns.is_empty() {
            return Err(invalid("inline relation requires a declared column"));
        }
        let mut names = std::collections::HashSet::new();
        let mut bytes = std::mem::size_of::<Self>()
            .checked_add(std::mem::size_of_val(columns.as_ref()))
            .ok_or_else(|| invalid("inline relation byte count overflow"))?;
        for column in &columns {
            work.step()?;
            if column.name.is_empty() || !names.insert(column.name.to_lowercase()) {
                return Err(invalid(
                    "inline relation contains an empty or duplicate column name",
                ));
            }
            let value = novarocks_type_contract::owned_resources::type_representation::function_value_type_retained_bytes(&column.value_type)
                .ok_or_else(|| invalid("inline relation type byte count overflow"))?;
            bytes = bytes
                .checked_add(2 * std::mem::size_of::<usize>())
                .and_then(|bytes| bytes.checked_add(column.name.len()))
                .and_then(|bytes| bytes.checked_add(value))
                .ok_or_else(|| invalid("inline relation byte count overflow"))?;
        }
        let mut retained_bytes =
            u64::try_from(bytes).map_err(|_| invalid("inline relation byte count overflow"))?;
        retained_bytes = retained_bytes
            .checked_add(
                u64::try_from(std::mem::size_of_val(batches.as_ref()))
                    .map_err(|_| invalid("inline relation byte count overflow"))?,
            )
            .ok_or_else(|| invalid("inline relation byte count overflow"))?;
        let mut rows = 0usize;
        for batch in &batches {
            work.step()?;
            if batch.len() != columns.len() {
                return Err(invalid(
                    "inline relation batch width differs from its declaration",
                ));
            }
            let count = batch[0].resource_facts().rows;
            // ConstantValue addresses its original pool with a u32 ordinal.
            if count
                .checked_sub(1)
                .is_some_and(|ordinal| u32::try_from(ordinal).is_err())
            {
                return Err(invalid(
                    "inline relation batch exceeds constant ordinal representation",
                ));
            }
            rows = rows
                .checked_add(
                    usize::try_from(count)
                        .map_err(|_| invalid("inline relation row count overflow"))?,
                )
                .ok_or_else(|| invalid("inline relation row count overflow"))?;
            retained_bytes = retained_bytes
                .checked_add(
                    u64::try_from(std::mem::size_of_val(batch.as_ref()))
                        .map_err(|_| invalid("inline relation byte count overflow"))?,
                )
                .ok_or_else(|| invalid("inline relation byte count overflow"))?;
            for (pool, column) in batch.iter().zip(&columns) {
                work.step()?;
                if pool.value_type() != &column.value_type
                    || pool.field().name().as_str() != column.name.as_ref()
                    || pool.resource_facts().rows != count
                {
                    return Err(invalid(
                        "inline relation pool differs from its declared column or batch",
                    ));
                }
                let pool_bytes = u64::try_from(pool.exchange_resource_bytes_observed(control)?)
                    .map_err(|_| invalid("inline relation byte count overflow"))?;
                retained_bytes = retained_bytes
                    .checked_add(pool_bytes)
                    .ok_or_else(|| invalid("inline relation byte count overflow"))?;
            }
        }
        work.finish()?;
        Ok(Self {
            columns,
            batches,
            rows,
            retained_bytes,
        })
    }
    pub(crate) const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
}
