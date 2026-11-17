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

//! Complete scope layouts supplied by the enclosing compiler, without slot identities.
use crate::ExprCompileError;
use arrow::datatypes::SchemaRef;
use novarocks_expression_contract::{RootScope, ValueId};
use novarocks_type_contract::{CompileCheckpoints, FunctionValueType, ValueTypeStorageLifetime};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ScopeColumn {
    pub value: ValueId,
    pub ty: FunctionValueType,
}
#[derive(Clone, Debug)]
pub struct ScopeLayout {
    schema: SchemaRef,
    columns: Arc<[ScopeColumn]>,
    storage: ValueTypeStorageLifetime,
}
impl ScopeLayout {
    /// The caller has admitted the supplied immutable buffers. Their order is actual input order.
    pub fn try_new(
        schema: SchemaRef,
        columns: Arc<[ScopeColumn]>,
        storage: ValueTypeStorageLifetime,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ExprCompileError> {
        if schema.fields().len() != columns.len() {
            return Err(ExprCompileError::Invalid(
                "scope schema and complete columns differ",
            ));
        }
        let mut values = std::collections::BTreeSet::new();
        for (field, column) in schema.fields().iter().zip(columns.iter()) {
            work.step()?;
            if !values.insert(column.value)
                || field.is_nullable() != column.ty.nullable()
                || !novarocks_type_contract::arrow_data_types_exact_with_profile_observed::<
                    ExprCompileError,
                >(
                    field.data_type(),
                    column.ty.data_type(),
                    novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
                    || work.step().map_err(Into::into),
                )?
            {
                return Err(ExprCompileError::Invalid(
                    "scope field differs from its complete input declaration",
                ));
            }
        }
        Ok(Self {
            schema,
            columns,
            storage,
        })
    }
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    pub fn columns(&self) -> &[ScopeColumn] {
        &self.columns
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
}
#[derive(Clone, Copy)]
pub struct ScopeLayouts<'a> {
    pub ports: &'a [ScopeLayout],
    pub all_ports: Option<&'a ScopeLayout>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputBinding {
    pub scope: RootScope,
    pub ordinal: u32,
}
