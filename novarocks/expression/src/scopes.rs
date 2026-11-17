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
use crate::creation::{ProgramCreation, ProgramCreationFailure};
use crate::{ExprCompileError, ExprCompileFailure};
use arrow::datatypes::SchemaRef;
use novarocks_expression_contract::{RootScope, ValueId};
use novarocks_type_contract::{
    CompileCheckpoints, FunctionValueType, ValueSchemaCreationScope, ValueTypeStorageLifetime,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ScopeColumn {
    pub value: ValueId,
    /// Actual column position in the complete runtime schema.
    pub ordinal: u32,
    pub ty: FunctionValueType,
}
#[derive(Clone, Debug)]
pub struct ScopeLayout {
    schema: SchemaRef,
    columns: Arc<[ScopeColumn]>,
    storage: ValueTypeStorageLifetime,
}
impl ScopeLayout {
    /// The caller has admitted the immutable schema and canonical value map.
    /// Repeated physical publications remain in the schema; their enclosing
    /// channel author supplies the proven representative's actual ordinal.
    pub fn try_new(
        schema: SchemaRef,
        columns: Arc<[ScopeColumn]>,
        storage: ValueTypeStorageLifetime,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ExprCompileError> {
        Self::try_new_core(
            schema,
            columns,
            storage,
            ProgramCreation::<std::convert::Infallible> { creator: None },
            work,
        )
        .map_err(ExprCompileFailure::into_pure)
    }

    /// Validate the original source on its caller's work scope. The temporary
    /// owner funds only this check's duplicate-value index; supplied schema,
    /// columns and their source lifetime remain with their original author.
    /// This entry creates no factory, retention grant, phase or footer.
    pub fn try_new_with_creator_in<H>(
        schema: SchemaRef,
        columns: Arc<[ScopeColumn]>,
        storage: ValueTypeStorageLifetime,
        temporary: &mut dyn ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ExprCompileFailure<H>> {
        Self::try_new_core(
            schema,
            columns,
            storage,
            ProgramCreation {
                creator: Some(temporary),
            },
            work,
        )
    }

    fn try_new_core<H>(
        schema: SchemaRef,
        columns: Arc<[ScopeColumn]>,
        storage: ValueTypeStorageLifetime,
        mut creation: ProgramCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ExprCompileFailure<H>> {
        if columns.len() > schema.fields().len()
            || (columns.is_empty() && !schema.fields().is_empty())
        {
            return Err(
                ExprCompileError::Invalid("scope schema and complete columns differ").into(),
            );
        }
        let mut values = std::collections::BTreeSet::new();
        for column in columns.iter() {
            work.step()?;
            let field =
                schema
                    .fields()
                    .get(column.ordinal as usize)
                    .ok_or(ExprCompileError::Invalid(
                        "scope input ordinal is outside its runtime schema",
                    ))?;
            creation
                .before_tree_insert::<ExprCompileError, ValueId, ()>(values.len(), work)
                .map_err(scope_creation_failure)?;
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
                )
                .into());
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

fn scope_creation_failure<H>(
    error: ProgramCreationFailure<ExprCompileError, H>,
) -> ExprCompileFailure<H> {
    match error {
        ProgramCreationFailure::Control(cause) => ExprCompileFailure::Control(cause),
        ProgramCreationFailure::Creator(cause) => ExprCompileFailure::Creator(cause),
        ProgramCreationFailure::Resource(cause) => ExprCompileFailure::Resource(cause),
        ProgramCreationFailure::Owner(cause) => cause.into(),
        ProgramCreationFailure::Capacity(cause) => ExprCompileFailure::Owner(
            ExprCompileError::Publication(ProgramCreationFailure::Capacity(cause)),
        ),
    }
}
