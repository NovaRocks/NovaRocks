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

//! Ordinary DML assignment declarations, independent from root result purposes.
//!
//! These facts preserve the real typed source V. They neither normalize a value
//! nor establish a non-NULL promise. Actual construction of W belongs to the
//! selected writer/event assignment boundary.

use novarocks_type_contract::{
    CheckedWriteInputProjection, CompilePhase, FunctionValueType, PureCompileControl,
};

use super::{SqlCompileControl, SqlCompileError};

#[derive(Clone, Debug)]
pub struct SqlWriteAssignmentSite {
    output_ordinal: usize,
    source_span: Option<novarocks_parser::Span>,
    input: FunctionValueType,
    storage: Option<FunctionValueType>,
}
impl SqlWriteAssignmentSite {
    pub fn try_new(
        output_ordinal: usize,
        input: FunctionValueType,
        storage: Option<FunctionValueType>,
        control: &SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        if CheckedWriteInputProjection::try_new_observed(
            input.typed_binding().borrowed(),
            input.typed_binding().borrowed(),
            storage.as_ref().map(|ty| ty.typed_binding().borrowed()),
            || {
                control
                    .checkpoint(CompilePhase::Validate, 1)
                    .map_err(SqlCompileError::from)
            },
        )?
        .is_none()
        {
            return Err(SqlCompileError::InvalidRequest(
                "write assignment input differs from its storage projection".into(),
            ));
        }
        Ok(Self {
            output_ordinal,
            source_span: None,
            input,
            storage,
        })
    }
    pub fn with_source_span(mut self, span: novarocks_parser::Span) -> Self {
        self.source_span = Some(span);
        self
    }
    pub const fn source_span(&self) -> Option<novarocks_parser::Span> {
        self.source_span
    }
    pub const fn output_ordinal(&self) -> usize {
        self.output_ordinal
    }
    pub const fn input(&self) -> &FunctionValueType {
        &self.input
    }
    pub const fn storage(&self) -> Option<&FunctionValueType> {
        self.storage.as_ref()
    }
}

/// Already owned, strictly ordered sparse occurrences. The application supplies
/// their original allocation; construction adds no policy cap or hidden sort.
#[derive(Clone, Debug)]
pub struct SqlWriteAssignments {
    sites: Box<[SqlWriteAssignmentSite]>,
    mutation_description: bool,
}
impl SqlWriteAssignments {
    pub fn try_new(
        sites: Box<[SqlWriteAssignmentSite]>,
        control: &SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        for pair in sites.windows(2) {
            control.checkpoint(CompilePhase::Validate, 1)?;
            if pair[0].output_ordinal >= pair[1].output_ordinal {
                return Err(SqlCompileError::InvalidRequest(
                    "write assignment occurrences must be unique and ordered".into(),
                ));
            }
        }
        Ok(Self {
            sites,
            mutation_description: false,
        })
    }
    /// The original UPDATE/MERGE source projects complete SET/INSERT sites
    /// alongside unaffected match fields. Its assignment conversion constrains
    /// a parameter occupying the original root of an assignment.
    pub fn for_mutation_description(mut self) -> Self {
        self.mutation_description = true;
        self
    }

    pub(crate) const fn is_mutation_description(&self) -> bool {
        self.mutation_description
    }

    pub fn sites(&self) -> &[SqlWriteAssignmentSite] {
        &self.sites
    }
    pub fn at_observed<E>(
        &self,
        output_ordinal: usize,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<&SqlWriteAssignmentSite>, E> {
        let mut lower = 0;
        let mut upper = self.sites.len();
        while lower < upper {
            observe()?;
            let middle = lower + (upper - lower) / 2;
            match self.sites[middle].output_ordinal.cmp(&output_ordinal) {
                std::cmp::Ordering::Less => lower = middle + 1,
                std::cmp::Ordering::Greater => upper = middle,
                std::cmp::Ordering::Equal => return Ok(Some(&self.sites[middle])),
            }
        }
        Ok(None)
    }
    pub fn validate_output_width(
        &self,
        width: usize,
        control: &SqlCompileControl,
    ) -> Result<(), SqlCompileError> {
        for site in self.sites() {
            control.checkpoint(CompilePhase::Validate, 1)?;
            if site.output_ordinal >= width {
                return Err(SqlCompileError::InvalidRequest(
                    "write assignment occurrence is outside its analyzed source".into(),
                ));
            }
        }
        Ok(())
    }
}
