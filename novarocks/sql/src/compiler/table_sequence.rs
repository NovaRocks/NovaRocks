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

use std::cell::RefCell;
use std::sync::Arc;

use novarocks_functions::{ConstantValue, PureEngineFunctionCatalog};
use novarocks_parser::{Span, ast};
use novarocks_type_contract::PureCompileControl;

use super::{CompileNeedId, PreparedRequiredConstant, SqlCompileControl, SqlCompileError};
use crate::analyze_error::AnalyzeError;

#[derive(Clone, Debug)]
pub struct TableSequenceArgumentNeed {
    id: CompileNeedId,
    prepared: Arc<PreparedRequiredConstant>,
}

impl PartialEq for TableSequenceArgumentNeed {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && Arc::ptr_eq(&self.prepared, &other.prepared)
    }
}

impl TableSequenceArgumentNeed {
    pub const fn id(&self) -> CompileNeedId {
        self.id
    }

    pub fn prepared(&self) -> &PreparedRequiredConstant {
        &self.prepared
    }
}

#[derive(Debug)]
pub struct TableSequenceArgumentFact {
    need: TableSequenceArgumentNeed,
    value: Option<i64>,
}

impl TableSequenceArgumentFact {
    pub fn from_evaluated(
        need: &TableSequenceArgumentNeed,
        value: ConstantValue,
    ) -> Result<Self, SqlCompileError> {
        let control = need.prepared().control();
        control.check()?;
        let expected = need.prepared().value_type();
        if !value
            .value_type()
            .exactly_equals_checked_observed(expected, || {
                control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)
            })?
        {
            return Err(SqlCompileError::InvalidRequest(
                "table sequence argument fact differs from the prepared type".into(),
            ));
        }
        Ok(Self {
            need: need.clone(),
            value: value.try_i64()?,
        })
    }

    pub const fn id(&self) -> CompileNeedId {
        self.need.id
    }

    pub(super) fn matches(&self, need: &TableSequenceArgumentNeed) -> bool {
        self.need == *need
    }
}

/// Each entry is one visit by the original relation analyzer. The order is
/// taken from its real CTE/view expansion, so equal spans and identical
/// expressions in different appearances remain separate computations.
pub(crate) struct TableSequenceOccurrence {
    syntax: Vec<(usize, ast::Expr)>,
    span: Span,
    arguments: Vec<TableSequenceArgumentNeed>,
}

// Design: ADR-0252 (docs/adr/ADR-0252-collection-calls-and-relational-sequence.md)
pub(crate) enum TableSequencePass {
    SchemaOnly(Arc<PureEngineFunctionCatalog>),
    Request(RefCell<TableSequenceRequestPass>),
    Resume(RefCell<std::vec::IntoIter<(TableSequenceOccurrence, [Option<i64>; 3])>>),
}

pub(crate) struct TableSequenceRequestPass {
    sealed: Arc<PureEngineFunctionCatalog>,
    next_id: u32,
    occurrences: Vec<TableSequenceOccurrence>,
}

impl TableSequencePass {
    pub(crate) fn request(sealed: Arc<PureEngineFunctionCatalog>, first_id: u32) -> Self {
        Self::Request(RefCell::new(TableSequenceRequestPass {
            sealed,
            next_id: first_id,
            occurrences: Vec::new(),
        }))
    }

    pub(crate) fn visit(
        &self,
        supplied: Vec<(usize, &ast::Expr)>,
        span: Span,
        mut prepare: impl FnMut(
            &ast::Expr,
            Arc<PureEngineFunctionCatalog>,
        ) -> Result<PreparedRequiredConstant, AnalyzeError>,
    ) -> Result<[i64; 3], AnalyzeError> {
        match self {
            Self::SchemaOnly(sealed) => {
                for (_, expression) in supplied {
                    prepare(expression, Arc::clone(sealed))?;
                }
                // A stored definition keeps its original syntax. Its reading
                // statement requests and evaluates the actual arguments.
                Ok([0, 0, 1])
            }
            Self::Request(request) => {
                let mut request = request.borrow_mut();
                let mut arguments = Vec::with_capacity(supplied.len());
                for (_, expression) in &supplied {
                    let prepared = prepare(expression, Arc::clone(&request.sealed))?;
                    let id = CompileNeedId::new(request.next_id);
                    request.next_id = request.next_id.checked_add(1).ok_or_else(|| {
                        AnalyzeError::internal("table sequence argument identity overflow")
                    })?;
                    arguments.push(TableSequenceArgumentNeed {
                        id,
                        prepared: Arc::new(prepared),
                    });
                }
                request.occurrences.push(TableSequenceOccurrence {
                    syntax: supplied
                        .into_iter()
                        .map(|(slot, expr)| (slot, expr.clone()))
                        .collect(),
                    span,
                    arguments,
                });
                // A discovery plan describes the relation's schema only. The
                // completion driver never sends this range to statistics.
                Ok([0, 0, 1])
            }
            Self::Resume(resume) => {
                let (occurrence, values) = resume.borrow_mut().next().ok_or_else(|| {
                    AnalyzeError::internal("missing requested table sequence occurrence")
                })?;
                if occurrence.span != span
                    || !occurrence
                        .syntax
                        .iter()
                        .map(|(slot, expr)| (*slot, expr))
                        .eq(supplied.iter().copied())
                {
                    return Err(AnalyzeError::internal("changed table sequence occurrence"));
                }
                for (slot, syntax) in supplied {
                    if values[slot].is_none() {
                        return Err(AnalyzeError::invalid_function_argument(
                            "Table function sequence arguments must not be NULL",
                            syntax.span(),
                        ));
                    }
                }
                validate_range(
                    values.map(|value| value.expect("all bound parameters checked")),
                    span,
                )
            }
        }
    }

    pub(super) fn has_requests(&self) -> bool {
        matches!(self, Self::Request(request) if !request.borrow().occurrences.is_empty())
    }

    pub(super) fn into_requests(self) -> (Vec<TableSequenceOccurrence>, u32) {
        let Self::Request(state) = self else {
            unreachable!("request pass created by the completion driver")
        };
        let state = state.into_inner();
        (state.occurrences, state.next_id)
    }

    pub(super) fn needs(
        occurrences: &[TableSequenceOccurrence],
    ) -> Box<[TableSequenceArgumentNeed]> {
        occurrences
            .iter()
            .flat_map(|occurrence| occurrence.arguments.iter().cloned())
            .collect()
    }

    pub(super) fn resume(
        occurrences: Vec<TableSequenceOccurrence>,
        facts: Box<[TableSequenceArgumentFact]>,
        control: &SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        let mut values: std::collections::BTreeMap<_, _> = facts
            .into_vec()
            .into_iter()
            .map(|fact| (fact.id(), fact.value))
            .collect();
        let mut resolved = Vec::with_capacity(occurrences.len());
        for occurrence in occurrences {
            let mut range = [Some(0), None, Some(1)];
            for ((slot, _), need) in occurrence.syntax.iter().zip(&occurrence.arguments) {
                control.check()?;
                range[*slot] = values.remove(&need.id()).ok_or_else(|| {
                    SqlCompileError::InvalidRequest("missing table sequence argument fact".into())
                })?;
            }
            resolved.push((occurrence, range));
        }
        Ok(Self::Resume(RefCell::new(resolved.into_iter())))
    }

    pub(crate) fn check_consumed(&self) -> Result<(), SqlCompileError> {
        if let Self::Resume(results) = self
            && results.borrow().len() != 0
        {
            return Err(SqlCompileError::InvalidRequest(
                "unused table sequence occurrence facts".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_range(
    [start, stop, step]: [i64; 3],
    span: Span,
) -> Result<[i64; 3], AnalyzeError> {
    if start == stop {
        return Ok([start, stop, 1]);
    }
    if step == 0 || (step > 0) != (start < stop) {
        return Err(AnalyzeError::invalid_function_argument(
            "Table function sequence step must advance from start towards stop",
            span,
        ));
    }
    Ok([start, stop, step])
}
