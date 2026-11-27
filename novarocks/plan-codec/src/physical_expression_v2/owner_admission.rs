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

use super::namespace::{add, cap, check};
use super::{
    ExpressionCodecError as Error, ExpressionNamespaceWriteFacts as Facts,
    ExpressionProjectionLimits as Limits,
};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, PureCompileControl};

pub(super) type Admit<'a> = dyn FnMut(&Facts) -> Result<(), CompileControlError> + 'a;

/// The two entry policies share all original numerical and semantic authors.
/// This callback replaces one growing contribution; it grants no resources.
pub(super) struct Admission<'borrow, 'callback> {
    pub(super) parent: Option<&'borrow mut Admit<'callback>>,
    pub(super) source: usize,
    pub(super) limits: Option<Limits>,
}
#[derive(Clone, Copy)]
pub(super) struct Arithmetic(pub(super) bool);
impl Arithmetic {
    pub(super) fn result<T>(self, value: Result<T, Error>) -> Result<T, Error> {
        if self.0 {
            value.map_err(|_| CompileControlError::ResourceExhausted.into())
        } else {
            value
        }
    }
    pub(super) fn add(self, a: usize, b: usize) -> Result<usize, Error> {
        self.result(super::namespace::add(a, b))
    }
    pub(super) fn mul(self, a: usize, b: usize) -> Result<usize, Error> {
        self.result(super::namespace::mul(a, b))
    }
    pub(super) fn bytes<T>(self, n: usize) -> Result<usize, Error> {
        self.result(super::namespace::bytes::<T>(n))
    }
}
impl Admission<'_, '_> {
    pub(super) fn observed(&self) -> bool {
        self.parent.is_some()
    }
    pub(super) fn numeric<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        if self.observed() {
            result.map_err(|_| CompileControlError::ResourceExhausted.into())
        } else {
            result
        }
    }
    pub(super) fn gate(&mut self, facts: &mut Facts) -> Result<(), Error> {
        if !self.observed() {
            return Ok(());
        }
        facts.coexisting_source_and_request_bytes_upper_bound = self.numeric(add(
            self.source,
            facts.new_allocation_request_bytes_upper_bound,
        ))?;
        self.fixed(facts)
    }
    pub(super) fn fixed(&mut self, facts: &Facts) -> Result<(), Error> {
        if let Some(parent) = &mut self.parent {
            if let Some(l) = self.limits {
                for (actual, maximum) in [
                    (facts.definition_count, l.max_definitions),
                    (facts.type_reference_count, l.max_type_references),
                    (
                        facts.expression_reference_count,
                        l.max_expression_references,
                    ),
                    (
                        facts.new_allocation_requests_upper_bound,
                        l.max_new_allocation_requests,
                    ),
                    (
                        facts.new_allocation_request_bytes_upper_bound,
                        l.max_new_allocation_request_bytes,
                    ),
                    (
                        facts.coexisting_source_and_request_bytes_upper_bound,
                        l.max_coexisting_source_and_request_bytes,
                    ),
                    (facts.cumulative_work_upper_bound, l.max_cumulative_work),
                ] {
                    if actual > maximum {
                        return Err(CompileControlError::ResourceExhausted.into());
                    }
                }
            }
            parent(facts)?;
        }
        Ok(())
    }
    pub(super) fn complete(
        &mut self,
        facts: &mut Facts,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.gate(facts)?;
        check(facts, self.limits, work)
    }
    pub(super) fn charge(
        &mut self,
        facts: &mut Facts,
        amount: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        if !self.observed() {
            return super::namespace::charge(facts, amount, self.limits, work);
        }
        facts.cumulative_work_upper_bound =
            self.numeric(add(facts.cumulative_work_upper_bound, amount))?;
        self.gate(facts)?;
        cap(
            facts.cumulative_work_upper_bound,
            self.limits.map(|limits| limits.max_cumulative_work),
            work,
        )
    }
    pub(super) fn remaining(&self, facts: &Facts) -> Result<Option<usize>, Error> {
        self.limits
            .map(|limits| {
                limits
                    .max_cumulative_work
                    .checked_sub(facts.cumulative_work_upper_bound)
                    .ok_or_else(|| CompileControlError::ResourceExhausted.into())
            })
            .transpose()
    }
}
pub(super) fn same_control(
    expected: &dyn PureCompileControl,
    work: &CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if std::ptr::addr_eq(expected, work.control()) {
        Ok(())
    } else {
        Err(Error::InvalidShape(
            "expression caller work has a different original control",
        ))
    }
}
/// One lookup contribution, with no namespace source invoice or allocation.
pub(super) fn lookup_facts(work: usize) -> Facts {
    Facts {
        definition_count: 1,
        cumulative_work_upper_bound: work,
        ..Facts::default()
    }
}
