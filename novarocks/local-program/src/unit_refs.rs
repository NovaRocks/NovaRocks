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

//! Node references into the program-owned table of compiled expression units.
use novarocks_expression::CompiledExprSet;
use novarocks_expression_contract::RootOrdinal;
use std::sync::Arc;
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ProgramUnitId(usize);
impl ProgramUnitId {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }
    pub const fn index(self) -> usize {
        self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ProgramRootRef {
    pub unit: ProgramUnitId,
    pub root: RootOrdinal,
}
impl ProgramRootRef {
    pub const fn new(unit: ProgramUnitId, root: RootOrdinal) -> Self {
        Self { unit, root }
    }
}
pub(crate) fn root_type(
    units: &[Arc<CompiledExprSet>],
    reference: ProgramRootRef,
) -> Option<&novarocks_type_contract::FunctionValueType> {
    let unit = units.get(reference.unit.index())?;
    let root = unit.root(reference.root)?;
    match unit.types().get(root.definition.index())? {
        novarocks_type_contract::FunctionArgumentType::Value(ty) => Some(ty),
        _ => None,
    }
}

pub(crate) fn root_has_demand(
    units: &[Arc<CompiledExprSet>],
    reference: ProgramRootRef,
    demand: novarocks_type_contract::EvaluationDemand,
) -> bool {
    units
        .get(reference.unit.index())
        .and_then(|unit| unit.root(reference.root))
        .is_some_and(|root| root.demand == demand)
}
