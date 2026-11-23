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

//! Read-only compiled unit views for relational operators and local programs.
use super::*;
pub use crate::{
    ComparisonSite, CompiledExprId, CompiledRoot, InputBinding, StaticExprKind, StaticExprNode,
};
use novarocks_functions::{PreparedPureKernel, PureCallSpecialization};
use novarocks_type_contract::ExpressionUseId;
pub(crate) fn call(unit: &CompiledExprSet, id: ExpressionUseId) -> Option<&PureCallSpecialization> {
    unit.calls().get(&id)
}
pub(crate) fn prepared(unit: &CompiledExprSet, id: ExpressionUseId) -> Option<&PreparedPureKernel> {
    call(unit, id).map(|c| c.prepared())
}

/// Exact unit-local occurrence ownership. This is not a resource grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnitCallScope {
    pub root: novarocks_expression_contract::RootOrdinal,
    pub occurrence: ExpressionUseId,
}

/// Borrows original prepared templates without reconstructing their bounds.
#[derive(Debug)]
pub enum UnitStateTemplate<'a> {
    Scalar {
        scope: UnitCallScope,
        kernel: &'a std::sync::Arc<dyn novarocks_functions::PreparedScalarKernel>,
    },
    ScalarInvocation {
        scope: UnitCallScope,
        kernel: &'a std::sync::Arc<dyn novarocks_functions::PreparedInvocationScalarKernel>,
    },
    ControlIntrinsic {
        scope: UnitCallScope,
        call: &'a novarocks_functions::FunctionCallContract,
    },
}

pub fn state_template(
    unit: &CompiledExprSet,
    id: ExpressionUseId,
) -> Option<UnitStateTemplate<'_>> {
    let scope = UnitCallScope {
        root: unit.root_of_use(id)?,
        occurrence: id,
    };
    match prepared(unit, id)? {
        PreparedPureKernel::Scalar(kernel) => Some(UnitStateTemplate::Scalar { scope, kernel }),
        PreparedPureKernel::ScalarInvocation(kernel) => {
            Some(UnitStateTemplate::ScalarInvocation { scope, kernel })
        }
        PreparedPureKernel::ControlIntrinsic(call) => {
            Some(UnitStateTemplate::ControlIntrinsic { scope, call })
        }
        PreparedPureKernel::NullIf(prepared) => Some(UnitStateTemplate::ControlIntrinsic {
            scope,
            call: prepared.call(),
        }),
        _ => None,
    }
}

impl CompiledExprSet {
    pub fn roots(&self) -> &[CompiledRoot] {
        &self.roots
    }
    pub fn root(&self, root: RootOrdinal) -> Option<&CompiledRoot> {
        self.roots.get(root.get() as usize)
    }
    pub fn scopes(&self) -> &[ScopeLayout] {
        &self.scopes
    }
    pub fn scope(&self, scope: RootScope) -> Option<&ScopeLayout> {
        match scope {
            RootScope::Port(i) => self.scopes.get(i as usize),
            RootScope::AllPorts => self.all_ports.as_ref(),
        }
    }
    pub fn control_flow(&self) -> &ExpressionControlFlow<CompiledExprId> {
        &self.parts.flow
    }
    pub fn definitions(&self) -> &ImmutableExpressions {
        &self.parts.definitions
    }
    pub fn types(&self) -> &FunctionArgumentTypes {
        &self.parts.types
    }
    pub fn compiled_definition(&self, id: ExprId) -> Option<CompiledExprId> {
        self.ids.get(&id).copied()
    }
    pub fn calls(&self) -> &BTreeMap<ExpressionUseId, PureCallSpecialization> {
        &self.calls
    }
    pub fn recipes(&self) -> &UnitRecipes {
        &self.recipes
    }
    pub fn input(&self, id: ExpressionUseId) -> Option<InputBinding> {
        self.inputs.get(&id).copied()
    }
    pub fn root_of_use(&self, id: ExpressionUseId) -> Option<RootOrdinal> {
        self.owners.get(&id).copied()
    }
    pub fn effects(&self) -> &BTreeMap<ExpressionUseId, ScopedExpressionEffects> {
        &self.effects
    }
}
impl UnitRecipes {
    pub fn comparison(
        &self,
        id: ComparisonSite,
    ) -> Option<&novarocks_functions::PreparedComparisonRecipe> {
        self.comparisons.get(&id)
    }
    pub fn null_safe_comparison(
        &self,
        id: ExpressionUseId,
    ) -> Option<&novarocks_functions::PreparedNullSafeComparisonRecipe> {
        self.null_safe.get(&id)
    }
    pub fn cast(&self, id: ExpressionUseId) -> Option<&novarocks_functions::PreparedCastRecipe> {
        self.casts.get(&id)
    }
    pub fn arithmetic(
        &self,
        id: ExpressionUseId,
    ) -> Option<&novarocks_functions::PreparedArithmeticRecipe> {
        self.arithmetic.get(&id)
    }
    pub fn native_inlist(
        &self,
        id: ExpressionUseId,
    ) -> Option<&novarocks_functions::PreparedNativeInListRecipe> {
        self.native_inlist.get(&id)
    }
    pub fn native_like(
        &self,
        id: ExpressionUseId,
    ) -> Option<&novarocks_functions::PreparedNativeLikeRecipe> {
        self.native_like.get(&id)
    }
}
