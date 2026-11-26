// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed recipes for one expression node, independent of program traversal.
use crate::{ComparisonSite, CompiledExprSet};
use novarocks_functions::*;
use novarocks_type_contract::ExpressionUseId;

#[derive(Clone, Copy)]
pub(super) enum ComparisonPosition {
    Binary,
    BetweenLower,
    BetweenUpper,
}

pub(super) trait NodeRecipes {
    fn comparison_recipe(&self, position: ComparisonPosition) -> Option<&PreparedComparisonRecipe>;
    fn case_comparison(&self, arm: u32) -> Option<&crate::primitives::PreparedNodeComparison>;
    fn null_safe_comparison_recipe(&self) -> Option<&PreparedNullSafeComparisonRecipe>;
    fn native_like_recipe(&self) -> Option<&PreparedNativeLikeRecipe>;
    fn native_inlist_recipe(&self) -> Option<&PreparedNativeInListRecipe>;
    fn arithmetic_recipe(&self) -> Option<&PreparedArithmeticRecipe>;
    fn cast_recipe(&self) -> Option<&PreparedCastRecipe>;
    fn prepared_call(&self) -> Option<&PreparedPureKernel>;
}

pub(super) struct UnitNodeRecipes<'a> {
    pub unit: &'a CompiledExprSet,
    pub occurrence: ExpressionUseId,
}
impl NodeRecipes for UnitNodeRecipes<'_> {
    fn comparison_recipe(&self, position: ComparisonPosition) -> Option<&PreparedComparisonRecipe> {
        let site = match position {
            ComparisonPosition::Binary => ComparisonSite::Binary(self.occurrence),
            ComparisonPosition::BetweenLower => ComparisonSite::BetweenLower(self.occurrence),
            ComparisonPosition::BetweenUpper => ComparisonSite::BetweenUpper(self.occurrence),
        };
        self.unit.recipes().comparison(site)
    }
    fn case_comparison(&self, arm: u32) -> Option<&crate::primitives::PreparedNodeComparison> {
        self.unit.recipes().case_comparison(self.occurrence, arm)
    }
    fn null_safe_comparison_recipe(&self) -> Option<&PreparedNullSafeComparisonRecipe> {
        self.unit.recipes().null_safe_comparison(self.occurrence)
    }
    fn native_like_recipe(&self) -> Option<&PreparedNativeLikeRecipe> {
        self.unit.recipes().native_like(self.occurrence)
    }
    fn native_inlist_recipe(&self) -> Option<&PreparedNativeInListRecipe> {
        self.unit.recipes().native_inlist(self.occurrence)
    }
    fn arithmetic_recipe(&self) -> Option<&PreparedArithmeticRecipe> {
        self.unit.recipes().arithmetic(self.occurrence)
    }
    fn cast_recipe(&self) -> Option<&PreparedCastRecipe> {
        self.unit.recipes().cast(self.occurrence)
    }
    fn prepared_call(&self) -> Option<&PreparedPureKernel> {
        self.unit
            .calls()
            .get(&self.occurrence)
            .map(|call| call.prepared())
    }
}
