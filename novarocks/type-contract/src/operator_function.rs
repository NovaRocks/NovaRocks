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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Design: ADR-0244 (docs/adr/ADR-0244-operators-are-identified-catalogue-functions.md)
use crate::{ArithmeticOperator, ComparisonOperator, FunctionId};

/// SQL operator identities backed by ordinary installed scalar functions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum OperatorFunction {
    Negation,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Identical,
    Distinct,
}

impl OperatorFunction {
    pub const ALL: [Self; 14] = [
        Self::Negation,
        Self::Add,
        Self::Subtract,
        Self::Multiply,
        Self::Divide,
        Self::Modulo,
        Self::Equal,
        Self::NotEqual,
        Self::LessThan,
        Self::LessThanOrEqual,
        Self::GreaterThan,
        Self::GreaterThanOrEqual,
        Self::Identical,
        Self::Distinct,
    ];

    pub const fn catalog_name(self) -> &'static str {
        match self {
            Self::Negation => "__negate",
            Self::Add => "__operator_add",
            Self::Subtract => "__operator_subtract",
            Self::Multiply => "__operator_multiply",
            Self::Divide => "__operator_divide",
            Self::Modulo => "__operator_modulo",
            Self::Equal => "__operator_equal",
            Self::NotEqual => "__operator_not_equal",
            Self::LessThan => "__operator_less_than",
            Self::LessThanOrEqual => "__operator_less_than_or_equal",
            Self::GreaterThan => "__operator_greater_than",
            Self::GreaterThanOrEqual => "__operator_greater_than_or_equal",
            Self::Identical => "__operator_identical",
            Self::Distinct => "__operator_distinct",
        }
    }

    pub const fn function_id_text(self) -> &'static str {
        match self {
            Self::Negation => "builtin.scalar/__negate/v1",
            Self::Add => "builtin.scalar/__operator_add/v1",
            Self::Subtract => "builtin.scalar/__operator_subtract/v1",
            Self::Multiply => "builtin.scalar/__operator_multiply/v1",
            Self::Divide => "builtin.scalar/__operator_divide/v1",
            Self::Modulo => "builtin.scalar/__operator_modulo/v1",
            Self::Equal => "builtin.scalar/__operator_equal/v1",
            Self::NotEqual => "builtin.scalar/__operator_not_equal/v1",
            Self::LessThan => "builtin.scalar/__operator_less_than/v1",
            Self::LessThanOrEqual => "builtin.scalar/__operator_less_than_or_equal/v1",
            Self::GreaterThan => "builtin.scalar/__operator_greater_than/v1",
            Self::GreaterThanOrEqual => "builtin.scalar/__operator_greater_than_or_equal/v1",
            Self::Identical => "builtin.scalar/__operator_identical/v1",
            Self::Distinct => "builtin.scalar/__operator_distinct/v1",
        }
    }

    pub const fn arithmetic(self) -> Option<ArithmeticOperator> {
        Some(match self {
            Self::Add => ArithmeticOperator::Add,
            Self::Subtract => ArithmeticOperator::Subtract,
            Self::Multiply => ArithmeticOperator::Multiply,
            Self::Divide => ArithmeticOperator::Divide,
            Self::Modulo => ArithmeticOperator::Modulo,
            _ => return None,
        })
    }

    pub const fn comparison(self) -> Option<ComparisonOperator> {
        Some(match self {
            Self::Equal => ComparisonOperator::Eq,
            Self::NotEqual => ComparisonOperator::Ne,
            Self::LessThan => ComparisonOperator::Lt,
            Self::LessThanOrEqual => ComparisonOperator::Le,
            Self::GreaterThan => ComparisonOperator::Gt,
            Self::GreaterThanOrEqual => ComparisonOperator::Ge,
            _ => return None,
        })
    }

    pub const fn is_null_safe_comparison(self) -> bool {
        matches!(self, Self::Identical | Self::Distinct)
    }

    pub const fn sql(self) -> &'static str {
        match self {
            Self::Negation => "-",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::Equal => "=",
            Self::NotEqual => "<>",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::Identical => "IS NOT DISTINCT FROM",
            Self::Distinct => "IS DISTINCT FROM",
        }
    }

    pub fn function_id(self) -> FunctionId {
        FunctionId::try_new(self.function_id_text()).expect("valid built-in operator identity")
    }

    pub fn from_function_id(id: &FunctionId) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|operator| operator.function_id_text() == id.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_identities_round_trip() {
        for operator in OperatorFunction::ALL {
            assert_eq!(
                OperatorFunction::from_function_id(&operator.function_id()),
                Some(operator)
            );
        }
        let other = FunctionId::try_new("builtin.scalar/abs/v1").unwrap();
        assert_eq!(OperatorFunction::from_function_id(&other), None);
    }
}
