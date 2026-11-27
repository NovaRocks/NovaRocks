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
    BooleanNot,
    Like,
    InList,
    ArraySubscript,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Equal,
    SubqueryFloatEqual,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Identical,
    Distinct,
    TemporalAdd,
    TemporalSubtract,
    TemporalMultiply,
    TemporalDivide,
    TemporalNegation,
}

impl OperatorFunction {
    pub const ALL: [Self; 24] = [
        Self::Negation,
        Self::BooleanNot,
        Self::Like,
        Self::InList,
        Self::Add,
        Self::Subtract,
        Self::Multiply,
        Self::Divide,
        Self::Modulo,
        Self::Equal,
        Self::SubqueryFloatEqual,
        Self::NotEqual,
        Self::LessThan,
        Self::LessThanOrEqual,
        Self::GreaterThan,
        Self::GreaterThanOrEqual,
        Self::Identical,
        Self::Distinct,
        Self::ArraySubscript,
        Self::TemporalAdd,
        Self::TemporalSubtract,
        Self::TemporalMultiply,
        Self::TemporalDivide,
        Self::TemporalNegation,
    ];

    pub const fn catalog_name(self) -> &'static str {
        match self {
            Self::Negation => "__negate",
            Self::BooleanNot => "__operator_not",
            Self::Like => "__operator_like",
            Self::InList => "__operator_in",
            Self::ArraySubscript => "__array_subscript",
            Self::Add => "__operator_add",
            Self::Subtract => "__operator_subtract",
            Self::Multiply => "__operator_multiply",
            Self::Divide => "__operator_divide",
            Self::Modulo => "__operator_modulo",
            Self::Equal => "__operator_equal",
            Self::SubqueryFloatEqual => "__subquery_float_equal",
            Self::NotEqual => "__operator_not_equal",
            Self::LessThan => "__operator_less_than",
            Self::LessThanOrEqual => "__operator_less_than_or_equal",
            Self::GreaterThan => "__operator_greater_than",
            Self::GreaterThanOrEqual => "__operator_greater_than_or_equal",
            Self::Identical => "__operator_identical",
            Self::Distinct => "__operator_distinct",
            Self::TemporalAdd => "__temporal_add",
            Self::TemporalSubtract => "__temporal_subtract",
            Self::TemporalMultiply => "__temporal_multiply",
            Self::TemporalDivide => "__temporal_divide",
            Self::TemporalNegation => "__temporal_negate",
        }
    }

    pub const fn function_id_text(self) -> &'static str {
        match self {
            Self::Negation => "builtin.scalar/__negate/v1",
            Self::BooleanNot => "builtin.scalar/__operator_not/v1",
            Self::Like => "builtin.scalar/__operator_like/v1",
            Self::InList => "builtin.scalar/__operator_in/v1",
            Self::ArraySubscript => "builtin.scalar/__array_subscript/v1",
            Self::Add => "builtin.scalar/__operator_add/v1",
            Self::Subtract => "builtin.scalar/__operator_subtract/v1",
            Self::Multiply => "builtin.scalar/__operator_multiply/v1",
            Self::Divide => "builtin.scalar/__operator_divide/v1",
            Self::Modulo => "builtin.scalar/__operator_modulo/v1",
            Self::Equal => "builtin.scalar/__operator_equal/v1",
            Self::SubqueryFloatEqual => "builtin.scalar/__subquery_float_equal/v1",
            Self::NotEqual => "builtin.scalar/__operator_not_equal/v1",
            Self::LessThan => "builtin.scalar/__operator_less_than/v1",
            Self::LessThanOrEqual => "builtin.scalar/__operator_less_than_or_equal/v1",
            Self::GreaterThan => "builtin.scalar/__operator_greater_than/v1",
            Self::GreaterThanOrEqual => "builtin.scalar/__operator_greater_than_or_equal/v1",
            Self::Identical => "builtin.scalar/__operator_identical/v1",
            Self::Distinct => "builtin.scalar/__operator_distinct/v1",
            Self::TemporalAdd => "builtin.scalar/__temporal_add/v1",
            Self::TemporalSubtract => "builtin.scalar/__temporal_subtract/v1",
            Self::TemporalMultiply => "builtin.scalar/__temporal_multiply/v1",
            Self::TemporalDivide => "builtin.scalar/__temporal_divide/v1",
            Self::TemporalNegation => "builtin.scalar/__temporal_negate/v1",
        }
    }

    pub const IN_LIST_OVERLOAD: &str = "builtin.scalar/__operator_in/candidate-group-v1";

    /// The fixed overload identity of each private comparison owner.
    pub const fn comparison_overload_text(self) -> Option<&'static str> {
        Some(match self {
            Self::Equal => "builtin.scalar/__operator_equal/comparison-v1",
            Self::SubqueryFloatEqual => "builtin.scalar/__subquery_float_equal/comparison-v1",
            Self::NotEqual => "builtin.scalar/__operator_not_equal/comparison-v1",
            Self::LessThan => "builtin.scalar/__operator_less_than/comparison-v1",
            Self::LessThanOrEqual => "builtin.scalar/__operator_less_than_or_equal/comparison-v1",
            Self::GreaterThan => "builtin.scalar/__operator_greater_than/comparison-v1",
            Self::GreaterThanOrEqual => {
                "builtin.scalar/__operator_greater_than_or_equal/comparison-v1"
            }
            Self::Identical => "builtin.scalar/__operator_identical/comparison-v1",
            Self::Distinct => "builtin.scalar/__operator_distinct/comparison-v1",
            _ => return None,
        })
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

    pub const fn is_infix(self) -> bool {
        !matches!(
            self,
            Self::Negation
                | Self::BooleanNot
                | Self::InList
                | Self::ArraySubscript
                | Self::TemporalNegation
        )
    }

    pub const fn sql(self) -> &'static str {
        match self {
            Self::Negation | Self::TemporalNegation => "-",
            Self::BooleanNot => "NOT",
            Self::Like => "LIKE",
            Self::InList => "IN",
            Self::Add | Self::TemporalAdd => "+",
            Self::Subtract | Self::TemporalSubtract => "-",
            Self::Multiply | Self::TemporalMultiply => "*",
            Self::Divide | Self::TemporalDivide => "/",
            Self::Modulo => "%",
            Self::Equal | Self::SubqueryFloatEqual => "=",
            Self::NotEqual => "<>",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::Identical => "IS NOT DISTINCT FROM",
            Self::Distinct => "IS DISTINCT FROM",
            Self::ArraySubscript => "[]",
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
