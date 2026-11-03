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

//! Synchronous evaluation of one frozen node through the compiled controller.
use arrow::array::ArrayRef;
use novarocks_functions::{
    ConstantValue, FunctionValueType, KernelFailure, PreparedPureKernel, RowDataError,
};
use novarocks_type_contract::{
    ArithmeticOperator, ComparisonOperator, CompileControlError, CompilePhase,
    DecimalOverflowPolicy, PureCompileControl,
};

pub enum ConstantNode<'a> {
    Arithmetic {
        operator: ArithmeticOperator,
        decimal_overflow_policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
    },
    Comparison {
        operator: ComparisonOperator,
        left: ConstantConversion<'a>,
        right: ConstantConversion<'a>,
    },
    NullSafeEquality {
        left: ConstantConversion<'a>,
        right: ConstantConversion<'a>,
    },
    Conjunction,
    Disjunction,
    Not,
    Conversion(ConstantConversion<'a>),
    Call(&'a PreparedPureKernel),
}
#[derive(Clone, Copy)]
pub struct ConstantCarrierStep<'a> {
    pub target: &'a FunctionValueType,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub allow_throw_exception: bool,
}
pub enum ConstantConversion<'a> {
    Identity,
    Carrier(ConstantCarrierStep<'a>),
    ValueConversion {
        call: &'a PreparedPureKernel,
        call_result: &'a FunctionValueType,
        tail: Option<ConstantCarrierStep<'a>>,
    },
}
#[derive(Debug)]
pub enum ConstantNodeOutcome {
    Value(ArrayRef),
    RowError(RowDataError),
    Declined(DeclineReason),
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeclineReason {
    CapabilityMissing,
    OpaqueHostRequired,
    TemporalSource,
    EffectfulCall,
}
#[derive(Debug)]
pub enum ConstantNodeFailure {
    Control(CompileControlError),
    InvalidFrozenNode(&'static str),
    Kernel(KernelFailure),
    Primitive(novarocks_local_program::ProgramPrimitiveError),
}
impl From<KernelFailure> for ConstantNodeFailure {
    fn from(error: KernelFailure) -> Self {
        match error {
            KernelFailure::CompileControl(error) => Self::Control(error),
            error => Self::Kernel(error),
        }
    }
}
impl std::fmt::Display for ConstantNodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::InvalidFrozenNode(e) => f.write_str(e),
            Self::Kernel(e) => e.fmt(f),
            Self::Primitive(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for ConstantNodeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Kernel(error) => Some(error),
            Self::Primitive(error) => Some(error),
            Self::InvalidFrozenNode(_) => None,
        }
    }
}

// Design: ADR-0191 (docs/adr/ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md)
pub fn evaluate_constant_node(
    node: ConstantNode<'_>,
    children: &[ConstantValue],
    result: &FunctionValueType,
    control: &dyn PureCompileControl,
    phase: CompilePhase,
) -> Result<ConstantNodeOutcome, ConstantNodeFailure> {
    super::guarded::constant_evaluator::evaluate(node, children, result, control, phase)
}
