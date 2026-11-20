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

//! SQL-owned frozen semantics for one optional constant calculation.
//! Installed preparation travels by identity; no function spelling dispatches a call.

use crate::binding::SqlFunctionBinding;
use novarocks_functions::{FunctionBindingSelection, PureCallSpecialization};
use novarocks_type_contract::{
    ArithmeticOperator, ComparisonOperator, DecimalOverflowPolicy, FunctionValueType,
    RegexpCountPatternSource, ToBase64ByteSource,
};
use std::sync::Arc;

// Design: ADR-0191 (docs/adr/ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md)
#[derive(Clone, Debug)]
pub enum FrozenFoldNode {
    Call(FrozenFoldCall),
    Intrinsic(FrozenFoldIntrinsic),
}

#[derive(Clone, Debug)]
pub struct FrozenFoldCall {
    pub(crate) binding: SqlFunctionBinding,
    pub(crate) selected: Arc<FunctionBindingSelection>,
    pub(crate) prepared: PureCallSpecialization,
    pub(crate) prepared_result_type: FunctionValueType,
    pub(crate) source: FrozenFoldArgumentSource,
    /// Canonical execution inputs. FoldRequest.args separately retains the
    /// original admitted CVs for dependency observation and backing provenance.
    pub(crate) arguments: Box<[super::FoldArg]>,
}
impl FrozenFoldCall {
    pub fn binding(&self) -> &SqlFunctionBinding {
        &self.binding
    }
    pub fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn prepared(&self) -> &PureCallSpecialization {
        &self.prepared
    }
    pub fn prepared_result_type(&self) -> &FunctionValueType {
        &self.prepared_result_type
    }
    /// Use these values for execution; a canonical bare NULL may have a new
    /// typed backing while the request continues to retain its original source.
    pub fn arguments(&self) -> &[super::FoldArg] {
        &self.arguments
    }
    pub fn source(&self) -> FrozenFoldArgumentSource {
        self.source
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenFoldArgumentSource {
    Selected,
    RegexpCountPattern(RegexpCountPatternSource),
    ToBase64Bytes(ToBase64ByteSource),
}

#[derive(Clone, Debug)]
pub enum FrozenFoldIntrinsic {
    Arithmetic {
        operator: ArithmeticOperator,
        decimal_overflow_policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
    },
    Comparison {
        operator: ComparisonOperator,
        left: ConversionPlan,
        right: ConversionPlan,
    },
    NullSafeEq {
        left: ConversionPlan,
        right: ConversionPlan,
    },
    Conjunction,
    Disjunction,
    Not,
    Negate,
    BitNot,
    Cast {
        plan: ConversionPlan,
    },
}

#[derive(Clone, Debug)]
pub enum ConversionPlan {
    Identity,
    Carrier(CarrierStep),
    ValueConversion {
        call: FrozenFoldCall,
        tail: Option<CarrierStep>,
    },
}
#[derive(Clone, Debug)]
pub struct CarrierStep {
    pub(crate) target: FunctionValueType,
    pub(crate) decimal_overflow_policy: DecimalOverflowPolicy,
    pub(crate) allow_throw_exception: bool,
    pub(crate) array_failure_mode: Option<novarocks_type_contract::ArrayCastFailureMode>,
}
impl CarrierStep {
    pub fn array_failure_mode(&self) -> Option<novarocks_type_contract::ArrayCastFailureMode> {
        self.array_failure_mode
    }
    pub fn target(&self) -> &FunctionValueType {
        &self.target
    }
    pub fn decimal_overflow_policy(&self) -> DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
    pub fn allow_throw_exception(&self) -> bool {
        self.allow_throw_exception
    }
}
