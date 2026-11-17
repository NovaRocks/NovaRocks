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

//! Old-parser expression facts retained only until the syntax cutover.

use crate::{
    Span,
    ast::{AstBox, Expr, IntervalField as CoreIntervalField},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    NamedArgument,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    ShiftLeft,
    ShiftRight,
    StringConcat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    BitwiseNot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntervalField {
    Quarter,
    Week,
    Millisecond,
    Microsecond,
}

/// A parenthesized expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParenthesizedExpr {
    pub expression: AstBox<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalExpr {
    pub value: AstBox<Expr>,
    pub leading_field: CoreIntervalField,
    pub leading_precision: Option<AstBox<Expr>>,
    pub last_field: Option<CoreIntervalField>,
    pub fractional_seconds_precision: Option<AstBox<Expr>>,
    pub span: Span,
}

/// Additional SELECT facts accepted only by the old parser.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SelectExtensions {
    pub qualify: Option<Expr>,
}
