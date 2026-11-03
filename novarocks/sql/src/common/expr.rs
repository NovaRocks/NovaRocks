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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    NullAwareLeftAnti,
    /// Preserves each probe row and produces its SQL three-valued mark.
    LeftMark {
        mark: crate::column_id::ColumnId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LambdaParam {
    pub name: String,
    pub slot_id: i32,
    pub value_type: novarocks_type_contract::FunctionValueType,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WindowFrame {
    pub frame_type: WindowFrameType,
    pub start: WindowBound,
    pub end: WindowBound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WindowFrameType {
    Rows,
    Range,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum WindowBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Int(i64),
    LargeInt(i128),
    Float(f64),
    Decimal(String),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    EqForNull,
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UnOp {
    Not,
    Negate,
    BitwiseNot,
}

impl JoinKind {
    pub(crate) fn mark_output(self) -> Option<crate::common::OutputColumn> {
        let Self::LeftMark { mark } = self else {
            return None;
        };
        Some(crate::common::OutputColumn {
            column_id: mark,
            name: format!("__in_mark_{}", mark.0),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Boolean,
                true,
            ),
            is_internal: true,
        })
    }
}
