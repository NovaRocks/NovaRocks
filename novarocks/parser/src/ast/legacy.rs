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

//! Temporary old-parser facts. Removed with their original lowering at cutover.
use super::{Expr, Ident};
use crate::Span;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinOperator {
    Inner,
    InnerExplicit,
    LeftOuter,
    LeftOuterExplicit,
    RightOuter,
    RightOuterExplicit,
    FullOuter,
    FullOuterExplicit,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableFunctionSyntax {
    TableWrapper,
    BareCall,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableFunction {
    pub expr: Expr,
    pub lateral: bool,
    pub syntax: TableFunctionSyntax,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
    pub explicit_as: bool,
    pub span: Span,
}
