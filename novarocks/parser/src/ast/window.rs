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

//! Complete window reference, specification, and frame syntax facts.

use super::{
    AstBox, AstVec, Expr, Ident, MeasureDefinition, OrderBy, PatternSearchMode, RowPattern, SkipTo,
    SubsetDefinition, VariableDefinition,
};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Window {
    Reference(WindowReference),
    Specification(AstBox<WindowSpecification>),
}
impl Window {
    pub fn span(&self) -> Span {
        match self {
            Self::Reference(node) => node.span,
            Self::Specification(node) => node.span,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowReference {
    pub name: Ident,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowDefinition {
    pub name: Ident,
    pub specification: WindowSpecification,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowSpecification {
    pub existing_window_name: Option<Ident>,
    pub partition_by: AstVec<Expr>,
    pub order_by: Option<OrderBy>,
    pub window_frame: Option<WindowFrame>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    pub end_bound: Option<WindowFrameBound>,
    pub exclusion: WindowFrameExclusion,
    pub measures: AstVec<MeasureDefinition>,
    pub after_match_skip_to: Option<SkipTo>,
    pub pattern_search_mode: Option<PatternSearchMode>,
    pub pattern: Option<RowPattern>,
    pub subsets: AstVec<SubsetDefinition>,
    pub variable_definitions: AstVec<VariableDefinition>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowOperation {
    pub name: Ident,
    pub window: Window,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WindowFrameBound {
    CurrentRow(Span),
    Preceding(Option<Expr>, Span),
    Following(Option<Expr>, Span),
}

impl WindowFrameBound {
    pub const fn span(&self) -> Span {
        match self {
            Self::CurrentRow(span) | Self::Preceding(_, span) | Self::Following(_, span) => *span,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowFrameExclusion {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}
