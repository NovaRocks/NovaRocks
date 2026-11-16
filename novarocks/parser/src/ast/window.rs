// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Window specification and frame syntax nodes.

use crate::{
    Span,
    ast::{Expr, Ident, query::OrderBy},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedWindow {
    pub name: Ident,
    pub specification: WindowSpec,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowSpec {
    pub existing_window_name: Option<Ident>,
    pub partition_by: Vec<Expr>,
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
