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

//! Old-parser query EXPLAIN vocabulary.

use crate::{Span, ast::Query};

/// A query EXPLAIN wrapper. It never grants a production execution route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainQuery {
    pub format: ExplainFormat,
    /// Whether the EXPLAIN targets the logical planning view. This remains
    /// separate from output detail so `EXPLAIN LOGICAL VERBOSE` is typed.
    pub logical: bool,
    pub query: Box<Query>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplainFormat {
    Default,
    Analyze,
    Verbose,
    Costs,
    Logical,
    /// The plan contract itself rather than the operators it describes.
    ///
    /// EXPLAIN answers what a statement will do, which is the operator tree.
    /// What the plan states to the backend -- every value and expression it
    /// defines, the partition spaces, the filter attachments, the cut
    /// evidence -- is a different question, asked separately.
    Contract,
}
