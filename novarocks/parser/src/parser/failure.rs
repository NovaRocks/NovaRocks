// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::Parser;
use crate::{ParseError, ParserError};

/// Grammar alternatives may recover only from syntax mismatch. Once matched
/// syntax reaches a node constructor, its rejected value must remain an error.
#[derive(Debug)]
pub(super) enum GrammarError {
    Syntax(ParserError),
    Construction(ParserError),
}

impl From<ParseError> for GrammarError {
    fn from(error: ParseError) -> Self {
        Self::Syntax(error.into())
    }
}

impl From<ParserError> for GrammarError {
    fn from(error: ParserError) -> Self {
        Self::Syntax(error)
    }
}

impl From<GrammarError> for ParserError {
    fn from(error: GrammarError) -> Self {
        match error {
            GrammarError::Syntax(error) | GrammarError::Construction(error) => error,
        }
    }
}

pub(super) struct Checkpoint {
    position: usize,
    parameter_count: usize,
}

impl Parser<'_> {
    pub(super) fn checkpoint(&self) -> Checkpoint {
        assert_eq!(self.parameter_events.len(), self.next_parameter);
        Checkpoint {
            position: self.position,
            parameter_count: self.next_parameter,
        }
    }

    pub(super) fn restore(&mut self, checkpoint: Checkpoint) {
        self.position = checkpoint.position;
        self.parameter_events.truncate(checkpoint.parameter_count);
        self.next_parameter = checkpoint.parameter_count;
    }

    pub(super) fn construction_error(&self, expected: &'static str) -> GrammarError {
        GrammarError::Construction(self.error(expected).into())
    }
}
