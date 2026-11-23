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

use std::collections::HashMap;
use std::ops::Range;

use super::Parser;
use crate::Span;
use crate::ast::{Fold, Parameter};

impl Parser<'_> {
    pub(super) fn new_parameter(&mut self, span: Span) -> Parameter {
        let position = self.next_parameter;
        self.next_parameter += 1;
        if self.track_parameter_events {
            self.parameter_events.push(span);
        }
        Parameter { position, span }
    }

    /// Orders already parsed child events exactly as the reference builder
    /// visits those children. The ranges refer to this invocation's original
    /// contiguous production slots; nested ordering never changes their length.
    pub(super) fn reorder_parameter_groups(&mut self, start: usize, groups: &[Range<usize>]) {
        if !self.track_parameter_events {
            return;
        }
        assert_eq!(self.parameter_events.len(), self.next_parameter);
        let mut partition = groups.to_vec();
        partition.sort_by_key(|range| (range.start, range.end));
        let mut cursor = start;
        for range in &partition {
            assert_eq!(
                range.start, cursor,
                "parameter groups partition the actual production"
            );
            assert!(range.end >= range.start && range.end <= self.next_parameter);
            cursor = range.end;
        }
        assert_eq!(cursor, self.next_parameter);
        if partition == groups {
            return;
        }
        let ordered: Vec<_> = groups
            .iter()
            .flat_map(|range| self.parameter_events[range.clone()].iter().copied())
            .collect();
        self.parameter_events[start..].copy_from_slice(&ordered);
    }

    pub(super) fn parameter_numbers(&self) -> ParameterNumbers {
        assert!(self.track_parameter_events);
        assert_eq!(self.parameter_events.len(), self.next_parameter);
        let mut positions = HashMap::with_capacity(self.next_parameter);
        for (position, span) in self.parameter_events.iter().copied().enumerate() {
            assert!(
                positions.insert(span, position).is_none(),
                "one event per committed parameter production"
            );
        }
        ParameterNumbers { positions }
    }
}

/// Final assignment at the new grammar's construction boundary. The event
/// ledger also retains visits to syntax which the reference builder discards
/// (for example a control call's ORDER BY), so retained nodes can have holes.
pub(super) struct ParameterNumbers {
    positions: HashMap<Span, usize>,
}

impl Fold for ParameterNumbers {
    fn fold_parameter(&mut self, mut parameter: Parameter) -> Parameter {
        parameter.position = *self
            .positions
            .get(&parameter.span)
            .expect("parameter retains its original committed token span");
        parameter
    }
}
