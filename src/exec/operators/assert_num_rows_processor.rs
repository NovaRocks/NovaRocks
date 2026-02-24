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
//! Assertion processor for row-count contract checks.
//!
//! Responsibilities:
//! - Validates row-count constraints required by ASSERT NUM ROWS semantics at runtime.
//! - Fails fast with explicit errors when produced row counts violate configured predicates.
//!
//! Key exported interfaces:
//! - Types: `AssertNumRowsProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;
use crate::exec::node::assert::Assertion;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for processors that enforce ASSERT NUM ROWS runtime constraints.
pub struct AssertNumRowsProcessorFactory {
    name: String,
    desired_num_rows: Option<usize>,
    assertion: Assertion,
    subquery_string: Option<String>,
}

impl AssertNumRowsProcessorFactory {
    pub fn new(
        node_id: i32,
        desired_num_rows: Option<usize>,
        assertion: Assertion,
        subquery_string: Option<String>,
    ) -> Self {
        let name = if node_id >= 0 {
            format!("AssertNumRows (id={node_id})")
        } else {
            "AssertNumRows".to_string()
        };
        Self {
            name,
            desired_num_rows,
            assertion,
            subquery_string,
        }
    }
}

impl OperatorFactory for AssertNumRowsProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(AssertNumRowsProcessorOperator {
            name: self.name.clone(),
            desired_num_rows: self.desired_num_rows.map(|v| v as u64),
            assertion: self.assertion.clone(),
            subquery_string: self.subquery_string.clone(),
            rows_seen: 0,
            done: false,
            pending_output: None,
            finishing: false,
            finished: false,
        })
    }
}

struct AssertNumRowsProcessorOperator {
    name: String,
    desired_num_rows: Option<u64>,
    assertion: Assertion,
    subquery_string: Option<String>,
    rows_seen: u64,
    done: bool,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl AssertNumRowsProcessorOperator {
    fn check_final(&self) -> Result<(), String> {
        let Some(desired) = self.desired_num_rows else {
            // No assertion configured, treat as no-op.
            return Ok(());
        };
        let actual = self.rows_seen;
        let ok = match self.assertion {
            Assertion::Eq => actual == desired,
            Assertion::Ne => actual != desired,
            Assertion::Lt => actual < desired,
            Assertion::Le => actual <= desired,
            Assertion::Gt => actual > desired,
            Assertion::Ge => actual >= desired,
        };
        if ok {
            return Ok(());
        }

        let op_str = match self.assertion {
            Assertion::Eq => "=",
            Assertion::Ne => "!=",
            Assertion::Lt => "<",
            Assertion::Le => "<=",
            Assertion::Gt => ">",
            Assertion::Ge => ">=",
        };
        let base = format!(
            "assert_num_rows failed: actual={} row(s), expected {} {} row(s)",
            actual, op_str, desired
        );
        let msg = if let Some(sql) = &self.subquery_string {
            format!("subquery '{}' {}", sql, base)
        } else {
            base
        };
        Err(msg)
    }

    fn maybe_early_fail(&self) -> Result<(), String> {
        let Some(desired) = self.desired_num_rows else {
            return Ok(());
        };
        let actual = self.rows_seen;

        let must_fail = match self.assertion {
            Assertion::Eq | Assertion::Le => actual > desired,
            Assertion::Lt => actual >= desired,
            // For these, we can't be sure until EOS.
            Assertion::Ne | Assertion::Gt | Assertion::Ge => false,
        };

        if !must_fail {
            return Ok(());
        }

        let op_str = match self.assertion {
            Assertion::Eq => "=",
            Assertion::Ne => "!=",
            Assertion::Lt => "<",
            Assertion::Le => "<=",
            Assertion::Gt => ">",
            Assertion::Ge => ">=",
        };
        let base = format!(
            "assert_num_rows failed (early): actual={} row(s), expected {} {} row(s)",
            actual, op_str, desired
        );
        let msg = if let Some(sql) = &self.subquery_string {
            format!("subquery '{}' {}", sql, base)
        } else {
            base
        };
        Err(msg)
    }
}

impl Operator for AssertNumRowsProcessorOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for AssertNumRowsProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending_output.is_none()
    }

    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if self.pending_output.is_some() {
            return Err("assert_num_rows received input while output buffer is full".to_string());
        }

        if !self.done {
            let rows = chunk.len() as u64;
            if rows > 0 {
                self.rows_seen = self.rows_seen.saturating_add(rows);
                self.maybe_early_fail()?;
            }
        }

        self.pending_output = Some(chunk);
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.finishing && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finishing || self.finished {
            return Ok(());
        }
        self.finishing = true;
        if !self.done {
            self.check_final()?;
            self.done = true;
        }
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use crate::runtime::runtime_state::RuntimeState;

    fn make_chunk(rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("c1", DataType::Int32, true),
            SlotId::new(1),
        )]));
        let data: Vec<i32> = (0..rows as i32).collect();
        let array = Arc::new(Int32Array::from(data)) as _;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        Chunk::new(batch)
    }

    fn run_ok(
        desired: Option<usize>,
        assertion: Assertion,
        chunks: &[usize],
    ) -> Result<(), String> {
        let rt = RuntimeState::default();
        let mut op = AssertNumRowsProcessorOperator {
            name: "test".to_string(),
            desired_num_rows: desired.map(|v| v as u64),
            assertion,
            subquery_string: Some("select c1 from test".to_string()),
            rows_seen: 0,
            done: false,
            pending_output: None,
            finishing: false,
            finished: false,
        };
        for &n in chunks {
            let chunk = make_chunk(n);
            op.push_chunk(&rt, chunk)?;
            if op.has_output() {
                let _ = op.pull_chunk(&rt)?;
            }
        }
        op.set_finishing(&rt)?;
        while op.has_output() {
            let _ = op.pull_chunk(&rt)?;
        }
        Ok(())
    }

    fn run_err(desired: Option<usize>, assertion: Assertion, chunks: &[usize]) -> String {
        let rt = RuntimeState::default();
        let mut op = AssertNumRowsProcessorOperator {
            name: "test".to_string(),
            desired_num_rows: desired.map(|v| v as u64),
            assertion,
            subquery_string: Some("select c1 from test".to_string()),
            rows_seen: 0,
            done: false,
            pending_output: None,
            finishing: false,
            finished: false,
        };
        for &n in chunks {
            let chunk = make_chunk(n);
            match op.push_chunk(&rt, chunk) {
                Err(msg) => return msg,
                Ok(()) => {
                    if op.has_output() {
                        let _ = op.pull_chunk(&rt);
                    }
                }
            }
        }
        match op.set_finishing(&rt) {
            Err(msg) => msg,
            Ok(()) => "no error".to_string(),
        }
    }

    #[test]
    fn assert_eq_pass_and_fail() {
        // desired = 1, actual = 1 -> ok
        run_ok(Some(1), Assertion::Eq, &[1]).expect("eq pass");

        // desired = 1, actual = 2 -> early fail
        let msg = run_err(Some(1), Assertion::Eq, &[1, 1]);
        assert!(msg.contains("assert_num_rows failed"));
        assert!(msg.contains("expected = 1 row(s)"));
    }

    #[test]
    fn assert_le_early_fail() {
        // desired <= 2, actual = 2 -> ok
        run_ok(Some(2), Assertion::Le, &[1, 1]).expect("le pass");

        // desired <= 2, actual = 3 -> early fail
        let msg = run_err(Some(2), Assertion::Le, &[2, 1]);
        assert!(msg.contains("failed (early)"));
    }

    #[test]
    fn assert_lt_and_ge_finalize() {
        // LT: desired = 2, actual = 1 -> ok
        run_ok(Some(2), Assertion::Lt, &[1]).expect("lt pass");

        // LT: desired = 2, actual = 2 -> early fail
        let msg = run_err(Some(2), Assertion::Lt, &[1, 1]);
        assert!(msg.contains("failed (early)"));

        // GE: desired >= 2, actual = 2 -> ok, checked at finish()
        run_ok(Some(2), Assertion::Ge, &[1, 1]).expect("ge pass");
    }
}
