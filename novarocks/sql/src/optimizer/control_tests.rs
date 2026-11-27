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

use super::*;
use crate::optimizer::operator::ValuesOp;
use std::sync::Mutex;

struct Control {
    units: Mutex<Vec<u32>>,
    error: Option<CompileControlError>,
    fail_at: u32,
}
impl Control {
    fn fail(error: CompileControlError, fail_at: u32) -> Self {
        Self {
            units: Mutex::new(Vec::new()),
            error: Some(error),
            fail_at,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        self.units.lock().unwrap().push(work);
        if work >= self.fail_at
            && let Some(error) = self.error.clone()
        {
            return Err(error);
        }
        Ok(())
    }
}
fn errors() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn values() -> Operator {
    Operator::LogicalValues(ValuesOp {
        rows: vec![],
        columns: vec![],
    })
}
fn environment<'a>(
    settings: &'a options::SessionOptimizerSettings,
    control: &'a dyn PureCompileControl,
) -> OptimizerEnvironment<'a> {
    OptimizerEnvironment::new(
        settings,
        crate::functions::test_function_catalog_snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        control,
    )
}
#[test]
fn optimizer_control_entry_preserves_all_three_categories() {
    let settings = without_constant_folding(options::SessionOptimizerSettings::default());
    for error in errors() {
        let control = Control::fail(error.clone(), 0);
        let result = optimize(
            OptExpr::leaf(values()),
            ScalarArena::new(),
            &QueryStatsSnapshot::default(),
            ColumnRefFactory::new(),
            vec![],
            environment(&settings, &control),
        );
        assert!(matches!(result, Err(ref actual) if *actual == SqlCompileError::from(error)));
        assert_eq!(*control.units.lock().unwrap(), vec![0]);
    }
}
#[test]
fn optimizer_control_observes_actual_wide_statistics_validation() {
    let settings = without_constant_folding(options::SessionOptimizerSettings::default());
    for error in errors() {
        let control = Control::fail(error.clone(), 256);
        let plan = OptExpr::new(
            Operator::LogicalUnion(crate::optimizer::operator::UnionOp {
                all: true,
                output_columns: vec![],
                child_output_columns: vec![vec![]; 320],
            }),
            (0..320).map(|_| OptExpr::leaf(values())).collect(),
        );
        let result = optimize(
            plan,
            ScalarArena::new(),
            &QueryStatsSnapshot::default(),
            ColumnRefFactory::new(),
            vec![],
            environment(&settings, &control),
        );
        assert!(matches!(result, Err(ref actual) if *actual == SqlCompileError::from(error)));
        assert_eq!(*control.units.lock().unwrap(), vec![0, 256]);
    }
}
fn groups(count: usize) -> Memo {
    let mut memo = Memo::new();
    for _ in 0..count {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: values(),
            children: vec![],
        });
    }
    memo
}
#[test]
fn optimizer_control_observes_real_explore_and_implement_work() {
    let mut options = options::OptimizerOptions::default_settings();
    options.cbo_max_groups = 1024;
    for error in errors() {
        for exploration in [true, false] {
            let control = Control::fail(error.clone(), 256);
            let mut memo = groups(320);
            let result = if exploration {
                explore(&mut memo, &[], &options, &control)
            } else {
                implement(&mut memo, &[], &options, &control)
            };
            assert_eq!(result, Err(SqlCompileError::from(error.clone())));
            assert_eq!(*control.units.lock().unwrap(), vec![0, 256]);
        }
    }
}
#[test]
fn optimizer_control_flushes_short_work_before_phase_success() {
    let options = options::OptimizerOptions::default_settings();
    for error in errors() {
        for exploration in [true, false] {
            let control = Control::fail(error.clone(), 1);
            let mut memo = groups(1);
            let result = if exploration {
                explore(&mut memo, &[], &options, &control)
            } else {
                implement(&mut memo, &[], &options, &control)
            };
            assert_eq!(result, Err(SqlCompileError::from(error.clone())));
            assert_eq!(*control.units.lock().unwrap(), vec![0, 4]);
        }
    }
}
#[test]
fn optimizer_control_existing_time_budget_only_shortens_request() {
    let owner = Control {
        units: Mutex::default(),
        error: None,
        fail_at: 0,
    };
    let control = OptimizerControl {
        request: &owner,
        deadline: Instant::now() - Duration::from_secs(1),
    };
    assert_eq!(
        control.checkpoint(CompilePhase::Validate, 37),
        Err(CompileControlError::DeadlineExceeded)
    );
    assert_eq!(*owner.units.lock().unwrap(), vec![37]);
    for error in errors() {
        let owner = Control::fail(error.clone(), 0);
        let control = OptimizerControl {
            request: &owner,
            deadline: Instant::now() + Duration::from_secs(30),
        };
        assert_eq!(control.checkpoint(CompilePhase::Validate, 0), Err(error));
    }
}
#[test]
fn optimizer_control_successful_product_does_not_retain_request_observation() {
    struct Observation;
    impl crate::compiler::SqlCancellationObservation for Observation {
        fn is_cancelled(&self) -> bool {
            false
        }
    }
    let observation = Arc::new(Observation);
    let weak = Arc::downgrade(&observation);
    let control = crate::compiler::SqlCompileControl::new(None, observation.clone());
    let settings = without_constant_folding(options::SessionOptimizerSettings::default());
    let output = optimize(
        OptExpr::leaf(values()),
        ScalarArena::new(),
        &QueryStatsSnapshot::default(),
        ColumnRefFactory::new(),
        vec![],
        environment(&settings, &control),
    )
    .unwrap();
    drop(control);
    drop(observation);
    assert!(weak.upgrade().is_none());
    assert!(output.op.is_physical());
}
