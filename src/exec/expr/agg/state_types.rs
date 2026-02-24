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
use arrow_buffer::i256;

#[derive(Clone, Debug)]
pub(super) struct SumIntState {
    pub(super) sum: i64,
    pub(super) has_value: bool,
}

#[derive(Clone, Debug)]
pub(super) struct SumFloatState {
    pub(super) sum: f64,
    pub(super) has_value: bool,
}

#[derive(Clone, Debug)]
pub(super) struct SumDecimal128State {
    pub(super) sum: i256,
    pub(super) has_value: bool,
}

#[derive(Clone, Debug)]
pub(super) struct SumDecimal256State {
    pub(super) sum: i256,
    pub(super) has_value: bool,
}

#[derive(Clone, Debug)]
pub(super) struct AvgState {
    pub(super) sum: f64,
    pub(super) count: i64,
}

#[derive(Clone, Debug)]
pub(super) struct AvgDecimal128State {
    pub(super) sum: i128,
    pub(super) count: i64,
}

#[derive(Clone, Debug)]
pub(super) struct AvgDecimal256State {
    pub(super) sum: i256,
    pub(super) count: i64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct DevFromAveState {
    pub(super) mean: f64,
    pub(super) m2: f64,
    pub(super) count: i64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct BoolState {
    pub(super) has_value: bool,
    pub(super) value: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct I32State {
    pub(super) has_value: bool,
    pub(super) value: i32,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct I64State {
    pub(super) has_value: bool,
    pub(super) value: i64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct I128State {
    pub(super) has_value: bool,
    pub(super) value: i128,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct I256State {
    pub(super) has_value: bool,
    pub(super) value: i256,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct F64State {
    pub(super) has_value: bool,
    pub(super) value: f64,
}

#[derive(Debug, Default)]
pub(super) struct Utf8State {
    pub(super) value: Option<String>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct CovarState {
    pub(super) mean_x: f64,
    pub(super) mean_y: f64,
    pub(super) c2: f64,
    pub(super) count: i64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct CorrState {
    pub(super) mean_x: f64,
    pub(super) mean_y: f64,
    pub(super) c2: f64,
    pub(super) m2x: f64,
    pub(super) m2y: f64,
    pub(super) count: i64,
}
