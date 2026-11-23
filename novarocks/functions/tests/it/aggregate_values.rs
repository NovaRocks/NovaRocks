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

use std::cmp::Ordering::{Equal, Greater, Less};

use novarocks_functions::aggregate_scalar::ExtremaDirection;

#[test]
fn aggregate_values_extrema_disfavor_nan_and_keep_equal_zero() {
    let min = ExtremaDirection::Minimum;
    let max = ExtremaDirection::Maximum;
    assert_eq!(min.compare_float(f64::INFINITY, f64::NAN), Less);
    assert_eq!(max.compare_float(f64::NEG_INFINITY, f64::NAN), Greater);
    for direction in [min, max] {
        assert_eq!(direction.compare_float(-0.0, 0.0), Equal);
        assert_eq!(direction.compare_float(f64::NAN, -f64::NAN), Equal);
    }
}
