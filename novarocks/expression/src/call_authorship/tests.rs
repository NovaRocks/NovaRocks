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

use super::controls::installed_scalar_control_shape;
use novarocks_type_contract::{ArgumentControl, ControlShape};

#[test]
fn installed_scalar_shapes_follow_the_owner_and_reject_relational_controls() {
    assert_eq!(
        installed_scalar_control_shape(ArgumentControl::Eager, 2),
        Some(ControlShape::Eager)
    );
    assert_eq!(
        installed_scalar_control_shape(ArgumentControl::TypeOnly, 3),
        Some(ControlShape::TypeOnly)
    );
    assert_eq!(
        installed_scalar_control_shape(ArgumentControl::SimpleCase, 5),
        Some(ControlShape::Case {
            simple: true,
            arms: 2,
            has_else: false,
        })
    );
    assert_eq!(
        installed_scalar_control_shape(ArgumentControl::SearchedCase, 3),
        Some(ControlShape::Case {
            simple: false,
            arms: 1,
            has_else: true,
        })
    );
    assert!(installed_scalar_control_shape(ArgumentControl::Aggregate, 1).is_none());
    assert!(installed_scalar_control_shape(ArgumentControl::Window, 1).is_none());
    assert!(installed_scalar_control_shape(ArgumentControl::Table, 1).is_none());
}
