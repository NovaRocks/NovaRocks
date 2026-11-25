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

//! Valid v1 documents for the result renderer's independent text fixtures.
use novarocks_functions::KernelFailure;
use novarocks_functions::json_value::{JsonWork, parse};

pub fn native_json(text: &str) -> Vec<u8> {
    struct Fixture;
    impl JsonWork for Fixture {
        type Reservation = ();

        fn step(&mut self) -> Result<(), KernelFailure> {
            Ok(())
        }

        fn reserve(&mut self, _bytes: usize) -> Result<(), KernelFailure> {
            Ok(())
        }

        fn reserve_output(&mut self, _bytes: usize) -> Result<(), KernelFailure> {
            Ok(())
        }
    }
    parse(text, &mut Fixture).unwrap()
}
