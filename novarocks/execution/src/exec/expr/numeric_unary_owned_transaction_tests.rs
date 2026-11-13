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
// Transitional re-export for the remaining Execution tests.
// Chain inputs and their SQL-to-runtime assertions live in compile-to-execute.
// Remove this bridge after the remaining consumers migrate.
pub(super) use novarocks_test_compile_to_execute::programs::{
    batch, producer_root, programs, programs_with_catalogue,
    programs_with_catalogue_and_original_inlist_source,
    programs_with_catalogue_and_original_scan_residuals,
};
