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

#[test]
fn root_positive_source_structural_compiled_statistics_canonical_owner() {
    for rows in [vec![], vec![(1, Some("a")), (2, None), (1, Some("a"))]] {
        let outcome = statistics_run(0, &[rows], |_| true);
        let compiled = outcome
            .compiled_root
            .expect("actual compiled finish publishes");
        assert_same_rows(&compiled, &outcome.expected_root);
        assert!(!compiled.is_empty());
        for chunk in compiled {
            // Actual preparation owns the canonical schema independently of
            // the retired metadata-origin receipts.
            assert!(chunk.chunk_schema().is_typed());
            assert!(chunk.schema().metadata().as_arc().is_none());
            for field in chunk.schema().fields() {
                assert!(field.metadata().as_arc().is_none());
            }
            assert!(chunk.chunk_schema().metadata_materializations().is_none());
            assert!(chunk.chunk_schema().field_metadata_origins().is_none());
            assert!(chunk.chunk_schema().schema_metadata_origin().is_none());
            let limits = crate::exec::chunk::RootArrayStorageLimits {
                bytes: 96 << 20,
                nodes: 8192,
                depth: 64,
            };
            let schema_bytes = crate::exec::chunk::borrowed_root_chunk_schema_storage(
                chunk.chunk_schema(),
                limits,
            )
            .expect("actual canonical schema creator");
            let input_bytes = crate::exec::chunk::borrowed_root_chunk_storage(&chunk, limits)
                .expect("canonical schema and actual batch backing");
            assert!(schema_bytes > 0);
            assert!(input_bytes > 0);
        }
    }
}
