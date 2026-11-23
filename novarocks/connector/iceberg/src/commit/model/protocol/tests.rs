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

use std::sync::Arc;

use super::*;
use crate::commit::conflict::{CandidateFile, MayMatch, RowDeltaValidationFacts};
use crate::commit::model::{
    Dependency, OperationIntent, OperationIntentParts, RequestShape, TableTarget,
};
use novarocks_spi::connector::{ConnectorError, ConnectorWriteOperationId};

#[derive(Debug)]
struct AllFiles;

impl std::fmt::Display for AllFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("true")
    }
}

impl MayMatch for AllFiles {
    fn may_match(&self, _file: &CandidateFile<'_>) -> Result<bool, ConnectorError> {
        Ok(true)
    }
}

#[test]
fn row_delta_cannot_also_freeze_the_target_ref() {
    let intent = OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: crate::iceberg::TableIdent::from_strs(["ns", "t"]).unwrap(),
            uuid: Some(uuid::Uuid::nil()),
        },
        target_ref: "main".into(),
        start: None,
        changes: Default::default(),
        dependencies: vec![Dependency::RefUnchanged],
        protocol: CommitProtocol::RowDelta {
            window: RowDeltaWindowCheck::new(
                IsolationLevel::Snapshot,
                Arc::new(AllFiles),
                Arc::new(RowDeltaValidationFacts::default()),
                false,
                false,
            ),
            writer: WriterSchemaSpec {
                schema_id: 0,
                default_spec_id: 0,
            },
        },
        shape: RequestShape::SnapshotProducing,
        summary: Default::default(),
        token: crate::commit::model::OperationToken::from_write(
            ConnectorWriteOperationId::from_bytes([1; 16]),
        ),
    });
    assert!(
        intent
            .unwrap_err()
            .message()
            .contains("one validation window")
    );
}

#[test]
fn rewrite_files_uses_its_own_window_and_transaction_style() {
    let protocol = CommitProtocol::RewriteFiles {
        output_spec_id: 0,
        replaced: Arc::from([]),
    };
    assert!(matches!(
        protocol.window(),
        WindowValidation::RewriteFiles([])
    ));
    assert_eq!(protocol.operation(), CommitOperation::Optimize);
    assert_eq!(protocol.operation().style(), CommitStyle::Transaction);
    assert_eq!(protocol.writer_schema_spec(), None);
}
