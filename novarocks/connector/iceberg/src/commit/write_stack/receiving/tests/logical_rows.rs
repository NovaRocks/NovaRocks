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

//! Observes the sealed receiving mode and validates its logical row projection.

use super::*;
use crate::commit::write_stack::tests::{logical_final_delete, project_managed_rows_fixture};
use novarocks_spi::connector::ConnectorManagedPublicationTechnique;
use novarocks_spi::connector::write_stack::{
    ConnectorManagedPublicationShape, ConnectorWriteRowChange,
};

#[test]
fn incremental_compact_rows_match_the_original_materialized_projection() {
    let target = |ordinal| WriteTargetOrdinal::try_new(ordinal).unwrap();
    let records = vec![
        (target(0), data_fragment("s3://b/new.parquet")),
        (
            target(1),
            logical_final_delete("s3://b/old-a.parquet", 3, 8, Some(("s3://b/new.puffin", 5)))
                .unwrap(),
        ),
        (
            target(1),
            logical_final_delete("s3://b/old-b.parquet", 4, 4, None).unwrap(),
        ),
    ];
    let project = |use_full_inventory| {
        project_managed_rows_fixture(
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationShape::RowMutation,
            records.clone(),
            use_full_inventory,
        )
        .unwrap()
    };
    let (compact, rows) = project(false);
    assert!(compact);
    assert_eq!(rows, project(true).1);
    assert_eq!(
        rows,
        Some(ConnectorWriteRowChange::Delta {
            inserted: 5,
            deleted: 7
        })
    );
}

#[test]
fn metadata_only_selects_compact_and_full_refresh_selects_whole_receiving() {
    let shape = ConnectorManagedPublicationShape::Data;
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let (compact, rows) = project_managed_rows_fixture(
        ConnectorManagedPublicationTechnique::MetadataOnly,
        shape,
        Vec::new(),
        false,
    )
    .unwrap();
    assert!(compact);
    assert_eq!(
        rows,
        Some(ConnectorWriteRowChange::Delta {
            inserted: 0,
            deleted: 0
        })
    );
    let (compact, rows) = project_managed_rows_fixture(
        ConnectorManagedPublicationTechnique::Full,
        shape,
        vec![(ordinal, data_fragment("s3://b/replaced.parquet"))],
        false,
    )
    .unwrap();
    assert!(!compact);
    assert_eq!(rows, Some(ConnectorWriteRowChange::Replace(5)));
}
