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
use crate::commit::write_stack::tests::{logical_final_delete, project_prepared_managed_rows};
use novarocks_spi::connector::ConnectorManagedPublicationTechnique;
use novarocks_spi::connector::write_stack::{
    ConnectorManagedPublicationShape, ConnectorWriteRowChange,
};

#[test]
fn incremental_rows_preserve_inserted_and_newly_deleted_counts() {
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
    let rows = project_prepared_managed_rows(
        ConnectorManagedPublicationTechnique::Incremental,
        ConnectorManagedPublicationShape::RowMutation,
        records,
    )
    .unwrap();
    assert_eq!(
        rows,
        Some(ConnectorWriteRowChange::Delta {
            inserted: 5,
            deleted: 7
        })
    );
}

#[test]
fn metadata_only_and_full_refresh_use_compact_receiving() {
    let shape = ConnectorManagedPublicationShape::Data;
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let rows = project_prepared_managed_rows(
        ConnectorManagedPublicationTechnique::MetadataOnly,
        shape,
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        rows,
        Some(ConnectorWriteRowChange::Delta {
            inserted: 0,
            deleted: 0
        })
    );
    let rows = project_prepared_managed_rows(
        ConnectorManagedPublicationTechnique::Full,
        shape,
        vec![(ordinal, data_fragment("s3://b/replaced.parquet"))],
    )
    .unwrap();
    assert_eq!(rows, Some(ConnectorWriteRowChange::Replace(5)));
}
