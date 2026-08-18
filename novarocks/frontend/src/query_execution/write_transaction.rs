// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! SQL-specific Iceberg write transaction policy captured before execution.

use std::collections::BTreeMap;

pub(crate) struct IcebergWriteCommitPolicy {
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) base_snapshot_map: BTreeMap<String, i64>,
    pub(crate) target_ref: String,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

pub(crate) struct IcebergWriteValidationPolicy {
    pub(crate) require_v3_for_branch: bool,
}

pub(crate) enum IcebergWriteSource {
    CoordinatedPlan,
}

pub(crate) struct IcebergWriteTransactionSpec {
    pub(crate) is_overwrite: bool,
    pub(crate) attempt_id: String,
    pub(crate) commit: IcebergWriteCommitPolicy,
    pub(crate) validation: IcebergWriteValidationPolicy,
    pub(crate) source: IcebergWriteSource,
}
