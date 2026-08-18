// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Canonical payload and write adapters for MV repartition.

use crate::mv::domain::refresh::capabilities::{RefreshCapabilities, RefreshIdentity};
use crate::mv::domain::refresh::snapshot::BaseSnapshotPolicy;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepartitionShape {
    ProjectionFilterSingleBase,
    AggregateSingleBase,
    JoinProjectionFilter,
    JoinAggregate,
    FanInAggregate,
    UnionProjectionFilter,
}

impl RepartitionShape {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::ProjectionFilterSingleBase => "projection/filter single-base",
            Self::AggregateSingleBase => "aggregate single-base",
            Self::JoinProjectionFilter => "join projection/filter",
            Self::JoinAggregate => "join aggregate",
            Self::FanInAggregate => "fan-in aggregate",
            Self::UnionProjectionFilter => "UNION ALL projection/filter",
        }
    }
}

pub fn select_repartition_shape(
    capabilities: &RefreshCapabilities,
) -> Result<RepartitionShape, String> {
    match (
        &capabilities.snapshot_policy,
        capabilities.has_agg_state,
        &capabilities.identity,
    ) {
        (BaseSnapshotPolicy::SingleBase, false, RefreshIdentity::BaseRowId) => {
            Ok(RepartitionShape::ProjectionFilterSingleBase)
        }
        (BaseSnapshotPolicy::SingleBase, true, RefreshIdentity::GroupRowId) => {
            Ok(RepartitionShape::AggregateSingleBase)
        }
        (BaseSnapshotPolicy::JoinPairPartialInitialSkip, false, RefreshIdentity::JoinRowKey) => {
            Ok(RepartitionShape::JoinProjectionFilter)
        }
        (BaseSnapshotPolicy::JoinPairPartialInitialSkip, true, RefreshIdentity::GroupRowId) => {
            Ok(RepartitionShape::JoinAggregate)
        }
        (BaseSnapshotPolicy::AllBasesRequired, true, RefreshIdentity::GroupRowId) => {
            Ok(RepartitionShape::FanInAggregate)
        }
        (BaseSnapshotPolicy::AllBasesRequired, false, RefreshIdentity::BranchScoped(inner))
            if matches!(inner.as_ref(), RefreshIdentity::BaseRowId) =>
        {
            Ok(RepartitionShape::UnionProjectionFilter)
        }
        _ => Err(format!(
            "UnsupportedRepartitionShape: ALTER MATERIALIZED VIEW ... REPARTITION does not support identity={:?}, snapshot_policy={:?}, aggregate_state={}; supported shapes are projection/filter single-base, aggregate single-base, join projection/filter, join aggregate, fan-in aggregate, and UNION ALL projection/filter",
            capabilities.identity, capabilities.snapshot_policy, capabilities.has_agg_state
        )),
    }
}
