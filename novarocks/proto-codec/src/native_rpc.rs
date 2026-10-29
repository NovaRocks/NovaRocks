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

//! Sole Native wire routing manifest. Scheduling reserves live inside these
//! physical owners; they do not create a second method classification.

use novarocks_execution_contract::operation::OperationShape;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum FrontendNativeLane {
    ResultData,
    Submission,
    Observation,
    LifecycleControl,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeEndpointDomain {
    BackendData,
    BackendControl,
    FrontendMembership,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeDirection {
    FrontendToBackend,
    BackendToBackend,
    BackendToFrontend,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeTrafficClass {
    Frontend(FrontendNativeLane),
    Exchange,
    RuntimeFilter,
    Membership,
    Retired,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeBodyKind {
    Unary,
    ServerStream,
    RetiredStream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum NativeRpcMethod {
    RetiredFetchTaskResult,
    FetchRootResult,
    ApplyTaskOperations,
    ApplyTaskControlOperations,
    SubscribeTaskStatus,
    FetchTaskDynamicFilters,
    GetFinalTaskInfo,
    Heartbeat,
    PruneCatalogs,
    ExchangeUnary,
    TransmitRuntimeFilterEnvelope,
    AnnounceBackend,
    RetiredFetchResult,
    RetiredExchange,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeMethodContract {
    pub method: NativeRpcMethod,
    pub path: &'static str,
    pub traffic: NativeTrafficClass,
    pub endpoint: NativeEndpointDomain,
    pub direction: NativeDirection,
    pub body: NativeBodyKind,
}
macro_rules! contract {
    ($method:ident, $name:literal, $traffic:expr, $endpoint:ident, $direction:ident, $body:ident) => {
        NativeMethodContract {
            method: NativeRpcMethod::$method,
            path: concat!("/novarocks.NovaRocksGrpc/", $name),
            traffic: $traffic,
            endpoint: NativeEndpointDomain::$endpoint,
            direction: NativeDirection::$direction,
            body: NativeBodyKind::$body,
        }
    };
}
use FrontendNativeLane::{LifecycleControl, Observation, ResultData, Submission};
use NativeTrafficClass::Frontend;
pub const NATIVE_METHODS: [NativeMethodContract; 14] = [
    contract!(
        RetiredFetchTaskResult,
        "FetchTaskResult",
        NativeTrafficClass::Retired,
        BackendData,
        FrontendToBackend,
        Unary
    ),
    contract!(
        FetchRootResult,
        "FetchRootResult",
        Frontend(ResultData),
        BackendData,
        FrontendToBackend,
        Unary
    ),
    contract!(
        ApplyTaskOperations,
        "ApplyTaskOperations",
        Frontend(Submission),
        BackendData,
        FrontendToBackend,
        Unary
    ),
    contract!(
        ApplyTaskControlOperations,
        "ApplyTaskControlOperations",
        Frontend(LifecycleControl),
        BackendControl,
        FrontendToBackend,
        Unary
    ),
    contract!(
        SubscribeTaskStatus,
        "SubscribeTaskStatus",
        Frontend(Observation),
        BackendData,
        FrontendToBackend,
        ServerStream
    ),
    contract!(
        FetchTaskDynamicFilters,
        "FetchTaskDynamicFilters",
        Frontend(Observation),
        BackendData,
        FrontendToBackend,
        Unary
    ),
    contract!(
        GetFinalTaskInfo,
        "GetFinalTaskInfo",
        Frontend(Observation),
        BackendData,
        FrontendToBackend,
        Unary
    ),
    contract!(
        Heartbeat,
        "Heartbeat",
        Frontend(LifecycleControl),
        BackendControl,
        FrontendToBackend,
        Unary
    ),
    contract!(
        PruneCatalogs,
        "PruneCatalogs",
        Frontend(Submission),
        BackendData,
        FrontendToBackend,
        Unary
    ),
    contract!(
        ExchangeUnary,
        "ExchangeUnary",
        NativeTrafficClass::Exchange,
        BackendData,
        BackendToBackend,
        Unary
    ),
    contract!(
        TransmitRuntimeFilterEnvelope,
        "TransmitRuntimeFilterEnvelope",
        NativeTrafficClass::RuntimeFilter,
        BackendData,
        BackendToBackend,
        Unary
    ),
    contract!(
        AnnounceBackend,
        "AnnounceBackend",
        NativeTrafficClass::Membership,
        FrontendMembership,
        BackendToFrontend,
        Unary
    ),
    contract!(
        RetiredFetchResult,
        "FetchResult",
        NativeTrafficClass::Retired,
        BackendData,
        FrontendToBackend,
        Unary
    ),
    contract!(
        RetiredExchange,
        "Exchange",
        NativeTrafficClass::Retired,
        BackendData,
        BackendToBackend,
        RetiredStream
    ),
];
impl NativeRpcMethod {
    pub fn contract(self) -> &'static NativeMethodContract {
        NATIVE_METHODS
            .iter()
            .find(|entry| entry.method == self)
            .expect("closed Native method manifest")
    }
    pub fn from_path(path: &str) -> Option<Self> {
        NATIVE_METHODS
            .iter()
            .find(|entry| entry.path == path)
            .map(|entry| entry.method)
    }
    pub fn is_allowed_at(self, endpoint: NativeEndpointDomain) -> bool {
        let contract = self.contract();
        contract.endpoint == endpoint && contract.traffic != NativeTrafficClass::Retired
    }
}

/// The caller proves a nonempty UpdateTask contains only closed destination
/// facts. Mixed or ordinary domain updates remain intact on Submission; a
/// frozen operation identity is never split into two different requests.
pub fn operation_method(kind: OperationShape, only_close_destinations: bool) -> NativeRpcMethod {
    use NativeRpcMethod::{ApplyTaskControlOperations, ApplyTaskOperations};
    match kind {
        OperationShape::RenewQueryExecutionLease
        | OperationShape::CancelTask
        | OperationShape::AbortQueryContext
        | OperationShape::QuiesceQueryContext
        | OperationShape::ReleaseQueryContext => ApplyTaskControlOperations,
        OperationShape::UpdateTask if only_close_destinations => ApplyTaskControlOperations,
        OperationShape::AcquireQueryContextAdmissionTicket
        | OperationShape::EstablishQueryContext
        | OperationShape::CreateTask
        | OperationShape::UpdateTask
        | OperationShape::AdvanceQueryContextDomain => ApplyTaskOperations,
        OperationShape::FetchTaskDynamicFilters => NativeRpcMethod::FetchTaskDynamicFilters,
        OperationShape::GetFinalTaskInfo => NativeRpcMethod::GetFinalTaskInfo,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_idl_method_has_one_explicit_owner() {
        let idl = include_str!("../../../idl/novarocks/service.proto");
        let methods: Vec<_> = idl
            .lines()
            .filter_map(|line| line.trim().strip_prefix("rpc "))
            .map(|line| line.split('(').next().unwrap().trim())
            .collect();
        assert_eq!(methods.len(), NATIVE_METHODS.len());
        for name in methods {
            let path = format!("/novarocks.NovaRocksGrpc/{name}");
            assert_eq!(
                NATIVE_METHODS
                    .iter()
                    .filter(|entry| entry.path == path)
                    .count(),
                1
            );
            let method = NativeRpcMethod::from_path(&path).unwrap();
            assert_eq!(method.contract().path, path);
        }
        assert_eq!(
            NativeRpcMethod::from_path("/novarocks.NovaRocksGrpc/Unknown"),
            None
        );
    }
    #[test]
    fn endpoint_domains_do_not_fall_back() {
        assert!(NativeRpcMethod::Heartbeat.is_allowed_at(NativeEndpointDomain::BackendControl));
        assert!(!NativeRpcMethod::Heartbeat.is_allowed_at(NativeEndpointDomain::BackendData));
        assert!(
            !NativeRpcMethod::RetiredFetchTaskResult
                .is_allowed_at(NativeEndpointDomain::BackendControl)
        );
        assert!(NativeRpcMethod::FetchRootResult.is_allowed_at(NativeEndpointDomain::BackendData));
        assert!(
            !NativeRpcMethod::FetchRootResult.is_allowed_at(NativeEndpointDomain::BackendControl)
        );
        assert_eq!(
            NativeRpcMethod::FetchRootResult.contract().traffic,
            NativeTrafficClass::Frontend(FrontendNativeLane::ResultData)
        );
        assert!(!NativeRpcMethod::RetiredExchange.is_allowed_at(NativeEndpointDomain::BackendData));
        assert!(
            !NativeRpcMethod::RetiredFetchTaskResult
                .is_allowed_at(NativeEndpointDomain::BackendData)
        );
        assert_eq!(
            NativeRpcMethod::RetiredFetchTaskResult.contract().traffic,
            NativeTrafficClass::Retired
        );
    }
    #[test]
    fn new_work_and_mixed_updates_never_use_urgent_control() {
        for kind in [
            OperationShape::AcquireQueryContextAdmissionTicket,
            OperationShape::EstablishQueryContext,
            OperationShape::CreateTask,
            OperationShape::UpdateTask,
            OperationShape::AdvanceQueryContextDomain,
        ] {
            assert_eq!(
                operation_method(kind, false),
                NativeRpcMethod::ApplyTaskOperations
            );
        }
        assert_eq!(
            operation_method(OperationShape::UpdateTask, true),
            NativeRpcMethod::ApplyTaskControlOperations
        );
        assert_eq!(
            operation_method(OperationShape::AbortQueryContext, false),
            NativeRpcMethod::ApplyTaskControlOperations
        );
    }
}
