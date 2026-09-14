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

//! Backend composition imports for the connector write data plane.
//!
//! Worker owns the driver-local writer lifecycle. Native adapter owns the
//! commit wire, role-local observation renderer, debug fault selection, and
//! metrics. Backend plan decode composes those typed capabilities without
//! becoming a second write-data-plane owner.

pub(crate) use novarocks_native_adapter::connector_write_data_plane::{
    NativeConnectorWriteObservationPort, QueryScopedTableWriteAggregateGuard,
    RoleBoundCommitFragmentEncoder, RootCommitFragmentCarrierValidator,
};
pub(crate) use novarocks_worker::connector_write_runtime::ObservedConnectorWriteExecution;
