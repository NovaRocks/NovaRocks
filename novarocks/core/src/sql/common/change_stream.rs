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

pub(crate) use crate::exec::change_op::{
    CHANGE_OP_DELETE, CHANGE_OP_INSERT, ChangeStreamBranchKind, ChangeStreamRouteKey,
    DATA_ROUTE_FRESH, DATA_ROUTE_REUSE,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_kind_maps_to_canonical_route_key() {
        assert_eq!(
            ChangeStreamBranchKind::DeleteDv.route_key(),
            ChangeStreamRouteKey {
                change_op: -1,
                data_route: None,
            }
        );
        assert_eq!(
            ChangeStreamBranchKind::ReuseData.route_key(),
            ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(1),
            }
        );
        assert_eq!(
            ChangeStreamBranchKind::FreshData.route_key(),
            ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(2),
            }
        );
    }
}
