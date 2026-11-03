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

//! Direct membership nodes. Nodes are allocated before admission, linked
//! without allocation while lifecycle gates are held, and unlinked on final drop.
use crate::{
    account::{Account, AccountState},
    domain::Domain,
    lane::handle::LaneShared,
    sync::{Arc, Mutex, Weak},
};

#[derive(Debug)]
pub(crate) enum Member {
    Account(Weak<AccountState>),
    Lane {
        lane: Weak<LaneShared>,
        funding: Option<Weak<Domain>>,
    },
}
#[derive(Debug, Default)]
struct Links {
    previous: Option<Weak<MemberNode>>,
    next: Option<Arc<MemberNode>>,
    attached: bool,
}
#[derive(Debug)]
pub(crate) struct MemberNode {
    member: Member,
    links: Mutex<Links>,
}
impl MemberNode {
    pub fn account(account: &Account) -> Arc<Self> {
        Arc::new(Self {
            member: Member::Account(Arc::downgrade(&account.0)),
            links: Mutex::new(Links::default()),
        })
    }
    pub fn lane(lane: &Arc<LaneShared>, funding: Option<&Arc<Domain>>) -> Arc<Self> {
        Arc::new(Self {
            member: Member::Lane {
                lane: Arc::downgrade(lane),
                funding: funding.map(Arc::downgrade),
            },
            links: Mutex::new(Links::default()),
        })
    }
}
#[derive(Debug, Default)]
pub(crate) struct Membership {
    head: Mutex<Option<Arc<MemberNode>>>,
}
#[derive(Debug)]
pub(crate) enum HeldMember {
    Account(Account),
    Lane(crate::Lane, Option<crate::FundingDomain>),
}
impl Membership {
    /// The caller has preallocated node and holds its owner's lifecycle gates.
    pub fn insert(&self, node: &Arc<MemberNode>) {
        let mut head = self.head.lock().unwrap();
        let mut links = node.links.lock().unwrap();
        assert!(!links.attached);
        links.attached = true;
        links.next = head.take();
        if let Some(next) = &links.next {
            next.links.lock().unwrap().previous = Some(Arc::downgrade(node));
        }
        *head = Some(node.clone());
    }
    pub fn remove(&self, node: &Arc<MemberNode>) {
        let mut head = self.head.lock().unwrap();
        let mut links = node.links.lock().unwrap();
        if !links.attached {
            return;
        }
        let previous = links.previous.take().and_then(|weak| weak.upgrade());
        let next = links.next.take();
        links.attached = false;
        if let Some(previous) = &previous {
            previous.links.lock().unwrap().next = next.clone();
        } else {
            assert!(head.as_ref().is_some_and(|h| Arc::ptr_eq(h, node)));
            *head = next.clone();
        }
        if let Some(next) = &next {
            next.links.lock().unwrap().previous = previous.as_ref().map(Arc::downgrade);
        }
    }
    /// Diagnostic/teardown collection allocates outside admission transactions.
    /// Each live node is visited once; dead members cannot hide later siblings.
    pub fn collect(&self) -> (Vec<HeldMember>, usize) {
        let head = self.head.lock().unwrap();
        let mut next = head.clone();
        let mut members = Vec::new();
        let mut visited = 0;
        while let Some(node) = next {
            visited += 1;
            match &node.member {
                Member::Account(account) => {
                    if let Some(account) = account.upgrade() {
                        members.push(HeldMember::Account(Account(account)));
                    }
                }
                Member::Lane { lane, funding } => {
                    if let Some(lane) = lane.upgrade() {
                        members.push(HeldMember::Lane(
                            crate::Lane(lane),
                            funding
                                .as_ref()
                                .and_then(|f| f.upgrade())
                                .map(crate::FundingDomain),
                        ));
                    }
                }
            }
            next = node.links.lock().unwrap().next.clone();
        }
        (members, visited)
    }
}

#[derive(Debug)]
pub(crate) struct Subtree {
    pub accounts: Vec<Account>,
    pub lanes: Vec<crate::Lane>,
    pub domains: Vec<crate::FundingDomain>,
    pub visited: usize,
}
impl Subtree {
    pub fn collect(root: &Account) -> Self {
        let mut result = Self {
            accounts: vec![root.clone()],
            lanes: Vec::new(),
            domains: Vec::new(),
            visited: 0,
        };
        let mut cursor = 0;
        while cursor < result.accounts.len() {
            let (members, visited) = result.accounts[cursor].0.members.collect();
            result.visited += visited;
            for member in members {
                match member {
                    HeldMember::Account(account) => result.accounts.push(account),
                    HeldMember::Lane(lane, funding) => {
                        result.lanes.push(lane);
                        if let Some(funding) = funding {
                            result.domains.push(funding);
                        }
                    }
                }
            }
            cursor += 1;
        }
        result
    }
}
