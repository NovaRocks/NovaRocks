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

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};

macro_rules! runtime_id {
    ($name:ident, $raw:ty) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub(crate) struct $name($raw);

        impl $name {
            pub(crate) const fn new(raw: $raw) -> Self {
                Self(raw)
            }

            pub(crate) const fn get(self) -> $raw {
                self.0
            }
        }
    };
}

runtime_id!(DeploymentEpoch, u64);
runtime_id!(RuntimeFilterParticipantId, u32);
runtime_id!(RouteEdgeId, u32);
runtime_id!(PartitionId, u32);
runtime_id!(ProducerSequence, u64);
runtime_id!(LogicalVersion, u64);

impl LogicalVersion {
    pub(crate) const FIRST: Self = Self(1);

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ProducerStreamId {
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    partition_id: PartitionId,
}

impl ProducerStreamId {
    pub(crate) const fn new(
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
    ) -> Self {
        Self {
            binding_id,
            fragment_instance_id,
            partition_id,
        }
    }

    pub(crate) const fn binding_id(self) -> BindingId {
        self.binding_id
    }

    pub(crate) const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) const fn partition_id(self) -> PartitionId {
        self.partition_id
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ContributionIdentity {
    query_id: UniqueId,
    participant_id: RuntimeFilterParticipantId,
    channel_id: ChannelId,
    epoch: DeploymentEpoch,
    stream: ProducerStreamId,
    sequence: ProducerSequence,
}

impl ContributionIdentity {
    pub(crate) const fn new(
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        channel_id: ChannelId,
        epoch: DeploymentEpoch,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
    ) -> Self {
        Self {
            query_id,
            participant_id,
            channel_id,
            epoch,
            stream,
            sequence,
        }
    }

    pub(crate) const fn query_id(self) -> UniqueId {
        self.query_id
    }

    pub(crate) const fn participant_id(self) -> RuntimeFilterParticipantId {
        self.participant_id
    }

    pub(crate) const fn channel_id(self) -> ChannelId {
        self.channel_id
    }

    pub(crate) const fn epoch(self) -> DeploymentEpoch {
        self.epoch
    }

    pub(crate) const fn stream(self) -> ProducerStreamId {
        self.stream
    }

    pub(crate) const fn sequence(self) -> ProducerSequence {
        self.sequence
    }
}

#[cfg(test)]
mod tests {
    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};

    use super::*;

    #[test]
    fn contribution_identity_keeps_stable_runtime_coordinates() {
        let stream =
            ProducerStreamId::new(BindingId::new(4), UniqueId::new(5, 6), PartitionId::new(7));
        let identity = ContributionIdentity::new(
            UniqueId::new(1, 2),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(8),
            DeploymentEpoch::new(9),
            stream,
            ProducerSequence::new(10),
        );

        assert_eq!(identity.query_id(), UniqueId::new(1, 2));
        assert_eq!(identity.participant_id().get(), 3);
        assert_eq!(identity.channel_id().get(), 8);
        assert_eq!(identity.epoch().get(), 9);
        assert_eq!(identity.stream(), stream);
        assert_eq!(identity.sequence().get(), 10);
        assert_eq!(LogicalVersion::FIRST.get(), 1);
        assert_eq!(
            LogicalVersion::FIRST.checked_next(),
            Some(LogicalVersion::new(2))
        );
        assert_eq!(LogicalVersion::new(u64::MAX).checked_next(), None);
    }
}
