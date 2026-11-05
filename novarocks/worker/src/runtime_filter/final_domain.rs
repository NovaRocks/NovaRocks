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

//! Worker-owned FinalDomain completion lifecycle.
//!
//! A completion claims each local partition once, validates its frozen
//! membership contract, and emits the canonical final-domain contribution.
//! It deliberately has no native envelope, RPC, or Backend application
//! dependency; the enclosing participant supplies the execution producer.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use novarocks_execution::runtime_filter::{
    PartitionId, ProducerSequence, RuntimeFilterContractViolation,
    RuntimeFilterContractViolationKind, RuntimeFilterContribution, RuntimeFilterContributionKind,
    RuntimeFilterFinalDomain, RuntimeFilterFinalDomainCompletion,
    RuntimeFilterFinalDomainPartition, RuntimeFilterFinalDomainPartitionObligation,
    RuntimeFilterProducerFailure, RuntimeFilterSubmitOutcome, SharedRuntimeFilterProducer,
};

/// One Worker-local FinalDomain completion for a bound producer.
pub struct WorkerRuntimeFilterFinalDomainCompletion {
    producer: SharedRuntimeFilterProducer,
    data_type: DataType,
    contract_digest: [u8; 32],
    max_domain_canonical_bytes: usize,
    local_partition_count: u32,
    claimed: Mutex<BTreeMap<PartitionId, ()>>,
}

impl WorkerRuntimeFilterFinalDomainCompletion {
    pub fn new(
        producer: SharedRuntimeFilterProducer,
        data_type: DataType,
        contract_digest: [u8; 32],
        max_domain_canonical_bytes: usize,
        local_partition_count: u32,
    ) -> Self {
        Self {
            producer,
            data_type,
            contract_digest,
            max_domain_canonical_bytes,
            local_partition_count,
            claimed: Mutex::new(BTreeMap::new()),
        }
    }

    fn clone_for_partition(&self) -> Self {
        Self {
            producer: Arc::clone(&self.producer),
            data_type: self.data_type.clone(),
            contract_digest: self.contract_digest,
            max_domain_canonical_bytes: self.max_domain_canonical_bytes,
            local_partition_count: self.local_partition_count,
            claimed: Mutex::new(BTreeMap::new()),
        }
    }
}

impl RuntimeFilterFinalDomainCompletion for WorkerRuntimeFilterFinalDomainCompletion {
    fn membership_key_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn max_domain_canonical_bytes(&self) -> usize {
        self.max_domain_canonical_bytes
    }

    fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }

    fn claim_partition(
        &self,
        partition: PartitionId,
    ) -> Result<RuntimeFilterFinalDomainPartitionObligation, RuntimeFilterContractViolation> {
        if partition.get() >= self.local_partition_count {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain partition is outside its declared local partition count",
            ));
        }
        let mut claimed = self
            .claimed
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if claimed.insert(partition, ()).is_some() {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain partition was claimed more than once",
            ));
        }
        Ok(Box::new(WorkerRuntimeFilterFinalDomainPartition {
            completion: Arc::new(self.clone_for_partition()),
            partition,
            sealed: false,
        }))
    }

    fn fail(
        &self,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<RuntimeFilterSubmitOutcome, RuntimeFilterContractViolation> {
        self.producer.fail(reason)
    }
}

struct WorkerRuntimeFilterFinalDomainPartition {
    completion: Arc<WorkerRuntimeFilterFinalDomainCompletion>,
    partition: PartitionId,
    sealed: bool,
}

impl RuntimeFilterFinalDomainPartition for WorkerRuntimeFilterFinalDomainPartition {
    fn seal(
        &mut self,
        domain: RuntimeFilterFinalDomain,
    ) -> Result<(), RuntimeFilterContractViolation> {
        if self.sealed {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain partition was sealed twice",
            ));
        }
        if domain.data_type() != &self.completion.data_type
            || domain.contract_digest() != self.completion.contract_digest
        {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain payload does not match the installed membership contract",
            ));
        }
        let value_domain = novarocks_execution::runtime_filter::contribution::decode_value_domain(
            domain.canonical_bytes(),
            &self.completion.data_type,
            self.completion.max_domain_canonical_bytes,
        )
        .map_err(|_| {
            violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain canonical payload is invalid",
            )
        })?;
        let encoded = novarocks_execution::runtime_filter::contribution::encode_contribution(
            &novarocks_execution::runtime_filter::contribution::RuntimeFilterContribution::final_domain(
                novarocks_execution::runtime_filter::contribution::FinalDomainShard::new(
                    self.completion.contract_digest,
                    value_domain,
                ),
            ),
            novarocks_execution::runtime_filter::contribution::ContributionCodecExpectation::final_domain(
                &self.completion.data_type,
                self.completion.contract_digest,
            ),
            self.completion.max_domain_canonical_bytes,
        )
        .map_err(|_| {
            violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain contribution cannot be encoded canonically",
            )
        })?;
        self.completion.producer.submit(
            self.partition,
            ProducerSequence::new(0),
            RuntimeFilterContribution::new(
                RuntimeFilterContributionKind::FinalDomain,
                *encoded.schema_digest(),
                encoded.into_parts().1,
            ),
        )?;
        self.sealed = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), RuntimeFilterContractViolation> {
        if !self.sealed {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain partition closed before seal",
            ));
        }
        self.completion
            .producer
            .close_partition(self.partition, ProducerSequence::new(1))?;
        Ok(())
    }
}

fn violation(
    kind: RuntimeFilterContractViolationKind,
    detail: impl Into<Arc<str>>,
) -> RuntimeFilterContractViolation {
    RuntimeFilterContractViolation::new(kind, detail)
}
