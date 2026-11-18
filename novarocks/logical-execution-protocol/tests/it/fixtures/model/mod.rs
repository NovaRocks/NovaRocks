// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Passive, seeded external ports for the real LP Core. No AP/Native/host
//! execution or physical-resource exit is inferred from these value models.
mod result;
pub(crate) use result::*;
mod internal;
pub(crate) use internal::*;
mod statement;
pub(crate) use statement::*;

pub(crate) fn audit_consumer_effects(f: &crate::support::Fixture) {
    use crate::support::*;
    assert!(
        !f.effects.iter().any(|effect| matches!(
            effect.body,
            LogicalEffectBody::Consumer(
                ConsumerEffect::WriteSchema { .. }
                    | ConsumerEffect::ReserveEndSlot { .. }
                    | ConsumerEffect::EnqueueEnd { .. }
                    | ConsumerEffect::PublishReady { .. }
                    | ConsumerEffect::ExposeOutput { .. }
                    | ConsumerEffect::PublishCompletion
            )
        )),
        "no duplicate or unauthorized consumer output may hide outside the model history"
    );
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Seed(u64);
impl Seed {
    pub(crate) fn new(value: u64) -> Self {
        Self(value)
    }
    pub(crate) fn delay(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 % 4
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod internal_tests;
#[cfg(test)]
mod statement_tests;

/// Explicit passive world fixture for the declared empty AP roster. Consumer
/// models only observe this endpoint fact. G9c's real AP Core supplies its own
/// verdict; this helper is not part of any data/domain/publication model.
#[cfg(test)]
fn world_success_seal(
    f: &mut crate::support::Fixture,
    record: impl FnOnce(crate::support::AttemptRequestToken),
) {
    use crate::support::*;
    let token = f
        .effects
        .iter()
        .find_map(|effect| match &effect.body {
            LogicalEffectBody::AttemptPort(message) => match message.body {
                LogicalToAttemptBody::SealRequest { token } => {
                    assert_eq!(message.activation, activation(1));
                    Some(token)
                }
                _ => None,
            },
            _ => None,
        })
        .expect("original world AP seal request");
    record(token);
    f.seal();
}
