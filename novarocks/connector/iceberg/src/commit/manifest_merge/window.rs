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

use std::collections::BTreeMap;
use std::future::Future;

use futures::stream::{FuturesUnordered, StreamExt};

use crate::iceberg::Result;

use super::invalid;

/// Admit work only through the original FileIO. The window controls polling,
/// and keeps every in-flight future until its own result has been observed.
pub(crate) async fn ordered_window<I, T, F>(
    width: usize,
    inputs: impl IntoIterator<Item = I>,
    mut check_active: impl FnMut() -> Result<()>,
    mut work: impl FnMut(I) -> F,
) -> Result<Vec<T>>
where
    F: Future<Output = Result<T>>,
{
    if width == 0 {
        return Err(invalid("Manifest preparation has no I/O window"));
    }
    let mut inputs = inputs.into_iter().enumerate();
    let mut running = FuturesUnordered::new();
    let mut finished = BTreeMap::new();
    let mut failure = check_active().err();

    loop {
        while failure.is_none() && running.len() < width {
            let Some((ordinal, input)) = inputs.next() else {
                break;
            };
            if let Err(error) = check_active() {
                failure = Some(error);
                break;
            }
            let future = work(input);
            running.push(async move { (ordinal, future.await) });
        }
        let Some((ordinal, result)) = running.next().await else {
            break;
        };
        match result {
            Ok(value) if failure.is_none() => {
                finished.insert(ordinal, value);
                failure = check_active().err();
            }
            Err(error) if failure.is_none() => failure = Some(error),
            _ => {}
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(finished.into_values().collect()),
    }
}
