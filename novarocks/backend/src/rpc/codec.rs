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

use std::marker::PhantomData;

use prost::Message;
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

// Tonic codec for the Backend RPC surface.

/// Native protobuf codec used by generated NovaRocks clients and servers.
///
/// It delegates to Prost unchanged. The pre-decode raw byte scan this codec
/// used to run went with `StageFragmentsRequest`: it was keyed on that exact
/// decoded type, so the retirement of the Stage RPC left it unreachable.
#[derive(Debug, Clone)]
pub(crate) struct NativeProstCodec<T, U> {
    marker: PhantomData<(T, U)>,
}

impl<T, U> Default for NativeProstCodec<T, U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T, U> Codec for NativeProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = NativeProstEncoder<T>;
    type Decoder = NativeProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        NativeProstEncoder::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        NativeProstDecoder::default()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NativeProstEncoder<T> {
    marker: PhantomData<T>,
    buffer_settings: BufferSettings,
}

impl<T> Default for NativeProstEncoder<T> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }
}

impl<T: Message> Encoder for NativeProstEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        item.encode(destination)
            .expect("Message only errors if not enough space");
        Ok(())
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NativeProstDecoder<U> {
    marker: PhantomData<U>,
    buffer_settings: BufferSettings,
}

impl<U> Default for NativeProstDecoder<U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }
}

impl<U> Decoder for NativeProstDecoder<U>
where
    U: Message + Default + Send + 'static,
{
    type Item = U;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        U::decode(source)
            .map(Some)
            .map_err(|error| Status::internal(error.to_string()))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}
