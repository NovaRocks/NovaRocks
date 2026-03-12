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

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TInputProtocol,
    TMessageIdentifier, TMessageType, TOutputProtocol, TSerializable, TStructIdentifier, TType,
};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel};

use crate::types;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerAction {
    Continue,
    Close,
}

type Handler = Box<
    dyn Fn(
            &str,
            i32,
            &mut dyn TInputProtocol,
            &mut dyn TOutputProtocol,
        ) -> thrift::Result<ServerAction>
        + Send
        + Sync,
>;

#[allow(dead_code)]
pub struct FakeFeRpcServer {
    addr: types::TNetworkAddress,
    accepts: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

#[allow(dead_code)]
impl FakeFeRpcServer {
    pub fn start(close_first_n: usize, handler: Handler) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake FE RPC server");
        listener
            .set_nonblocking(true)
            .expect("set fake FE RPC server nonblocking");
        let local_addr = listener.local_addr().expect("read fake FE RPC server addr");
        let accepts = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let accepts_for_thread = Arc::clone(&accepts);
        let stop_for_thread = Arc::clone(&stop);
        let handler = Arc::new(handler);
        let join = std::thread::spawn(move || {
            while !stop_for_thread.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let accept_idx = accepts_for_thread.fetch_add(1, Ordering::AcqRel);
                        if accept_idx < close_first_n {
                            drop(stream);
                            continue;
                        }
                        let handler = Arc::clone(&handler);
                        std::thread::spawn(move || {
                            let channel = TTcpChannel::with_stream(stream);
                            let (r, w) = match channel.split() {
                                Ok(parts) => parts,
                                Err(_) => return,
                            };
                            let r = TBufferedReadTransport::new(r);
                            let w = TBufferedWriteTransport::new(w);
                            let mut i_prot = TBinaryInputProtocol::new(r, true);
                            let mut o_prot = TBinaryOutputProtocol::new(w, true);
                            loop {
                                let message = match i_prot.read_message_begin() {
                                    Ok(message) => message,
                                    Err(thrift::Error::Transport(_)) => break,
                                    Err(_) => break,
                                };
                                let action = match handler(
                                    &message.name,
                                    message.sequence_number,
                                    &mut i_prot,
                                    &mut o_prot,
                                ) {
                                    Ok(action) => action,
                                    Err(_) => break,
                                };
                                if action == ServerAction::Close {
                                    break;
                                }
                            }
                        });
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr: types::TNetworkAddress::new(
                local_addr.ip().to_string(),
                local_addr.port() as i32,
            ),
            accepts,
            stop,
            join: Some(join),
        }
    }

    pub fn addr(&self) -> &types::TNetworkAddress {
        &self.addr
    }

    pub fn accepts(&self) -> usize {
        self.accepts.load(Ordering::Acquire)
    }
}

impl Drop for FakeFeRpcServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = std::net::TcpStream::connect(format!("127.0.0.1:{}", self.addr.port));
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

pub fn read_struct_arg<T: TSerializable>(i_prot: &mut dyn TInputProtocol) -> thrift::Result<T> {
    i_prot.read_struct_begin()?;
    let mut value = None;
    loop {
        let field = i_prot.read_field_begin()?;
        if field.field_type == TType::Stop {
            break;
        }
        match field.id {
            Some(1) => {
                value = Some(T::read_from_in_protocol(i_prot)?);
            }
            _ => i_prot.skip(field.field_type)?,
        }
        i_prot.read_field_end()?;
    }
    i_prot.read_struct_end()?;
    i_prot.read_message_end()?;
    value.ok_or_else(|| thrift::Error::from("missing method argument"))
}

pub fn write_struct_reply<T: TSerializable>(
    o_prot: &mut dyn TOutputProtocol,
    method: &str,
    seq: i32,
    value: &T,
) -> thrift::Result<()> {
    o_prot.write_message_begin(&TMessageIdentifier::new(method, TMessageType::Reply, seq))?;
    o_prot.write_struct_begin(&TStructIdentifier::new(&format!("{method}_result")))?;
    o_prot.write_field_begin(&TFieldIdentifier::new("success", TType::Struct, 0))?;
    value.write_to_out_protocol(o_prot)?;
    o_prot.write_field_end()?;
    o_prot.write_field_stop()?;
    o_prot.write_struct_end()?;
    o_prot.write_message_end()?;
    o_prot.flush()
}
