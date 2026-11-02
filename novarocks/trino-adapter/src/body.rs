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

//! Response bytes retain their source window through the actual socket flush.
use crate::entry::Page;
use axum::{body::Body, response::Response};
use bytes::Bytes;
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

pub(crate) const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// This custody is transferred out of response extensions by the transport.
/// Keeping only a Bytes frame alive would not retain the original window alias.
#[derive(Clone)]
struct PageCustody {
    _inner: Arc<PageCustodyInner>,
}
struct PageCustodyInner {
    _page: Option<Arc<Page>>,
    progress: Arc<Notify>,
}
impl Drop for PageCustodyInner {
    fn drop(&mut self) {
        // Release the actual backing before waking a producer that tries to
        // reclaim it; a wake before field destruction can otherwise be lost.
        self._page.take();
        self.progress.notify_waiters();
    }
}

pub(crate) fn response(page: Arc<Page>, progress: Arc<Notify>) -> Response {
    let remaining = page
        .chunks
        .iter()
        .try_fold(0_u64, |total, chunk| total.checked_add(chunk.len() as u64))
        .expect("bounded Trino page length");
    let mut response = Response::new(Body::new(PageBody {
        page: page.clone(),
        next: 0,
        remaining,
    }));
    *response.status_mut() = page.status;
    *response.headers_mut() = page.headers.clone();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response.extensions_mut().insert(PageCustody {
        _inner: Arc::new(PageCustodyInner {
            _page: Some(page),
            progress,
        }),
    });
    response
}
struct PageBody {
    page: Arc<Page>,
    next: usize,
    remaining: u64,
}
impl HttpBody for PageBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let Some(chunk) = self.page.chunks.get(self.next).cloned() else {
            return Poll::Ready(None);
        };
        self.next += 1;
        self.remaining -= chunk.len() as u64;
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }
    fn is_end_stream(&self) -> bool {
        self.next == self.page.chunks.len()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}

#[derive(Clone)]
pub(crate) struct WriteDeadlines {
    state: Arc<Mutex<WriteState>>,
    changed: Arc<Notify>,
    timeout: Duration,
}
#[derive(Default)]
struct WriteState {
    next_id: u64,
    responses: BTreeMap<u64, PendingWrite>,
}
struct PendingWrite {
    deadline: Instant,
    body_finished: bool,
    _custody: Option<PageCustody>,
}
impl WriteDeadlines {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriteState::default())),
            changed: Arc::new(Notify::new()),
            timeout,
        }
    }
    /// Called by the connection service before handing its response to Hyper.
    pub(crate) fn wrap(&self, mut response: Response) -> Response {
        let custody = response.extensions_mut().remove::<PageCustody>();
        response.map(|inner| {
            Body::new(DeadlineBody {
                inner,
                deadlines: self.clone(),
                id: None,
                custody,
            })
        })
    }
    fn started(&self, custody: Option<PageCustody>) -> u64 {
        let mut state = self.state.lock().expect("Trino response write deadlines");
        state.next_id = state
            .next_id
            .checked_add(1)
            .expect("Trino response identity exhausted");
        let id = state.next_id;
        state.responses.insert(
            id,
            PendingWrite {
                deadline: Instant::now() + self.timeout,
                body_finished: false,
                _custody: custody,
            },
        );
        drop(state);
        self.changed.notify_one();
        id
    }
    fn body_finished(&self, id: u64) {
        if let Some(response) = self
            .state
            .lock()
            .expect("Trino response write deadlines")
            .responses
            .get_mut(&id)
        {
            response.body_finished = true;
        }
    }
    /// Hyper invokes the underlying flush only after its own write buffer was
    /// drained. Body completion alone cannot prove those bytes left that buffer.
    pub(crate) fn flushed(&self) {
        self.state
            .lock()
            .expect("Trino response write deadlines")
            .responses
            .retain(|_, response| !response.body_finished);
        self.changed.notify_one();
    }
    pub(crate) fn has_pending(&self) -> bool {
        !self
            .state
            .lock()
            .expect("Trino response write deadlines")
            .responses
            .is_empty()
    }
    pub(crate) async fn expired(&self) -> u64 {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let next = self
                .state
                .lock()
                .expect("Trino response write deadlines")
                .responses
                .iter()
                .min_by_key(|(_, response)| response.deadline)
                .map(|(id, response)| (*id, response.deadline));
            match next {
                Some((id, deadline)) => tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => {
                        // A flush can race the deadline; its completed response
                        // must not close a later response on the same socket.
                        if self.state.lock().expect("Trino response write deadlines").responses.get(&id).is_some_and(|response| response.deadline <= Instant::now()) {
                            return id;
                        }
                    }
                    _ = &mut changed => {}
                },
                None => {
                    changed.await;
                }
            }
        }
    }
}
struct DeadlineBody {
    inner: Body,
    deadlines: WriteDeadlines,
    id: Option<u64>,
    custody: Option<PageCustody>,
}
impl HttpBody for DeadlineBody {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        // Start on the first poll, including Pending. A stalled producer does
        // not postpone the response's write deadline.
        if self.id.is_none() {
            let custody = self.custody.take();
            self.id = Some(self.deadlines.started(custody));
        }
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(result, Poll::Ready(None) | Poll::Ready(Some(Err(_))))
            || self.inner.is_end_stream()
        {
            self.deadlines
                .body_finished(self.id.expect("started response"));
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
impl Drop for DeadlineBody {
    fn drop(&mut self) {
        // Hyper may drop a body once it has the final frame, before that frame
        // reaches the socket. Custody and the timer stay with WriteDeadlines.
        if let Some(id) = self.id {
            self.deadlines.body_finished(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, StatusCode};
    use futures::{FutureExt, future::poll_fn};
    #[tokio::test]
    async fn body_drop_preserves_original_page_until_actual_flush() {
        let page = Arc::new(Page {
            chunks: vec![Bytes::from_static(b"{\"data\":[[1]]}")],
            headers: HeaderMap::new(),
            status: StatusCode::OK,
            _window: None,
        });
        let weak = Arc::downgrade(&page);
        let progress = Arc::new(Notify::new());
        let notice = progress.notified();
        tokio::pin!(notice);
        notice.as_mut().enable();
        let deadlines = WriteDeadlines::new(WRITE_TIMEOUT);
        let mut body = deadlines.wrap(response(page, progress.clone())).into_body();
        let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.data_ref().unwrap().as_ref(), b"{\"data\":[[1]]}");
        drop(frame);
        drop(body);
        assert!(deadlines.has_pending());
        assert!(weak.upgrade().is_some());
        assert!(notice.as_mut().now_or_never().is_none());
        deadlines.flushed();
        assert!(!deadlines.has_pending());
        assert!(weak.upgrade().is_none());
        notice.await;
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_begins_on_pending_first_poll_and_flush_cancels_it() {
        let deadlines = WriteDeadlines::new(WRITE_TIMEOUT);
        let body = Body::from_stream(futures::stream::pending::<Result<Bytes, Infallible>>());
        let mut body = deadlines.wrap(Response::new(body)).into_body();
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(!deadlines.has_pending());
        assert!(
            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .now_or_never()
                .is_none()
        );
        assert!(deadlines.has_pending());
        tokio::time::advance(WRITE_TIMEOUT - Duration::from_millis(1)).await;
        assert!(deadlines.expired().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(deadlines.expired().await, 1);
        drop(body);
        deadlines.flushed();
        assert!(!deadlines.has_pending());
        tokio::time::advance(WRITE_TIMEOUT).await;
        assert!(deadlines.expired().now_or_never().is_none());
    }
}
