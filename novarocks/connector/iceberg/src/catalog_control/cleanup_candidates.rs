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

//! Provider-private cleanup object identity helpers.

pub(super) fn canonical_object_mtime_ms(value: i64) -> i64 {
    value.div_euclid(1_000) * 1_000
}

#[cfg(test)]
pub(super) mod gc_io_test {
    use crate::opendal::{self, OperationContext, raw::*};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Clone, Debug, Default)]
    pub(in crate::catalog_control) struct GcIoCalls {
        lists: Arc<AtomicUsize>,
        deletes: Arc<AtomicUsize>,
    }
    impl GcIoCalls {
        pub(in crate::catalog_control) fn counts(&self) -> (usize, usize) {
            (
                self.lists.load(Ordering::SeqCst),
                self.deletes.load(Ordering::SeqCst),
            )
        }
    }
    impl Layer for GcIoCalls {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(RecordedGcAccess {
                inner,
                calls: self.clone(),
            })
        }
    }
    #[derive(Debug)]
    struct RecordedGcAccess {
        inner: Servicer,
        calls: GcIoCalls,
    }
    impl Service for RecordedGcAccess {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;
        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }
        fn capability(&self) -> opendal::Capability {
            self.inner.capability()
        }
        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> opendal::Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }
        async fn stat(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpStat,
        ) -> opendal::Result<RpStat> {
            self.inner.stat(ctx, path, args).await
        }
        fn read(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpRead,
        ) -> opendal::Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }
        fn write(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }
        fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
            self.calls.deletes.fetch_add(1, Ordering::SeqCst);
            self.inner.delete(ctx)
        }
        fn list(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpList,
        ) -> opendal::Result<Self::Lister> {
            self.calls.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list(ctx, path, args)
        }
        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
            opts: OpCopier,
        ) -> opendal::Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args, opts)
        }
        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> opendal::Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }
        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> opendal::Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }
}
