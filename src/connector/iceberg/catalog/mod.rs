//! Iceberg catalog registry, table loading, hadoop/S3 storage backends,
//! and `ADD FILES` support. Migrated here from `src/standalone/iceberg/`
//! during the standalone/connector decoupling refactor (2026-04-24).
//!
//! Files will be added incrementally by the next tasks in this plan.

pub(crate) mod add_files;
pub(crate) mod hadoop_catalog;
pub(crate) mod s3_storage;
