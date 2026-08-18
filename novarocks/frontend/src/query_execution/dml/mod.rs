//! Query-execution DML reverse ports, sealed write facts, and provider-neutral helpers.

pub mod add_files;
pub(crate) mod aggregate;
pub mod ctas;
pub mod delete;
pub(crate) mod delete_predicate_translate;
pub mod external_write_fence;
pub(crate) mod iceberg_ctas;
pub(crate) mod iceberg_writer;
pub mod insert;
pub mod mutation;
pub(crate) mod mutation_flow;
pub mod truncate;
pub(crate) mod write;
