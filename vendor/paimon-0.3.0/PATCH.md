# NovaRocks read-host patch

Source package: `paimon` 0.3.0 from crates.io.

- Crate archive SHA-256: `525d11131f96bd6fe858c39b1a662179fee701e49fee92dad273445c56e936dc`
- Upstream git revision: `7b54d44c487590f4d84952533f110ba16e9346a4`
- Workspace dependency policy: exact `=0.3.0`, `default-features = false`

NovaRocks adds a storage-client-neutral `ReadOnlyFileIO` and request-local
`ReadControl` contract. `FileIO::from_read_only` never constructs an SDK
storage client or local cache, streams and charges listing entries before
collection, checks cancellation/resource checkpoints around I/O, and rejects
all mutation entry points before invoking the host. `FileSystemCatalog::with_file_io`
accepts this authorized instance without parsing credentials from catalog
properties.

The read control also covers SDK-retained data after I/O returns: range and
whole-file `Bytes` own their reservation until their final clone is dropped;
Avro schema caches reserve a conservative bound for the owned JSON key, parser
workspace, and recursive writer schema before parsing, install one lease per
cache entry under the cache lock, and retain it until cache drop; the versioned
`SchemaManager` cache reserves each parsed `TableSchema` with a conservative
recursive retained-size estimate, installs only one charged value per schema ID,
and embeds the shared lease in the schema so returned `Arc` and owned schema
clones remain charged after a temporary manager or its cache is dropped; every Avro OCF
block reserves its actual borrowed or decompressed payload, and that reservation
moves together with the fixed decoded-object reservation through manifest
pruning and split construction into the returned `Plan`, then remains live until
the plan is dropped or the host has adopted its splits under its own reservation;
`DataFileReader` checkpoints and charges decoded input and projected output
batches; Parquet reserves one conservative row-group decode unit before the
decoder can be polled, including uncompressed variable-width bytes and
per-value workspace, and keeps it through stream drop; and the primary-key
sort-merge reader charges cursor batches,
arrow-row keys, same-key histories (including user sequence values), buffered
batches, output indices, materialized rows, output construction, and yielded
batches. A neutral output handoff lets the embedding host adopt the exact
yielded-batch reservation without charging the same Arrow buffers again; hosts
that do not accept the handoff retain the SDK default, which keeps the
reservation across the yield. The merge loop checkpoints each history row and the no-output omit
path so large delete-only key histories remain cancellable. Reservations move
with retained buffers and are released on compaction, error, cancellation, or
stream drop.

The patched read path also fails closed on physical corruption before logical
merge. Every decoded KV batch must declare `_SEQUENCE_NUMBER` as non-null
Int64 and `_VALUE_KIND` as non-null Int8, contain no NULL system values, and
use only row-kind values 0 through 3. Schema evolution synthesizes NULL only
when the exact historical schema does not contain the requested field ID; a
column declared by that schema must be present in the decoded physical batch
with its exact historical Arrow type before any supported evolution cast.

`BinaryTableStats` is publicly re-exported so a provider-private, validated
split codec can reconstruct the complete SDK `DataFileMeta` without dropping
manifest statistics or copying an internal SDK representation.

The upstream license and notice files are retained verbatim. No upstream API
is removed; the patch is additive except for the internal `FileIO` storage
representation needed to host the authorized backend.

## Removal condition

Remove `[patch.crates-io]` and this vendored source once an upstream release
provides equivalent authorized read-only FileIO injection, request checkpoints,
owner-lifetime retained-memory reservations, strict physical input validation,
and the minimal public metadata needed by the provider-private split codec, and
that release passes NovaRocks resource-limit, cancellation, corrupt-input, and
cross-engine Paimon fixtures. Upstreaming these general-purpose hooks is the
preferred exit path; the local patch must not become a reason to freeze the SDK
version or silently widen the supported read surface.
