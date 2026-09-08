# Workload control

This crate owns one role's process-local work admission and responsibility.
It does not own SQL, MV, statistics, maintenance, Task, or provider state machines.
Construct it in role composition with validated policy and actual local memory
configuration. Inject scopes into work entry points, not the root-issuing
`WorkloadControl` capability.

Construction starts in Initializing. Role composition calls `mark_ready` after
required bootstrap services are ready. Closing admission is irreversible and
preserves existing work/control; drain policy remains outside this crate.

## Integration contract

- `try_begin_root` creates one `WorkOwner` and one `BusinessPermit`. Derive child
  scopes for synchronous cross-domain work. Scope clones acquire no additional
  business or execution capacity. Preparation and execution use independent
  `StagePermit` values; validate the permit against the exact scope at the entry
  point and continue checking cancellation during work.
- Product completion calls `WorkOwner::complete`. Parent responsibility survives
  until children, obligations, allocations, and necessary control work converge.
  Owner drop records an orphan and requests cancellation. The role supervisor can
  adopt the owner or recover an existing obligation reporter. Handoff moves the
  same owner and records the transfer; it does not register a second job.
- Register exact-key obligations before initiating work with a potentially unknown
  outcome. Retry the same key to recover its reporter. Only validated domain/local
  facts resolve obligations. Cancellation, timeout, failure, and process identity
  replacement do not do so. A retired-attempt registration consumes the logical
  scope's restart allowance without resetting its inherited deadline.
- Queue declarations count actual retained capacity once. Backing memory also
  needs a local allocation charge. Product object locks remain external: acquire
  one condition, try the other without waiting, and return the stage/requeue when
  the other condition is unavailable. Never hold parent computation capacity
  while waiting for child computation capacity.
- `capacity_wait_timeout` bounds each stage/resource capacity wait to at most
  30 seconds. Stage and data resource waits use an earlier inherited deadline
  when present; control resource waits retain their independent timeout so
  expired work can clean up. Capacity timeout
  returns `CapacityWaitTimeout` without cancelling otherwise unlimited logical
  work or releasing existing allocations. Notifications do not restart this
  absolute deadline, and dispatch cannot grant an already expired request.
- A resource wait registers exactly one `(WorkId, ResourceClass)` entry on first
  poll. It shares `waiting_limit` with stage admissions; duplicates or overflow
  fail before subscribing. The RAII registration retains scope responsibility
  and is removed on success, timeout, cancellation, or future drop. Requested
  bytes are demand, never a physical reservation or retained queue payload.
- Control dispatch uses `next_control` / `wait_control_ready` independently of
  data queues. Full ready queues retain coalesced intent in bounded owner records.
  Drop of an unacknowledged control permit retries its intent. Acknowledgement is
  control progress, not an allocation release or physical-stop fact. The control
  resource class is for trusted role cleanup adapters; data work must use Data.
- Role supervision calls `expire_deadlines` at `next_deadline`. Cancellation
  subscribers and queued admissions also observe inherited deadlines directly.
  The crate creates no supervision tasks, OS threads, or polling loops that spin.

## Resource accounting coverage

`ResourceConfig` has no default: Server must supply the resolved local total,
control partition, and per-scope bound. `resources()` returns the same authority
for that controller. A foreign controller's scope is rejected. Remote samples
are observations and never reserve bytes in this authority.

Reservation converts unused bytes into actual allocation charges without double
charging. A shared backing allocation uses one `AllocationCharge`; Arrow slices
and other aliases clone that charge. Transfer changes the responsible scope under
the same authority without temporarily returning capacity. The final backing
owner releases the charge. An empty reservation carries no resource claim and
cannot revive a completed scope.

At this crate boundary, the managed paths are reservation, growth, conversion to
an allocation charge, sharing, transfer, and release. Existing engine allocations
are not automatically managed. Worker/kernel integration must attach these
charges to result/Exchange/scan buffers and their true shared backing ownership;
FE integration must cover preparation, decoded output, and in-flight result
reservations. Passing sampled tracker bytes to governance is not that integration.
Allocator metadata/fragmentation, thread stacks, unwrapped Arrow allocations,
provider/runtime internals, and caches without an adapter remain outside this
accounting. The API does not claim complete RSS or remote physical-memory safety.

Policy queues, scope records, obligations, and control mailboxes are bounded.
The two additional bookkeeping limits default to 8192 scope records and 8192
obligation records; they include unresolved work and reject excess admission.
Physical allocation accounting updates do not traverse these policy records.

## Focused verification

The crate tests cover root/child attribution, admission races and fairness,
lock/requeue ordering, deadline/cancellation, unknown work, real release, shared
allocation ownership, control saturation, configuration rejection, and scope
construction compile failures. They verify this library's contracts. Native FE/BE
integration and product acceptance require the later application/Worker gates.
