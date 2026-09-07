use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext};
use crate::scenarios::task_evidence;
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{
    QueryExecutionResourceSnapshot, QueryLifecycleStructuredSnapshot, ServerHandle,
};
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;

const REQUIRED_BACKENDS: usize = 3;
const IO_TIMEOUT_CAP: Duration = Duration::from_secs(10);
const RESOURCE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const BASELINE_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";
/// The query the three identity-fence cases below issue.
///
/// Unlike the coordinator-local baseline it has to place real tasks on a
/// backend, because every one of these faults is claimed on an operation about
/// a task: two on `CreateTask` and one on a status frame. The sleep keeps a
/// task running rather than being needed by any fault -- a status frame exists
/// only while a task does, and a constant relation can finish before its
/// subscription has carried one.
const NID2_FENCE_QUERY: &str =
    "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(DistributedBaseline),
        Box::new(MysqlDisconnect),
        Box::new(QueryTimeout),
        Box::new(Nid2CreateConflict),
        Box::new(Nid2CreateReceiptForeignTask),
        Box::new(Nid2ForeignStatusProcess),
    ]
}

struct DistributedBaseline;

impl Scenario for DistributedBaseline {
    fn name(&self) -> &'static str {
        "query-lifecycle/distributed-baseline"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline");

        let connect_timeout = bounded_io_timeout(context, "connect baseline MySQL client")?;
        let mut connection =
            mysql_actor::connect(context.mysql_user(), context.mysql_port(), connect_timeout)?;
        context.action("connected baseline client through public MySQL protocol");

        let before_first = latest_execution_id(context)?;
        execute_baseline_query(&mut connection, "first")?;
        let first = await_terminal_snapshot(context, before_first.as_deref())?;

        let before_second = first.execution_id.clone();
        execute_baseline_query(&mut connection, "second")?;
        let second = await_terminal_snapshot(context, before_second.as_deref())?;
        let before_third = second.execution_id.clone();
        execute_baseline_query(&mut connection, "third")?;
        let third = await_terminal_snapshot(context, before_third.as_deref())?;
        let snapshots = [&first, &second, &third];
        assert_process_attribution(&snapshots)?;
        assert_process_attribution_diagnostics(context, &snapshots)?;
        context.action(format!(
            "verified three native terminal snapshots share namespace=0x{:016x}, use consecutive sequence {}, {}, {}, and attempt=1",
            first.process_namespace,
            first.local_sequence,
            second.local_sequence,
            third.local_sequence,
        ));

        await_resource_convergence(context, &baseline)
    }
}

struct MysqlDisconnect;

impl Scenario for MysqlDisconnect {
    fn name(&self) -> &'static str {
        "query-lifecycle/mysql-disconnect"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline");

        let stream = send_raw_mysql_query(
            context.mysql_user(),
            context.mysql_port(),
            "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v",
            bounded_io_timeout(context, "open disconnecting MySQL client")?,
        )?;
        context.action("sent a blocking query through a raw public MySQL connection");
        await_resource_activity(context, &baseline)?;
        context.action("observed in-flight distributed query resources");

        stream
            .shutdown(Shutdown::Both)
            .context("close raw public MySQL client connection")?;
        context.action("closed the raw public MySQL client connection");

        await_resource_convergence(context, &baseline)
    }
}

struct QueryTimeout;

impl Scenario for QueryTimeout {
    fn name(&self) -> &'static str {
        "query-lifecycle/query-timeout"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline");

        let mut stream = connect_raw_mysql(
            context.mysql_user(),
            context.mysql_port(),
            bounded_io_timeout(context, "open timeout MySQL client")?,
        )?;
        context.action("connected timeout client through public MySQL protocol");
        send_query(&mut stream, "SET query_timeout = 1")?;
        expect_ok_packet(&mut stream, "SET query_timeout")?;
        context.action("set query_timeout = 1 through the public MySQL protocol");

        send_query(
            &mut stream,
            "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v",
        )?;
        context.action("sent a blocking query expected to time out");
        await_resource_activity(context, &baseline)?;
        context.action("observed in-flight distributed query resources before timeout");
        let (_, response) = read_packet(&mut stream).context("read timed query response")?;
        ensure!(
            response.first().copied() == Some(0xff),
            "expected timed query to return a MySQL error packet, got payload={response:?}"
        );
        let error = mysql_error_text(&response)?;
        ensure!(
            error.contains("timed out") || error.contains("timeout"),
            "expected MySQL timeout error, got: {error}"
        );
        context.action(format!("received expected MySQL timeout error: {error}"));

        await_resource_convergence(context, &baseline)
    }
}

/// One NID-2 identity fence, expressed as the wire value a backend misstates
/// and the frontend refusal that must follow.
struct Nid2Fence {
    /// The runner-owned fault kind, armed by name through the cluster harness.
    fault: &'static str,
    /// The marker that fault prints, token-scoped to one arming.
    marker: &'static str,
    /// What the misstated value was, for the accepted-action line.
    subject: &'static str,
    /// Substrings the client-visible error must carry.
    ///
    /// Both halves matter. The first is the shape of the refusal --- which
    /// `TaskExecutionError` variant answered --- and the second names the fence
    /// inside it, so a statement that failed for an unrelated reason cannot
    /// satisfy the case. A bare `expect_err` would have been satisfied by any
    /// failure at all, including one the fault did not cause.
    error_fragments: &'static [&'static str],
}

/// A `CreateTask` conflict verdict on a task that really was admitted.
///
/// # Why the subject moved, and why it had to
///
/// This case was `nid-2-stage-conflict`. Its fault claimed
/// `handle_stage_fragments` (`novarocks/backend/src/query_lifecycle/rpc.rs`)
/// and rewrote a staged participant's outcome to
/// `StageFragmentsRejectedConflict`; no production query reaches that handler
/// any more, so on the task path the fault was armed, nothing consumed it, the
/// query succeeded, and the case waited out its whole budget for a marker with
/// no emitter.
///
/// The fence itself did not move far. `CreateTask` is the task protocol's
/// single per-task admission point and `CreateConflict` is its refusal, so the
/// successor fault answers an admitted create with that verdict. The frontend
/// half is `RemoteTask::on_create_ack` reporting `CreateSettlement::FailedClosed`
/// and `QueryTaskExecution::acknowledge_task` turning it into
/// `TaskExecutionError::OperationFailed`
/// (`novarocks/frontend/src/task_execution/execution.rs`).
///
/// What is preserved verbatim is the "after apply" half, which is the whole
/// reason this case can fail: the task is admitted and running on that backend,
/// so a frontend that retried the conflict or ignored it would find working
/// state behind the lie and the statement would return rows.
struct Nid2CreateConflict;

impl Scenario for Nid2CreateConflict {
    fn name(&self) -> &'static str {
        "query-lifecycle/nid-2-create-conflict"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_nid2_fence(
            context,
            &Nid2Fence {
                fault: "create-task-conflict-after-apply",
                marker: "NOVAROCKS_TASK_CREATE_CONFLICT_AFTER_APPLY",
                subject: "a CreateTask conflict verdict answered after the task was admitted",
                error_fragments: &["failed closed", "runner-owned CreateTask conflict"],
            },
        )
    }
}

/// A `CreateTask` acknowledgement that names a task the frontend never asked
/// about.
///
/// # Why the digest this replaces has no counterpart
///
/// This case was `nid-2-start-digest-conflict`. Its fault flipped a bit of the
/// `stage_digest` carried by `StartPreparedQuery`
/// (`novarocks/backend/src/query_lifecycle/rpc.rs`), and the fence it met was
/// `QueryLifecycleRegistry::start_prepared_query` refusing a start whose digest
/// was not the one it had staged.
///
/// That fence does not exist on the task protocol, and no substitute was
/// invented for it. There is no second operation that commits an already
/// staged plan --- a descriptor travels once, on `CreateTask` --- and
/// `WireFragmentPlan::parse`
/// (`novarocks/proto-codec/src/task_execution/descriptor.rs`) derives a plan's
/// fingerprint from the bytes the receiver just read, so no field of a first
/// delivery can be corrupted into disagreeing with a plan the receiver already
/// holds. The fingerprint is compared only against an installed one, on a
/// replay, and that comparison is the same `CreateConflict` the case above now
/// covers; restating it here would be a second copy of one fact.
///
/// What survives as a distinct fence is the identity half. `TaskIdentity` is
/// indivisible, and an answer that names another task is refused rather than
/// adopted --- by `decode_create_task_ack` against the request's own identity
/// (`novarocks/proto-codec/src/task_execution/operation.rs`) and again by
/// `TaskIdentity::verify_matches` in `RemoteTask::on_create_ack`. So the
/// forgery is the acknowledgement's task id, and the direction of the
/// substitution flips with it: the corrupted value is now in the answer rather
/// than in the request, because a `CreateTask` request is a validated neutral
/// value whose identity and context are checked against each other at
/// construction and therefore cannot be built inconsistent.
///
/// A frontend that adopted the foreign receipt would mark this attempt's task
/// created and the statement would return rows, so the refusal is what the case
/// rests on.
struct Nid2CreateReceiptForeignTask;

impl Scenario for Nid2CreateReceiptForeignTask {
    fn name(&self) -> &'static str {
        "query-lifecycle/nid-2-create-receipt-foreign-task"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_nid2_fence(
            context,
            &Nid2Fence {
                fault: "create-task-receipt-foreign-task",
                marker: "NOVAROCKS_TASK_CREATE_RECEIPT_FOREIGN_TASK",
                subject: "a CreateTask acknowledgement naming a task the request never addressed",
                error_fragments: &["failed closed", "InvalidStateOrRequest"],
            },
        )
    }
}

/// A delivered task status event that names a foreign backend process.
///
/// # Why the subject moved, and why the verdict changed with it
///
/// This case was `nid-2-post-init-foreign-ref`. Its fault replaced the
/// `ParticipantAttemptRef` of a fragment observation published on the retired
/// control stream (`observation_participant_ref` in
/// `novarocks/backend/src/query_lifecycle/registry.rs`), and the frontend
/// dropped that observation before it could reach mutable telemetry state ---
/// so the old case asserted that the statement *succeeded* while the forged
/// reference was refused.
///
/// The surviving observation channel is `SubscribeTaskStatus`. It names no
/// participant of its own, so the forgeable fact is the backend process inside
/// the event's `TaskIdentity`, and the fence is `observe_event`
/// (`novarocks/frontend/src/native/task_transport.rs`) comparing it against the
/// subscription's own query context.
///
/// The verdict is genuinely different, and this case now asserts the new one
/// rather than the old one: `SubscriptionState::ProcessMismatch` is fatal by
/// design, and `TaskRound::turn` reads it through `settled_fatally` and fails
/// the attempt with `TaskExecutionError::ParticipantUnobservable` naming the
/// backend. Asserting the old success would now be asserting the opposite of
/// what the protocol does.
///
/// The "never reached mutable state" half is still proved, just not from a
/// cluster: `observe_event` returns before `intake.publish`, and
/// `a_status_event_from_another_process_is_fatal` in that module asserts the
/// intake stayed empty. What only a cluster can show is that the fence fires at
/// all on a real subscription --- which is this case.
struct Nid2ForeignStatusProcess;

impl Scenario for Nid2ForeignStatusProcess {
    fn name(&self) -> &'static str {
        "query-lifecycle/nid-2-foreign-status-process"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_nid2_fence(
            context,
            &Nid2Fence {
                fault: "task-status-foreign-process",
                marker: "NOVAROCKS_TASK_STATUS_FOREIGN_PROCESS",
                subject: "a task status event naming a backend process other than its own",
                error_fragments: &["no longer observable", "process_mismatch"],
            },
        )
    }
}

/// Drives one identity fence: arm, run, prove the refusal, disarm, converge.
///
/// The arming is cleared on every path, including a failed assertion, and that
/// is not tidiness. An arm the frontend has already bound to this attempt is
/// harmless -- its trigger names a dead execution identity and can never match
/// again -- but a case that fails before its statement is scheduled leaves
/// three *unbound* arms behind, and the scheduler binds whatever it finds to
/// the next attempt it plans. That would perturb an unrelated scenario, and the
/// failure would be attributed to it.
fn run_nid2_fence(context: &mut ScenarioContext, fence: &Nid2Fence) -> Result<()> {
    require_three_backends(context)?;
    let baseline = resource_snapshot(context)?;
    let connect_timeout = bounded_io_timeout(context, "connect NID-2 identity-fence client")?;
    let mut connection =
        mysql_actor::connect(context.mysql_user(), context.mysql_port(), connect_timeout)?;
    let tokens = arm_on_every_backend(context, fence.fault)?;
    context.action(format!(
        "armed NID-2 {} across every Backend participant with tokens {tokens:?}",
        fence.fault
    ));

    let observed = observe_nid2_fence(context, fence, &tokens, &mut connection);
    let cleared = context
        .handle()
        .clear_query_lifecycle_faults()
        .with_context(|| format!("clear {} tokens", fence.fault));
    let (backend, error) = observed?;
    cleared?;
    await_resource_convergence(context, &baseline)?;
    context.action(format!(
        "BE[{backend}] published {} and the frontend fenced {}: {error}",
        fence.marker, fence.subject
    ));
    Ok(())
}

/// Runs the query under one armed fence and returns the backend that fired
/// together with the client-visible refusal.
fn observe_nid2_fence(
    context: &mut ScenarioContext,
    fence: &Nid2Fence,
    tokens: &[String],
    connection: &mut mysql::Conn,
) -> Result<(usize, String)> {
    let error = match connection.query::<i64, _>(NID2_FENCE_QUERY) {
        Ok(rows) => bail!(
            "{} was armed on every backend but the public query returned {rows:?} instead of \
             failing closed",
            fence.fault
        ),
        Err(error) => error.to_string(),
    };
    for fragment in fence.error_fragments {
        ensure!(
            error.contains(fragment),
            "{} rejected the public query without naming its fence: expected {fragment:?} in {error}",
            fence.fault
        );
    }
    // Read after the refusal, and polled rather than sampled once. The marker
    // is printed before the answer that carries the forgery is sent, so it is
    // always already produced by now -- but backend output reaches the harness
    // through an asynchronous pump, so a single read can be a few milliseconds
    // early. The previous form read once and accepted any nonzero count, which
    // could be satisfied by an earlier scenario's marker in the same log.
    let backend = await_token_scoped_marker(context, fence.marker, tokens)?;
    Ok((backend, error))
}

/// Arms one fault on every backend and returns each arming's token.
///
/// Every backend is armed because the fence is not about placement: the
/// scheduler places this query's tasks without regard to where an arming sits,
/// and a query with no split assignment can establish a single context. Arming
/// one backend would make the case a coin flip on which one that is. Each arm
/// carries its own token, which is what keeps the assertion scoped to this
/// scenario rather than to whatever the shared backend logs already hold.
fn arm_on_every_backend(context: &mut ScenarioContext, fault: &'static str) -> Result<Vec<String>> {
    let mut tokens = Vec::with_capacity(REQUIRED_BACKENDS);
    for backend_index in 0..REQUIRED_BACKENDS {
        context
            .handle()
            .arm_query_lifecycle_fault(backend_index, fault)
            .with_context(|| format!("arm {fault} for BE[{backend_index}]"))?;
        let token = context
            .handle()
            .armed_query_lifecycle_fault_token(backend_index, fault)
            .with_context(|| format!("read the {fault} token armed for BE[{backend_index}]"))?
            .with_context(|| format!("armed {fault} for BE[{backend_index}] has no token"))?;
        tokens.push(token);
    }
    Ok(tokens)
}

/// Waits until some backend logged `marker` carrying one of `tokens`.
///
/// The marker name and the token have to come from the same line, so the
/// evidence is one emission of this scenario's own arming rather than a marker
/// from an earlier case standing next to a token from this one.
fn await_token_scoped_marker(
    context: &mut ScenarioContext,
    marker: &str,
    tokens: &[String],
) -> Result<usize> {
    let needles = tokens
        .iter()
        .map(|token| format!("token={token}"))
        .collect::<Vec<_>>();
    loop {
        for backend_index in 0..context.handle().be_count() {
            let log = context
                .handle()
                .be_log_contents(backend_index)
                .with_context(|| format!("read BE[{backend_index}] log for {marker}"))?;
            if log.lines().any(|line| {
                line.contains(marker) && needles.iter().any(|needle| line.contains(needle))
            }) {
                return Ok(backend_index);
            }
        }
        let remaining = context.remaining(&format!(
            "observe {marker} carrying one of this arming's tokens {tokens:?}"
        ))?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let actual = context.handle().be_count();
    ensure!(
        actual == REQUIRED_BACKENDS,
        "{} requires native 1FE+3BE, but the runner launched 1FE+{actual}BE",
        context.name()
    );
    context.action("verified native 1FE+3BE topology");
    Ok(())
}

fn resource_snapshot(context: &mut ScenarioContext) -> Result<QueryExecutionResourceSnapshot> {
    context
        .handle()
        .query_execution_resource_snapshot()?
        .context("cross-process harness did not expose the query-resource oracle")
}

fn latest_execution_id(context: &mut ScenarioContext) -> Result<Option<String>> {
    Ok(context
        .handle()
        .query_lifecycle_structured_snapshot()?
        .and_then(|snapshot| snapshot.execution_id))
}

fn execute_baseline_query(connection: &mut mysql::Conn, ordinal: &str) -> Result<()> {
    let rows: Vec<i64> = connection
        .query(BASELINE_QUERY)
        .with_context(|| format!("execute {ordinal} distributed baseline query"))?;
    ensure!(
        rows == vec![1, 2],
        "{ordinal} distributed baseline query returned unexpected rows: {rows:?}"
    );
    Ok(())
}

fn await_terminal_snapshot(
    context: &mut ScenarioContext,
    before_execution_id: Option<&str>,
) -> Result<QueryLifecycleStructuredSnapshot> {
    let deadline = context.deadline();
    context
        .handle()
        .await_query_lifecycle_structured_snapshot_after(before_execution_id, deadline)
        .context("await a new typed query lifecycle terminal snapshot")
}

fn assert_process_attribution(snapshots: &[&QueryLifecycleStructuredSnapshot]) -> Result<()> {
    ensure!(
        !snapshots.is_empty(),
        "attribution requires at least one snapshot"
    );
    for pair in snapshots.windows(2) {
        let previous = pair[0];
        let next = pair[1];
        ensure!(
            previous.process_namespace == next.process_namespace,
            "consecutive baseline queries changed process namespace: previous=0x{:016x}, next=0x{:016x}",
            previous.process_namespace,
            next.process_namespace,
        );
        ensure!(
            previous.local_sequence.checked_add(1) == Some(next.local_sequence),
            "consecutive baseline query sequences are not adjacent: previous={}, next={}",
            previous.local_sequence,
            next.local_sequence,
        );
    }
    for (ordinal, snapshot) in snapshots.iter().enumerate() {
        let ordinal = ordinal + 1;
        ensure!(
            snapshot.attempt_id == 1,
            "baseline query {ordinal} used unexpected attempt id {}",
            snapshot.attempt_id
        );
        // Three participant-outcome assertions stood here: the list was not
        // empty, it held no more entries than the topology, and every entry
        // was a positive proof. All three are retired rather than restated,
        // because their subject no longer exists: the task protocol mints no
        // `ParticipantTerminalOutcome` at all, and the frontend deliberately
        // publishes an empty list instead of inventing proofs the protocol
        // never made (ADR-0135). What they were proving -- that a backend
        // really executed the attempt and its work completed -- is now proved
        // per query by `assert_query_completed_across_boundary`, from the
        // establish and release receipts and the frontend's own report that
        // every placed context answered.
    }
    ensure!(
        snapshots.len() >= 2,
        "attribution acceptance requires at least two consecutive queries"
    );
    Ok(())
}

fn assert_process_attribution_diagnostics(
    context: &mut ScenarioContext,
    snapshots: &[&QueryLifecycleStructuredSnapshot],
) -> Result<()> {
    let snapshot = snapshots
        .last()
        .context("attribution diagnostics require at least one snapshot")?;
    let namespace = format!("0x{:016x}", snapshot.process_namespace);
    let namespace_field = format!("query_process_namespace={namespace}");
    let startup_message = "NOVAROCKS_QUERY_PROCESS_NAMESPACE";
    let startup_count = context.handle().fe_log_count(startup_message)?;
    ensure!(
        startup_count == 1,
        "expected exactly one FE process namespace startup publication, found {startup_count}"
    );
    ensure!(
        context
            .handle()
            .fe_log_contents()?
            .contains(&namespace_field),
        "FE startup diagnostics did not publish {namespace_field}"
    );
    // A per-backend loop over `NOVAROCKS_QUERY_INIT_APPLIED` and
    // `query_process_namespace=<namespace>` stood here. Both were emitted only
    // by the retired lifecycle registry, so neither has a producer on the task
    // path, and neither can be restated as an all-three-backends fact: a query
    // context is established only where the scheduler placed a task, and this
    // baseline query -- constant rows, no scan, no splits -- places tasks on
    // one backend. The loop also matched the whole accumulated log, so it
    // never actually attributed a marker to this query.
    //
    // The replacement is scoped to the attempt instead of to the cluster.
    // Backend markers carry the execution identity, whose high and low halves
    // are the namespace and local sequence the frontend published, so an
    // establish admitted under this identity is the same cross-process
    // attribution fact the retired `namespace_field` assertion was making --
    // now provably about this query.
    //
    // Checked for every baseline query rather than only the last, because the
    // retired participant-outcome assertions it replaces also ran per query.
    let mut backends = BTreeSet::new();
    for (ordinal, snapshot) in snapshots.iter().enumerate() {
        backends.extend(task_evidence::assert_query_completed_across_boundary(
            context,
            snapshot,
            &format!("baseline query {}", ordinal + 1),
        )?);
    }
    context.action(format!(
        "verified one FE startup namespace publication for {namespace} and matching task-protocol \
         attribution on the backends that ran the attempts: {backends:?}"
    ));
    Ok(())
}

fn await_resource_activity(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<()> {
    loop {
        if resource_snapshot(context)? != *baseline {
            return Ok(());
        }
        let remaining = context.remaining("observe in-flight distributed query resources")?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

fn await_resource_convergence(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<()> {
    let deadline = context.deadline();
    context
        .handle()
        .await_query_execution_resource_convergence(
            baseline, deadline)
        .context("await query-resource convergence after terminal lifecycle outcome")?;
    context.action("verified query resources converged after the terminal lifecycle outcome");
    Ok(())
}

fn bounded_io_timeout(context: &ScenarioContext, operation: &str) -> Result<Duration> {
    Ok(context.remaining(operation)?.min(IO_TIMEOUT_CAP))
}

fn connect_raw_mysql(user: &str, port: u16, timeout: Duration) -> Result<TcpStream> {
    const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
    const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
    const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
    const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
    const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&address, timeout)
        .with_context(|| format!("connect raw public MySQL client at {address}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .context("set raw MySQL read timeout")?;
    stream
        .set_write_timeout(Some(timeout))
        .context("set raw MySQL write timeout")?;

    let (_, handshake) = read_packet(&mut stream).context("read MySQL handshake")?;
    ensure!(
        handshake.first().copied() == Some(10),
        "expected MySQL protocol v10 handshake, got payload={handshake:?}"
    );

    let client_flags = CLIENT_LONG_PASSWORD
        | CLIENT_LONG_FLAG
        | CLIENT_PROTOCOL_41
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH;
    let mut response = Vec::with_capacity(user.len() + 64);
    response.extend_from_slice(&client_flags.to_le_bytes());
    response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
    response.push(45);
    response.extend_from_slice(&[0u8; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(0);
    response.extend_from_slice(b"mysql_native_password");
    response.push(0);
    write_packet(&mut stream, 1, &response).context("write MySQL handshake response")?;

    let (_, auth_result) = read_packet(&mut stream).context("read MySQL authentication result")?;
    if auth_result.first().copied() == Some(0xff) {
        bail!(
            "raw public MySQL authentication failed: {}",
            mysql_error_text(&auth_result)?
        );
    }
    ensure!(
        auth_result.first().copied() == Some(0),
        "unexpected raw MySQL authentication response: {auth_result:?}"
    );
    Ok(stream)
}

fn send_raw_mysql_query(user: &str, port: u16, sql: &str, timeout: Duration) -> Result<TcpStream> {
    let mut stream = connect_raw_mysql(user, port, timeout)?;
    send_query(&mut stream, sql)?;
    Ok(stream)
}

fn send_query(stream: &mut TcpStream, sql: &str) -> Result<()> {
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    write_packet(stream, 0, &payload).context("write MySQL COM_QUERY packet")
}

fn expect_ok_packet(stream: &mut TcpStream, operation: &str) -> Result<()> {
    let (_, response) =
        read_packet(stream).with_context(|| format!("read response for {operation}"))?;
    if response.first().copied() == Some(0xff) {
        bail!("{operation} failed: {}", mysql_error_text(&response)?);
    }
    ensure!(
        response.first().copied() == Some(0),
        "{operation} expected a MySQL OK packet, got payload={response:?}"
    );
    Ok(())
}

fn mysql_error_text(payload: &[u8]) -> Result<String> {
    ensure!(
        payload.first().copied() == Some(0xff),
        "expected a MySQL error packet, got payload={payload:?}"
    );
    ensure!(
        payload.len() >= 3,
        "truncated MySQL error packet: {payload:?}"
    );
    let message_offset = if payload.get(3).copied() == Some(b'#') {
        9
    } else {
        3
    };
    Ok(String::from_utf8_lossy(&payload[message_offset..]).into_owned())
}

fn read_packet(stream: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .context("read MySQL packet header")?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0u8; length];
    stream
        .read_exact(&mut payload)
        .context("read MySQL packet payload")?;
    Ok((header[3], payload))
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len()).context("MySQL packet payload length fits u32")?;
    ensure!(length <= 0x00ff_ffff, "MySQL packet payload is too large");
    let header = [
        (length & 0xff) as u8,
        ((length >> 8) & 0xff) as u8,
        ((length >> 16) & 0xff) as u8,
        sequence,
    ];
    stream
        .write_all(&header)
        .context("write MySQL packet header")?;
    stream
        .write_all(payload)
        .context("write MySQL packet payload")?;
    stream.flush().context("flush MySQL packet")
}
