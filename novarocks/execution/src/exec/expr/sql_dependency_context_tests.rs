//! Actual admitted context ownership probes; no FE factory or resource grant is fabricated.
use novarocks_query_application::{
    admitted_query_context::{RequestAdmission, RequestContext, StatementAdmissionContext},
    api::BackendTopologySnapshot,
    cancellation::QueryCancellationSource,
};
use novarocks_sql::compiler::{
    SessionOptimizerSettings, SqlFoldDependencyInput, SqlFoldDependencyObserver,
    SqlFoldEvaluationOutcome,
};
use novarocks_type_contract::{CompileControlError, PureCompileControl};
use novarocks_types::ClusterRole;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Observer(Arc<AtomicUsize>);
impl Drop for Observer {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
impl SqlFoldDependencyObserver for Observer {
    fn before_fold_dependency_observed(
        &self,
        _: SqlFoldDependencyInput<'_>,
        _: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        Ok(())
    }
    fn after_fold_dependency_observed(
        &self,
        _: SqlFoldDependencyInput<'_>,
        _: SqlFoldEvaluationOutcome<'_>,
    ) {
    }
}
fn observer() -> (Arc<dyn SqlFoldDependencyObserver>, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (Arc::new(Observer(Arc::clone(&drops))), drops)
}
fn statement() -> StatementAdmissionContext {
    StatementAdmissionContext::new(
        Some("fixture_catalog".into()),
        "fixture_database".into(),
        ClusterRole::Fe,
        None,
        QueryCancellationSource::new().view(),
        SessionOptimizerSettings::default(),
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        novarocks_result_contract::ClientPresentationFamily::MysqlText,
    )
}

#[test]
fn sql_dependency_context_default_admission_and_round_have_no_observer() {
    let statement = statement();
    assert!(statement.fold_dependency_observer().is_none());
    assert!(
        statement
            .for_topology(BackendTopologySnapshot::empty(1))
            .execution()
            .fold_dependency_observer()
            .is_none()
    );
    let request = RequestContext::admit(RequestAdmission::new(
        None,
        "fixture".into(),
        ClusterRole::Fe,
        BackendTopologySnapshot::empty(2),
        None,
        QueryCancellationSource::new().view(),
        SessionOptimizerSettings::default(),
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        novarocks_result_contract::ClientPresentationFamily::MysqlText,
    ));
    assert!(request.execution().fold_dependency_observer().is_none());
}

#[test]
fn sql_dependency_context_rounds_and_clone_retain_the_same_owner_until_last_drop() {
    let (observer, drops) = observer();
    let statement = statement().with_fold_dependency_observer(Arc::clone(&observer));
    let first = statement.for_topology(BackendTopologySnapshot::empty(1));
    let second = statement.for_topology(BackendTopologySnapshot::empty(2));
    let clone = first.clone();
    for request in [&first, &second, &clone] {
        assert!(Arc::ptr_eq(
            request.execution().fold_dependency_observer().unwrap(),
            &observer
        ));
    }
    assert_eq!(first.execution().topology().revision(), 1);
    assert_eq!(second.execution().topology().revision(), 2);
    drop(observer);
    drop(statement);
    drop(first);
    drop(second);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(clone);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn sql_dependency_context_distinct_statements_never_share_an_implicit_observer() {
    let (a, drops_a) = observer();
    let (b, drops_b) = observer();
    let first = statement().with_fold_dependency_observer(a);
    let second = statement().with_fold_dependency_observer(b);
    assert!(!Arc::ptr_eq(
        first.fold_dependency_observer().unwrap(),
        second.fold_dependency_observer().unwrap()
    ));
    drop(first);
    assert_eq!(drops_a.load(Ordering::Relaxed), 1);
    assert_eq!(drops_b.load(Ordering::Relaxed), 0);
    drop(second);
    assert_eq!(drops_b.load(Ordering::Relaxed), 1);
}

#[test]
fn sql_dependency_context_request_attachment_keeps_original_session_and_control_facts() {
    let statement = statement();
    let request = statement.for_topology(BackendTopologySnapshot::empty(3));
    let (observer, _) = observer();
    let request = request.with_fold_dependency_observer(Arc::clone(&observer));
    assert_eq!(request.session().current_catalog(), Some("fixture_catalog"));
    assert_eq!(request.session().current_database(), "fixture_database");
    assert_eq!(request.execution().role(), ClusterRole::Fe);
    assert_eq!(request.execution().deadline(), statement.deadline());
    assert!(Arc::ptr_eq(
        request
            .preparation()
            .execution()
            .fold_dependency_observer()
            .unwrap(),
        &observer
    ));
    assert!(statement.fold_dependency_observer().is_none());
}

#[test]
fn sql_dependency_context_retry_projection_borrows_the_original_observer_explicitly() {
    let (observer, _) = observer();
    let request = statement()
        .for_topology(BackendTopologySnapshot::empty(4))
        .with_fold_dependency_observer(Arc::clone(&observer));
    let retry = statement()
        .with_optional_fold_dependency_observer(
            request.execution().fold_dependency_observer().cloned(),
        )
        .for_topology(BackendTopologySnapshot::empty(5));
    assert!(Arc::ptr_eq(
        retry.execution().fold_dependency_observer().unwrap(),
        &observer
    ));
    let absent = statement()
        .with_optional_fold_dependency_observer(None)
        .for_topology(BackendTopologySnapshot::empty(6));
    assert!(absent.execution().fold_dependency_observer().is_none());
}
