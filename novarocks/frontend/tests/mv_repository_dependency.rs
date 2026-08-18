mod mv_repository_definition;

use novarocks_frontend::mv::domain::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks_frontend::mv::domain::persistence::dependency::CreateMvDependencyRequest;
use novarocks_frontend::mv::domain::repository::MvRepository;

#[test]
fn dependency_replace_delete_guard_and_ordering_are_provider_backed() {
    let (_temp, _runtime, _host, repository) = mv_repository_definition::repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            mv_repository_definition::create_request("dependency"),
        )
        .expect("create definition");
    let upstream = MvDependencyObjectRef {
        catalog: Some("ice".to_string()),
        database_or_namespace: "sales".to_string(),
        name: "customers".to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    };
    repository
        .replace_dependencies_for_mv(
            definition.mv_id,
            vec![CreateMvDependencyRequest {
                upstream: upstream.clone(),
                created_at_ms: 2,
            }],
        )
        .expect("replace dependencies");
    assert_eq!(
        repository
            .list_downstream_dependencies(&upstream)
            .expect("upstream index"),
        repository
            .list_dependencies_by_downstream(definition.mv_id)
            .expect("downstream index")
    );
    assert!(
        repository
            .ensure_no_downstream_dependencies(&upstream)
            .is_err()
    );
    repository
        .delete_dependencies_for_mv(definition.mv_id)
        .expect("delete dependencies");
    repository
        .ensure_no_downstream_dependencies(&upstream)
        .expect("guard clears after delete");
}
