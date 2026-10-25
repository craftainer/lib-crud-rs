//! Integration tier: the generic CRUD layer end to end against the
//! devcontainer stack's real Postgres (`POSTGRES_*`), through a plain-`sqlx`
//! `widgets` repository (`tests/support`).

mod support;

use craftainer_crud::controllers::crud_actions::{
    ArchivedScope, DeleteOutcome, ListOrGet, UpdateOutcome, resolve_delete, resolve_list_or_get,
    resolve_update, resolve_update_by_id,
};
use craftainer_crud::controllers::crud_query::{FieldSpec, parse_filters, parse_sort};
use craftainer_crud::crud::CrudService;
use craftainer_crud::dyn_repository;
use craftainer_crud::error::CrudError;
use craftainer_crud::repositories::filtering::{FilterClause, FilterOp, FilterValue, SortClause};
use craftainer_crud::repositories::{RepoError, Repository};
use support::{NewWidget, PgWidgets, Widget, WidgetPatch};

const SPECS: &[FieldSpec] = &[
    FieldSpec::number("quantity"),
    FieldSpec::string("name"),
    FieldSpec::datetime("created_at"),
    FieldSpec::boolean("in_stock"),
];

fn params(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

/// A service holding alice's gizmo (3), gadget (7) and Sprocket (12), and
/// bob's widget (5).
async fn seeded() -> CrudService<PgWidgets> {
    let service = CrudService::new(PgWidgets::connect().await);
    for (owner, name, quantity) in [
        ("alice", "gizmo", 3),
        ("alice", "gadget", 7),
        ("alice", "Sprocket", 12),
        ("bob", "widget_%", 5),
    ] {
        service
            .create(owner, NewWidget::new(name, quantity))
            .await
            .unwrap();
    }
    service
}

fn names(widgets: &[Widget]) -> Vec<&str> {
    widgets.iter().map(|w| w.name.as_str()).collect()
}

fn filters(pairs: &[(&str, &str)]) -> Vec<FilterClause> {
    parse_filters(SPECS, &params(pairs)).unwrap()
}

#[tokio::test]
async fn created_records_can_be_listed_paged_and_fetched() {
    let service = seeded().await;

    let all = service
        .list(0, 100, false, false, vec![], vec![])
        .await
        .unwrap();
    assert_eq!(names(&all), ["gizmo", "gadget", "Sprocket", "widget_%"]);

    let page = service
        .list(1, 2, false, false, vec![], vec![])
        .await
        .unwrap();
    assert_eq!(names(&page), ["gadget", "Sprocket"]);

    let one = service.get(all[0].id, false).await.unwrap().unwrap();
    assert_eq!(one.owner_id, "alice");
    assert!(service.get(99_999, false).await.unwrap().is_none());
}

#[tokio::test]
async fn filters_parsed_from_a_query_string_reach_the_database() {
    let service = seeded().await;
    let found = |pairs: &[(&str, &str)]| {
        let service = &service;
        let filters = filters(pairs);
        async move {
            let widgets = service
                .list(0, 100, false, false, filters, vec![])
                .await
                .unwrap();
            names(&widgets)
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        }
    };

    assert_eq!(found(&[("name", "gizmo")]).await, ["gizmo"]);
    assert_eq!(
        found(&[("quantity__min", "7")]).await,
        ["gadget", "Sprocket"]
    );
    assert_eq!(
        found(&[("quantity__max", "5")]).await,
        ["gizmo", "widget_%"]
    );
    assert_eq!(
        found(&[("quantity__in", "3,12")]).await,
        ["gizmo", "Sprocket"]
    );
    assert_eq!(
        found(&[("name__contains", "g")]).await,
        ["gizmo", "gadget", "widget_%"]
    );
    assert_eq!(found(&[("name__icontains", "SPROCK")]).await, ["Sprocket"]);
    // `%` and `_` in a pattern are literals, not wildcards.
    assert_eq!(found(&[("name__contains", "t_%")]).await, ["widget_%"]);
    assert_eq!(found(&[("name__contains", "g_z")]).await.len(), 0);
    assert_eq!(
        found(&[("created_at__min", "2000-01-01T00:00:00")])
            .await
            .len(),
        4
    );
}

#[tokio::test]
async fn every_filter_operator_is_interpreted() {
    let service = seeded().await;
    let clause = |op, value| FilterClause {
        field: "quantity".to_string(),
        op,
        value: FilterValue::Int(value),
    };
    for (op, expected) in [
        (FilterOp::Eq, 1),
        (FilterOp::Ne, 3),
        (FilterOp::Lt, 2),
        (FilterOp::Lte, 3),
        (FilterOp::Gt, 1),
        (FilterOp::Gte, 2),
    ] {
        let count = service.count(&[clause(op, 7)], false, false).await.unwrap();
        assert_eq!(count, expected, "{op:?}");
    }
    let by_bool = FilterClause {
        field: "id".to_string(),
        op: FilterOp::In,
        value: FilterValue::List(vec![FilterValue::Int(-1)]),
    };
    assert_eq!(service.count(&[by_bool], false, false).await.unwrap(), 0);
}

#[tokio::test]
async fn sorting_orders_the_page() {
    let service = seeded().await;
    let sort = parse_sort(SPECS, &params(&[("sort", "-quantity")])).unwrap();
    let widgets = service
        .list(0, 100, false, false, vec![], sort)
        .await
        .unwrap();
    assert_eq!(names(&widgets), ["Sprocket", "gadget", "widget_%", "gizmo"]);

    let sort = parse_sort(SPECS, &params(&[("sort", "name,quantity")])).unwrap();
    let widgets = service
        .list(0, 100, false, false, vec![], sort)
        .await
        .unwrap();
    assert_eq!(names(&widgets), ["Sprocket", "gadget", "gizmo", "widget_%"]);
}

#[tokio::test]
async fn an_unknown_column_is_a_backend_error_mapped_to_internal() {
    let service = seeded().await;
    let bad = FilterClause {
        field: "nope".to_string(),
        op: FilterOp::Eq,
        value: FilterValue::Int(1),
    };
    let err = service
        .list(0, 10, false, false, vec![bad.clone()], vec![])
        .await
        .unwrap_err();
    assert!(matches!(err, RepoError::Backend(_)));
    assert!(matches!(CrudError::from(err), CrudError::Internal(_)));

    let bad_sort = SortClause {
        field: "nope".to_string(),
        descending: false,
    };
    assert!(
        service
            .list(0, 10, false, false, vec![], vec![bad_sort])
            .await
            .is_err()
    );
    // A nested list is not a valid IN member.
    let nested = FilterClause {
        field: "id".to_string(),
        op: FilterOp::In,
        value: FilterValue::List(vec![FilterValue::List(vec![])]),
    };
    assert!(service.count(&[nested], false, false).await.is_err());
}

#[tokio::test]
async fn resolve_list_or_get_returns_one_record_or_a_page() {
    let service = seeded().await;

    let ListOrGet::Many(page) =
        resolve_list_or_get(&service, None, 0, 2, ArchivedScope::Live, vec![], vec![])
            .await
            .unwrap()
    else {
        panic!("expected a page");
    };
    assert_eq!(page.len(), 2);

    let ListOrGet::One(one) = resolve_list_or_get(
        &service,
        Some(page[0].id),
        0,
        10,
        ArchivedScope::Live,
        vec![],
        vec![],
    )
    .await
    .unwrap() else {
        panic!("expected one record");
    };
    assert_eq!(one.id, page[0].id);

    let err = resolve_list_or_get(
        &service,
        Some(99_999),
        0,
        10,
        ArchivedScope::Live,
        vec![],
        vec![],
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(err, CrudError::NotFound(_)));
}

#[tokio::test]
async fn updates_are_scoped_to_the_owner() {
    let service = seeded().await;
    let all = service
        .list(0, 100, false, false, vec![], vec![])
        .await
        .unwrap();
    let alices = all[0].id;
    let patch = || WidgetPatch {
        name: Some("renamed".to_string()),
        quantity: None,
    };

    let UpdateOutcome::One(updated) =
        resolve_update(&service, Some(alices), "alice", vec![], patch(), 10)
            .await
            .unwrap()
    else {
        panic!("expected one record");
    };
    assert_eq!((updated.name.as_str(), updated.quantity), ("renamed", 3));

    // Bob cannot touch alice's record.
    let err = resolve_update_by_id(&service, alices, "bob", patch())
        .await
        .unwrap_err();
    assert!(matches!(err, CrudError::NotFound(_)));
}

#[tokio::test]
async fn bulk_update_and_delete_are_capped_scoped_and_need_a_target() {
    let service = seeded().await;
    let patch = || WidgetPatch {
        name: None,
        quantity: Some(100),
    };

    // No id and no filter: refused.
    let err = resolve_update(&service, None, "alice", vec![], patch(), 10)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, CrudError::UnprocessableEntity(_)));
    let err = resolve_delete(&service, None, "alice", vec![], 10)
        .await
        .unwrap_err();
    assert!(matches!(err, CrudError::UnprocessableEntity(_)));

    // Over the cap: refused.
    let wide = filters(&[("quantity__min", "0")]);
    let err = resolve_update(&service, None, "alice", wide.clone(), patch(), 2)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, CrudError::UnprocessableEntity(_)));
    let err = resolve_delete(&service, None, "alice", wide, 2)
        .await
        .unwrap_err();
    assert!(matches!(err, CrudError::UnprocessableEntity(_)));

    // Within the cap: only alice's matching records change, never bob's.
    let UpdateOutcome::Bulk(result) = resolve_update(
        &service,
        None,
        "alice",
        filters(&[("quantity__min", "0")]),
        patch(),
        10,
    )
    .await
    .unwrap() else {
        panic!("expected a bulk result");
    };
    assert_eq!(result.matched, 3);
    assert_eq!(result.ids.len(), 3);
    let bobs = filters(&[("name", "widget_%")]);
    let bob = service
        .list(0, 10, false, false, bobs, vec![])
        .await
        .unwrap();
    assert_eq!(bob[0].quantity, 5);

    let DeleteOutcome::Bulk(result) =
        resolve_delete(&service, None, "alice", filters(&[("quantity", "100")]), 10)
            .await
            .unwrap()
    else {
        panic!("expected a bulk result");
    };
    assert_eq!(result.matched, 3);
    assert_eq!(service.count(&[], false, false).await.unwrap(), 1);
}

#[tokio::test]
async fn a_float_filter_compares_against_the_stored_column() {
    let service = seeded().await;
    let float_specs = [FieldSpec::float("quantity"), FieldSpec::string("name")];
    let clauses = parse_filters(&float_specs, &params(&[("quantity__min", "6.5")])).unwrap();
    let found = service
        .list(0, 100, false, false, clauses, vec![])
        .await
        .unwrap();
    assert_eq!(names(&found), ["gadget", "Sprocket"]);
}

#[tokio::test]
async fn an_id_together_with_filters_is_refused_before_anything_runs() {
    let service = seeded().await;
    let all = service
        .list(0, 100, false, false, vec![], vec![])
        .await
        .unwrap();
    let id = all[0].id;
    // `DELETE ?id=..&quantity__min=1000` must not delete the record just
    // because the guard the caller wrote was dropped.
    let guard = filters(&[("quantity__min", "1000")]);
    let patch = || WidgetPatch {
        name: Some("renamed".to_string()),
        quantity: None,
    };

    let err = resolve_list_or_get(
        &service,
        Some(id),
        0,
        10,
        ArchivedScope::Live,
        guard.clone(),
        vec![],
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(err, CrudError::UnprocessableEntity(_)));
    let err = resolve_update(&service, Some(id), "alice", guard.clone(), patch(), 10)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, CrudError::UnprocessableEntity(_)));
    let err = resolve_delete(&service, Some(id), "alice", guard, 10)
        .await
        .unwrap_err();
    assert!(matches!(err, CrudError::UnprocessableEntity(_)));

    let untouched = service.get(id, false).await.unwrap().unwrap();
    assert_eq!(untouched.name, all[0].name);
}

#[tokio::test]
async fn deleting_soft_deletes_and_hides_the_record() {
    let service = seeded().await;
    let all = service
        .list(0, 100, false, false, vec![], vec![])
        .await
        .unwrap();
    let id = all[0].id;

    // Bob cannot delete alice's record.
    let err = resolve_delete(&service, Some(id), "bob", vec![], 10)
        .await
        .unwrap_err();
    assert!(matches!(err, CrudError::NotFound(_)));

    assert!(matches!(
        resolve_delete(&service, Some(id), "alice", vec![], 10)
            .await
            .unwrap(),
        DeleteOutcome::One
    ));
    // Already archived: a second delete finds nothing.
    assert!(matches!(
        resolve_delete(&service, Some(id), "alice", vec![], 10)
            .await
            .unwrap_err(),
        CrudError::NotFound(_)
    ));

    assert!(service.get(id, false).await.unwrap().is_none());
    let archived = service.get(id, true).await.unwrap().unwrap();
    assert!(archived.archived_at.is_some());
    assert_eq!(service.count(&[], false, false).await.unwrap(), 3);
    assert_eq!(service.count(&[], true, false).await.unwrap(), 4);
    let listed = service
        .list(0, 100, true, false, vec![], vec![])
        .await
        .unwrap();
    assert_eq!(listed.len(), 4);
}

#[tokio::test]
async fn only_archived_lists_and_counts_just_the_archived_rows() {
    let service = seeded().await;
    let all = service
        .list(0, 100, false, false, vec![], vec![])
        .await
        .unwrap();
    let id = all[0].id;
    let doomed = all[1].id;
    for victim in [id, doomed] {
        resolve_delete(&service, Some(victim), "alice", vec![], 10)
            .await
            .unwrap();
    }

    let archived = service
        .list(0, 100, false, true, vec![], vec![])
        .await
        .unwrap();
    assert_eq!(archived.len(), 2);
    assert!(archived.iter().all(|w| w.archived_at.is_some()));
    // It implies include_archived: the same set either way.
    let implied = service
        .list(0, 100, true, true, vec![], vec![])
        .await
        .unwrap();
    assert_eq!(implied.len(), 2);
    assert_eq!(service.count(&[], false, true).await.unwrap(), 2);
    assert_eq!(service.count(&[], true, true).await.unwrap(), 2);
    // false changes nothing.
    assert_eq!(service.count(&[], false, false).await.unwrap(), 2);
    assert_eq!(service.count(&[], true, false).await.unwrap(), 4);

    // Combines with filters, sort, skip and limit.
    let by_id = |id: i32| FilterClause {
        field: "id".to_string(),
        op: FilterOp::Eq,
        value: FilterValue::Int(i64::from(id)),
    };
    let one = service
        .list(0, 100, false, true, vec![by_id(id)], vec![])
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].id, id);
    let live = service
        .list(0, 100, false, true, vec![by_id(all[2].id)], vec![])
        .await
        .unwrap();
    assert_eq!(live.len(), 0);
    let sort = vec![SortClause {
        field: "id".to_string(),
        descending: true,
    }];
    let paged = service.list(1, 1, false, true, vec![], sort).await.unwrap();
    assert_eq!(paged.len(), 1);
    assert_eq!(paged[0].id, id.min(doomed));
}

#[tokio::test]
async fn resolve_list_or_get_only_archived_lists_archived_and_refuses_an_id() {
    let service = seeded().await;
    let all = service
        .list(0, 100, false, false, vec![], vec![])
        .await
        .unwrap();
    let id = all[0].id;
    resolve_delete(&service, Some(id), "alice", vec![], 10)
        .await
        .unwrap();

    let ListOrGet::Many(trash) =
        resolve_list_or_get(&service, None, 0, 10, ArchivedScope::Only, vec![], vec![])
            .await
            .unwrap()
    else {
        panic!("expected a list");
    };
    assert_eq!(trash.len(), 1);
    assert_eq!(trash[0].id, id);

    let err = resolve_list_or_get(
        &service,
        Some(id),
        0,
        10,
        ArchivedScope::Only,
        vec![],
        vec![],
    )
    .await
    .err()
    .unwrap();
    let CrudError::UnprocessableEntity(errors) = err else {
        panic!("expected 422");
    };
    assert_eq!(errors[0].field, "only_archived");
    assert_eq!(errors[0].msg, "applies to list reads only");
}

dyn_repository!(
    DynWidgets,
    model = Widget,
    create = NewWidget,
    update = WidgetPatch
);

#[tokio::test]
async fn dyn_repository_forwards_every_method() {
    let repo = DynWidgets(Box::new(PgWidgets::connect().await));
    let created = repo.create("alice", NewWidget::new("a", 1)).await.unwrap();
    repo.create("alice", NewWidget::new("b", 2)).await.unwrap();
    let by_name = |name: &str| FilterClause {
        field: "name".to_string(),
        op: FilterOp::Eq,
        value: FilterValue::Str(name.to_string()),
    };

    assert_eq!(repo.count(&[], false, false).await.unwrap(), 2);
    assert_eq!(
        repo.get(created.id, false).await.unwrap().unwrap().name,
        "a"
    );
    let listed = repo
        .list(craftainer_crud::repositories::ListOptions {
            skip: 0,
            limit: 10,
            include_archived: false,
            only_archived: false,
            filters: vec![by_name("b")],
            sort: vec![],
        })
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);

    let patch = WidgetPatch {
        name: Some("a2".to_string()),
        quantity: None,
    };
    let updated = repo
        .update(created.id, "alice", patch.clone())
        .await
        .unwrap();
    assert_eq!(updated.unwrap().name, "a2");
    let many = repo.update_many(&[by_name("b")], patch).await.unwrap();
    assert_eq!(many.len(), 1);

    assert!(repo.delete(created.id, "alice").await.unwrap());
    let deleted = repo.delete_many(&[by_name("a2")]).await.unwrap();
    assert_eq!(deleted.len(), 1);
}

fn assert_internal<T>(result: Result<T, CrudError>) {
    assert!(matches!(result.err(), Some(CrudError::Internal(_))));
}

#[tokio::test]
async fn a_lost_database_surfaces_as_internal_from_every_resolver() {
    let crud = CrudService::new(PgWidgets::connect().await.closed().await);
    let target = || filters(&[("quantity__min", "0")]);

    assert_internal(
        resolve_list_or_get(&crud, Some(1), 0, 10, ArchivedScope::Live, vec![], vec![]).await,
    );
    assert_internal(
        resolve_list_or_get(&crud, None, 0, 10, ArchivedScope::Live, vec![], vec![]).await,
    );
    assert_internal(resolve_update(&crud, Some(1), "o", vec![], WidgetPatch::default(), 10).await);
    assert_internal(resolve_update(&crud, None, "o", target(), WidgetPatch::default(), 10).await);
    assert_internal(resolve_update_by_id(&crud, 1, "o", WidgetPatch::default()).await);
    assert_internal(resolve_delete(&crud, Some(1), "o", vec![], 10).await);
    assert_internal(resolve_delete(&crud, None, "o", target(), 10).await);
}

#[tokio::test]
async fn a_bulk_write_failing_after_the_size_check_is_internal_too() {
    let crud = CrudService::new(PgWidgets::connect().await.failing_bulk_writes());
    crud.create("o", NewWidget::new("a", 1)).await.unwrap();
    let target = || filters(&[("quantity__min", "0")]);

    assert_internal(resolve_update(&crud, None, "o", target(), WidgetPatch::default(), 10).await);
    assert_internal(resolve_delete(&crud, None, "o", target(), 10).await);
}
