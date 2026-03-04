use std::time::Duration;

use quebec::context::{ConcurrencyConstraint, TableConfig};
use quebec::entities::*;
use quebec::query_builder;
use quebec::schema_builder;
use quebec::semaphore::{acquire_semaphore, acquire_semaphore_with_constraint};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

async fn setup_db() -> (sea_orm::DatabaseConnection, TableConfig) {
    let db = sea_orm::Database::connect("sqlite::memory:")
        .await
        .expect("Failed to connect to database");
    let tc = TableConfig::default();
    schema_builder::setup_database(&db, &tc)
        .await
        .expect("Failed to setup database");
    (db, tc)
}

mod schema {
    use super::*;

    #[tokio::test]
    async fn test_setup_database() {
        let (db, tc) = setup_db().await;

        // Verify tables exist by running counts (would fail if tables missing)
        assert_eq!(query_builder::jobs::count_all(&db, &tc).await.unwrap(), 0);
        assert_eq!(
            query_builder::claimed_executions::count_all(&db, &tc, None, None)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_builder::blocked_executions::count_all(&db, &tc)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_builder::failed_executions::count_all(&db, &tc, None, None)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_builder::scheduled_executions::count_all(&db, &tc)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_builder::processes::count_all(&db, &tc).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_idempotent() {
        let (db, tc) = setup_db().await;

        // Second call should not fail (IF NOT EXISTS)
        schema_builder::setup_database(&db, &tc)
            .await
            .expect("setup_database should be idempotent");
    }

    #[tokio::test]
    async fn test_check_tables_exist() {
        let tc = TableConfig::default();

        // Empty database — tables don't exist yet
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        assert!(!schema_builder::check_tables_exist(&db, &tc).await.unwrap());

        // After setup — tables exist
        schema_builder::setup_database(&db, &tc).await.unwrap();
        assert!(schema_builder::check_tables_exist(&db, &tc).await.unwrap());
    }
}

mod jobs {
    use super::*;

    #[tokio::test]
    async fn test_insert_and_find() {
        let (db, tc) = setup_db().await;

        let job_id = query_builder::jobs::insert(
            &db,
            &tc,
            "default",
            "TestJob",
            Some(r#"{"args":["a","b"]}"#),
            0,
            Some("job-001"),
            None,
            None,
        )
        .await
        .unwrap();
        assert!(job_id > 0);

        let job = query_builder::jobs::find_by_id(&db, &tc, job_id)
            .await
            .unwrap()
            .expect("Job not found");
        assert_eq!(job.class_name, "TestJob");
        assert_eq!(job.queue_name, "default");
        assert_eq!(job.priority, 0);
        assert!(job.finished_at.is_none());
    }

    #[tokio::test]
    async fn test_insert_returning() {
        let (db, tc) = setup_db().await;

        let job = query_builder::jobs::insert_returning(
            &db,
            &tc,
            "emails",
            "SendEmail",
            Some(r#"{"to":"user@test.com"}"#),
            10,
            Some("job-ret-001"),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(job.class_name, "SendEmail");
        assert_eq!(job.queue_name, "emails");
        assert_eq!(job.priority, 10);
    }

    #[tokio::test]
    async fn test_mark_finished() {
        let (db, tc) = setup_db().await;

        let job_id = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        let rows = query_builder::jobs::mark_finished(&db, &tc, job_id)
            .await
            .unwrap();
        assert_eq!(rows, 1);

        let job = query_builder::jobs::find_by_id(&db, &tc, job_id)
            .await
            .unwrap()
            .unwrap();
        assert!(job.finished_at.is_some());

        assert_eq!(
            query_builder::jobs::count_finished(&db, &tc).await.unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_mark_finished_by_ids() {
        let (db, tc) = setup_db().await;

        let id1 = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();
        let id2 = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();
        let _id3 = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        let rows = query_builder::jobs::mark_finished_by_ids(&db, &tc, vec![id1, id2])
            .await
            .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(
            query_builder::jobs::count_finished(&db, &tc).await.unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn test_update_arguments() {
        let (db, tc) = setup_db().await;

        let job_id = query_builder::jobs::insert(
            &db,
            &tc,
            "q",
            "J",
            Some(r#"{"arguments":[],"executions":0,"exception_executions":{}}"#),
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let new_args =
            r#"{"arguments":[],"executions":2,"exception_executions":{"[ValueError]":2}}"#;
        query_builder::jobs::update_arguments(&db, &tc, job_id, new_args)
            .await
            .unwrap();

        let job = query_builder::jobs::find_by_id(&db, &tc, job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.arguments.as_deref(), Some(new_args));
    }

    #[tokio::test]
    async fn test_find_by_ids() {
        let (db, tc) = setup_db().await;

        let id1 = query_builder::jobs::insert(&db, &tc, "q", "A", None, 0, None, None, None)
            .await
            .unwrap();
        let id2 = query_builder::jobs::insert(&db, &tc, "q", "B", None, 0, None, None, None)
            .await
            .unwrap();
        let _id3 = query_builder::jobs::insert(&db, &tc, "q", "C", None, 0, None, None, None)
            .await
            .unwrap();

        let jobs = query_builder::jobs::find_by_ids(&db, &tc, vec![id1, id2])
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
    }

    #[tokio::test]
    async fn test_count_and_queue_names() {
        let (db, tc) = setup_db().await;

        query_builder::jobs::insert(&db, &tc, "emails", "A", None, 0, None, None, None)
            .await
            .unwrap();
        query_builder::jobs::insert(&db, &tc, "default", "B", None, 0, None, None, None)
            .await
            .unwrap();
        query_builder::jobs::insert(&db, &tc, "emails", "C", None, 0, None, None, None)
            .await
            .unwrap();

        assert_eq!(query_builder::jobs::count_all(&db, &tc).await.unwrap(), 3);
        assert_eq!(
            query_builder::jobs::count_by_queue(&db, &tc, "emails")
                .await
                .unwrap(),
            2
        );

        let mut queues = query_builder::jobs::get_queue_names(&db, &tc)
            .await
            .unwrap();
        queues.sort();
        assert_eq!(queues, vec!["default", "emails"]);

        let mut classes = query_builder::jobs::get_class_names(&db, &tc)
            .await
            .unwrap();
        classes.sort();
        assert_eq!(classes, vec!["A", "B", "C"]);
    }

    #[tokio::test]
    async fn test_delete_by_id() {
        let (db, tc) = setup_db().await;

        let job_id = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        let rows = query_builder::jobs::delete_by_id(&db, &tc, job_id)
            .await
            .unwrap();
        assert_eq!(rows, 1);

        let found = query_builder::jobs::find_by_id(&db, &tc, job_id)
            .await
            .unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_pagination() {
        let (db, tc) = setup_db().await;

        for i in 0..5 {
            query_builder::jobs::insert(
                &db,
                &tc,
                "q",
                &format!("Job{}", i),
                None,
                0,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }

        let page = query_builder::jobs::find_by_queue_paginated(&db, &tc, "q", 0, 3)
            .await
            .unwrap();
        assert_eq!(page.len(), 3);

        let page2 = query_builder::jobs::find_by_queue_paginated(&db, &tc, "q", 3, 3)
            .await
            .unwrap();
        assert_eq!(page2.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_finished_before() {
        let (db, tc) = setup_db().await;

        let id1 = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();
        let id2 = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        query_builder::jobs::mark_finished(&db, &tc, id1)
            .await
            .unwrap();
        query_builder::jobs::mark_finished(&db, &tc, id2)
            .await
            .unwrap();

        let future = chrono::Utc::now().naive_utc() + chrono::Duration::hours(1);
        let deleted = query_builder::jobs::delete_finished_before(&db, &tc, future, 100)
            .await
            .unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(query_builder::jobs::count_all(&db, &tc).await.unwrap(), 0);
    }
}

mod ready_executions {
    use super::*;

    #[tokio::test]
    async fn test_crud() {
        let (db, tc) = setup_db().await;

        let job_id =
            query_builder::jobs::insert(&db, &tc, "default", "J", None, 0, None, None, None)
                .await
                .unwrap();

        let re_id = query_builder::ready_executions::insert(&db, &tc, job_id, "default", 0)
            .await
            .unwrap();
        assert!(re_id > 0);

        let found = query_builder::ready_executions::find_by_queue(&db, &tc, "default", 10)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].job_id, job_id);

        let rows = query_builder::ready_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        assert_eq!(rows, 1);

        let empty = query_builder::ready_executions::find_by_queue(&db, &tc, "default", 10)
            .await
            .unwrap();
        assert!(empty.is_empty());
    }
}

mod claimed_executions {
    use super::*;

    #[tokio::test]
    async fn test_crud() {
        let (db, tc) = setup_db().await;

        let job_id = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        let ce_id = query_builder::claimed_executions::insert(&db, &tc, job_id, Some(12345))
            .await
            .unwrap();
        assert!(ce_id > 0);

        let found = query_builder::claimed_executions::find_by_job_id(&db, &tc, job_id)
            .await
            .unwrap()
            .expect("Claimed execution not found");
        assert_eq!(found.process_id, Some(12345));

        assert_eq!(
            query_builder::claimed_executions::count_all(&db, &tc, None, None)
                .await
                .unwrap(),
            1
        );

        let by_pid = query_builder::claimed_executions::find_by_process_id(&db, &tc, 12345)
            .await
            .unwrap();
        assert_eq!(by_pid.len(), 1);

        query_builder::claimed_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        assert_eq!(
            query_builder::claimed_executions::count_all(&db, &tc, None, None)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_delete_by_process_id() {
        let (db, tc) = setup_db().await;

        let j1 = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();
        let j2 = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        query_builder::claimed_executions::insert(&db, &tc, j1, Some(100))
            .await
            .unwrap();
        query_builder::claimed_executions::insert(&db, &tc, j2, Some(100))
            .await
            .unwrap();

        let deleted = query_builder::claimed_executions::delete_by_process_id(&db, &tc, 100)
            .await
            .unwrap();
        assert_eq!(deleted, 2);
    }
}

mod failed_executions {
    use super::*;

    #[tokio::test]
    async fn test_crud() {
        let (db, tc) = setup_db().await;

        let job_id = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        let fe_id =
            query_builder::failed_executions::insert(&db, &tc, job_id, Some("something broke"))
                .await
                .unwrap();
        assert!(fe_id > 0);

        let found = query_builder::failed_executions::find_by_job_id(&db, &tc, job_id)
            .await
            .unwrap()
            .expect("Failed execution not found");
        assert_eq!(found.error.as_deref(), Some("something broke"));

        assert_eq!(
            query_builder::failed_executions::count_all(&db, &tc, None, None)
                .await
                .unwrap(),
            1
        );

        query_builder::failed_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        assert_eq!(
            query_builder::failed_executions::count_all(&db, &tc, None, None)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_delete_by_job_ids() {
        let (db, tc) = setup_db().await;

        let j1 = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();
        let j2 = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        query_builder::failed_executions::insert(&db, &tc, j1, Some("err1"))
            .await
            .unwrap();
        query_builder::failed_executions::insert(&db, &tc, j2, Some("err2"))
            .await
            .unwrap();

        let deleted = query_builder::failed_executions::delete_by_job_ids(&db, &tc, vec![j1, j2])
            .await
            .unwrap();
        assert_eq!(deleted, 2);
    }
}

mod blocked_executions {
    use super::*;

    #[tokio::test]
    async fn test_crud() {
        let (db, tc) = setup_db().await;

        let job_id =
            query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, Some("my_key"))
                .await
                .unwrap();

        let expires = chrono::Utc::now().naive_utc() + chrono::Duration::minutes(5);
        let be_id =
            query_builder::blocked_executions::insert(&db, &tc, job_id, "q", 0, "my_key", expires)
                .await
                .unwrap();
        assert!(be_id > 0);

        let found =
            query_builder::blocked_executions::find_by_concurrency_key(&db, &tc, "my_key", 10)
                .await
                .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].job_id, job_id);

        assert_eq!(
            query_builder::blocked_executions::count_all(&db, &tc)
                .await
                .unwrap(),
            1
        );

        query_builder::blocked_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        assert_eq!(
            query_builder::blocked_executions::count_all(&db, &tc)
                .await
                .unwrap(),
            0
        );
    }
}

mod scheduled_executions {
    use super::*;

    #[tokio::test]
    async fn test_crud() {
        let (db, tc) = setup_db().await;

        let job_id = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        let scheduled_at = chrono::Utc::now().naive_utc() - chrono::Duration::seconds(10);
        let se_id =
            query_builder::scheduled_executions::insert(&db, &tc, job_id, "q", 0, scheduled_at)
                .await
                .unwrap();
        assert!(se_id > 0);

        // find_due should return jobs scheduled in the past
        let due = query_builder::scheduled_executions::find_due(&db, &tc, 10, false)
            .await
            .unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].job_id, job_id);

        assert_eq!(
            query_builder::scheduled_executions::count_all(&db, &tc)
                .await
                .unwrap(),
            1
        );

        query_builder::scheduled_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        assert_eq!(
            query_builder::scheduled_executions::count_all(&db, &tc)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_not_due_yet() {
        let (db, tc) = setup_db().await;

        let job_id = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();

        let future = chrono::Utc::now().naive_utc() + chrono::Duration::hours(1);
        query_builder::scheduled_executions::insert(&db, &tc, job_id, "q", 0, future)
            .await
            .unwrap();

        let due = query_builder::scheduled_executions::find_due(&db, &tc, 10, false)
            .await
            .unwrap();
        assert!(due.is_empty());
    }
}

mod processes {
    use super::*;

    #[tokio::test]
    async fn test_crud() {
        let (db, tc) = setup_db().await;

        let pid = query_builder::processes::insert(
            &db,
            &tc,
            "Worker",
            std::process::id() as i32,
            Some("localhost"),
            None,
            "worker-1",
        )
        .await
        .unwrap();
        assert!(pid > 0);

        let proc = query_builder::processes::find_by_id(&db, &tc, pid)
            .await
            .unwrap()
            .expect("Process not found");
        assert_eq!(proc.kind, "Worker");
        assert_eq!(proc.name, "worker-1");

        let rows = query_builder::processes::update_heartbeat(&db, &tc, pid)
            .await
            .unwrap();
        assert_eq!(rows, 1);

        assert_eq!(
            query_builder::processes::count_all(&db, &tc).await.unwrap(),
            1
        );

        let workers = query_builder::processes::find_by_kind(&db, &tc, "Worker")
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);

        let dispatchers = query_builder::processes::find_by_kind(&db, &tc, "Dispatcher")
            .await
            .unwrap();
        assert!(dispatchers.is_empty());

        query_builder::processes::delete_by_id(&db, &tc, pid)
            .await
            .unwrap();
        assert_eq!(
            query_builder::processes::count_all(&db, &tc).await.unwrap(),
            0
        );
    }
}

mod pauses {
    use super::*;

    #[tokio::test]
    async fn test_crud() {
        let (db, tc) = setup_db().await;

        let now = chrono::Utc::now().naive_utc();
        query_builder::pauses::insert(&db, &tc, "emails", now)
            .await
            .unwrap();

        let found = query_builder::pauses::find_by_queue_name(&db, &tc, "emails")
            .await
            .unwrap();
        assert!(found.is_some());

        let all_names = query_builder::pauses::find_all_queue_names(&db, &tc)
            .await
            .unwrap();
        assert_eq!(all_names, vec!["emails"]);

        query_builder::pauses::delete_by_queue_name(&db, &tc, "emails")
            .await
            .unwrap();
        let found = query_builder::pauses::find_by_queue_name(&db, &tc, "emails")
            .await
            .unwrap();
        assert!(found.is_none());
    }
}

mod recurring_executions {
    use super::*;

    #[tokio::test]
    async fn test_try_insert_idempotent() {
        let (db, tc) = setup_db().await;

        let job_id = query_builder::jobs::insert(&db, &tc, "q", "J", None, 0, None, None, None)
            .await
            .unwrap();
        let run_at = chrono::Utc::now().naive_utc();

        let inserted = query_builder::recurring_executions::try_insert(
            &db,
            &tc,
            job_id,
            "daily_report",
            run_at,
        )
        .await
        .unwrap();
        assert!(inserted);

        // Duplicate should be rejected (idempotent)
        let duplicate = query_builder::recurring_executions::try_insert(
            &db,
            &tc,
            job_id,
            "daily_report",
            run_at,
        )
        .await
        .unwrap();
        assert!(!duplicate);
    }
}

mod semaphore {
    use super::*;

    #[tokio::test]
    async fn test_concurrency_control() {
        let (db, tc) = setup_db().await;

        let key = "test_concurrency_key";
        let limit = 2;

        assert!(acquire_semaphore(&db, &tc, key.to_string(), limit, None)
            .await
            .unwrap());
        assert!(acquire_semaphore(&db, &tc, key.to_string(), limit, None)
            .await
            .unwrap());

        // Third acquire should fail (limit reached)
        assert!(!acquire_semaphore(&db, &tc, key.to_string(), limit, None)
            .await
            .unwrap());

        // Test ConcurrencyConstraint
        let constraint = ConcurrencyConstraint {
            key: "test_constraint_key".to_string(),
            limit: 1,
            duration: Some(chrono::Duration::minutes(5)),
        };

        assert!(acquire_semaphore_with_constraint(&db, &tc, &constraint)
            .await
            .unwrap());
        assert!(!acquire_semaphore_with_constraint(&db, &tc, &constraint)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_expiration() {
        let (db, tc) = setup_db().await;

        let key = "expiring_semaphore";
        let duration = Some(chrono::Duration::seconds(1));

        assert!(acquire_semaphore(&db, &tc, key.to_string(), 1, duration)
            .await
            .unwrap());

        let sem = quebec_semaphores::Entity::find()
            .filter(quebec_semaphores::Column::Key.eq(key))
            .one(&db)
            .await
            .unwrap()
            .expect("Semaphore not found");
        assert_eq!(sem.key, key);
        assert_eq!(sem.value, 0);
    }

    #[tokio::test]
    async fn test_delete_expired() {
        let (db, tc) = setup_db().await;

        let duration = Some(chrono::Duration::seconds(-1)); // already expired
        acquire_semaphore(&db, &tc, "expired_key".to_string(), 1, duration)
            .await
            .unwrap();

        let deleted = query_builder::semaphores::delete_expired(&db, &tc)
            .await
            .unwrap();
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn test_high_concurrency() {
        let (db, tc) = setup_db().await;

        let num_operations = 50;
        let start_time = std::time::Instant::now();

        let mut results = Vec::new();
        for i in 0..num_operations {
            let key = format!("concurrent_key_{}", i % 10);
            let result = acquire_semaphore(&db, &tc, key, 2, None).await;
            results.push(result);
        }

        for result in &results {
            assert!(result.is_ok());
        }

        assert!(start_time.elapsed() < Duration::from_secs(5));
    }
}

mod lifecycle {
    use super::*;

    /// Full job lifecycle: insert -> ready -> claimed -> failed -> retry -> finished
    #[tokio::test]
    async fn test_job_lifecycle() {
        let (db, tc) = setup_db().await;

        // 1. Create job
        let job_id = query_builder::jobs::insert(
            &db,
            &tc,
            "default",
            "ProcessOrder",
            Some(r#"{"order_id":42}"#),
            0,
            Some("job-lifecycle-001"),
            None,
            None,
        )
        .await
        .unwrap();

        // 2. Add to ready queue
        query_builder::ready_executions::insert(&db, &tc, job_id, "default", 0)
            .await
            .unwrap();
        assert_eq!(
            query_builder::ready_executions::find_by_queue(&db, &tc, "default", 10)
                .await
                .unwrap()
                .len(),
            1
        );

        // 3. Claim the job (worker picks it up)
        query_builder::ready_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        query_builder::claimed_executions::insert(&db, &tc, job_id, Some(9999))
            .await
            .unwrap();
        assert_eq!(
            query_builder::claimed_executions::count_all(&db, &tc, None, None)
                .await
                .unwrap(),
            1
        );

        // 4. Job fails - update arguments to track execution count
        query_builder::claimed_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        let new_args =
            quebec::utils::increment_executions(Some(r#"{"order_id":42}"#), Some("TimeoutError"));
        query_builder::jobs::update_arguments(&db, &tc, job_id, &new_args)
            .await
            .unwrap();
        query_builder::failed_executions::insert(&db, &tc, job_id, Some("timeout"))
            .await
            .unwrap();

        // 5. Retry: move from failed back to ready
        query_builder::failed_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        query_builder::ready_executions::insert(&db, &tc, job_id, "default", 0)
            .await
            .unwrap();

        // 6. Claim again, succeed, mark finished
        query_builder::ready_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        query_builder::claimed_executions::insert(&db, &tc, job_id, Some(9999))
            .await
            .unwrap();
        query_builder::claimed_executions::delete_by_job_id(&db, &tc, job_id)
            .await
            .unwrap();
        query_builder::jobs::mark_finished(&db, &tc, job_id)
            .await
            .unwrap();

        // Verify final state
        let job = query_builder::jobs::find_by_id(&db, &tc, job_id)
            .await
            .unwrap()
            .unwrap();
        assert!(job.finished_at.is_some());
        // executions count is stored in arguments JSON
        assert_eq!(quebec::utils::get_executions(job.arguments.as_deref()), 1);
        assert_eq!(
            query_builder::failed_executions::count_all(&db, &tc, None, None)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_builder::claimed_executions::count_all(&db, &tc, None, None)
                .await
                .unwrap(),
            0
        );
    }
}
