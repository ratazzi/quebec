use std::collections::BTreeMap;

use quebec::context::TableConfig;
use quebec::semaphore::{
    acquire_semaphore, release_semaphore, try_consume_rate_token, ConsumeResult,
};
use sea_orm::{ConnectionTrait, DbBackend, MockDatabase, MockExecResult, Statement, Value};

fn no_rows() -> Vec<BTreeMap<&'static str, Value>> {
    Vec::new()
}

fn id_row(id: i64) -> Vec<BTreeMap<&'static str, Value>> {
    vec![BTreeMap::from([("id", Value::from(id))])]
}

fn ttl() -> chrono::Duration {
    chrono::Duration::minutes(3)
}

fn executed(rows_affected: u64) -> MockExecResult {
    MockExecResult {
        last_insert_id: 0,
        rows_affected,
    }
}

#[tokio::test]
async fn mysql_semaphore_statements_quote_key() {
    let db = MockDatabase::new(DbBackend::MySql)
        .append_query_results([no_rows(), id_row(1)])
        .append_exec_results([
            MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            },
            executed(0),
            executed(1),
        ])
        .into_connection();
    let tc = TableConfig::default();
    let key = "customer's semaphore";

    // Read the missing key, create it in one UPSERT, read back the id of the
    // semaphore taken, then exercise both release statements. The increment
    // finds the semaphore already all free, so the release only renews it and
    // gives nothing back.
    assert_eq!(
        acquire_semaphore(&db, &tc, key.into(), 2, ttl())
            .await
            .unwrap(),
        Some(1)
    );
    assert!(!release_semaphore(&db, &tc, key.into(), 2, ttl())
        .await
        .unwrap());

    let log = db.into_transaction_log();
    assert_eq!(log.len(), 5);
    let key_uses = [
        "WHERE `key` =",
        "(`key`,",
        "WHERE `key` =",
        "WHERE `key` =",
        "WHERE `key` =",
    ];
    for (transaction, key_use) in log.iter().zip(key_uses) {
        for statement in transaction.statements() {
            assert!(statement.sql.contains(key_use), "{}", statement.sql);
            assert!(!statement.sql.contains(key));
            assert!(statement.values.as_ref().unwrap().0.contains(&key.into()));
        }
    }
    assert!(log[0].statements()[0].sql.starts_with("SELECT value"));
    assert!(log[1].statements()[0]
        .sql
        .contains("ON DUPLICATE KEY UPDATE"));
    assert!(log[1].statements()[0].sql.contains("value = IF("));
    assert_eq!(log[1].statements()[0].values.as_ref().unwrap().0.len(), 7);
    // A locking read, so MySQL's snapshot cannot hand back a deleted semaphore.
    assert!(log[2].statements()[0].sql.starts_with("SELECT id"));
    assert!(log[2].statements()[0].sql.ends_with("FOR UPDATE"));
}

#[tokio::test]
async fn mysql_full_semaphore_does_not_acquire_with_found_rows() {
    // The row appears between the read and the UPSERT (creation race).
    let db = MockDatabase::new(DbBackend::MySql)
        .append_query_results([no_rows()])
        .append_exec_results([MockExecResult {
            last_insert_id: 0,
            rows_affected: 1,
        }])
        .into_connection();
    assert!(
        acquire_semaphore(&db, &TableConfig::default(), "busy".into(), 1, ttl())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(db.into_transaction_log().len(), 2);
}

#[tokio::test]
async fn full_semaphore_is_rejected_under_row_lock_without_writing() {
    // A full key is read again with a row lock, which the caller then holds
    // until its blocked execution commits (Solid Queue #456).
    let db = MockDatabase::new(DbBackend::MySql)
        .append_query_results([
            vec![BTreeMap::from([("value", Value::from(0_i32))])],
            vec![BTreeMap::from([("value", Value::from(0_i32))])],
        ])
        .into_connection();
    assert!(
        acquire_semaphore(&db, &TableConfig::default(), "busy".into(), 2, ttl())
            .await
            .unwrap()
            .is_none()
    );
    let log = db.into_transaction_log();
    assert_eq!(log.len(), 2);
    assert!(log[0].statements()[0].sql.starts_with("SELECT value"));
    assert!(!log[0].statements()[0].sql.contains("FOR UPDATE"));
    assert!(log[1].statements()[0].sql.starts_with("SELECT value"));
    assert!(log[1].statements()[0].sql.ends_with("FOR UPDATE"));
}

#[tokio::test]
async fn last_slot_lost_after_read_is_decided_under_row_lock() {
    // The read saw a free slot, but another transaction took it before our
    // UPDATE, which then matched nothing. The key is read again with a row
    // lock, which the caller holds until its blocked execution commits.
    let db = MockDatabase::new(DbBackend::MySql)
        .append_query_results([
            vec![BTreeMap::from([("value", Value::from(1_i32))])],
            vec![BTreeMap::from([("value", Value::from(0_i32))])],
        ])
        .append_exec_results([executed(0)])
        .into_connection();
    assert!(
        acquire_semaphore(&db, &TableConfig::default(), "busy".into(), 2, ttl())
            .await
            .unwrap()
            .is_none()
    );
    let log = db.into_transaction_log();
    assert_eq!(log.len(), 3);
    assert!(log[1].statements()[0].sql.starts_with("UPDATE"));
    assert!(log[2].statements()[0].sql.ends_with("FOR UPDATE"));
}

#[tokio::test]
async fn slot_freed_while_waiting_on_full_semaphore_is_taken() {
    // The locking read waited for a release to commit and now sees its slot.
    let db = MockDatabase::new(DbBackend::MySql)
        .append_query_results([
            vec![BTreeMap::from([("value", Value::from(0_i32))])],
            vec![BTreeMap::from([("value", Value::from(1_i32))])],
            id_row(4),
        ])
        .append_exec_results([executed(1)])
        .into_connection();
    assert_eq!(
        acquire_semaphore(&db, &TableConfig::default(), "busy".into(), 2, ttl())
            .await
            .unwrap(),
        Some(4)
    );
    let log = db.into_transaction_log();
    assert_eq!(log.len(), 4);
    assert!(log[2].statements()[0].sql.starts_with("UPDATE"));
}

#[tokio::test]
async fn open_semaphore_decrements_without_upsert() {
    let db = MockDatabase::new(DbBackend::MySql)
        .append_query_results([
            vec![BTreeMap::from([("value", Value::from(1_i32))])],
            id_row(9),
        ])
        .append_exec_results([executed(1)])
        .into_connection();
    assert_eq!(
        acquire_semaphore(&db, &TableConfig::default(), "open".into(), 2, ttl())
            .await
            .unwrap(),
        Some(9)
    );
    let log = db.into_transaction_log();
    assert_eq!(log.len(), 3);
    let update = &log[1].statements()[0].sql;
    assert!(update.starts_with("UPDATE"), "{update}");
    assert!(update.contains("value > 0"), "{update}");
    assert!(!update.contains("ON DUPLICATE KEY"), "{update}");
}

#[tokio::test]
async fn mysql_rate_statements_quote_key() {
    let current_key = "rate:ApiJob/customer:60";
    let db = MockDatabase::new(DbBackend::MySql)
        .append_query_results([
            vec![BTreeMap::from([("now_secs", Value::from(3600_i64))])],
            vec![BTreeMap::from([
                ("key", Value::from(current_key)),
                ("value", Value::from(2_i32)),
            ])],
        ])
        .append_exec_results([executed(1), executed(1)])
        .into_connection();

    let outcome = try_consume_rate_token(
        &db,
        &TableConfig::default(),
        "ApiJob",
        "customer",
        chrono::Duration::seconds(60),
        1,
        1,
    )
    .await
    .unwrap();
    assert!(matches!(outcome, ConsumeResult::Throttled { .. }));

    let log = db.into_transaction_log();
    assert_eq!(log.len(), 4);
    // The first query only reads the server clock. The remaining queries
    // cover UPSERT, reading both windows, and compensating a rejected token.
    let key_uses = [
        "(`key`,",
        "SELECT `key`, value FROM solid_queue_semaphores WHERE `key` IN",
        "WHERE `key` =",
    ];
    for (transaction, key_use) in log[1..].iter().zip(key_uses) {
        for statement in transaction.statements() {
            assert!(statement.sql.contains(key_use), "{}", statement.sql);
            assert!(!statement.sql.contains(current_key));
            let values = &statement.values.as_ref().unwrap().0;
            assert!(values.contains(&current_key.into()));
        }
    }
}

#[tokio::test]
#[ignore = "requires TEST_MYSQL_URL pointing to a disposable MySQL database"]
async fn mysql_live_semaphore_and_rate_limit() {
    let dsn = std::env::var("TEST_MYSQL_URL").expect("set TEST_MYSQL_URL for this test");
    let db = sea_orm::Database::connect(dsn).await.unwrap();
    let prefix = format!("mysql_audit_{}", std::process::id());
    let tc = TableConfig::with_prefix(&prefix);
    let create = quebec::schema_builder::create_semaphores_table(&tc, DbBackend::MySql)
        .to_string(sea_orm::sea_query::MysqlQueryBuilder);
    db.execute_raw(Statement::from_string(DbBackend::MySql, create))
        .await
        .unwrap();

    let key = "customer's semaphore";
    assert!(acquire_semaphore(&db, &tc, key.into(), 2, ttl())
        .await
        .unwrap()
        .is_some());
    assert!(acquire_semaphore(&db, &tc, key.into(), 2, ttl())
        .await
        .unwrap()
        .is_some());
    assert!(acquire_semaphore(&db, &tc, key.into(), 2, ttl())
        .await
        .unwrap()
        .is_none());
    assert!(release_semaphore(&db, &tc, key.into(), 2, ttl())
        .await
        .unwrap());
    assert!(release_semaphore(&db, &tc, key.into(), 2, ttl())
        .await
        .unwrap());
    // Force a changed expiry so MySQL reports an affected row when the release
    // only renews an already all-free semaphore (second UPDATE path). That
    // gives no slot back.
    db.execute_raw(Statement::from_string(
        DbBackend::MySql,
        format!(
            "UPDATE `{}` SET expires_at = '2020-01-01 00:00:00', updated_at = '2020-01-01 00:00:00'",
            tc.semaphores
        ),
    ))
    .await
    .unwrap();
    assert!(!release_semaphore(&db, &tc, key.into(), 2, ttl())
        .await
        .unwrap());
    let values =
        quebec::query_builder::semaphores::find_values_by_keys(&db, &tc, &[key.to_string()])
            .await
            .unwrap();
    assert_eq!(values.get(key), Some(&2));

    for granted in [true, false] {
        let outcome = try_consume_rate_token(
            &db,
            &tc,
            "ApiJob",
            "customer",
            chrono::Duration::seconds(3600),
            1,
            1,
        )
        .await
        .unwrap();
        assert_eq!(matches!(outcome, ConsumeResult::Granted), granted);
    }
    let counters = quebec::query_builder::semaphores::find_all(&db, &tc)
        .await
        .unwrap();
    let consumed: i32 = counters
        .iter()
        .filter(|row| row.key.starts_with("rate:ApiJob/customer:"))
        .map(|row| row.value)
        .sum();
    assert_eq!(consumed, 1, "rejected rate token must be compensated");

    db.execute_raw(Statement::from_string(
        DbBackend::MySql,
        format!("DROP TABLE `{}`", tc.semaphores),
    ))
    .await
    .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires TEST_MYSQL_URL pointing to a disposable MySQL database"]
async fn mysql_live_slot_taken_after_a_lock_wait_gets_a_fresh_expiry() {
    use sea_orm::TransactionTrait;

    let dsn = std::env::var("TEST_MYSQL_URL").expect("set TEST_MYSQL_URL for this test");
    let db = sea_orm::Database::connect(dsn.clone()).await.unwrap();
    let prefix = format!("mysql_wait_{}", std::process::id());
    let tc = TableConfig::with_prefix(&prefix);
    let create = quebec::schema_builder::create_semaphores_table(&tc, DbBackend::MySql)
        .to_string(sea_orm::sea_query::MysqlQueryBuilder);
    db.execute_raw(Statement::from_string(DbBackend::MySql, create))
        .await
        .unwrap();
    let key = "slow";
    assert!(acquire_semaphore(&db, &tc, key.into(), 2, ttl())
        .await
        .unwrap()
        .is_some());

    // Another transaction holds the row for longer than the period, so the
    // expiry computed before our UPDATE waited for it is already past.
    let holder = db.begin().await.unwrap();
    holder
        .execute_raw(Statement::from_string(
            DbBackend::MySql,
            format!(
                "SELECT id FROM `{}` WHERE `key` = '{key}' FOR UPDATE",
                tc.semaphores
            ),
        ))
        .await
        .unwrap();
    let period = chrono::Duration::milliseconds(250);
    let waiter = {
        let dsn = dsn.clone();
        let tc = tc.clone();
        tokio::spawn(async move {
            let db = sea_orm::Database::connect(dsn).await.unwrap();
            let txn = db.begin().await.unwrap();
            let acquired = acquire_semaphore(&txn, &tc, key.into(), 2, period)
                .await
                .unwrap();
            txn.commit().await.unwrap();
            acquired
        })
    };
    // Count the period from when the waiter is actually waiting on the lock,
    // not from when its task started (connecting can take a while).
    wait_for_lock_waiter(&db, &tc.semaphores).await;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    holder.commit().await.unwrap();
    assert!(waiter.await.unwrap().is_some());

    let expires_at: chrono::NaiveDateTime = db
        .query_one_raw(Statement::from_string(
            DbBackend::MySql,
            format!(
                "SELECT expires_at FROM `{}` WHERE `key` = '{key}'",
                tc.semaphores
            ),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "expires_at")
        .unwrap();
    assert!(
        expires_at > chrono::Utc::now().naive_utc(),
        "expiry {expires_at} must be counted from when the slot was taken"
    );

    db.execute_raw(Statement::from_string(
        DbBackend::MySql,
        format!("DROP TABLE `{}`", tc.semaphores),
    ))
    .await
    .unwrap();
    db.close().await.unwrap();
}

/// Wait until another connection is running a statement on `table`: the
/// waiter's write, blocked on the row lock the test holds. (The process list
/// works on MySQL and MariaDB alike; `innodb_trx` doesn't always list the
/// waiter.)
async fn wait_for_lock_waiter(db: &sea_orm::DatabaseConnection, table: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: i64 = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::MySql,
                "SELECT COUNT(*) AS n FROM information_schema.processlist \
                 WHERE id <> CONNECTION_ID() AND command IN ('Query', 'Execute') \
                 AND info LIKE ?",
                [format!("%{table}%").into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        if waiting > 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "waiter never blocked on the row lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
