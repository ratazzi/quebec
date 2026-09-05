use std::collections::BTreeMap;

use quebec::context::TableConfig;
use quebec::semaphore::{
    acquire_semaphore, release_semaphore, try_consume_rate_token, ConsumeResult,
};
use sea_orm::{ConnectionTrait, DbBackend, MockDatabase, MockExecResult, Statement, Value};

fn executed(rows_affected: u64) -> MockExecResult {
    MockExecResult {
        last_insert_id: 0,
        rows_affected,
    }
}

#[tokio::test]
async fn mysql_semaphore_statements_quote_key() {
    let db = MockDatabase::new(DbBackend::MySql)
        .append_exec_results([executed(0), executed(1), executed(0), executed(1)])
        .into_connection();
    let tc = TableConfig::default();
    let key = "customer's semaphore";

    // Exercise both acquisition statements and both release statements.
    assert!(acquire_semaphore(&db, &tc, key.into(), 2, None)
        .await
        .unwrap());
    assert!(release_semaphore(&db, &tc, key.into(), 2, None)
        .await
        .unwrap());

    let log = db.into_transaction_log();
    assert_eq!(log.len(), 4);
    let key_uses = ["(`key`,", "WHERE `key` =", "WHERE `key` =", "WHERE `key` ="];
    for (transaction, key_use) in log.iter().zip(key_uses) {
        for statement in transaction.statements() {
            assert!(statement.sql.contains(key_use), "{}", statement.sql);
            assert!(!statement.sql.contains(key));
            assert!(statement.values.as_ref().unwrap().0.contains(&key.into()));
        }
    }
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
    let create = quebec::schema_builder::create_semaphores_table(&tc)
        .to_string(sea_orm::sea_query::MysqlQueryBuilder);
    db.execute(Statement::from_string(DbBackend::MySql, create))
        .await
        .unwrap();

    let key = "customer's semaphore";
    assert!(acquire_semaphore(&db, &tc, key.into(), 2, None)
        .await
        .unwrap());
    assert!(acquire_semaphore(&db, &tc, key.into(), 2, None)
        .await
        .unwrap());
    assert!(!acquire_semaphore(&db, &tc, key.into(), 2, None)
        .await
        .unwrap());
    assert!(release_semaphore(&db, &tc, key.into(), 2, None)
        .await
        .unwrap());
    assert!(release_semaphore(&db, &tc, key.into(), 2, None)
        .await
        .unwrap());
    // Force a changed expiry so MySQL reports an affected row even when the
    // release only renews an already-full semaphore (second UPDATE path).
    db.execute(Statement::from_string(
        DbBackend::MySql,
        format!(
            "UPDATE `{}` SET expires_at = '2020-01-01 00:00:00', updated_at = '2020-01-01 00:00:00'",
            tc.semaphores
        ),
    ))
    .await
    .unwrap();
    assert!(release_semaphore(&db, &tc, key.into(), 2, None)
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

    db.execute(Statement::from_string(
        DbBackend::MySql,
        format!("DROP TABLE `{}`", tc.semaphores),
    ))
    .await
    .unwrap();
    db.close().await.unwrap();
}
