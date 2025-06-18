use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement};
use crate::context::ConcurrencyConstraint;

pub async fn acquire_semaphore<C>(db: &C, key: String, concurrency_limit: i32, duration: Option<chrono::Duration>) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + duration.unwrap_or_else(|| chrono::Duration::minutes(2)); // Default to 2 minutes

    // First, try to create a new semaphore (attempt_creation)
    let create_sql = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            "INSERT INTO solid_queue_semaphores (key, value, expires_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (key) DO NOTHING"
        },
        DatabaseBackend::Sqlite => {
            "INSERT OR IGNORE INTO solid_queue_semaphores (key, value, expires_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5)"
        },
        DatabaseBackend::MySql => {
            "INSERT IGNORE INTO solid_queue_semaphores (key, value, expires_at, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?)"
        },
    };

    let create_result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            create_sql,
            vec![key.clone().into(), (concurrency_limit - 1).into(), expires_at.into(), now.into(), now.into()],
        ))
        .await?;

    if create_result.rows_affected() > 0 {
        // Successfully created semaphore, we got it!
        return Ok(true);
    }

    // Semaphore already exists, try to decrement if value > 0 (attempt_decrement)
    // But first check the limit == 1 case (check_limit_or_decrement)
    if concurrency_limit == 1 {
        return Ok(false); // limit == 1, don't try to decrement
    }

    let decrement_sql = match db.get_database_backend() {
        DatabaseBackend::Postgres | DatabaseBackend::Sqlite => {
            "UPDATE solid_queue_semaphores SET \
             value = value - 1, \
             expires_at = $2, \
             updated_at = $3 \
             WHERE key = $1 AND value > 0"
        },
        DatabaseBackend::MySql => {
            "UPDATE solid_queue_semaphores SET \
             value = value - 1, \
             expires_at = ?, \
             updated_at = ? \
             WHERE key = ? AND value > 0"
        },
    };

    let decrement_result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            decrement_sql,
            vec![key.into(), expires_at.into(), now.into()],
        ))
        .await?;

    Ok(decrement_result.rows_affected() > 0)
}

/// Convenience function to acquire semaphore using ConcurrencyConstraint
pub async fn acquire_semaphore_with_constraint<C>(db: &C, constraint: &ConcurrencyConstraint) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    acquire_semaphore(db, constraint.key.clone(), constraint.limit, constraint.duration).await
}

pub async fn release_semaphore<C>(db: &C, key: String) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let now = chrono::Utc::now().naive_utc();

    let sql = match db.get_database_backend() {
        DatabaseBackend::Postgres | DatabaseBackend::Sqlite => {
            "INSERT INTO solid_queue_semaphores (key, value, expires_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (key) DO UPDATE SET \
             value = solid_queue_semaphores.value + 1, \
             updated_at = EXCLUDED.updated_at"
        }
        DatabaseBackend::MySql => {
            "INSERT INTO solid_queue_semaphores (key, value, expires_at, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
             value = value + 1, \
             updated_at = VALUES(updated_at)"
        }
    };

    let result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            vec![
                key.into(),
                1.into(),
                now.into(), // expires_at is not relevant for release
                now.into(),
                now.into(),
            ],
        ))
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
