use crate::context::{ConcurrencyConstraint, TableConfig};
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement};

pub async fn acquire_semaphore<C>(
    db: &C,
    table_config: &TableConfig,
    key: String,
    concurrency_limit: i32,
    duration: Option<chrono::Duration>,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + duration.unwrap_or_else(|| chrono::Duration::minutes(2)); // Default to 2 minutes

    // First, try to create a new semaphore (attempt_creation)
    let create_sql = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            format!(
                "INSERT INTO {} (key, value, expires_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (key) DO NOTHING",
                table_config.semaphores
            )
        }
        DatabaseBackend::Sqlite => {
            format!(
                "INSERT OR IGNORE INTO {} (key, value, expires_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5)",
                table_config.semaphores
            )
        }
        DatabaseBackend::MySql => {
            format!(
                "INSERT IGNORE INTO {} (key, value, expires_at, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?)",
                table_config.semaphores
            )
        }
    };

    let create_result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            create_sql,
            vec![
                key.clone().into(),
                (concurrency_limit - 1).into(),
                expires_at.into(),
                now.into(),
                now.into(),
            ],
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
            format!(
                "UPDATE {} SET \
             value = value - 1, \
             expires_at = $2, \
             updated_at = $3 \
             WHERE key = $1 AND value > 0",
                table_config.semaphores
            )
        }
        DatabaseBackend::MySql => {
            format!(
                "UPDATE {} SET \
             value = value - 1, \
             expires_at = ?, \
             updated_at = ? \
             WHERE key = ? AND value > 0",
                table_config.semaphores
            )
        }
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
pub async fn acquire_semaphore_with_constraint<C>(
    db: &C,
    table_config: &TableConfig,
    constraint: &ConcurrencyConstraint,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    acquire_semaphore(
        db,
        table_config,
        constraint.key.clone(),
        constraint.limit,
        constraint.duration,
    )
    .await
}

/// Release a semaphore, incrementing its value up to the limit.
/// When value reaches limit (all slots free), keeps the record and updates expires_at.
/// This matches Solid Queue's behavior in Semaphore#signal -> attempt_increment.
///
/// Semaphore value model (matching Solid Queue):
/// - value = limit: all slots are free
/// - value = limit - 1: initial state after first job acquires
/// - value = 0: no slots available (all slots occupied)
/// - Acquire: decrement value (if value > 0)
/// - Release: increment value (if value < limit), or just update expires_at
pub async fn release_semaphore<C>(
    db: &C,
    table_config: &TableConfig,
    key: String,
    limit: i32,
    duration: Option<chrono::Duration>,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + duration.unwrap_or_else(|| chrono::Duration::minutes(2));

    // Try to increment the semaphore value if below max (limit)
    // value ranges from 0 to limit, where limit means all slots are free
    // This matches Solid Queue's: Semaphore.where(key: key, value: ...limit).update_all(...)
    let increment_sql = match db.get_database_backend() {
        DatabaseBackend::Postgres | DatabaseBackend::Sqlite => {
            format!(
                "UPDATE {} SET \
             value = value + 1, \
             expires_at = $2, \
             updated_at = $3 \
             WHERE key = $1 AND value < $4",
                table_config.semaphores
            )
        }
        DatabaseBackend::MySql => {
            format!(
                "UPDATE {} SET \
             value = value + 1, \
             expires_at = ?, \
             updated_at = ? \
             WHERE key = ? AND value < ?",
                table_config.semaphores
            )
        }
    };

    let increment_result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            increment_sql,
            vec![
                key.clone().into(),
                expires_at.into(),
                now.into(),
                limit.into(),
            ],
        ))
        .await?;

    if increment_result.rows_affected() > 0 {
        // Successfully incremented
        return Ok(true);
    }

    // If increment didn't work, either:
    // 1. The semaphore doesn't exist
    // 2. The value is already at limit - 1 (all slots free)
    // In case 2, just update expires_at to keep the semaphore alive (don't delete!)
    let update_expires_sql = match db.get_database_backend() {
        DatabaseBackend::Postgres | DatabaseBackend::Sqlite => {
            format!(
                "UPDATE {} SET \
             expires_at = $2, \
             updated_at = $3 \
             WHERE key = $1",
                table_config.semaphores
            )
        }
        DatabaseBackend::MySql => {
            format!(
                "UPDATE {} SET \
             expires_at = ?, \
             updated_at = ? \
             WHERE key = ?",
                table_config.semaphores
            )
        }
    };

    let update_result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            update_expires_sql,
            vec![key.into(), expires_at.into(), now.into()],
        ))
        .await?;

    Ok(update_result.rows_affected() > 0)
}

/// Convenience function to release semaphore using ConcurrencyConstraint
pub async fn release_semaphore_with_constraint<C>(
    db: &C,
    table_config: &TableConfig,
    constraint: &ConcurrencyConstraint,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    release_semaphore(
        db,
        table_config,
        constraint.key.clone(),
        constraint.limit,
        constraint.duration,
    )
    .await
}
