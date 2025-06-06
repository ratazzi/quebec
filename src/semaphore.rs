use tracing::trace;
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement};

pub async fn acquire_semaphore<C>(db: &C, key: String) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + chrono::Duration::minutes(1);

    let sql = match db.get_database_backend() {
        DatabaseBackend::Postgres | DatabaseBackend::Sqlite => {
            "INSERT INTO solid_queue_semaphores (key, value, expires_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (key) DO UPDATE SET \
             value = CASE WHEN solid_queue_semaphores.value > 0 THEN solid_queue_semaphores.value - 1 ELSE solid_queue_semaphores.value END, \
             expires_at = EXCLUDED.expires_at, \
             updated_at = EXCLUDED.updated_at \
             WHERE solid_queue_semaphores.value > 0"
        },
        DatabaseBackend::MySql => {
            "INSERT INTO solid_queue_semaphores (key, value, expires_at, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
             value = CASE WHEN value > 0 THEN value - 1 ELSE value END, \
             expires_at = VALUES(expires_at), \
             updated_at = VALUES(updated_at)"
        },
    };

    let result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            vec![key.into(), 1.into(), expires_at.into(), now.into(), now.into()],
        ))
        .await;

    match result {
        Ok(exec_result) => {
            trace!("Semaphore operation result: {:?}", exec_result);
            if exec_result.rows_affected() > 0 {
                Ok(true) // Successfully acquired the semaphore
            } else {
                Ok(false) // Semaphore is already acquired by someone else or value is 0
            }
        }
        Err(e) => Err(e),
    }
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
