//! Dynamic schema builder for creating tables with configurable names.
//!
//! This module provides functions to create database tables using sea-query,
//! allowing table names to be dynamically set at runtime via TableConfig.

use sea_orm::sea_query::{
    Alias, ColumnDef, ForeignKey, ForeignKeyAction, Index, MysqlQueryBuilder, PostgresQueryBuilder,
    SqliteQueryBuilder, Table, TableCreateStatement,
};
use sea_orm::{ConnectionTrait, DbBackend, DbErr, Statement};

use crate::context::TableConfig;

// Time columns use `.date_time()`, never `.timestamp()`: on MySQL, TIMESTAMP
// caps at 2038-01-19 and applies session time-zone conversion to the naive
// UTC values we store, while Solid Queue's Rails schema uses DATETIME.
// Postgres and SQLite render the same type either way.

/// Helper to create a column alias
fn col(name: &str) -> Alias {
    Alias::new(name)
}

/// Helper to create a table alias
fn tbl(name: &str) -> Alias {
    Alias::new(name)
}

/// Build SQL string from a TableCreateStatement based on database backend
fn build_create_table_sql(backend: DbBackend, stmt: &TableCreateStatement) -> String {
    match backend {
        DbBackend::Postgres => stmt.to_string(PostgresQueryBuilder),
        DbBackend::Sqlite => stmt.to_string(SqliteQueryBuilder),
        DbBackend::MySql => stmt.to_string(MysqlQueryBuilder),
    }
}

/// Execute a CREATE TABLE statement
async fn execute_create_table<C>(db: &C, stmt: TableCreateStatement) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let sql = build_create_table_sql(db.get_database_backend(), &stmt);
    db.execute(Statement::from_string(db.get_database_backend(), sql))
        .await?;
    Ok(())
}

/// Create the jobs table
pub fn create_jobs_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.jobs))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(col("queue_name")).string().not_null())
        .col(ColumnDef::new(col("class_name")).string().not_null())
        .col(ColumnDef::new(col("arguments")).text())
        .col(
            ColumnDef::new(col("priority"))
                .integer()
                .not_null()
                .default(0),
        )
        .col(ColumnDef::new(col("active_job_id")).string())
        .col(ColumnDef::new(col("scheduled_at")).date_time())
        .col(ColumnDef::new(col("finished_at")).date_time())
        .col(ColumnDef::new(col("concurrency_key")).string())
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .col(ColumnDef::new(col("updated_at")).date_time().not_null())
        .to_owned()
}

/// Create the ready_executions table
pub fn create_ready_executions_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.ready_executions))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(col("job_id"))
                .big_integer()
                .not_null()
                .unique_key(),
        )
        .col(ColumnDef::new(col("queue_name")).string().not_null())
        .col(
            ColumnDef::new(col("priority"))
                .integer()
                .not_null()
                .default(0),
        )
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .foreign_key(
            ForeignKey::create()
                .from(tbl(&table_config.ready_executions), col("job_id"))
                .to(tbl(&table_config.jobs), col("id"))
                .on_delete(ForeignKeyAction::Cascade)
                .on_update(ForeignKeyAction::NoAction),
        )
        .to_owned()
}

/// Create the claimed_executions table
pub fn create_claimed_executions_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.claimed_executions))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(col("job_id"))
                .big_integer()
                .not_null()
                .unique_key(),
        )
        .col(ColumnDef::new(col("process_id")).big_integer())
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .foreign_key(
            ForeignKey::create()
                .from(tbl(&table_config.claimed_executions), col("job_id"))
                .to(tbl(&table_config.jobs), col("id"))
                .on_delete(ForeignKeyAction::Cascade)
                .on_update(ForeignKeyAction::NoAction),
        )
        .to_owned()
}

/// Create the blocked_executions table
pub fn create_blocked_executions_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.blocked_executions))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(col("job_id"))
                .big_integer()
                .not_null()
                .unique_key(),
        )
        .col(ColumnDef::new(col("queue_name")).string().not_null())
        .col(
            ColumnDef::new(col("priority"))
                .integer()
                .not_null()
                .default(0),
        )
        .col(ColumnDef::new(col("concurrency_key")).string().not_null())
        .col(ColumnDef::new(col("expires_at")).date_time().not_null())
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .foreign_key(
            ForeignKey::create()
                .from(tbl(&table_config.blocked_executions), col("job_id"))
                .to(tbl(&table_config.jobs), col("id"))
                .on_delete(ForeignKeyAction::Cascade)
                .on_update(ForeignKeyAction::NoAction),
        )
        .to_owned()
}

/// Create the scheduled_executions table
pub fn create_scheduled_executions_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.scheduled_executions))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(col("job_id"))
                .big_integer()
                .not_null()
                .unique_key(),
        )
        .col(ColumnDef::new(col("queue_name")).string().not_null())
        .col(
            ColumnDef::new(col("priority"))
                .integer()
                .not_null()
                .default(0),
        )
        .col(ColumnDef::new(col("scheduled_at")).date_time().not_null())
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .foreign_key(
            ForeignKey::create()
                .from(tbl(&table_config.scheduled_executions), col("job_id"))
                .to(tbl(&table_config.jobs), col("id"))
                .on_delete(ForeignKeyAction::Cascade)
                .on_update(ForeignKeyAction::NoAction),
        )
        .to_owned()
}

/// Create the failed_executions table
pub fn create_failed_executions_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.failed_executions))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(col("job_id"))
                .big_integer()
                .not_null()
                .unique_key(),
        )
        .col(ColumnDef::new(col("error")).text())
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .foreign_key(
            ForeignKey::create()
                .from(tbl(&table_config.failed_executions), col("job_id"))
                .to(tbl(&table_config.jobs), col("id"))
                .on_delete(ForeignKeyAction::Cascade)
                .on_update(ForeignKeyAction::NoAction),
        )
        .to_owned()
}

/// Create the recurring_executions table
pub fn create_recurring_executions_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.recurring_executions))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(col("job_id"))
                .big_integer()
                .not_null()
                .unique_key(),
        )
        .col(ColumnDef::new(col("task_key")).string().not_null())
        .col(ColumnDef::new(col("run_at")).date_time().not_null())
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .foreign_key(
            ForeignKey::create()
                .from(tbl(&table_config.recurring_executions), col("job_id"))
                .to(tbl(&table_config.jobs), col("id"))
                .on_delete(ForeignKeyAction::Cascade)
                .on_update(ForeignKeyAction::NoAction),
        )
        .to_owned()
}

/// Create the recurring_tasks table
pub fn create_recurring_tasks_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.recurring_tasks))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(col("key")).string().not_null().unique_key())
        .col(ColumnDef::new(col("schedule")).string().not_null())
        .col(ColumnDef::new(col("command")).string())
        .col(ColumnDef::new(col("class_name")).string())
        .col(ColumnDef::new(col("arguments")).text())
        .col(ColumnDef::new(col("queue_name")).string())
        .col(ColumnDef::new(col("priority")).integer())
        .col(
            ColumnDef::new(col("static"))
                .boolean()
                .not_null()
                .default(false),
        )
        .col(ColumnDef::new(col("description")).text())
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .col(ColumnDef::new(col("updated_at")).date_time().not_null())
        .to_owned()
}

/// Create the processes table
pub fn create_processes_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.processes))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(col("kind")).string().not_null())
        .col(
            ColumnDef::new(col("last_heartbeat_at"))
                .date_time()
                .not_null(),
        )
        .col(ColumnDef::new(col("supervisor_id")).big_integer())
        .col(ColumnDef::new(col("pid")).integer().not_null())
        .col(ColumnDef::new(col("hostname")).string())
        .col(ColumnDef::new(col("metadata")).text())
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .col(ColumnDef::new(col("name")).string().not_null())
        .to_owned()
}

/// Create the semaphores table
pub fn create_semaphores_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.semaphores))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(col("key")).string().not_null().unique_key())
        .col(ColumnDef::new(col("value")).integer().not_null().default(0))
        .col(ColumnDef::new(col("expires_at")).date_time().not_null())
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .col(ColumnDef::new(col("updated_at")).date_time().not_null())
        .to_owned()
}

/// Create the pauses table
pub fn create_pauses_table(table_config: &TableConfig) -> TableCreateStatement {
    Table::create()
        .table(tbl(&table_config.pauses))
        .if_not_exists()
        .col(
            ColumnDef::new(col("id"))
                .big_integer()
                .auto_increment()
                .primary_key(),
        )
        .col(
            ColumnDef::new(col("queue_name"))
                .string()
                .not_null()
                .unique_key(),
        )
        .col(ColumnDef::new(col("created_at")).date_time().not_null())
        .to_owned()
}

/// Create all tables in the correct order (jobs first, then tables with foreign keys)
pub async fn create_all_tables<C>(db: &C, table_config: &TableConfig) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    // First create tables without foreign key dependencies
    execute_create_table(db, create_jobs_table(table_config)).await?;
    execute_create_table(db, create_recurring_tasks_table(table_config)).await?;
    execute_create_table(db, create_processes_table(table_config)).await?;
    execute_create_table(db, create_semaphores_table(table_config)).await?;
    execute_create_table(db, create_pauses_table(table_config)).await?;

    // Then create tables with foreign keys to jobs
    execute_create_table(db, create_ready_executions_table(table_config)).await?;
    execute_create_table(db, create_claimed_executions_table(table_config)).await?;
    execute_create_table(db, create_blocked_executions_table(table_config)).await?;
    execute_create_table(db, create_scheduled_executions_table(table_config)).await?;
    execute_create_table(db, create_failed_executions_table(table_config)).await?;
    execute_create_table(db, create_recurring_executions_table(table_config)).await?;

    Ok(())
}

/// Create indexes for better query performance
pub async fn create_indexes<C>(db: &C, table_config: &TableConfig) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let mut indexes = vec![
        // Jobs indexes
        Index::create()
            .if_not_exists()
            .name(&format!("idx_{}_queue_priority", table_config.jobs))
            .table(tbl(&table_config.jobs))
            .col(col("queue_name"))
            .col(col("priority"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name(&format!("idx_{}_class_name", table_config.jobs))
            .table(tbl(&table_config.jobs))
            .col(col("class_name"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name(&format!("idx_{}_finished_at", table_config.jobs))
            .table(tbl(&table_config.jobs))
            .col(col("finished_at"))
            .to_owned(),
        // Ready executions indexes
        // Covering index for poll: WHERE queue_name = ? ORDER BY priority, job_id LIMIT N
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_queue_priority_job",
                table_config.ready_executions
            ))
            .table(tbl(&table_config.ready_executions))
            .col(col("queue_name"))
            .col(col("priority"))
            .col(col("job_id"))
            .to_owned(),
        // Blocked executions indexes
        // Covering index for release: WHERE concurrency_key = ? ORDER BY priority, job_id LIMIT 1
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_key_priority_job",
                table_config.blocked_executions
            ))
            .table(tbl(&table_config.blocked_executions))
            .col(col("concurrency_key"))
            .col(col("priority"))
            .col(col("job_id"))
            .to_owned(),
        // Maintenance index: WHERE expires_at < ? DISTINCT concurrency_key
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_expires_at_key",
                table_config.blocked_executions
            ))
            .table(tbl(&table_config.blocked_executions))
            .col(col("expires_at"))
            .col(col("concurrency_key"))
            .to_owned(),
        // Scheduled executions indexes
        // Covering index for dispatch: WHERE scheduled_at <= ? ORDER BY scheduled_at, priority, job_id
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_dispatch",
                table_config.scheduled_executions
            ))
            .table(tbl(&table_config.scheduled_executions))
            .col(col("scheduled_at"))
            .col(col("priority"))
            .col(col("job_id"))
            .to_owned(),
        // Poll-all index: ORDER BY priority, job_id (no queue filter)
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_priority_job",
                table_config.ready_executions
            ))
            .table(tbl(&table_config.ready_executions))
            .col(col("priority"))
            .col(col("job_id"))
            .to_owned(),
        // Claimed executions: process cleanup index
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_process_job",
                table_config.claimed_executions
            ))
            .table(tbl(&table_config.claimed_executions))
            .col(col("process_id"))
            .col(col("job_id"))
            .to_owned(),
        // Semaphores indexes are backend-dependent; see below.
        // Processes indexes
        Index::create()
            .if_not_exists()
            .name(&format!("idx_{}_kind", table_config.processes))
            .table(tbl(&table_config.processes))
            .col(col("kind"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name(&format!("idx_{}_last_heartbeat", table_config.processes))
            .table(tbl(&table_config.processes))
            .col(col("last_heartbeat_at"))
            .to_owned(),
        // Recurring executions unique index - required for ON CONFLICT handling
        // Matches Solid Queue: index_solid_queue_recurring_executions_on_task_key_and_run_at
        Index::create()
            .if_not_exists()
            .unique()
            .name(&format!(
                "index_{}_on_task_key_and_run_at",
                table_config.recurring_executions
            ))
            .table(tbl(&table_config.recurring_executions))
            .col(col("task_key"))
            .col(col("run_at"))
            .to_owned(),
    ];

    // Semaphores: the (key, value) and (expires_at) indexes that Solid Queue
    // declares are kept on SQLite and MySQL, but skipped on Postgres.
    //
    // Every wait/signal rewrites both indexed columns:
    //
    //     UPDATE ... SET value = value +/- 1, expires_at = $2 WHERE key = $1
    //
    // On Postgres, indexing a column that every UPDATE touches makes HOT
    // updates structurally impossible, so each write leaves a dead tuple that
    // only autovacuum can reclaim. Under sustained load the version chains grow
    // faster than autovacuum can prune them and reads degrade from a handful of
    // buffer hits per call into the thousands. Skipping both indexes lets HOT --
    // and with it opportunistic page pruning -- reclaim versions inline; the
    // unique constraint on `key` still serves the only hot-path lookup, and
    // `semaphores::delete_expired` (once per `concurrency_maintenance_interval`,
    // default 600s) falls back to a seq scan that costs far less than
    // maintaining an index on every write.
    //
    // InnoDB's undo-log MVCC and SQLite's rollback journal do not accumulate
    // heap bloat this way, so there the indexes are a plain win and stay.
    if !matches!(db.get_database_backend(), DbBackend::Postgres) {
        indexes.push(
            Index::create()
                .if_not_exists()
                .name(&format!("idx_{}_key_value", table_config.semaphores))
                .table(tbl(&table_config.semaphores))
                .col(col("key"))
                .col(col("value"))
                .to_owned(),
        );
        indexes.push(
            Index::create()
                .if_not_exists()
                .name(&format!("idx_{}_expires_at", table_config.semaphores))
                .table(tbl(&table_config.semaphores))
                .col(col("expires_at"))
                .to_owned(),
        );
    }

    for index in indexes {
        let sql = match db.get_database_backend() {
            DbBackend::Postgres => index.to_string(PostgresQueryBuilder),
            DbBackend::Sqlite => index.to_string(SqliteQueryBuilder),
            DbBackend::MySql => index.to_string(MysqlQueryBuilder),
        };

        // Check if this is the critical unique index for recurring_executions
        let is_recurring_unique_index = sql.contains("task_key_and_run_at");

        match db
            .execute(Statement::from_string(
                db.get_database_backend(),
                sql.clone(),
            ))
            .await
        {
            Ok(_) => {}
            Err(e) => {
                // Warn for unique index failures on recurring_executions
                // as this is required for ON CONFLICT handling
                if is_recurring_unique_index {
                    tracing::warn!(
                        "Failed to create unique index for recurring_executions: {}. \
                         This may cause scheduler race condition handling to fail. \
                         Check for duplicate (task_key, run_at) data.",
                        e
                    );
                }
                // Ignore other index errors (index may already exist)
            }
        }
    }

    Ok(())
}

/// Create all tables and indexes
pub async fn setup_database<C>(db: &C, table_config: &TableConfig) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    create_all_tables(db, table_config).await?;
    create_indexes(db, table_config).await?;
    tune_storage_parameters(db, table_config).await?;
    Ok(())
}

/// Apply Postgres storage parameters to the semaphores table.
///
/// Postgres only: SQLite has no equivalent, and InnoDB's undo-log MVCC does not
/// accumulate heap bloat the same way. The statement is idempotent, so calling
/// `create_tables()` again on an existing database re-applies it.
///
/// Failures are logged and swallowed -- these are tuning hints rather than
/// correctness requirements, and the connecting role may not own the tables
/// (for instance when the schema is managed by a Rails-side Solid Queue).
pub async fn tune_storage_parameters<C>(db: &C, table_config: &TableConfig) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    if !matches!(db.get_database_backend(), DbBackend::Postgres) {
        return Ok(());
    }

    // `fillfactor` reserves room on each page so a HOT update can place the new
    // row version alongside the old one; on a full page it has to go elsewhere
    // and the HOT chain breaks, which is the whole reason the secondary indexes
    // were dropped (see `create_indexes`).
    //
    // The absolute autovacuum threshold matters because the default
    // `scale_factor = 0.2` scales off the live row count: on a table holding a
    // few dozen rows, 20% is reached only long after tens of thousands of dead
    // tuples have piled up.
    let sql = format!(
        "ALTER TABLE {} SET (\
         fillfactor = 70, \
         autovacuum_vacuum_scale_factor = 0, \
         autovacuum_vacuum_threshold = 1000)",
        table_config.semaphores
    );

    if let Err(e) = db
        .execute(Statement::from_string(DbBackend::Postgres, sql))
        .await
    {
        tracing::warn!(
            "Could not set storage parameters on {}: {}. The table still works, \
             but under sustained concurrency-key churn it may accumulate dead \
             tuples faster than autovacuum reclaims them.",
            table_config.semaphores,
            e
        );
    }

    Ok(())
}

/// Check if the required tables exist by probing the jobs table.
/// Returns Ok(true) if tables exist, Ok(false) if table doesn't exist,
/// or Err for other database errors (connection, auth, etc.).
pub async fn check_tables_exist<C>(db: &C, table_config: &TableConfig) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let sql = format!("SELECT 1 FROM {} LIMIT 1", table_config.jobs);
    match db
        .execute(Statement::from_string(db.get_database_backend(), sql))
        .await
    {
        Ok(_) => Ok(true),
        Err(e) => {
            let err_str = e.to_string().to_lowercase();
            // Check for table/relation not found errors across different databases
            // Each pattern is specific to avoid false positives (e.g., "database does not exist")
            if err_str.contains("no such table")  // SQLite: "no such table: xxx"
                || (err_str.contains("relation") && err_str.contains("does not exist"))  // PostgreSQL: "relation \"xxx\" does not exist"
                || (err_str.contains("table") && err_str.contains("doesn't exist"))  // MySQL: "Table 'xxx' doesn't exist"
                || (err_str.contains("table") && err_str.contains("not found"))
            // Generic fallback
            {
                Ok(false)
            } else {
                // Connection, auth, or other errors should propagate
                Err(e)
            }
        }
    }
}

/// Add the nullable `recurring_tasks.paused_at` column behind
/// `AppContext::recurring_pause`. Idempotent: probes for the column first, and
/// again after the `ALTER TABLE`, so losing a race with another process that
/// adds it at the same time is not an error.
pub async fn ensure_recurring_paused_at<C>(db: &C, table_config: &TableConfig) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let table = &table_config.recurring_tasks;
    if column_exists(db, table, "paused_at").await {
        return Ok(());
    }

    let backend = db.get_database_backend();
    let stmt = Table::alter()
        .table(tbl(table))
        .add_column(ColumnDef::new(col("paused_at")).date_time().null())
        .to_owned();
    let sql = match backend {
        DbBackend::Postgres => stmt.to_string(PostgresQueryBuilder),
        DbBackend::Sqlite => stmt.to_string(SqliteQueryBuilder),
        DbBackend::MySql => stmt.to_string(MysqlQueryBuilder),
    };
    let altered = db.execute(Statement::from_string(backend, sql)).await;

    if column_exists(db, table, "paused_at").await {
        return Ok(());
    }
    altered.map(|_| ()).and_then(|()| {
        Err(DbErr::Custom(format!(
            "column `paused_at` still missing on `{table}` after ALTER TABLE"
        )))
    })
}

/// Whether `recurring_tasks.paused_at` exists, without trying to add it.
pub async fn recurring_paused_at_exists<C>(db: &C, table_config: &TableConfig) -> bool
where
    C: ConnectionTrait,
{
    column_exists(db, &table_config.recurring_tasks, "paused_at").await
}

/// Whether `table` has `column`, via the backend's catalog. A catalog lookup
/// rather than a probing `SELECT column ... WHERE 1 = 0`: the probe would
/// have to *fail* to report "missing", which logs an error on the PostgreSQL
/// server and would poison any enclosing transaction — and on SQLite a
/// double-quoted unknown column is silently read as a string literal, so it
/// would not even fail. This runs repeatedly while the feature is
/// unavailable, so it has to be quiet and cheap.
async fn column_exists<C>(db: &C, table: &str, column: &str) -> bool
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let sql = match backend {
        DbBackend::Postgres => {
            "SELECT 1 FROM information_schema.columns \
             WHERE table_schema = ANY (current_schemas(false)) \
             AND table_name = $1 AND column_name = $2 LIMIT 1"
        }
        DbBackend::MySql => {
            "SELECT 1 FROM information_schema.columns \
             WHERE table_schema = DATABASE() \
             AND table_name = ? AND column_name = ? LIMIT 1"
        }
        DbBackend::Sqlite => "SELECT 1 FROM pragma_table_info(?) WHERE name = ? LIMIT 1",
    };
    let stmt = Statement::from_sql_and_values(backend, sql, [table.into(), column.into()]);
    match db.query_one(stmt).await {
        Ok(row) => row.is_some(),
        Err(e) => {
            tracing::debug!("column probe for {table}.{column} failed, assuming missing: {e}");
            false
        }
    }
}
