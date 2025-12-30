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
        .col(
            ColumnDef::new(col("failed_attempts"))
                .integer()
                .not_null()
                .default(0),
        )
        .col(ColumnDef::new(col("active_job_id")).string())
        .col(ColumnDef::new(col("scheduled_at")).timestamp())
        .col(ColumnDef::new(col("finished_at")).timestamp())
        .col(ColumnDef::new(col("concurrency_key")).string())
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
        .col(ColumnDef::new(col("updated_at")).timestamp().not_null())
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
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
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
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
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
        .col(ColumnDef::new(col("expires_at")).timestamp().not_null())
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
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
        .col(ColumnDef::new(col("scheduled_at")).timestamp().not_null())
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
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
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
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
        .col(ColumnDef::new(col("run_at")).timestamp().not_null())
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
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
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
        .col(ColumnDef::new(col("updated_at")).timestamp().not_null())
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
                .timestamp()
                .not_null(),
        )
        .col(ColumnDef::new(col("supervisor_id")).big_integer())
        .col(ColumnDef::new(col("pid")).integer().not_null())
        .col(ColumnDef::new(col("hostname")).string())
        .col(ColumnDef::new(col("metadata")).text())
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
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
        .col(ColumnDef::new(col("expires_at")).timestamp().not_null())
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
        .col(ColumnDef::new(col("updated_at")).timestamp().not_null())
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
        .col(ColumnDef::new(col("created_at")).timestamp().not_null())
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
    let indexes = vec![
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
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_queue_priority",
                table_config.ready_executions
            ))
            .table(tbl(&table_config.ready_executions))
            .col(col("queue_name"))
            .col(col("priority"))
            .to_owned(),
        // Blocked executions indexes
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_concurrency_key",
                table_config.blocked_executions
            ))
            .table(tbl(&table_config.blocked_executions))
            .col(col("concurrency_key"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_expires_at",
                table_config.blocked_executions
            ))
            .table(tbl(&table_config.blocked_executions))
            .col(col("expires_at"))
            .to_owned(),
        // Scheduled executions indexes
        Index::create()
            .if_not_exists()
            .name(&format!(
                "idx_{}_scheduled_at",
                table_config.scheduled_executions
            ))
            .table(tbl(&table_config.scheduled_executions))
            .col(col("scheduled_at"))
            .to_owned(),
        // Semaphores indexes
        Index::create()
            .if_not_exists()
            .name(&format!("idx_{}_expires_at", table_config.semaphores))
            .table(tbl(&table_config.semaphores))
            .col(col("expires_at"))
            .to_owned(),
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
    Ok(())
}
