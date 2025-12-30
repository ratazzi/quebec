//! Dynamic query builder for supporting configurable table names.
//!
//! This module provides functions to build and execute SQL queries using sea-query,
//! allowing table names to be dynamically set at runtime via TableConfig.

use sea_orm::sea_query::{
    Alias, Asterisk, Cond, DeleteStatement, Expr, InsertStatement, MysqlQueryBuilder, Order,
    PostgresQueryBuilder, Query, Returning, SelectStatement, SqliteQueryBuilder, UpdateStatement,
};
use sea_orm::{ConnectionTrait, DbBackend, DbErr, FromQueryResult, Statement, Value};

use crate::context::TableConfig;

/// Build SQL string from a SelectStatement based on database backend
pub fn build_select_sql(backend: DbBackend, query: &SelectStatement) -> (String, Vec<Value>) {
    let (sql, values) = match backend {
        DbBackend::Postgres => query.build(PostgresQueryBuilder),
        DbBackend::Sqlite => query.build(SqliteQueryBuilder),
        DbBackend::MySql => query.build(MysqlQueryBuilder),
    };
    (sql, values.into_iter().collect())
}

/// Build SQL string from an InsertStatement based on database backend
pub fn build_insert_sql(backend: DbBackend, query: &InsertStatement) -> (String, Vec<Value>) {
    let (sql, values) = match backend {
        DbBackend::Postgres => query.build(PostgresQueryBuilder),
        DbBackend::Sqlite => query.build(SqliteQueryBuilder),
        DbBackend::MySql => query.build(MysqlQueryBuilder),
    };
    (sql, values.into_iter().collect())
}

/// Build SQL string from an UpdateStatement based on database backend
pub fn build_update_sql(backend: DbBackend, query: &UpdateStatement) -> (String, Vec<Value>) {
    let (sql, values) = match backend {
        DbBackend::Postgres => query.build(PostgresQueryBuilder),
        DbBackend::Sqlite => query.build(SqliteQueryBuilder),
        DbBackend::MySql => query.build(MysqlQueryBuilder),
    };
    (sql, values.into_iter().collect())
}

/// Build SQL string from a DeleteStatement based on database backend
pub fn build_delete_sql(backend: DbBackend, query: &DeleteStatement) -> (String, Vec<Value>) {
    let (sql, values) = match backend {
        DbBackend::Postgres => query.build(PostgresQueryBuilder),
        DbBackend::Sqlite => query.build(SqliteQueryBuilder),
        DbBackend::MySql => query.build(MysqlQueryBuilder),
    };
    (sql, values.into_iter().collect())
}

/// Execute a SELECT query and return multiple results
pub async fn execute_select<T, C>(db: &C, query: SelectStatement) -> Result<Vec<T>, DbErr>
where
    T: FromQueryResult,
    C: ConnectionTrait,
{
    let (sql, values) = build_select_sql(db.get_database_backend(), &query);
    let stmt = Statement::from_sql_and_values(db.get_database_backend(), &sql, values);
    T::find_by_statement(stmt).all(db).await
}

/// Execute a SELECT query and return a single result
/// Adds LIMIT 1 to the query for efficiency
pub async fn execute_select_one<T, C>(
    db: &C,
    mut query: SelectStatement,
) -> Result<Option<T>, DbErr>
where
    T: FromQueryResult,
    C: ConnectionTrait,
{
    // Add LIMIT 1 to avoid fetching unnecessary rows
    query.limit(1);
    let (sql, values) = build_select_sql(db.get_database_backend(), &query);
    let stmt = Statement::from_sql_and_values(db.get_database_backend(), &sql, values);
    T::find_by_statement(stmt).one(db).await
}

/// Execute an INSERT query and return the last insert ID
pub async fn execute_insert<C>(db: &C, query: InsertStatement) -> Result<i64, DbErr>
where
    C: ConnectionTrait,
{
    let (sql, values) = build_insert_sql(db.get_database_backend(), &query);
    let stmt = Statement::from_sql_and_values(db.get_database_backend(), &sql, values);

    // For PostgreSQL with RETURNING clause, we need to query the result
    // For SQLite and MySQL, use last_insert_id()
    match db.get_database_backend() {
        DbBackend::Postgres => {
            // PostgreSQL returns the ID via RETURNING clause as a query result
            let row = db.query_one(stmt).await?;
            match row {
                Some(row) => {
                    let id: i64 = row.try_get("", "id")?;
                    Ok(id)
                }
                None => Err(DbErr::Custom("Insert did not return an ID".to_string())),
            }
        }
        DbBackend::Sqlite | DbBackend::MySql => {
            let result = db.execute(stmt).await?;
            Ok(result.last_insert_id() as i64)
        }
    }
}

/// Execute an INSERT query with RETURNING * and return the full model
/// For PostgreSQL/SQLite: uses RETURNING * to get all columns in one query
/// For MySQL: falls back to insert + select (MySQL doesn't support RETURNING)
pub async fn execute_insert_returning<T, C>(
    db: &C,
    mut query: InsertStatement,
    fallback_select: impl FnOnce(i64) -> SelectStatement,
) -> Result<T, DbErr>
where
    T: FromQueryResult,
    C: ConnectionTrait,
{
    match db.get_database_backend() {
        DbBackend::Postgres | DbBackend::Sqlite => {
            // Add RETURNING * for Postgres/SQLite
            query.returning(Returning::new().all());
            let (sql, values) = build_insert_sql(db.get_database_backend(), &query);
            let stmt = Statement::from_sql_and_values(db.get_database_backend(), &sql, values);
            T::find_by_statement(stmt).one(db).await?.ok_or_else(|| {
                DbErr::Custom("Insert with RETURNING did not return a row".to_string())
            })
        }
        DbBackend::MySql => {
            // MySQL doesn't support RETURNING, fall back to insert + select
            query.returning_col(col("id"));
            let (sql, values) = build_insert_sql(db.get_database_backend(), &query);
            let stmt = Statement::from_sql_and_values(db.get_database_backend(), &sql, values);
            let result = db.execute(stmt).await?;
            let id = result.last_insert_id() as i64;

            // Use the provided fallback select query
            let select_query = fallback_select(id);
            execute_select_one(db, select_query)
                .await?
                .ok_or_else(|| DbErr::Custom("Failed to find inserted row".to_string()))
        }
    }
}

/// Execute an UPDATE query and return the number of affected rows
pub async fn execute_update<C>(db: &C, query: UpdateStatement) -> Result<u64, DbErr>
where
    C: ConnectionTrait,
{
    let (sql, values) = build_update_sql(db.get_database_backend(), &query);
    let stmt = Statement::from_sql_and_values(db.get_database_backend(), &sql, values);
    let result = db.execute(stmt).await?;
    Ok(result.rows_affected())
}

/// Execute a DELETE query and return the number of affected rows
pub async fn execute_delete<C>(db: &C, query: DeleteStatement) -> Result<u64, DbErr>
where
    C: ConnectionTrait,
{
    let (sql, values) = build_delete_sql(db.get_database_backend(), &query);
    let stmt = Statement::from_sql_and_values(db.get_database_backend(), &sql, values);
    let result = db.execute(stmt).await?;
    Ok(result.rows_affected())
}

/// Execute a SELECT COUNT query and return the count
pub async fn execute_count<C>(db: &C, query: SelectStatement) -> Result<u64, DbErr>
where
    C: ConnectionTrait,
{
    let (sql, values) = build_select_sql(db.get_database_backend(), &query);
    let stmt = Statement::from_sql_and_values(db.get_database_backend(), &sql, values);
    let row = db.query_one(stmt).await?;
    match row {
        Some(row) => {
            let count: i64 = row.try_get_by_index(0)?;
            Ok(count as u64)
        }
        None => Ok(0),
    }
}

/// Helper to create a column alias
pub fn col(name: &str) -> Alias {
    Alias::new(name)
}

// =============================================================================
// Jobs table queries
// =============================================================================

pub mod jobs {
    use super::*;
    use crate::entities::quebec_jobs;

    /// Find a job by ID
    pub async fn find_by_id<C>(
        db: &C,
        table_config: &TableConfig,
        id: i64,
    ) -> Result<Option<quebec_jobs::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_select_one(db, query).await
    }

    /// Find jobs by IDs
    pub async fn find_by_ids<C>(
        db: &C,
        table_config: &TableConfig,
        ids: Vec<i64>,
    ) -> Result<Vec<quebec_jobs::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let table = Alias::new(&table_config.jobs);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("id")).is_in(ids))
            .to_owned();

        execute_select(db, query).await
    }

    /// Insert a new job and return its ID
    #[allow(clippy::too_many_arguments)]
    pub async fn insert<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
        class_name: &str,
        arguments: Option<&str>,
        priority: i32,
        active_job_id: Option<&str>,
        scheduled_at: Option<chrono::NaiveDateTime>,
        concurrency_key: Option<&str>,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        insert_with_failed_attempts(
            db,
            table_config,
            queue_name,
            class_name,
            arguments,
            priority,
            0, // default failed_attempts
            active_job_id,
            scheduled_at,
            concurrency_key,
        )
        .await
    }

    /// Insert a new job with specific failed_attempts count (for retry jobs)
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_with_failed_attempts<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
        class_name: &str,
        arguments: Option<&str>,
        priority: i32,
        failed_attempts: i32,
        active_job_id: Option<&str>,
        scheduled_at: Option<chrono::NaiveDateTime>,
        concurrency_key: Option<&str>,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let now = chrono::Utc::now().naive_utc();

        let mut query = Query::insert()
            .into_table(table)
            .columns([
                col("queue_name"),
                col("class_name"),
                col("arguments"),
                col("priority"),
                col("failed_attempts"),
                col("active_job_id"),
                col("scheduled_at"),
                col("concurrency_key"),
                col("created_at"),
                col("updated_at"),
            ])
            .values_panic([
                queue_name.into(),
                class_name.into(),
                arguments.into(),
                priority.into(),
                failed_attempts.into(),
                active_job_id.into(),
                scheduled_at.into(),
                concurrency_key.into(),
                now.into(),
                now.into(),
            ])
            .to_owned();

        // For PostgreSQL, add RETURNING clause
        if db.get_database_backend() == DbBackend::Postgres {
            query.returning_col(col("id"));
        }

        execute_insert(db, query).await
    }

    /// Insert a new job and return the full model (optimized with RETURNING * for Postgres/SQLite)
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_returning<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
        class_name: &str,
        arguments: Option<&str>,
        priority: i32,
        active_job_id: Option<&str>,
        scheduled_at: Option<chrono::NaiveDateTime>,
        concurrency_key: Option<&str>,
    ) -> Result<quebec_jobs::Model, DbErr>
    where
        C: ConnectionTrait,
    {
        let table_name = table_config.jobs.clone();
        let table = Alias::new(&table_name);
        let now = chrono::Utc::now().naive_utc();

        let query = Query::insert()
            .into_table(table)
            .columns([
                col("queue_name"),
                col("class_name"),
                col("arguments"),
                col("priority"),
                col("failed_attempts"),
                col("active_job_id"),
                col("scheduled_at"),
                col("concurrency_key"),
                col("created_at"),
                col("updated_at"),
            ])
            .values_panic([
                queue_name.into(),
                class_name.into(),
                arguments.into(),
                priority.into(),
                0i32.into(), // failed_attempts default
                active_job_id.into(),
                scheduled_at.into(),
                concurrency_key.into(),
                now.into(),
                now.into(),
            ])
            .to_owned();

        // Use execute_insert_returning which handles RETURNING * for Postgres/SQLite
        // and falls back to insert + select for MySQL
        execute_insert_returning(db, query, |id| {
            Query::select()
                .column(Asterisk)
                .from(Alias::new(&table_name))
                .and_where(Expr::col(col("id")).eq(id))
                .to_owned()
        })
        .await
    }

    /// Update a job's finished_at timestamp
    pub async fn mark_finished<C>(db: &C, table_config: &TableConfig, id: i64) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let now = chrono::Utc::now().naive_utc();

        let query = Query::update()
            .table(table)
            .values([
                (col("finished_at"), now.into()),
                (col("updated_at"), now.into()),
            ])
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_update(db, query).await
    }

    /// Increment failed_attempts counter
    pub async fn increment_failed_attempts<C>(
        db: &C,
        table_config: &TableConfig,
        id: i64,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let now = chrono::Utc::now().naive_utc();

        let query = Query::update()
            .table(table)
            .value(
                col("failed_attempts"),
                Expr::col(col("failed_attempts")).add(1),
            )
            .value(col("updated_at"), Expr::val(now))
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_update(db, query).await
    }

    /// Delete a job by ID
    pub async fn delete_by_id<C>(db: &C, table_config: &TableConfig, id: i64) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Mark multiple jobs as finished by their IDs
    pub async fn mark_finished_by_ids<C>(
        db: &C,
        table_config: &TableConfig,
        ids: Vec<i64>,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        if ids.is_empty() {
            return Ok(0);
        }
        let table = Alias::new(&table_config.jobs);
        let now = chrono::Utc::now().naive_utc();

        let query = Query::update()
            .table(table)
            .values([
                (col("finished_at"), now.into()),
                (col("updated_at"), now.into()),
            ])
            .and_where(Expr::col(col("id")).is_in(ids))
            .to_owned();

        execute_update(db, query).await
    }

    /// Count all jobs
    pub async fn count_all<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .to_owned();

        execute_count(db, query).await
    }

    /// Count finished jobs in a time range
    pub async fn count_finished_in_range<C>(
        db: &C,
        table_config: &TableConfig,
        start: chrono::NaiveDateTime,
        end: Option<chrono::NaiveDateTime>,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let mut query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .and_where(Expr::col(col("finished_at")).is_not_null())
            .and_where(Expr::col(col("finished_at")).gte(start))
            .to_owned();

        if let Some(end_time) = end {
            query = query
                .and_where(Expr::col(col("finished_at")).lt(end_time))
                .to_owned();
        }

        execute_count(db, query).await
    }

    /// Count jobs created in a time range
    pub async fn count_created_in_range<C>(
        db: &C,
        table_config: &TableConfig,
        start: chrono::NaiveDateTime,
        end: Option<chrono::NaiveDateTime>,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let mut query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .and_where(Expr::col(col("created_at")).gte(start))
            .to_owned();

        if let Some(end_time) = end {
            query = query
                .and_where(Expr::col(col("created_at")).lt(end_time))
                .to_owned();
        }

        execute_count(db, query).await
    }

    /// Count finished jobs
    pub async fn count_finished<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .and_where(Expr::col(col("finished_at")).is_not_null())
            .to_owned();

        execute_count(db, query).await
    }

    /// Find finished jobs with pagination
    pub async fn find_finished_paginated<C>(
        db: &C,
        table_config: &TableConfig,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<quebec_jobs::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("finished_at")).is_not_null())
            .order_by(col("finished_at"), Order::Desc)
            .offset(offset)
            .limit(limit)
            .to_owned();

        execute_select(db, query).await
    }

    /// Find jobs by queue with pagination
    pub async fn find_by_queue_paginated<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<quebec_jobs::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("queue_name")).eq(queue_name))
            .order_by(col("created_at"), Order::Desc)
            .offset(offset)
            .limit(limit)
            .to_owned();

        execute_select(db, query).await
    }

    /// Count jobs by queue name
    pub async fn count_by_queue<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .and_where(Expr::col(col("queue_name")).eq(queue_name))
            .to_owned();

        execute_count(db, query).await
    }

    /// Count unfinished jobs by queue name
    pub async fn count_by_queue_unfinished<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .and_where(Expr::col(col("queue_name")).eq(queue_name))
            .and_where(Expr::col(col("finished_at")).is_null())
            .to_owned();

        execute_count(db, query).await
    }

    /// Find unfinished jobs by queue with pagination
    pub async fn find_by_queue_unfinished_paginated<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<quebec_jobs::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.jobs);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("queue_name")).eq(queue_name))
            .and_where(Expr::col(col("finished_at")).is_null())
            .order_by(col("created_at"), Order::Desc)
            .offset(offset)
            .limit(limit)
            .to_owned();

        execute_select(db, query).await
    }

    /// Get distinct queue names
    pub async fn get_queue_names<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<String>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table_name = &table_config.jobs;
        let backend = db.get_database_backend();

        let (q, _) = match backend {
            DbBackend::Postgres | DbBackend::Sqlite => ('"', '"'),
            DbBackend::MySql => ('`', '`'),
        };

        let sql = format!(
            r#"SELECT DISTINCT {q}queue_name{q} FROM {q}{table}{q} ORDER BY {q}queue_name{q} ASC"#,
            q = q,
            table = table_name
        );

        let stmt = Statement::from_string(backend, sql);
        let rows = db.query_all(stmt).await?;

        rows.into_iter()
            .map(|row| row.try_get("", "queue_name"))
            .collect()
    }

    /// Get distinct class names
    pub async fn get_class_names<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<String>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table_name = &table_config.jobs;
        let backend = db.get_database_backend();

        let (q, _) = match backend {
            DbBackend::Postgres | DbBackend::Sqlite => ('"', '"'),
            DbBackend::MySql => ('`', '`'),
        };

        let sql = format!(
            r#"SELECT DISTINCT {q}class_name{q} FROM {q}{table}{q} ORDER BY {q}class_name{q} ASC"#,
            q = q,
            table = table_name
        );

        let stmt = Statement::from_string(backend, sql);
        let rows = db.query_all(stmt).await?;

        rows.into_iter()
            .map(|row| row.try_get("", "class_name"))
            .collect()
    }
}

// =============================================================================
// Ready executions table queries
// =============================================================================

pub mod ready_executions {
    use super::*;
    use crate::entities::quebec_ready_executions;

    /// Insert a ready execution record
    pub async fn insert<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
        queue_name: &str,
        priority: i32,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.ready_executions);
        let now = chrono::Utc::now().naive_utc();

        let mut query = Query::insert()
            .into_table(table)
            .columns([
                col("job_id"),
                col("queue_name"),
                col("priority"),
                col("created_at"),
            ])
            .values_panic([
                job_id.into(),
                queue_name.into(),
                priority.into(),
                now.into(),
            ])
            .to_owned();

        if db.get_database_backend() == DbBackend::Postgres {
            query.returning_col(col("id"));
        }

        execute_insert(db, query).await
    }

    /// Find ready executions for a queue, ordered by priority
    pub async fn find_by_queue<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
        limit: u64,
    ) -> Result<Vec<quebec_ready_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.ready_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("queue_name")).eq(queue_name))
            .order_by(col("priority"), Order::Asc)
            .order_by(col("job_id"), Order::Asc)
            .limit(limit)
            .to_owned();

        execute_select(db, query).await
    }

    /// Delete a ready execution by job_id
    pub async fn delete_by_job_id<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.ready_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("job_id")).eq(job_id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Delete a ready execution by ID
    pub async fn delete_by_id<C>(db: &C, table_config: &TableConfig, id: i64) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.ready_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Find one ready execution with FOR UPDATE SKIP LOCKED (for job claiming)
    /// Returns None if no execution found or all are locked
    pub async fn find_one_for_update<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: Option<&str>,
        exclude_queues: &[String],
        use_skip_locked: bool,
    ) -> Result<Option<quebec_ready_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table_name = &table_config.ready_executions;
        let backend = db.get_database_backend();
        let mut params: Vec<sea_orm::Value> = Vec::new();

        // Helper to generate placeholder based on backend
        let placeholder = |idx: usize| -> String {
            match backend {
                DbBackend::Postgres => format!("${}", idx),
                DbBackend::MySql | DbBackend::Sqlite => "?".to_string(),
            }
        };

        // Helper for column quoting
        let (q, tq) = match backend {
            DbBackend::Postgres | DbBackend::Sqlite => ('"', '"'),
            DbBackend::MySql => ('`', '`'),
        };

        // Build WHERE clause parts
        let mut conditions = Vec::new();

        if let Some(qn) = queue_name {
            params.push(qn.into());
            conditions.push(format!(
                "{}queue_name{} = {}",
                q,
                q,
                placeholder(params.len())
            ));
        }

        if !exclude_queues.is_empty() {
            let placeholders: Vec<String> = exclude_queues
                .iter()
                .map(|eq| {
                    params.push(eq.clone().into());
                    placeholder(params.len())
                })
                .collect();
            conditions.push(format!(
                "{}queue_name{} NOT IN ({})",
                q,
                q,
                placeholders.join(", ")
            ));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let lock_clause = if use_skip_locked && backend != DbBackend::Sqlite {
            "FOR UPDATE SKIP LOCKED"
        } else {
            ""
        };

        let sql = format!(
            r#"SELECT * FROM {tq}{table}{tq} {where_clause} ORDER BY {q}priority{q} ASC, {q}job_id{q} ASC LIMIT 1 {lock}"#,
            tq = tq,
            table = table_name,
            where_clause = where_clause,
            q = q,
            lock = lock_clause
        );

        let stmt = Statement::from_sql_and_values(backend, sql, params);
        quebec_ready_executions::Model::find_by_statement(stmt)
            .one(db)
            .await
    }

    /// Find multiple ready executions with FOR UPDATE SKIP LOCKED (for batch job claiming)
    /// Returns empty Vec if no executions found or all are locked
    pub async fn find_many_for_update<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: Option<&str>,
        exclude_queues: &[String],
        use_skip_locked: bool,
        limit: u64,
    ) -> Result<Vec<quebec_ready_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table_name = &table_config.ready_executions;
        let backend = db.get_database_backend();
        let mut params: Vec<sea_orm::Value> = Vec::new();

        // Helper to generate placeholder based on backend
        let placeholder = |idx: usize| -> String {
            match backend {
                DbBackend::Postgres => format!("${}", idx),
                DbBackend::MySql | DbBackend::Sqlite => "?".to_string(),
            }
        };

        // Helper for column quoting
        let (q, tq) = match backend {
            DbBackend::Postgres | DbBackend::Sqlite => ('"', '"'),
            DbBackend::MySql => ('`', '`'),
        };

        // Build WHERE clause parts
        let mut conditions = Vec::new();

        if let Some(qn) = queue_name {
            params.push(qn.into());
            conditions.push(format!(
                "{}queue_name{} = {}",
                q,
                q,
                placeholder(params.len())
            ));
        }

        if !exclude_queues.is_empty() {
            let placeholders: Vec<String> = exclude_queues
                .iter()
                .map(|eq| {
                    params.push(eq.clone().into());
                    placeholder(params.len())
                })
                .collect();
            conditions.push(format!(
                "{}queue_name{} NOT IN ({})",
                q,
                q,
                placeholders.join(", ")
            ));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let lock_clause = if use_skip_locked && backend != DbBackend::Sqlite {
            "FOR UPDATE SKIP LOCKED"
        } else {
            ""
        };

        // Add limit parameter
        params.push((limit as i64).into());
        let limit_placeholder = placeholder(params.len());

        let sql = format!(
            r#"SELECT * FROM {tq}{table}{tq} {where_clause} ORDER BY {q}priority{q} ASC, {q}job_id{q} ASC LIMIT {limit_ph} {lock}"#,
            tq = tq,
            table = table_name,
            where_clause = where_clause,
            q = q,
            limit_ph = limit_placeholder,
            lock = lock_clause
        );

        let stmt = Statement::from_sql_and_values(backend, sql, params);
        quebec_ready_executions::Model::find_by_statement(stmt)
            .all(db)
            .await
    }

    /// Find distinct queue names matching a pattern (for wildcard expansion)
    pub async fn find_matching_queue_names<C>(
        db: &C,
        table_config: &TableConfig,
        pattern: &str,
    ) -> Result<Vec<String>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table_name = &table_config.ready_executions;
        let like_pattern = format!("{}%", pattern);

        let sql = match db.get_database_backend() {
            DbBackend::Postgres => format!(
                r#"SELECT DISTINCT "queue_name" FROM "{}" WHERE "queue_name" LIKE $1 ORDER BY "queue_name" ASC"#,
                table_name
            ),
            DbBackend::MySql => format!(
                r#"SELECT DISTINCT `queue_name` FROM `{}` WHERE `queue_name` LIKE ? ORDER BY `queue_name` ASC"#,
                table_name
            ),
            DbBackend::Sqlite => format!(
                r#"SELECT DISTINCT "queue_name" FROM "{}" WHERE "queue_name" LIKE ? ORDER BY "queue_name" ASC"#,
                table_name
            ),
        };

        let stmt =
            Statement::from_sql_and_values(db.get_database_backend(), sql, [like_pattern.into()]);
        let rows = db.query_all(stmt).await?;

        rows.into_iter()
            .map(|row| row.try_get("", "queue_name"))
            .collect()
    }
}

// =============================================================================
// Claimed executions table queries
// =============================================================================

pub mod claimed_executions {
    use super::*;
    use crate::entities::quebec_claimed_executions;

    /// Insert a claimed execution record
    pub async fn insert<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
        process_id: Option<i64>,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let now = chrono::Utc::now().naive_utc();

        let mut query = Query::insert()
            .into_table(table)
            .columns([col("job_id"), col("process_id"), col("created_at")])
            .values_panic([job_id.into(), process_id.into(), now.into()])
            .to_owned();

        if db.get_database_backend() == DbBackend::Postgres {
            query.returning_col(col("id"));
        }

        execute_insert(db, query).await
    }

    /// Find a claimed execution by job_id
    pub async fn find_by_job_id<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
    ) -> Result<Option<quebec_claimed_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("job_id")).eq(job_id))
            .to_owned();

        execute_select_one(db, query).await
    }

    /// Delete a claimed execution by job_id
    pub async fn delete_by_job_id<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("job_id")).eq(job_id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Delete a claimed execution by ID
    pub async fn delete_by_id<C>(db: &C, table_config: &TableConfig, id: i64) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Count all claimed executions
    pub async fn count_all<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .to_owned();

        execute_count(db, query).await
    }

    /// Find all claimed executions
    pub async fn find_all<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<quebec_claimed_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::select().column(Asterisk).from(table).to_owned();

        execute_select(db, query).await
    }

    /// Find claimed executions with pagination
    pub async fn find_paginated<C>(
        db: &C,
        table_config: &TableConfig,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<quebec_claimed_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .order_by(col("created_at"), Order::Desc)
            .offset(offset)
            .limit(limit)
            .to_owned();

        execute_select(db, query).await
    }

    /// Find a claimed execution by ID
    pub async fn find_by_id<C>(
        db: &C,
        table_config: &TableConfig,
        id: i64,
    ) -> Result<Option<quebec_claimed_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_select_one(db, query).await
    }

    /// Delete all claimed executions
    pub async fn delete_all<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::delete().from_table(table).to_owned();

        execute_delete(db, query).await
    }

    /// Find claimed executions by process_id
    pub async fn find_by_process_id<C>(
        db: &C,
        table_config: &TableConfig,
        process_id: i64,
    ) -> Result<Vec<quebec_claimed_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("process_id")).eq(process_id))
            .to_owned();

        execute_select(db, query).await
    }

    /// Find orphaned claimed executions (process_id is NULL or process doesn't exist)
    /// This uses a LEFT JOIN to find executions where the process record is missing
    pub async fn find_orphaned<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<quebec_claimed_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let claimed_table = Alias::new(&table_config.claimed_executions);
        let processes_table = Alias::new(&table_config.processes);

        // Find claimed executions where:
        // 1. process_id IS NULL, OR
        // 2. process_id references a non-existent process (via LEFT JOIN)
        let query = Query::select()
            .columns([
                (claimed_table.clone(), col("id")),
                (claimed_table.clone(), col("job_id")),
                (claimed_table.clone(), col("process_id")),
                (claimed_table.clone(), col("created_at")),
            ])
            .from(claimed_table.clone())
            .left_join(
                processes_table.clone(),
                Expr::col((claimed_table.clone(), col("process_id")))
                    .equals((processes_table.clone(), col("id"))),
            )
            .cond_where(
                Cond::any()
                    .add(Expr::col((claimed_table.clone(), col("process_id"))).is_null())
                    .add(Expr::col((processes_table, col("id"))).is_null()),
            )
            .to_owned();

        execute_select(db, query).await
    }

    /// Delete claimed executions by process_id and return deleted count
    pub async fn delete_by_process_id<C>(
        db: &C,
        table_config: &TableConfig,
        process_id: i64,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.claimed_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("process_id")).eq(process_id))
            .to_owned();

        execute_delete(db, query).await
    }
}

// =============================================================================
// Blocked executions table queries
// =============================================================================

pub mod blocked_executions {
    use super::*;
    use crate::entities::quebec_blocked_executions;

    /// Insert a blocked execution record
    pub async fn insert<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
        queue_name: &str,
        priority: i32,
        concurrency_key: &str,
        expires_at: chrono::NaiveDateTime,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.blocked_executions);
        let now = chrono::Utc::now().naive_utc();

        let mut query = Query::insert()
            .into_table(table)
            .columns([
                col("job_id"),
                col("queue_name"),
                col("priority"),
                col("concurrency_key"),
                col("expires_at"),
                col("created_at"),
            ])
            .values_panic([
                job_id.into(),
                queue_name.into(),
                priority.into(),
                concurrency_key.into(),
                expires_at.into(),
                now.into(),
            ])
            .to_owned();

        if db.get_database_backend() == DbBackend::Postgres {
            query.returning_col(col("id"));
        }

        execute_insert(db, query).await
    }

    /// Find blocked executions by concurrency key
    pub async fn find_by_concurrency_key<C>(
        db: &C,
        table_config: &TableConfig,
        concurrency_key: &str,
        limit: u64,
    ) -> Result<Vec<quebec_blocked_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.blocked_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("concurrency_key")).eq(concurrency_key))
            .order_by(col("priority"), Order::Asc)
            .order_by(col("job_id"), Order::Asc)
            .limit(limit)
            .to_owned();

        execute_select(db, query).await
    }

    /// Delete a blocked execution by job_id
    pub async fn delete_by_job_id<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.blocked_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("job_id")).eq(job_id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Delete a blocked execution by ID
    pub async fn delete_by_id<C>(db: &C, table_config: &TableConfig, id: i64) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.blocked_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Find one blocked execution by concurrency_key with FOR UPDATE SKIP LOCKED
    pub async fn find_one_by_key_for_update<C>(
        db: &C,
        table_config: &TableConfig,
        concurrency_key: &str,
    ) -> Result<Option<quebec_blocked_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table_name = &table_config.blocked_executions;
        let sql = match db.get_database_backend() {
            DbBackend::Postgres => format!(
                r#"SELECT * FROM "{}" WHERE "concurrency_key" = $1 ORDER BY "priority" ASC, "job_id" ASC LIMIT 1 FOR UPDATE SKIP LOCKED"#,
                table_name
            ),
            DbBackend::MySql => format!(
                r#"SELECT * FROM `{}` WHERE `concurrency_key` = ? ORDER BY `priority` ASC, `job_id` ASC LIMIT 1 FOR UPDATE SKIP LOCKED"#,
                table_name
            ),
            DbBackend::Sqlite => format!(
                // SQLite doesn't support SKIP LOCKED
                r#"SELECT * FROM "{}" WHERE "concurrency_key" = ? ORDER BY "priority" ASC, "job_id" ASC LIMIT 1"#,
                table_name
            ),
        };
        let stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            [concurrency_key.into()],
        );
        quebec_blocked_executions::Model::find_by_statement(stmt)
            .one(db)
            .await
    }

    /// Count all blocked executions
    pub async fn count_all<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.blocked_executions);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .to_owned();

        execute_count(db, query).await
    }

    /// Find all blocked executions
    pub async fn find_all<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<quebec_blocked_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.blocked_executions);
        let query = Query::select().column(Asterisk).from(table).to_owned();

        execute_select(db, query).await
    }

    /// Find blocked executions with pagination
    pub async fn find_paginated<C>(
        db: &C,
        table_config: &TableConfig,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<quebec_blocked_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.blocked_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .order_by(col("created_at"), Order::Desc)
            .offset(offset)
            .limit(limit)
            .to_owned();

        execute_select(db, query).await
    }

    /// Find a blocked execution by ID
    pub async fn find_by_id<C>(
        db: &C,
        table_config: &TableConfig,
        id: i64,
    ) -> Result<Option<quebec_blocked_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.blocked_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_select_one(db, query).await
    }

    /// Delete all blocked executions
    pub async fn delete_all<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.blocked_executions);
        let query = Query::delete().from_table(table).to_owned();

        execute_delete(db, query).await
    }
}

// =============================================================================
// Failed executions table queries
// =============================================================================

pub mod failed_executions {
    use super::*;
    use crate::entities::quebec_failed_executions;

    /// Insert a failed execution record (idempotent - like Solid Queue's create_or_find_by!)
    /// Uses ON CONFLICT DO NOTHING (PostgreSQL/SQLite) or INSERT IGNORE (MySQL)
    /// to handle concurrent cleanup of the same job gracefully.
    pub async fn insert<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
        error: Option<&str>,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        let backend = db.get_database_backend();
        let table = &table_config.failed_executions;
        let now = chrono::Utc::now().naive_utc();

        // Build database-specific INSERT with conflict handling
        match backend {
            DbBackend::Postgres => {
                // For PostgreSQL, use query_one to handle RETURNING properly
                // When conflict occurs, no row is returned
                let sql = format!(
                    r#"INSERT INTO "{}" ("job_id", "error", "created_at")
                       VALUES ($1, $2, $3)
                       ON CONFLICT ("job_id") DO NOTHING
                       RETURNING "id""#,
                    table
                );
                let stmt = Statement::from_sql_and_values(
                    backend,
                    sql,
                    [job_id.into(), error.into(), now.into()],
                );

                match db.query_one(stmt).await? {
                    Some(row) => {
                        let id: i64 = row.try_get("", "id")?;
                        Ok(id)
                    }
                    None => Ok(0), // Conflict - record already exists
                }
            }
            DbBackend::Sqlite => {
                let sql = format!(
                    r#"INSERT OR IGNORE INTO "{}" ("job_id", "error", "created_at")
                       VALUES (?, ?, ?)"#,
                    table
                );
                let stmt = Statement::from_sql_and_values(
                    backend,
                    sql,
                    [job_id.into(), error.into(), now.into()],
                );
                let result = db.execute(stmt).await?;
                Ok(result.last_insert_id() as i64)
            }
            DbBackend::MySql => {
                let sql = format!(
                    r#"INSERT IGNORE INTO `{}` (`job_id`, `error`, `created_at`)
                       VALUES (?, ?, ?)"#,
                    table
                );
                let stmt = Statement::from_sql_and_values(
                    backend,
                    sql,
                    [job_id.into(), error.into(), now.into()],
                );
                let result = db.execute(stmt).await?;
                Ok(result.last_insert_id() as i64)
            }
        }
    }

    /// Find all failed executions
    pub async fn find_all<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<quebec_failed_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.failed_executions);
        let query = Query::select().column(Asterisk).from(table).to_owned();

        execute_select(db, query).await
    }

    /// Find a failed execution by job_id
    pub async fn find_by_job_id<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
    ) -> Result<Option<quebec_failed_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.failed_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("job_id")).eq(job_id))
            .to_owned();

        execute_select_one(db, query).await
    }

    /// Delete failed executions by job_ids
    pub async fn delete_by_job_ids<C>(
        db: &C,
        table_config: &TableConfig,
        job_ids: Vec<i64>,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        if job_ids.is_empty() {
            return Ok(0);
        }
        let table = Alias::new(&table_config.failed_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("job_id")).is_in(job_ids))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Delete a failed execution by job_id
    pub async fn delete_by_job_id<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.failed_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("job_id")).eq(job_id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Count all failed executions
    pub async fn count_all<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.failed_executions);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .to_owned();

        execute_count(db, query).await
    }

    /// Count failed executions created in a time range
    pub async fn count_created_in_range<C>(
        db: &C,
        table_config: &TableConfig,
        start: chrono::NaiveDateTime,
        end: Option<chrono::NaiveDateTime>,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.failed_executions);
        let mut query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .and_where(Expr::col(col("created_at")).gte(start))
            .to_owned();

        if let Some(end_time) = end {
            query = query
                .and_where(Expr::col(col("created_at")).lt(end_time))
                .to_owned();
        }

        execute_count(db, query).await
    }

    /// Find failed executions with pagination
    pub async fn find_paginated<C>(
        db: &C,
        table_config: &TableConfig,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<quebec_failed_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.failed_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .order_by(col("created_at"), Order::Desc)
            .offset(offset)
            .limit(limit)
            .to_owned();

        execute_select(db, query).await
    }
}

// =============================================================================
// Scheduled executions table queries
// =============================================================================

pub mod scheduled_executions {
    use super::*;
    use crate::entities::quebec_scheduled_executions;

    /// Insert a scheduled execution record
    pub async fn insert<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
        queue_name: &str,
        priority: i32,
        scheduled_at: chrono::NaiveDateTime,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.scheduled_executions);
        let now = chrono::Utc::now().naive_utc();

        let mut query = Query::insert()
            .into_table(table)
            .columns([
                col("job_id"),
                col("queue_name"),
                col("priority"),
                col("scheduled_at"),
                col("created_at"),
            ])
            .values_panic([
                job_id.into(),
                queue_name.into(),
                priority.into(),
                scheduled_at.into(),
                now.into(),
            ])
            .to_owned();

        if db.get_database_backend() == DbBackend::Postgres {
            query.returning_col(col("id"));
        }

        execute_insert(db, query).await
    }

    /// Find due scheduled executions (with FOR UPDATE SKIP LOCKED support)
    pub async fn find_due<C>(
        db: &C,
        table_config: &TableConfig,
        limit: u64,
        use_skip_locked: bool,
    ) -> Result<Vec<quebec_scheduled_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table_name = &table_config.scheduled_executions;
        let now = chrono::Utc::now().naive_utc();

        if use_skip_locked {
            // Use raw SQL for FOR UPDATE SKIP LOCKED since sea-query SelectStatement
            // doesn't directly support lock behavior
            let sql = match db.get_database_backend() {
                DbBackend::Postgres => format!(
                    r#"SELECT * FROM "{}" WHERE "scheduled_at" <= $1 ORDER BY "scheduled_at" ASC, "priority" ASC, "job_id" ASC LIMIT $2 FOR UPDATE SKIP LOCKED"#,
                    table_name
                ),
                DbBackend::MySql => format!(
                    r#"SELECT * FROM `{}` WHERE `scheduled_at` <= ? ORDER BY `scheduled_at` ASC, `priority` ASC, `job_id` ASC LIMIT ? FOR UPDATE SKIP LOCKED"#,
                    table_name
                ),
                DbBackend::Sqlite => format!(
                    // SQLite doesn't support SKIP LOCKED, fallback to regular query
                    r#"SELECT * FROM "{}" WHERE "scheduled_at" <= ? ORDER BY "scheduled_at" ASC, "priority" ASC, "job_id" ASC LIMIT ?"#,
                    table_name
                ),
            };
            let stmt = Statement::from_sql_and_values(
                db.get_database_backend(),
                sql,
                [now.into(), (limit as i64).into()],
            );
            quebec_scheduled_executions::Model::find_by_statement(stmt)
                .all(db)
                .await
        } else {
            let table = Alias::new(table_name);
            let query = Query::select()
                .column(Asterisk)
                .from(table)
                .and_where(Expr::col(col("scheduled_at")).lte(now))
                .order_by(col("scheduled_at"), Order::Asc)
                .order_by(col("priority"), Order::Asc)
                .order_by(col("job_id"), Order::Asc)
                .limit(limit)
                .to_owned();

            execute_select(db, query).await
        }
    }

    /// Delete a scheduled execution by job_id
    pub async fn delete_by_job_id<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.scheduled_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("job_id")).eq(job_id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Find a scheduled execution by ID
    pub async fn find_by_id<C>(
        db: &C,
        table_config: &TableConfig,
        id: i64,
    ) -> Result<Option<quebec_scheduled_executions::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.scheduled_executions);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_select_one(db, query).await
    }

    /// Delete a scheduled execution by ID
    pub async fn delete_by_id<C>(db: &C, table_config: &TableConfig, id: i64) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.scheduled_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Count all scheduled executions
    pub async fn count_all<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.scheduled_executions);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .to_owned();

        execute_count(db, query).await
    }
}

// =============================================================================
// Processes table queries
// =============================================================================

pub mod processes {
    use super::*;
    use crate::entities::quebec_processes;

    /// Insert a process record
    #[allow(clippy::too_many_arguments)]
    pub async fn insert<C>(
        db: &C,
        table_config: &TableConfig,
        kind: &str,
        pid: i32,
        hostname: Option<&str>,
        metadata: Option<&str>,
        name: &str,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let now = chrono::Utc::now().naive_utc();

        let mut query = Query::insert()
            .into_table(table)
            .columns([
                col("kind"),
                col("last_heartbeat_at"),
                col("pid"),
                col("hostname"),
                col("metadata"),
                col("created_at"),
                col("name"),
            ])
            .values_panic([
                kind.into(),
                now.into(),
                pid.into(),
                hostname.into(),
                metadata.into(),
                now.into(),
                name.into(),
            ])
            .to_owned();

        if db.get_database_backend() == DbBackend::Postgres {
            query.returning_col(col("id"));
        }

        execute_insert(db, query).await
    }

    /// Update heartbeat for a process
    pub async fn update_heartbeat<C>(
        db: &C,
        table_config: &TableConfig,
        id: i64,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let now = chrono::Utc::now().naive_utc();

        let query = Query::update()
            .table(table)
            .value(col("last_heartbeat_at"), Expr::val(now))
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_update(db, query).await
    }

    /// Find a process by ID
    pub async fn find_by_id<C>(
        db: &C,
        table_config: &TableConfig,
        id: i64,
    ) -> Result<Option<quebec_processes::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_select_one(db, query).await
    }

    /// Delete a process by ID
    pub async fn delete_by_id<C>(db: &C, table_config: &TableConfig, id: i64) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("id")).eq(id))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Count all processes
    pub async fn count_all<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .to_owned();

        execute_count(db, query).await
    }

    /// Count active processes (with recent heartbeat)
    pub async fn count_active<C>(
        db: &C,
        table_config: &TableConfig,
        heartbeat_threshold: chrono::NaiveDateTime,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let query = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(table)
            .and_where(Expr::col(col("last_heartbeat_at")).gte(heartbeat_threshold))
            .to_owned();

        execute_count(db, query).await
    }

    /// Find all processes
    pub async fn find_all<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<quebec_processes::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .order_by(col("created_at"), Order::Desc)
            .to_owned();

        execute_select(db, query).await
    }

    /// Find all processes (for workers list - filtering done by caller)
    pub async fn find_all_by_heartbeat<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<quebec_processes::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .order_by(col("created_at"), Order::Desc)
            .to_owned();

        execute_select(db, query).await
    }

    /// Find all processes with heartbeat after threshold
    pub async fn find_active_by_heartbeat<C>(
        db: &C,
        table_config: &TableConfig,
        heartbeat_threshold: chrono::NaiveDateTime,
    ) -> Result<Vec<quebec_processes::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("last_heartbeat_at")).gte(heartbeat_threshold))
            .order_by(col("created_at"), Order::Desc)
            .to_owned();

        execute_select(db, query).await
    }

    /// Find processes by kind
    pub async fn find_by_kind<C>(
        db: &C,
        table_config: &TableConfig,
        kind: &str,
    ) -> Result<Vec<quebec_processes::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("kind")).eq(kind))
            .order_by(col("created_at"), Order::Desc)
            .to_owned();

        execute_select(db, query).await
    }

    /// Find prunable (stale) processes with heartbeat before threshold
    /// These are processes that have stopped sending heartbeats
    pub async fn find_prunable<C>(
        db: &C,
        table_config: &TableConfig,
        heartbeat_threshold: chrono::NaiveDateTime,
        exclude_id: Option<i64>,
    ) -> Result<Vec<quebec_processes::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.processes);
        let mut query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("last_heartbeat_at")).lt(heartbeat_threshold))
            .to_owned();

        if let Some(id) = exclude_id {
            query = query.and_where(Expr::col(col("id")).ne(id)).to_owned();
        }

        execute_select(db, query).await
    }

    /// Delete a process and return the number of rows deleted
    pub async fn prune<C>(db: &C, table_config: &TableConfig, id: i64) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        delete_by_id(db, table_config, id).await
    }
}

// =============================================================================
// Recurring executions table queries
// =============================================================================

pub mod recurring_executions {
    use super::*;

    /// Insert a recurring execution record
    pub async fn insert<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
        task_key: &str,
        run_at: chrono::NaiveDateTime,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.recurring_executions);
        let now = chrono::Utc::now().naive_utc();

        let mut query = Query::insert()
            .into_table(table)
            .columns([
                col("job_id"),
                col("task_key"),
                col("run_at"),
                col("created_at"),
            ])
            .values_panic([job_id.into(), task_key.into(), run_at.into(), now.into()])
            .to_owned();

        if db.get_database_backend() == DbBackend::Postgres {
            query.returning_col(col("id"));
        }

        execute_insert(db, query).await
    }

    /// Try to insert a recurring execution record, returns true if inserted, false if already exists.
    /// Uses INSERT ... ON CONFLICT DO NOTHING (PostgreSQL/SQLite) or INSERT IGNORE (MySQL)
    /// to handle concurrent scheduler instances gracefully.
    pub async fn try_insert<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
        task_key: &str,
        run_at: chrono::NaiveDateTime,
    ) -> Result<bool, DbErr>
    where
        C: ConnectionTrait,
    {
        let backend = db.get_database_backend();
        let table = &table_config.recurring_executions;
        let now = chrono::Utc::now().naive_utc();

        // Build database-specific INSERT with conflict handling
        let sql = match backend {
            DbBackend::Postgres => {
                format!(
                    r#"INSERT INTO "{}" ("job_id", "task_key", "run_at", "created_at")
                       VALUES ($1, $2, $3, $4)
                       ON CONFLICT ("task_key", "run_at") DO NOTHING"#,
                    table
                )
            }
            DbBackend::Sqlite => {
                format!(
                    r#"INSERT OR IGNORE INTO "{}" ("job_id", "task_key", "run_at", "created_at")
                       VALUES (?, ?, ?, ?)"#,
                    table
                )
            }
            DbBackend::MySql => {
                format!(
                    r#"INSERT IGNORE INTO `{}` (`job_id`, `task_key`, `run_at`, `created_at`)
                       VALUES (?, ?, ?, ?)"#,
                    table
                )
            }
        };

        let values = vec![
            Value::from(job_id),
            Value::from(task_key),
            Value::from(run_at),
            Value::from(now),
        ];

        let result = db
            .execute(Statement::from_sql_and_values(backend, sql, values))
            .await?;

        // rows_affected > 0 means insert succeeded, 0 means conflict (already exists)
        Ok(result.rows_affected() > 0)
    }

    /// Delete a recurring execution by job_id
    pub async fn delete_by_job_id<C>(
        db: &C,
        table_config: &TableConfig,
        job_id: i64,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.recurring_executions);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("job_id")).eq(job_id))
            .to_owned();

        execute_delete(db, query).await
    }
}

// =============================================================================
// Semaphores table queries (for operations not covered by semaphore.rs)
// =============================================================================

pub mod semaphores {
    use super::*;
    use crate::entities::quebec_semaphores;

    /// Find all semaphores
    pub async fn find_all<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<quebec_semaphores::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.semaphores);
        let query = Query::select().column(Asterisk).from(table).to_owned();

        execute_select(db, query).await
    }

    /// Delete expired semaphores
    pub async fn delete_expired<C>(db: &C, table_config: &TableConfig) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.semaphores);
        let now = chrono::Utc::now().naive_utc();

        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("expires_at")).lt(now))
            .to_owned();

        execute_delete(db, query).await
    }
}

// =============================================================================
// Pauses table queries
// =============================================================================

pub mod pauses {
    use super::*;
    use crate::entities::quebec_pauses;

    /// Find all paused queue names
    pub async fn find_all_queue_names<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<String>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table_name = &table_config.pauses;
        let sql = match db.get_database_backend() {
            DbBackend::Postgres => format!(r#"SELECT "queue_name" FROM "{}""#, table_name),
            DbBackend::MySql => format!(r#"SELECT `queue_name` FROM `{}`"#, table_name),
            DbBackend::Sqlite => format!(r#"SELECT "queue_name" FROM "{}""#, table_name),
        };
        let stmt = Statement::from_string(db.get_database_backend(), sql);
        let rows = db.query_all(stmt).await?;

        rows.into_iter()
            .map(|row| row.try_get("", "queue_name"))
            .collect()
    }

    /// Find all pauses
    pub async fn find_all<C>(
        db: &C,
        table_config: &TableConfig,
    ) -> Result<Vec<quebec_pauses::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.pauses);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .order_by(col("queue_name"), Order::Asc)
            .to_owned();

        execute_select(db, query).await
    }

    /// Insert a pause record
    pub async fn insert<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
        created_at: chrono::NaiveDateTime,
    ) -> Result<i64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.pauses);

        let mut query = Query::insert()
            .into_table(table)
            .columns([col("queue_name"), col("created_at")])
            .values_panic([queue_name.into(), created_at.into()])
            .to_owned();

        if db.get_database_backend() == DbBackend::Postgres {
            query.returning_col(col("id"));
        }

        execute_insert(db, query).await
    }

    /// Delete a pause by queue name
    pub async fn delete_by_queue_name<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
    ) -> Result<u64, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.pauses);
        let query = Query::delete()
            .from_table(table)
            .and_where(Expr::col(col("queue_name")).eq(queue_name))
            .to_owned();

        execute_delete(db, query).await
    }

    /// Find a pause by queue name
    pub async fn find_by_queue_name<C>(
        db: &C,
        table_config: &TableConfig,
        queue_name: &str,
    ) -> Result<Option<quebec_pauses::Model>, DbErr>
    where
        C: ConnectionTrait,
    {
        let table = Alias::new(&table_config.pauses);
        let query = Query::select()
            .column(Asterisk)
            .from(table)
            .and_where(Expr::col(col("queue_name")).eq(queue_name))
            .to_owned();

        execute_select_one(db, query).await
    }
}
