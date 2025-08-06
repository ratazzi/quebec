use std::collections::HashMap;
use std::error::Error;
use chrono::{NaiveDateTime, Utc};
use sea_orm::{DbErr, EntityTrait, QueryFilter, ColumnTrait, QuerySelect, Order, QueryOrder, ConnectionTrait, PaginatorTrait};
use tera::Context;
use tracing::{error, debug, info};
use axum::http::StatusCode;

use crate::entities::{solid_queue_jobs, solid_queue_pauses, solid_queue_processes};

use super::templates;
use super::ControlPlane;

impl ControlPlane {
    /// Format timestamp with custom format
    pub fn format_timestamp(datetime: Result<NaiveDateTime, DbErr>, format: &str) -> String {
        match datetime {
            Ok(dt) => dt.format(format).to_string(),
            Err(_) => "Unknown time".to_string(),
        }
    }

    /// Format timestamp with default datetime format
    pub fn format_datetime(datetime: Result<NaiveDateTime, DbErr>) -> String {
        Self::format_timestamp(datetime, "%Y-%m-%d %H:%M:%S")
    }

    /// Format optional timestamp with default datetime format
    pub fn format_optional_datetime(datetime: Option<NaiveDateTime>) -> Option<String> {
        datetime.map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
    }

    /// Format chrono::NaiveDateTime directly
    pub fn format_naive_datetime(datetime: NaiveDateTime) -> String {
        datetime.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    /// Calculate runtime from start time to now
    pub fn calculate_runtime(started_at: NaiveDateTime) -> String {
        let duration = Utc::now().naive_utc() - started_at;
        format!("{:.2}s", duration.num_milliseconds() as f64 / 1000.0)
    }

    /// Calculate time difference from now
    pub fn calculate_time_diff(from: NaiveDateTime) -> String {
        let duration = Utc::now().naive_utc() - from;
        let seconds = duration.num_seconds();
        
        if seconds < 60 {
            format!("{}s", seconds)
        } else if seconds < 3600 {
            format!("{}m", seconds / 60)
        } else if seconds < 86400 {
            format!("{}h", seconds / 3600)
        } else {
            format!("{}d", seconds / 86400)
        }
    }

    /// Get queue names from database
    pub async fn get_queue_names(&self) -> Result<Vec<String>, DbErr> {
        let db = self.ctx.get_db().await;
        let db = db.as_ref();
        
        let queue_names: Vec<String> = solid_queue_jobs::Entity::find()
            .select_only()
            .column(solid_queue_jobs::Column::QueueName)
            .distinct()
            .order_by(solid_queue_jobs::Column::QueueName, Order::Asc)
            .into_tuple()
            .all(db)
            .await?;
        
        Ok(queue_names)
    }

    /// Get job class names from database
    pub async fn get_job_classes(&self) -> Result<Vec<String>, DbErr> {
        let db = self.ctx.get_db().await;
        let db = db.as_ref();
        
        let class_names: Vec<String> = solid_queue_jobs::Entity::find()
            .select_only()
            .column(solid_queue_jobs::Column::ClassName)
            .distinct()
            .order_by(solid_queue_jobs::Column::ClassName, Order::Asc)
            .into_tuple()
            .all(db)
            .await?;
        
        Ok(class_names)
    }

    /// Populate navigation statistics for templates
    pub async fn populate_nav_stats(&self, context: &mut Context) -> Result<(), DbErr> {
        let db = self.ctx.get_db().await;
        let db = db.as_ref();
        
        // Count total jobs
        let total_jobs = solid_queue_jobs::Entity::find()
            .count(db)
            .await?;
        
        // Count scheduled jobs
        let scheduled_count = db
            .query_one(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Postgres,
                r#"
                SELECT COUNT(*) as count 
                FROM solid_queue_scheduled_executions
                "#.to_string(),
            ))
            .await?
            .and_then(|row| row.try_get::<i64>("", "count").ok())
            .unwrap_or(0);
        
        // Count in-progress jobs
        let in_progress_count = db
            .query_one(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Postgres,
                r#"
                SELECT COUNT(*) as count 
                FROM solid_queue_claimed_executions
                "#.to_string(),
            ))
            .await?
            .and_then(|row| row.try_get::<i64>("", "count").ok())
            .unwrap_or(0);
        
        // Count failed jobs
        let failed_count = db
            .query_one(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Postgres,
                r#"
                SELECT COUNT(*) as count 
                FROM solid_queue_failed_executions
                "#.to_string(),
            ))
            .await?
            .and_then(|row| row.try_get::<i64>("", "count").ok())
            .unwrap_or(0);
        
        // Count blocked jobs
        let blocked_count = db
            .query_one(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Postgres,
                r#"
                SELECT COUNT(*) as count 
                FROM solid_queue_blocked_executions
                "#.to_string(),
            ))
            .await?
            .and_then(|row| row.try_get::<i64>("", "count").ok())
            .unwrap_or(0);
        
        // Count active workers
        let active_workers = solid_queue_processes::Entity::find()
            .count(db)
            .await?;
        
        context.insert("nav_total_jobs", &total_jobs);
        context.insert("nav_scheduled_jobs", &scheduled_count);
        context.insert("nav_in_progress_jobs", &in_progress_count);
        context.insert("nav_failed_jobs", &failed_count);
        context.insert("nav_blocked_jobs", &blocked_count);
        context.insert("nav_active_workers", &active_workers);
        
        // Add template variables for stats.html (without nav_ prefix)
        context.insert("failed_jobs_count", &failed_count);
        context.insert("in_progress_jobs_count", &in_progress_count);
        context.insert("blocked_jobs_count", &blocked_count);
        context.insert("scheduled_jobs_count", &scheduled_count);
        
        // Calculate finished jobs count (total jobs minus active executions)
        let finished_count = total_jobs as i64 - scheduled_count - in_progress_count - failed_count - blocked_count;
        context.insert("finished_jobs_count", &finished_count);
        
        Ok(())
    }

    /// Check if a queue is paused
    pub async fn is_queue_paused(&self, queue_name: &str) -> Result<bool, DbErr> {
        let db = self.ctx.get_db().await;
        let db = db.as_ref();
        
        let pause = solid_queue_pauses::Entity::find()
            .filter(solid_queue_pauses::Column::QueueName.eq(queue_name))
            .one(db)
            .await?;
        
        Ok(pause.is_some())
    }

    /// Get pagination parameters
    pub fn get_pagination_params(&self, page: u64, total_items: u64) -> HashMap<&'static str, u64> {
        let total_pages = (total_items as f64 / self.page_size as f64).ceil() as u64;
        let total_pages = total_pages.max(1);
        
        let mut pagination = HashMap::new();
        pagination.insert("current_page", page);
        pagination.insert("total_pages", total_pages);
        pagination.insert("total_items", total_items);
        pagination.insert("page_size", self.page_size);
        pagination.insert("has_prev", if page > 1 { 1 } else { 0 });
        pagination.insert("has_next", if page < total_pages { 1 } else { 0 });
        pagination.insert("prev_page", if page > 1 { page - 1 } else { 1 });
        pagination.insert("next_page", if page < total_pages { page + 1 } else { total_pages });
        
        pagination
    }

    /// Parse JSON arguments safely
    pub fn parse_arguments(arguments: &str) -> String {
        match serde_json::from_str::<serde_json::Value>(arguments) {
            Ok(json) => serde_json::to_string_pretty(&json).unwrap_or_else(|_| arguments.to_string()),
            Err(_) => arguments.to_string(),
        }
    }

    /// Format error message with optional backtrace
    pub fn format_error_with_backtrace(error: Option<String>, backtrace: Option<String>) -> String {
        match (error, backtrace) {
            (Some(err), Some(bt)) => format!("{}\n\nBacktrace:\n{}", err, bt),
            (Some(err), None) => err,
            (None, Some(bt)) => format!("Backtrace:\n{}", bt),
            (None, None) => String::new(),
        }
    }

    /// Render template with context
    pub async fn render_template(&self, template_name: &str, context: &mut Context) -> Result<String, (StatusCode, String)> {
        // In debug compilation mode, reload templates
        #[cfg(debug_assertions)]
        {
            debug!("Debug mode detected, reloading templates");
            match tera::Tera::new(&self.template_path) {
                Ok(new_tera) => {
                    // Successfully loaded new templates, replace existing Tera instance
                    match self.tera.write() {
                        Ok(mut tera) => {
                            *tera = new_tera;
                            tera.autoescape_on(vec!["html"]);
                            debug!("Templates reloaded successfully");
                        },
                        Err(e) => error!("Failed to acquire write lock: {}", e)
                    }
                },
                Err(e) => error!("Error reloading templates: {}", e)
            }
        }

        // Add database connection information
        let _db = self.ctx.get_db().await;

        // Get database DSN information directly from AppContext
        let dsn = self.ctx.dsn.to_string();
        debug!("Original DSN: {}", dsn);

        // Handle different types of database connections
        let (db_type, connection_info) = if dsn.starts_with("postgres:") {
            // If it's a PostgreSQL database
            let clean_dsn = if dsn.contains("@") {
                // Contains username and password, need to remove
                let parts: Vec<&str> = dsn.split("@").collect();
                if parts.len() > 1 {
                    // Get protocol part
                    let protocol = if parts[0].contains("://") {
                        parts[0].split("://").next().unwrap_or("postgres")
                    } else {
                        "postgres"
                    };

                    // Reconstruct URL, removing username and password
                    let host_part = parts[1];
                    format!("{0}://{1}", protocol, host_part)
                } else {
                    dsn.clone()
                }
            } else {
                // Does not contain username and password
                dsn.clone()
            };

            // If there are query parameters, remove them
            let final_dsn = if clean_dsn.contains("?") {
                clean_dsn.split("?").next().unwrap_or(&clean_dsn).to_string()
            } else {
                clean_dsn
            };

            debug!("Cleaned PostgreSQL DSN: {}", final_dsn);
            ("PostgreSQL", final_dsn)
        } else if dsn.starts_with("sqlite:") {
            // If it's a SQLite database
            let path = dsn.replace("sqlite:", "").replace("//", "");
            debug!("SQLite path: {}", path);
            ("SQLite", format!("sqlite://{}", path))
        } else {
            // Other types of databases
            debug!("Unknown database type: {}", dsn);
            ("Unknown", dsn)
        };

        let db_info = serde_json::json!({
            "database_type": db_type,
            "connection_type": "Pool",
            "status": "Connected",
            "dsn": connection_info
        });
        context.insert("db_info", &db_info);

        // Add navigation stats
        if let Err(e) = self.populate_nav_stats(context).await {
            error!("Failed to populate navigation stats: {}", e);
        }

        // In debug compilation mode, reload templates
        #[cfg(debug_assertions)]
        {
            debug!("Debug mode detected, reloading templates before rendering");

            // Initialize new Tera instance
            let mut new_tera = tera::Tera::default();

            // Use embedded template system to reload all templates
            for template_name in templates::list_templates() {
                if let Some(content) = templates::get_template_content(&template_name) {
                    if let Err(e) = new_tera.add_raw_template(&template_name, &content) {
                        error!("Failed to add template {}: {}", template_name, e);
                    }
                }
            }

            // Set automatic HTML escaping
            new_tera.autoescape_on(vec!["html"]);

            // Update Tera instance
            if let Ok(mut tera) = self.tera.write() {
                *tera = new_tera;
                debug!("Templates reloaded successfully");
            } else {
                error!("Failed to acquire write lock for template reloading");
            }
        }

        // Check if template exists in Tera instance
        let tera_read_result = self.tera.read();
        if let Err(e) = tera_read_result {
            error!("Failed to acquire read lock: {}", e);
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to acquire template lock: {}", e)));
        }

        let tera = match tera_read_result {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to acquire read lock: {}", e);
                return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to acquire template lock: {}", e)));
            }
        };
        let templates = tera.get_template_names().collect::<Vec<_>>();
        debug!("Available templates: {:?}", templates);

        // Try to render template
        match tera.render(template_name, context) {
            Ok(html) => Ok(html),
            Err(e) => {
                error!("Failed to render {}: {}", template_name, e);

                // Output complete error chain
                let mut error_detail = format!("Template error: {}", e);
                let mut current_error = e.source();
                let mut level = 1;

                while let Some(source) = current_error {
                    error_detail.push_str(&format!("\n  Caused by ({}): {}", level, source));
                    current_error = source.source();
                    level += 1;
                }

                // Output Tera-specific error details
                error!("Detailed error: {}", error_detail);

                // Output source template content
                let template_path = format!("src/templates/{}", template_name);
                match std::fs::read_to_string(&template_path) {
                    Ok(content) => {
                        info!("Template content preview (first 100 chars): {}",
                             content.chars().take(100).collect::<String>());
                    },
                    Err(e) => error!("Could not read template file {}: {}", template_path, e),
                }

                Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Template error: {}", e)))
            }
        }
    }
}