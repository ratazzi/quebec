use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{NaiveDateTime, Utc};
use sea_orm::DbErr;
use std::collections::HashMap;
use std::error::Error;
use tera::Context;
use tracing::{debug, error, info};
use url::Url;

use crate::query_builder;

use super::{templates, ControlPlane};

/// Clean SQL string by removing extra whitespace and joining lines
pub fn clean_sql(sql: &str) -> String {
    sql.lines().map(str::trim).collect::<Vec<_>>().join(" ")
}

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
    #[allow(dead_code)]
    pub fn calculate_runtime(started_at: NaiveDateTime) -> String {
        let duration = Utc::now().naive_utc() - started_at;
        format!("{:.2}s", duration.num_milliseconds() as f64 / 1000.0)
    }

    /// Calculate time difference from now
    #[allow(dead_code)]
    pub fn calculate_time_diff(from: NaiveDateTime) -> String {
        let duration = Utc::now().naive_utc() - from;
        let seconds = duration.num_seconds();

        if seconds < 60 {
            format!("{seconds}s")
        } else if seconds < 3600 {
            format!("{}m", seconds / 60)
        } else if seconds < 86400 {
            format!("{}h", seconds / 3600)
        } else {
            format!("{}d", seconds / 86400)
        }
    }

    /// Get queue names from database using query_builder
    pub async fn get_queue_names(&self) -> Result<Vec<String>, DbErr> {
        let db = self.ctx.get_db().await?;
        let db = db.as_ref();
        let table_config = &self.ctx.table_config;

        query_builder::jobs::get_queue_names(db, table_config).await
    }

    /// Get job class names from database using query_builder
    pub async fn get_job_classes(&self) -> Result<Vec<String>, DbErr> {
        let db = self.ctx.get_db().await?;
        let db = db.as_ref();
        let table_config = &self.ctx.table_config;

        query_builder::jobs::get_class_names(db, table_config).await
    }

    /// Populate navigation statistics for templates using query_builder
    pub async fn populate_nav_stats(&self, context: &mut Context) -> Result<(), DbErr> {
        let db = self.ctx.get_db().await?;
        let db = db.as_ref();
        let table_config = &self.ctx.table_config;

        // Count total jobs
        let total_jobs = query_builder::jobs::count_all(db, table_config).await?;

        // Count scheduled jobs
        let scheduled_count =
            query_builder::scheduled_executions::count_all(db, table_config).await? as i64;

        // Count in-progress jobs
        let in_progress_count =
            query_builder::claimed_executions::count_all(db, table_config, None, None).await?
                as i64;

        // Count failed jobs
        let failed_count =
            query_builder::failed_executions::count_all(db, table_config, None, None).await? as i64;

        // Count blocked jobs
        let blocked_count =
            query_builder::blocked_executions::count_all(db, table_config, None, None).await?
                as i64;

        // Count active workers
        let active_workers = query_builder::processes::count_all(db, table_config).await?;

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

        // Count finished jobs by finished_at to match the /finished-jobs page
        let finished_count =
            query_builder::jobs::count_finished(db, table_config, None, None).await? as i64;
        context.insert("finished_jobs_count", &finished_count);

        Ok(())
    }

    /// Check if a queue is paused using query_builder
    pub async fn is_queue_paused(&self, queue_name: &str) -> Result<bool, DbErr> {
        let db = self.ctx.get_db().await?;
        let db = db.as_ref();
        let table_config = &self.ctx.table_config;

        let pause = query_builder::pauses::find_by_queue_name(db, table_config, queue_name).await?;

        Ok(pause.is_some())
    }

    /// Extract path+query from Referer header, falling back to default_path
    pub fn referer_or(headers: &HeaderMap, default_path: &str) -> String {
        Self::referer_or_inner(headers, default_path, false)
    }

    /// Like [`referer_or`] but strips any `page` query parameter, so bulk actions
    /// that shrink the result set won't bounce the user onto an out-of-range page.
    pub fn referer_without_page_or(headers: &HeaderMap, default_path: &str) -> String {
        Self::referer_or_inner(headers, default_path, true)
    }

    fn referer_or_inner(headers: &HeaderMap, default_path: &str, drop_page: bool) -> String {
        headers
            .get(header::REFERER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| Url::parse(s).ok())
            .and_then(|u| {
                let path = u.path();
                // Reject scheme-relative URLs (e.g. "//evil.example/x") to prevent open redirect
                if path.starts_with("//") {
                    return None;
                }
                let mut target = path.to_string();
                if drop_page {
                    let mut ser = url::form_urlencoded::Serializer::new(String::new());
                    let mut any = false;
                    for (k, v) in u.query_pairs() {
                        if k == "page" {
                            continue;
                        }
                        ser.append_pair(&k, &v);
                        any = true;
                    }
                    if any {
                        target.push('?');
                        target.push_str(&ser.finish());
                    }
                } else if let Some(q) = u.query() {
                    target.push('?');
                    target.push_str(q);
                }
                Some(target)
            })
            .unwrap_or_else(|| default_path.to_string())
    }

    /// Return a 500 Internal Server Error response
    pub fn error_response() -> Response {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::empty())
            .expect("static response builder is infallible")
    }

    /// Return a 404 Not Found response (for stale/missing rows)
    pub fn not_found_response() -> Response {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::empty())
            .expect("static response builder is infallible")
    }

    /// Redirect back to the given URL with 303 See Other
    pub fn redirect_back(url: &str) -> Response {
        Response::builder()
            .status(StatusCode::SEE_OTHER)
            .header(header::LOCATION, url)
            .body(axum::body::Body::empty())
            .expect("static response builder is infallible")
    }

    /// Get pagination parameters
    #[allow(dead_code)]
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
        pagination.insert(
            "next_page",
            if page < total_pages {
                page + 1
            } else {
                total_pages
            },
        );

        pagination
    }

    /// Parse JSON arguments safely
    pub fn parse_arguments(arguments: &str) -> String {
        match serde_json::from_str::<serde_json::Value>(arguments) {
            Ok(json) => {
                serde_json::to_string_pretty(&json).unwrap_or_else(|_| arguments.to_string())
            }
            Err(_) => arguments.to_string(),
        }
    }

    /// Parse error JSON and populate error/backtrace fields on JobDetailsInfo.
    /// The error column stores JSON like:
    /// {"backtrace":["line1","line2"],"exception_class":"Error","message":"..."}
    pub fn parse_error_fields(job: &mut super::models::JobDetailsInfo) {
        let raw = match &job.error {
            Some(s) if !s.is_empty() => s.clone(),
            _ => return,
        };

        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
            let exception_class = json
                .get("exception_class")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let message = json.get("message").and_then(|v| v.as_str()).unwrap_or("");

            // Build readable error line
            if !exception_class.is_empty() {
                job.error = Some(format!("{exception_class}: {message}"));
            } else if !message.is_empty() {
                job.error = Some(message.to_string());
            }

            // Extract backtrace array → newline-separated string
            if let Some(bt) = json.get("backtrace").and_then(|v| v.as_array()) {
                let lines: Vec<&str> = bt.iter().filter_map(|v| v.as_str()).collect();
                if !lines.is_empty() {
                    job.backtrace = Some(lines.join("\n"));
                }
            } else if let Some(bt) = json.get("backtrace").and_then(|v| v.as_str()) {
                // backtrace may also be a plain string
                job.backtrace = Some(bt.to_string());
            }
        }
        // If not valid JSON, keep the original error string as-is

        // Also parse error JSON in execution_history items
        for item in &mut job.execution_history {
            let raw = match &item.error {
                Some(s) if !s.is_empty() => s.clone(),
                _ => continue,
            };
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
                let exception_class = json
                    .get("exception_class")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let message = json.get("message").and_then(|v| v.as_str()).unwrap_or("");
                if !exception_class.is_empty() {
                    item.error = Some(format!("{exception_class}: {message}"));
                } else if !message.is_empty() {
                    item.error = Some(message.to_string());
                }
            }
        }
    }

    /// Render template with context
    pub async fn render_template(
        &self,
        template_name: &str,
        context: &mut Context,
    ) -> Result<String, (StatusCode, String)> {
        // Add database connection information
        let _db = self
            .ctx
            .get_db()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let db_info = serde_json::json!({
            "database_type": self.ctx.dsn.kind().as_str(),
            "connection_type": "Pool",
            "status": "Connected",
            "dsn": self.ctx.dsn.masked(),
        });
        context.insert("db_info", &db_info);
        context.insert("version", env!("CARGO_PKG_VERSION"));
        context.insert("base_path", &self.base_path);

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
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to acquire template lock: {e}"),
            ));
        }

        let tera = match tera_read_result {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to acquire read lock: {}", e);
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to acquire template lock: {e}"),
                ));
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
                let mut error_detail = format!("Template error: {e}");
                let mut current_error = e.source();
                let mut level = 1;

                while let Some(source) = current_error {
                    error_detail.push_str(&format!("\n  Caused by ({level}): {source}"));
                    current_error = source.source();
                    level += 1;
                }

                // Output Tera-specific error details
                error!("Detailed error: {}", error_detail);

                // Output source template content
                if let Some(template_path) = templates::template_file_path(template_name) {
                    match std::fs::read_to_string(&template_path) {
                        Ok(content) => {
                            info!(
                                "Template content preview (first 100 chars): {}",
                                content.chars().take(100).collect::<String>()
                            );
                        }
                        Err(e) => error!(
                            "Could not read template file {}: {}",
                            template_path.display(),
                            e
                        ),
                    }
                } else {
                    debug!(
                        "No filesystem template path available for {}, skipping source preview",
                        template_name
                    );
                }

                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Template error: {e}"),
                ))
            }
        }
    }
}
