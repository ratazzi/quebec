use std::sync::Arc;
use std::time::{Duration, Instant};
use std::path::PathBuf;
use std::env;
use std::error::Error;
use std::collections::HashMap;
use axum::{
    Router,
    routing::{get, post},
    extract::{State, Query, Path},
    response::{Html, IntoResponse, Response},
    http::StatusCode,
    body::Body,
};
use serde::{Deserialize, Serialize};
use tera::Tera;
use crate::context::AppContext;
use sea_orm::{EntityTrait, ActiveModelTrait, Set, ActiveValue};
use sea_orm::{QueryFilter, ColumnTrait, DeleteMany, DbErr};
use sea_orm::QuerySelect;
use sea_orm::prelude::Expr;
use sea_orm::Order;
use sea_orm::ConnectionTrait;
use sea_orm::TransactionTrait;
use sea_orm::sea_query::Func;
use sea_orm::PaginatorTrait;
use sea_orm::Value;
use sea_orm::Statement;
use sea_orm::DbBackend;
use sea_orm::QueryOrder;
use crate::entities::solid_queue_jobs;
use crate::entities::solid_queue_pauses;
use crate::entities::solid_queue_failed_executions;
use crate::entities::solid_queue_claimed_executions;
use crate::entities::solid_queue_ready_executions;
use crate::entities::solid_queue_scheduled_executions;
use crate::entities::solid_queue_blocked_executions;
use crate::entities::solid_queue_processes;
use tracing::{error, info, debug, warn};
use tokio::time::sleep;
use tower_http::trace::{self, TraceLayer};
use tracing::{Level, Span, instrument};
use chrono::DateTime;
use chrono::NaiveDateTime;

use std::sync::RwLock;

pub struct ControlPlane {
    ctx: Arc<AppContext>,
    tera: RwLock<Tera>,  // 使用 RwLock 提供线程安全的内部可变性
    template_path: String,
    page_size: usize,
}

#[derive(Debug, Serialize)]
struct QueueInfo {
    name: String,
    jobs_count: usize,
    status: String,
}

#[derive(Debug, Serialize)]
struct WorkerInfo {
    id: i64,
    name: String,
    kind: String,
    hostname: Option<String>,
    pid: i32,
    last_heartbeat_at: String,
    seconds_since_heartbeat: i64,
    status: String,
}

#[derive(Debug, Deserialize)]
struct Pagination {
    #[serde(default = "default_page")]
    page: usize,
}

fn default_page() -> usize {
    1
}

// 添加失败任务的数据结构
#[derive(Debug, Serialize)]
struct FailedJobInfo {
    id: i64,
    queue_name: String,
    class_name: String,
    error: String,
    failed_at: String,
}

// 添加 InProgressJobInfo 结构
#[derive(Debug, Serialize)]
struct InProgressJobInfo {
    id: i64,
    job_id: i64,
    queue_name: String,
    class_name: String,
    worker_id: String,
    started_at: String,
    runtime: String,
}

// 添加队列内作业的数据结构
#[derive(Debug, Serialize)]
struct QueueJobInfo {
    id: i64,
    class_name: String,
    status: String,
    created_at: String,
    execution_id: Option<i64>,
}

// 在适当位置添加 ScheduledJobInfo 结构
#[derive(Debug, Serialize)]
struct ScheduledJobInfo {
    id: i64,
    job_id: i64,
    queue_name: String,
    class_name: String,
    created_at: String,
    scheduled_at: String,
    scheduled_in: String,
}

// 添加 BlockedJobInfo 结构
#[derive(Debug, Serialize)]
struct BlockedJobInfo {
    id: i64,
    job_id: i64,
    queue_name: String,
    class_name: String,
    concurrency_key: String,
    created_at: String,
    waiting_time: String,
    expires_at: String,
}

// 添加 FinishedJobInfo 结构
#[derive(Debug, Serialize)]
struct FinishedJobInfo {
    id: i64,
    queue_name: String,
    class_name: String,
    created_at: String,
    finished_at: String,
    runtime: String,
}

impl ControlPlane {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        let start = Instant::now();

        let current_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        debug!("Current directory: {}", current_dir.display());

        let current_dir_templates = format!("{}/src/web/templates/**/*", current_dir.display());
        let template_paths = vec![
            // "src/web/templates/**/*",
            // "web/templates/**/*",
            // "templates/**/*",
            &current_dir_templates,
        ];

        // 先尝试标准路径
        let mut tera = match Tera::new(&template_paths[0]) {
            Ok(t) => {
                debug!("Successfully loaded templates from '{}'", template_paths[0]);
                t
            },
            Err(e) => {
                error!("Failed to load templates from '{}': {}", template_paths[0], e);

                let mut found = false;
                let mut t = Tera::default();

                // for path in &template_paths[1..] {
                //     match Tera::new(path) {
                //         Ok(loaded) => {
                //             info!("Successfully loaded templates from '{}'", path);
                //             t = loaded;
                //             found = true;
                //             break;
                //         },
                //         Err(e) => {
                //             error!("Failed to load templates from '{}': {}", path, e);
                //         }
                //     }
                // }

                // if !found {
                //     warn!("No template directories found, falling back to manually adding templates");
                // }

                t
            }
        };

        // 设置自动转义
        tera.autoescape_on(vec!["html"]);

        // 输出最终模板列表
        let templates = tera.get_template_names().collect::<Vec<_>>();
        debug!("Available templates: {:?}", templates);
        debug!("Tera template engine initialized in {:?}", start.elapsed());

        Self {
            ctx,
            tera: RwLock::new(tera),
            template_path: template_paths[0].to_string(),
            page_size: 10
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/", get(Self::overview))
            .route("/queues", get(Self::queues))
            .route("/queues/:name", get(Self::queue_details))
            .route("/queues/:name/pause", post(Self::pause_queue))
            .route("/queues/:name/resume", post(Self::resume_queue))
            .route("/failed-jobs", get(Self::failed_jobs))
            .route("/failed-jobs/:id/retry", post(Self::retry_failed_job))
            .route("/failed-jobs/:id/delete", post(Self::delete_failed_job))
            .route("/failed-jobs/all/retry", post(Self::retry_all_failed_jobs))
            .route("/failed-jobs/all/delete", post(Self::discard_all_failed_jobs))
            .route("/in-progress-jobs", get(Self::in_progress_jobs))
            .route("/in-progress-jobs/:id/cancel", post(Self::cancel_in_progress_job))
            .route("/in-progress-jobs/all/cancel", post(Self::cancel_all_in_progress_jobs))
            .route("/blocked-jobs", get(Self::blocked_jobs))
            .route("/blocked-jobs/:id/unblock", post(Self::unblock_job))
            .route("/blocked-jobs/:id/cancel", post(Self::cancel_blocked_job))
            .route("/blocked-jobs/all/unblock", post(Self::unblock_all_jobs))
            .route("/scheduled-jobs", get(Self::scheduled_jobs))
            .route("/scheduled-jobs/:id/cancel", post(Self::cancel_scheduled_job))
            .route("/finished-jobs", get(Self::finished_jobs))
            .route("/stats", get(Self::stats))
            .route("/workers", get(Self::workers))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                    .on_request(|request: &axum::http::Request<_>, _span: &Span| {
                        debug!("Started {} {}", request.method(), request.uri());
                    })
                    .on_response(|response: &axum::http::Response<_>, latency: Duration, _span: &Span| {
                        info!(
                            "Finished in {:?} with status {}",
                            latency,
                            response.status()
                        );
                    })
                    .on_failure(trace::DefaultOnFailure::new().level(Level::ERROR))
            )
            .with_state(Arc::new(self))
    }

    async fn index() -> &'static str {
        "Quebec Control Plane"
    }

    #[instrument(skip(state), fields(path = "/"))]
    async fn overview(
        State(state): State<Arc<ControlPlane>>,
        Query(params): Query<std::collections::HashMap<String, String>>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        // 获取时间范围参数，默认为24小时
        let hours: i64 = params.get("hours")
            .and_then(|h| h.parse().ok())
            .unwrap_or(24);

        let now = chrono::Utc::now().naive_utc();
        let period_start = now - chrono::Duration::hours(hours);
        let previous_period_start = period_start - chrono::Duration::hours(hours);

        // 获取当前周期内已完成的作业总数
        let total_jobs_processed = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .filter(solid_queue_jobs::Column::FinishedAt.gt(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 获取上一个周期内已完成的作业总数，用于计算变化率
        let previous_jobs_processed = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .filter(solid_queue_jobs::Column::FinishedAt.gt(previous_period_start))
            .filter(solid_queue_jobs::Column::FinishedAt.lte(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 计算作业处理数量的变化率
        let jobs_processed_change = if previous_jobs_processed > 0 {
            ((total_jobs_processed as f64 - previous_jobs_processed as f64) / previous_jobs_processed as f64 * 100.0).round() as i32
        } else {
            0
        };

        // 获取当前周期内作业的平均处理时间
        let avg_duration_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT AVG(EXTRACT(EPOCH FROM (finished_at - created_at))) as avg_duration
               FROM solid_queue_jobs
               WHERE finished_at IS NOT NULL
               AND finished_at > $1"#,
            [period_start.into()]
        );

        let avg_duration: Option<f64> = db
            .query_one(avg_duration_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get("", "avg_duration").unwrap_or(0.0));

        // 获取上一个周期内作业的平均处理时间
        let prev_avg_duration_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT AVG(EXTRACT(EPOCH FROM (finished_at - created_at))) as avg_duration
               FROM solid_queue_jobs
               WHERE finished_at IS NOT NULL
               AND finished_at > $1
               AND finished_at <= $2"#,
            [previous_period_start.into(), period_start.into()]
        );

        let prev_avg_duration: Option<f64> = db
            .query_one(prev_avg_duration_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get("", "avg_duration").unwrap_or(0.0));

        // 计算平均处理时间的变化率
        let avg_duration_change = match (avg_duration, prev_avg_duration) {
            (Some(curr), Some(prev)) if prev > 0.0 => {
                ((curr - prev) / prev * 100.0).round() as i32
            },
            _ => 0
        };

        // 格式化平均处理时间
        let avg_job_duration = match avg_duration {
            Some(secs) if secs >= 3600.0 => {
                format!("{:.1}h", secs / 3600.0)
            },
            Some(secs) if secs >= 60.0 => {
                format!("{:.1}m", secs / 60.0)
            },
            Some(secs) => {
                format!("{:.1}s", secs)
            },
            None => "N/A".to_string()
        };

        // 获取活跃的 worker 数量
        let active_workers = solid_queue_processes::Entity::find()
            .filter(solid_queue_processes::Column::LastHeartbeatAt.gt(now - chrono::Duration::seconds(30)))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 获取上一个周期的活跃 worker 数量（这里简化处理，实际上应该查询历史数据）
        let prev_active_workers = active_workers; // 假设没有变化，实际应该从历史数据中获取

        // 计算活跃 worker 数量的变化
        let active_workers_change = active_workers as i32 - prev_active_workers as i32;

        // 计算失败率
        let failed_jobs = solid_queue_failed_executions::Entity::find()
            .filter(solid_queue_failed_executions::Column::CreatedAt.gt(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let total_jobs = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::CreatedAt.gt(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let failed_jobs_rate = if total_jobs > 0 {
            ((failed_jobs as f64 / total_jobs as f64) * 100.0).round() as i32
        } else {
            0
        };

        // 获取上一个周期的失败率
        let prev_failed_jobs = solid_queue_failed_executions::Entity::find()
            .filter(solid_queue_failed_executions::Column::CreatedAt.gt(previous_period_start))
            .filter(solid_queue_failed_executions::Column::CreatedAt.lte(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let prev_total_jobs = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::CreatedAt.gt(previous_period_start))
            .filter(solid_queue_jobs::Column::CreatedAt.lte(period_start))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let prev_failed_jobs_rate = if prev_total_jobs > 0 {
            ((prev_failed_jobs as f64 / prev_total_jobs as f64) * 100.0).round() as i32
        } else {
            0
        };

        let failed_rate_change = failed_jobs_rate - prev_failed_jobs_rate;

        // 准备时间标签和作业处理数据（用于图表）
        let mut time_labels = Vec::new();
        let mut jobs_processed_data = Vec::new();

        // 根据选择的时间范围确定时间间隔
        let (interval_hours, format_string) = if hours <= 24 {
            (1, "%H:%M") // 每小时，显示时:分
        } else if hours <= 168 { // 7天
            (6, "%m-%d %H:%M") // 每6小时，显示月-日 时:分
        } else {
            (24, "%m-%d") // 每天，显示月-日
        };

        // 生成时间序列数据
        for i in 0..(hours / interval_hours) {
            let end_time = now - chrono::Duration::hours(i * interval_hours);
            let start_time = end_time - chrono::Duration::hours(interval_hours);

            time_labels.push(end_time.format(format_string).to_string());

            // 查询该时间段内完成的作业数
            let period_jobs = solid_queue_jobs::Entity::find()
                .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
                .filter(solid_queue_jobs::Column::FinishedAt.gt(start_time))
                .filter(solid_queue_jobs::Column::FinishedAt.lte(end_time))
                .count(db)
                .await
                .unwrap_or(0);

            jobs_processed_data.push(period_jobs);
        }

        // 反转数组以便按时间顺序显示
        time_labels.reverse();
        jobs_processed_data.reverse();

        // 获取作业类型分布
        let job_types_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT class_name, COUNT(*) as count
               FROM solid_queue_jobs
               WHERE created_at > $1
               GROUP BY class_name
               ORDER BY count DESC
               LIMIT 7"#,
            [period_start.into()]
        );

        let job_types_result = db
            .query_all(job_types_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut job_types_labels = Vec::new();
        let mut job_types_data = Vec::new();

        for row in job_types_result {
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let count: i64 = row.try_get("", "count").unwrap_or_default();

            job_types_labels.push(class_name);
            job_types_data.push(count);
        }

        // 获取队列性能统计
        let queue_performance_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT
                 j.queue_name,
                 COUNT(*) FILTER (WHERE j.finished_at IS NOT NULL) as jobs_processed,
                 AVG(EXTRACT(EPOCH FROM (j.finished_at - j.created_at))) FILTER (WHERE j.finished_at IS NOT NULL) as avg_duration,
                 COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM solid_queue_failed_executions f WHERE f.job_id = j.id)) as failed_jobs,
                 COUNT(*) as total_jobs
               FROM solid_queue_jobs j
               WHERE j.created_at > $1
               GROUP BY j.queue_name"#,
            [period_start.into()]
        );

        let queue_performance_result = db
            .query_all(queue_performance_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 获取暂停的队列
        let paused_queues = solid_queue_pauses::Entity::find()
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let paused_queue_names: Vec<String> = paused_queues.iter()
            .map(|p| p.queue_name.clone())
            .collect();

        let mut queue_stats = Vec::new();

        for row in queue_performance_result {
            let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();
            let jobs_processed: i64 = row.try_get("", "jobs_processed").unwrap_or_default();
            let avg_duration: Option<f64> = row.try_get("", "avg_duration").ok();
            let failed_jobs: i64 = row.try_get("", "failed_jobs").unwrap_or_default();
            let total_jobs: i64 = row.try_get("", "total_jobs").unwrap_or_default();

            // 格式化平均处理时间
            let avg_duration_str = match avg_duration {
                Some(secs) if secs >= 3600.0 => {
                    format!("{:.1}h", secs / 3600.0)
                },
                Some(secs) if secs >= 60.0 => {
                    format!("{:.1}m", secs / 60.0)
                },
                Some(secs) => {
                    format!("{:.1}s", secs)
                },
                None => "N/A".to_string()
            };

            // 计算失败率
            let failed_rate = if total_jobs > 0 {
                ((failed_jobs as f64 / total_jobs as f64) * 100.0).round() as i32
            } else {
                0
            };

            // 检查队列状态
            let status = if paused_queue_names.contains(&queue_name) {
                "paused"
            } else {
                "active"
            };

            queue_stats.push(serde_json::json!({
                "name": queue_name,
                "jobs_processed": jobs_processed,
                "avg_duration": avg_duration_str,
                "failed_rate": failed_rate,
                "status": status
            }));
        }

        // 准备模板上下文
        let mut context = tera::Context::new();
        context.insert("total_jobs_processed", &total_jobs_processed);
        context.insert("jobs_processed_change", &jobs_processed_change);
        context.insert("avg_job_duration", &avg_job_duration);
        context.insert("avg_duration_change", &avg_duration_change);
        context.insert("active_workers", &active_workers);
        context.insert("active_workers_change", &active_workers_change);
        context.insert("failed_jobs_rate", &failed_jobs_rate);
        context.insert("failed_rate_change", &failed_rate_change);

        // 获取最近失败的作业
        let recent_failed_jobs_stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            r#"SELECT
                f.id,
                j.class_name,
                j.queue_name,
                f.created_at as failed_at,
                f.error
              FROM solid_queue_failed_executions f
              JOIN solid_queue_jobs j ON f.job_id = j.id
              WHERE f.created_at > $1
              ORDER BY f.created_at DESC
              LIMIT 10"#,
            [period_start.into()]
        );

        let recent_failed_jobs_result = db
            .query_all(recent_failed_jobs_stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut recent_failed_jobs = Vec::new();

        for row in recent_failed_jobs_result {
            let id: i64 = row.try_get("", "id").unwrap_or_default();
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();
            let failed_at: Option<NaiveDateTime> = row.try_get("", "failed_at").ok();
            let error: String = row.try_get("", "error").unwrap_or_default();

            let formatted_failed_at = failed_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "N/A".to_string());

            recent_failed_jobs.push(serde_json::json!({
                "id": id,
                "class_name": class_name,
                "queue_name": queue_name,
                "failed_at": formatted_failed_at,
                "error": error
            }));
        }

        // 将数组序列化为 JSON 字符串
        context.insert("time_labels", &serde_json::to_string(&time_labels).unwrap_or_else(|_| "[]".to_string()));
        context.insert("jobs_processed_data", &serde_json::to_string(&jobs_processed_data).unwrap_or_else(|_| "[]".to_string()));
        context.insert("job_types_labels", &serde_json::to_string(&job_types_labels).unwrap_or_else(|_| "[]".to_string()));
        context.insert("job_types_data", &serde_json::to_string(&job_types_data).unwrap_or_else(|_| "[]".to_string()));
        context.insert("queue_stats", &queue_stats);
        context.insert("recent_failed_jobs", &recent_failed_jobs);
        context.insert("active_page", "overview");

        // 渲染模板
        let html = state.render_template("overview.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    #[instrument(skip(state), fields(path = "/queues"))]
    async fn queues(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // 使用 Sea-ORM 的原生 SQL 查询功能，仅统计未完成的作业
        // 获取所有的队列名称
        let all_queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            
        let stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT queue_name, COUNT(*) as count
             FROM solid_queue_jobs
             WHERE finished_at IS NULL
             GROUP BY queue_name",
            []
        );

        // 获取有未完成作业的队列计数
        let queue_counts_map: HashMap<String, i64> = db
            .query_all(stmt)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .into_iter()
            .map(|row| {
                let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();
                let count: i64 = row.try_get("", "count").unwrap_or_default();
                (queue_name, count)
            })
            .collect();
            
        // 确保所有队列都包含在结果中，即使没有未完成的作业
        let queue_counts: Vec<(String, i64)> = all_queue_names
            .into_iter()
            .map(|queue_name| (queue_name.clone(), *queue_counts_map.get(&queue_name).unwrap_or(&0)))
            .collect();

        debug!("Fetched jobs in {:?}", start.elapsed());

        let start = Instant::now();
        // Get paused queues
        let paused_queues = solid_queue_pauses::Entity::find()
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        debug!("Fetched paused queues in {:?}", start.elapsed());

        let start = Instant::now();
        let paused_queue_names: Vec<String> = paused_queues.iter()
            .map(|p| p.queue_name.clone())
            .collect();

        let queue_infos: Vec<QueueInfo> = queue_counts
            .into_iter()
            .map(|(name, count)| QueueInfo {
                name: name.clone(),
                jobs_count: count as usize,
                status: if paused_queue_names.contains(&name) {
                    "paused".to_string()
                } else {
                    "active".to_string()
                },
            })
            .collect();
        debug!("Processed queue data in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &1);
        context.insert("queues", &queue_infos);
        context.insert("active_page", "queues");

        let html = state.render_template("queues.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    async fn pause_queue(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Create a new pause record
        let pause = solid_queue_pauses::ActiveModel {
            id: ActiveValue::NotSet,
            queue_name: ActiveValue::Set(queue_name.clone()),
            created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        };

        pause.insert(db).await
            .map(|_| StatusCode::OK)
            .map_err(|e| {
                error!("Failed to pause queue: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    async fn resume_queue(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // Delete the pause record for this queue
        solid_queue_pauses::Entity::delete_many()
            .filter(solid_queue_pauses::Column::QueueName.eq(queue_name.clone()))
            .exec(db)
            .await
            .map(|_| StatusCode::OK)
            .map_err(|e| {
                error!("Failed to resume queue: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs"))]
    async fn failed_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // 计算分页偏移
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // 获取失败的执行记录
        let failed_executions = solid_queue_failed_executions::Entity::find()
            .offset(offset as u64)
            .limit(page_size as u64)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 创建一个存储失败任务信息的向量
        let mut failed_jobs: Vec<FailedJobInfo> = Vec::with_capacity(failed_executions.len());

        // 获取每个失败执行对应的作业信息
        for execution in failed_executions {
            if let Ok(Some(job)) = solid_queue_jobs::Entity::find_by_id(execution.job_id).one(db).await {
                failed_jobs.push(FailedJobInfo {
                    id: job.id,
                    queue_name: job.queue_name.clone(),
                    class_name: job.class_name.clone(),
                    error: execution.error.unwrap_or_default(),
                    failed_at: execution.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                });
            }
        }

        debug!("Fetched failed jobs in {:?}", start.elapsed());
        info!("Found {} failed jobs", failed_jobs.len());

        // 获取失败任务总数以计算分页
        let start = Instant::now();

        // 直接执行 SQL count 查询
        let total_count: i64 = solid_queue_failed_executions::Entity::find()
            .select_only()
            .column_as(sea_orm::sea_query::Expr::expr(sea_orm::sea_query::Func::count(sea_orm::sea_query::Expr::asterisk())), "count")
            .into_tuple()
            .one(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .unwrap_or(0);

        info!("Total failed jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("failed_jobs", &failed_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "failed-jobs");

        // 使用辅助方法渲染模板
        let html = state.render_template("failed-jobs.html", &mut context).await?;

        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/retry"))]
    async fn retry_failed_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Model as FailedExecutionModel, Retryable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // 先获取失败的执行记录
                let failed_execution = FailedExecutionEntity::find()
                    .filter(crate::entities::solid_queue_failed_executions::Column::JobId.eq(id))
                    .one(txn)
                    .await?;

                match failed_execution {
                    Some(execution) => {
                        // 使用 Retryable trait 实现的 retry 方法
                        execution.retry(txn).await?;
                        Ok(())
                    },
                    None => {
                        Err(DbErr::Custom(format!("Failed execution for job {} not found", id)))
                    }
                }
            })
        })
        .await
        .map(|_| {
            info!("Retried failed job {}", id);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to retry job {}: {}", id, e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/:id/delete"))]
    async fn delete_failed_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Model as FailedExecutionModel, Discardable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // 先获取失败的执行记录
                let failed_execution = FailedExecutionEntity::find()
                    .filter(crate::entities::solid_queue_failed_executions::Column::JobId.eq(id))
                    .one(txn)
                    .await?;

                match failed_execution {
                    Some(execution) => {
                        // 使用 Discardable trait 实现的 discard 方法
                        execution.discard(txn).await?;
                        Ok(())
                    },
                    None => {
                        Err(DbErr::Custom(format!("Failed execution for job {} not found", id)))
                    }
                }
            })
        })
        .await
        .map(|_| {
            info!("Deleted failed job {}", id);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to delete job {}: {}", id, e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/retry"))]
    async fn retry_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Retryable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // 使用 Retryable trait 实现的 retry_all 方法
                let count = FailedExecutionEntity.retry_all(txn).await?;
                Ok(count)
            })
        })
        .await
        .map(|count| {
            info!("Retried all {} failed jobs", count);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to retry all jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/failed-jobs/all/delete"))]
    async fn discard_all_failed_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        use crate::entities::solid_queue_failed_executions::{Entity as FailedExecutionEntity, Discardable};

        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // 使用 Discardable trait 实现的 discard_all 方法
                let count = FailedExecutionEntity.discard_all(txn).await?;
                Ok(count)
            })
        })
        .await
        .map(|count| {
            info!("Discarded all {} failed jobs", count);
            (StatusCode::SEE_OTHER, [("Location", "/failed-jobs")])
        })
        .map_err(|e| {
            error!("Failed to discard all jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/failed-jobs")])
        })
    }

    // 添加以下辅助方法，用于提取共用功能

    /// 获取所有唯一的队列名称
    async fn get_queue_names(&self, db: &sea_orm::DatabaseConnection) -> Result<Vec<String>, DbErr> {
        db.query_all(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT DISTINCT queue_name FROM solid_queue_jobs",
            []
        ))
        .await
        .map(|rows| {
            rows.into_iter()
                .filter_map(|row| row.try_get::<String>("", "queue_name").ok())
                .collect()
        })
    }

    /// 获取所有唯一的作业类名
    async fn get_job_classes(&self, db: &sea_orm::DatabaseConnection) -> Result<Vec<String>, DbErr> {
        db.query_all(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT DISTINCT class_name FROM solid_queue_jobs",
            []
        ))
        .await
        .map(|rows| {
            rows.into_iter()
                .filter_map(|row| row.try_get::<String>("", "class_name").ok())
                .collect()
        })
    }

    /// 渲染模板并处理错误
    async fn render_template(&self, template_name: &str, context: &mut tera::Context) -> Result<String, (StatusCode, String)> {
        // 在 debug 编译模式下，重新加载模板
        #[cfg(debug_assertions)]
        {
            debug!("Debug mode detected, reloading templates");
            match Tera::new(&self.template_path) {
                Ok(new_tera) => {
                    // 成功加载新模板，替换现有的 Tera 实例
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
        // 添加数据库连接信息
        let db = self.ctx.get_db().await;

        // 直接从 AppContext 获取数据库 DSN 信息
        let dsn = self.ctx.dsn.to_string();
        debug!("Original DSN: {}", dsn);

        // 处理不同类型的数据库连接
        let (db_type, connection_info) = if dsn.starts_with("postgres:") {
            // 如果是 PostgreSQL数据库
            let clean_dsn = if dsn.contains("@") {
                // 包含用户名密码，需要删除
                let parts: Vec<&str> = dsn.split("@").collect();
                if parts.len() > 1 {
                    // 获取协议部分
                    let protocol = if parts[0].contains("://") {
                        parts[0].split("://").next().unwrap_or("postgres")
                    } else {
                        "postgres"
                    };

                    // 重新构造URL，去掉用户名密码
                    let host_part = parts[1];
                    format!("{0}://{1}", protocol, host_part)
                } else {
                    dsn.clone()
                }
            } else {
                // 不包含用户名密码
                dsn.clone()
            };

            // 如果有查询参数，删除它们
            let final_dsn = if clean_dsn.contains("?") {
                clean_dsn.split("?").next().unwrap_or(&clean_dsn).to_string()
            } else {
                clean_dsn
            };

            debug!("Cleaned PostgreSQL DSN: {}", final_dsn);
            ("PostgreSQL", final_dsn)
        } else if dsn.starts_with("sqlite:") {
            // 如果是 SQLite 数据库
            let path = dsn.replace("sqlite:", "").replace("//", "");
            debug!("SQLite path: {}", path);
            ("SQLite", format!("sqlite://{}", path))
        } else {
            // 其他类型的数据库
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

        // 在 debug 编译模式下，每次渲染模板前重新加载模板文件
        #[cfg(debug_assertions)]
        {
            debug!("Debug mode detected, reloading templates before rendering");
            // 使用 RwLock 的 write 方法获取对 Tera 的可变访问
            if let Ok(mut tera) = self.tera.write() {
                match tera.full_reload() {
                    Ok(_) => debug!("Templates reloaded successfully"),
                    Err(e) => error!("Error reloading templates: {}", e)
                }
            } else {
                error!("Failed to acquire write lock for template reloading");
            }
        }
        
        // 检查模板是否在 Tera 实例中存在
        let tera_read_result = self.tera.read();
        if let Err(e) = tera_read_result {
            error!("Failed to acquire read lock: {}", e);
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to acquire template lock: {}", e)));
        }
        
        let tera = tera_read_result.unwrap();
        let templates = tera.get_template_names().collect::<Vec<_>>();
        debug!("Available templates: {:?}", templates);

        // 尝试渲染模板
        match tera.render(template_name, context) {
            Ok(html) => Ok(html),
            Err(e) => {
                error!("Failed to render {}: {}", template_name, e);

                // 输出完整的错误链
                let mut error_detail = format!("Template error: {}", e);
                let mut current_error = e.source();
                let mut level = 1;

                while let Some(source) = current_error {
                    error_detail.push_str(&format!("\n  Caused by ({}): {}", level, source));
                    current_error = source.source();
                    level += 1;
                }

                // 输出 Tera 特定的错误详情
                error!("Detailed error: {}", error_detail);

                // 输出源模板内容
                let template_path = format!("src/web/templates/{}", template_name);
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

    /// 填充通用的导航统计信息到模板上下文
    async fn populate_nav_stats(&self, db: &sea_orm::DatabaseConnection, context: &mut tera::Context) -> Result<(), DbErr> {
        // 获取失败的作业数
        let failed_jobs_count = solid_queue_failed_executions::Entity::find()
            .count(db)
            .await?;

        // 获取进行中的作业数
        let in_progress_jobs_count = solid_queue_claimed_executions::Entity::find()
            .count(db)
            .await?;

        // 获取计划中的作业数
        let scheduled_jobs_count = solid_queue_scheduled_executions::Entity::find()
            .count(db)
            .await?;

        // 获取被阻塞的作业数
        let blocked_jobs_count = solid_queue_blocked_executions::Entity::find()
            .count(db)
            .await?;

        // 获取已完成的作业数
        let finished_jobs_count = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .count(db)
            .await?;

        let failed_count = if let Ok(count) = usize::try_from(failed_jobs_count) { count } else { 0 };
        let in_progress_count = if let Ok(count) = usize::try_from(in_progress_jobs_count) { count } else { 0 };
        let scheduled_count = if let Ok(count) = usize::try_from(scheduled_jobs_count) { count } else { 0 };
        let blocked_count = if let Ok(count) = usize::try_from(blocked_jobs_count) { count } else { 0 };
        let finished_count = if let Ok(count) = usize::try_from(finished_jobs_count) { count } else { 0 };

        context.insert("failed_jobs_count", &failed_count);
        context.insert("in_progress_jobs_count", &in_progress_count);
        context.insert("scheduled_jobs_count", &scheduled_count);
        context.insert("blocked_jobs_count", &blocked_count);
        context.insert("finished_jobs_count", &finished_count);

        Ok(())
    }

    /// 解析时间戳并格式化为人类可读的形式
    fn format_timestamp(&self, datetime: Result<chrono::NaiveDateTime, DbErr>, format: &str) -> String {
        match datetime {
            Ok(dt) => dt.format(format).to_string(),
            Err(_) => "未知时间".to_string(),
        }
    }

    // 修改 scheduled_jobs 方法，使用这些辅助方法
    #[instrument(skip(state), fields(path = "/scheduled-jobs"))]
    async fn scheduled_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // 计算分页偏移
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // 获取计划中的作业，包含相关信息
        let scheduled_jobs_result = db
            .query_all(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT
                    s.id as execution_id,
                    s.job_id,
                    s.scheduled_at,
                    j.class_name,
                    j.queue_name,
                    j.created_at
                FROM solid_queue_scheduled_executions s
                JOIN solid_queue_jobs j ON s.job_id = j.id
                WHERE j.finished_at IS NULL
                ORDER BY s.scheduled_at ASC
                LIMIT $1 OFFSET $2",
                [
                    Value::from(page_size as i32),
                    Value::from(offset as i32)
                ]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 获取当前时间，用于计算距离执行还有多长时间
        let now = chrono::Utc::now().naive_utc();

        let mut scheduled_jobs = Vec::with_capacity(scheduled_jobs_result.len());
        for row in scheduled_jobs_result {
            let execution_id: i64 = row.try_get("", "execution_id").unwrap_or_default();
            let job_id: i64 = row.try_get("", "job_id").unwrap_or_default();
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let queue_name: String = row.try_get("", "queue_name").unwrap_or_default();

            // 解析创建时间
            let created_at_str = match row.try_get::<chrono::NaiveDateTime>("", "created_at") {
                Ok(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                Err(_) => "未知时间".to_string(),
            };

            // 解析计划执行时间
            let scheduled_at = match row.try_get::<chrono::NaiveDateTime>("", "scheduled_at") {
                Ok(dt) => {
                    // 计算还有多长时间执行
                    let scheduled_in = if dt > now {
                        let duration = dt.signed_duration_since(now);
                        if duration.num_hours() > 0 {
                            format!("in {}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                        } else if duration.num_minutes() > 0 {
                            format!("in {}m {}s", duration.num_minutes(), duration.num_seconds() % 60)
                        } else {
                            format!("in {}s", duration.num_seconds())
                        }
                    } else {
                        "overdue".to_string()
                    };

                    (dt.format("%Y-%m-%d %H:%M:%S").to_string(), scheduled_in)
                },
                Err(_) => ("未知时间".to_string(), "未知".to_string()),
            };

            scheduled_jobs.push(ScheduledJobInfo {
                id: execution_id,
                job_id,
                queue_name,
                class_name,
                created_at: created_at_str,
                scheduled_at: scheduled_at.0,
                scheduled_in: scheduled_at.1,
            });
        }

        debug!("Fetched scheduled jobs in {:?}", start.elapsed());

        // 获取计划作业总数以计算分页
        let total_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_scheduled_executions s
                 JOIN solid_queue_jobs j ON s.job_id = j.id
                 WHERE j.finished_at IS NULL",
                []
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }

        // 使用抽象方法获取所有队列名称和作业类
        let queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("scheduled_jobs", &scheduled_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);
        context.insert("active_page", "scheduled-jobs");

        let html = state.render_template("scheduled-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // 修改 in_progress_jobs 方法，使用辅助方法
    #[instrument(skip(state), fields(path = "/in-progress-jobs"))]
    async fn in_progress_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // 计算分页偏移
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // 获取声明（正在执行）的作业
        let claimed_executions = crate::entities::solid_queue_claimed_executions::Entity::find()
            .offset(offset as u64)
            .limit(page_size as u64)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 获取进程信息
        let processes = crate::entities::solid_queue_processes::Entity::find()
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 创建一个存储正在进行中作业信息的向量
        let mut in_progress_jobs: Vec<InProgressJobInfo> = Vec::with_capacity(claimed_executions.len());

        // 获取当前时间，用于计算运行时间
        let now = chrono::Utc::now().naive_utc();

        // 获取每个正在进行中执行对应的作业信息
        for execution in claimed_executions {
            if let Ok(Some(job)) = solid_queue_jobs::Entity::find_by_id(execution.job_id).one(db).await {
                // 查找进程信息
                let worker_id = match execution.process_id {
                    Some(pid) => {
                        let process = processes.iter().find(|p| p.id == pid);
                        match process {
                            Some(p) => format!("{} ({})", p.name, p.hostname.clone().unwrap_or_default()),
                            None => format!("Unknown (PID: {})", pid),
                        }
                    },
                    None => "Unknown".to_string(),
                };

                // 计算运行时间
                let runtime = match now.signed_duration_since(execution.created_at) {
                    duration if duration.num_hours() >= 1 => {
                        format!("{}h {}m", duration.num_hours(), duration.num_minutes() % 60)
                    },
                    duration if duration.num_minutes() >= 1 => {
                        format!("{}m {}s", duration.num_minutes(), duration.num_seconds() % 60)
                    },
                    duration => {
                        format!("{}s", duration.num_seconds())
                    },
                };

                in_progress_jobs.push(InProgressJobInfo {
                    id: execution.id,
                    job_id: execution.job_id,
                    queue_name: job.queue_name.clone(),
                    class_name: job.class_name.clone(),
                    worker_id,
                    started_at: execution.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                    runtime,
                });
            }
        }

        debug!("Fetched in-progress jobs in {:?}", start.elapsed());
        info!("Found {} in-progress jobs", in_progress_jobs.len());

        // 获取正在进行中任务总数以计算分页
        let start = Instant::now();

        // 执行 SQL count 查询
        let total_count: i64 = crate::entities::solid_queue_claimed_executions::Entity::find()
            .select_only()
            .column_as(sea_orm::sea_query::Expr::expr(sea_orm::sea_query::Func::count(sea_orm::sea_query::Expr::asterisk())), "count")
            .into_tuple()
            .one(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .unwrap_or(0);

        info!("Total in-progress jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        // 使用抽象方法获取所有队列名称和作业类
        let queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("in_progress_jobs", &in_progress_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "in-progress-jobs");
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);

        let html = state.render_template("in-progress-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // 实现取消单个正在进行中作业的方法
    #[instrument(skip(state), fields(path = "/in-progress-jobs/:id/cancel"))]
    async fn cancel_in_progress_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // 查找需要取消的执行记录
                let claimed_execution = crate::entities::solid_queue_claimed_executions::Entity::find_by_id(id)
                    .one(txn)
                    .await?;

                if let Some(execution) = claimed_execution {
                    // 首先将作业标记为已完成
                    let job_result = solid_queue_jobs::Entity::find_by_id(execution.job_id)
                        .one(txn)
                        .await?;

                    if let Some(job) = job_result {
                        // 更新作业状态为已完成
                        let mut job_model: solid_queue_jobs::ActiveModel = job.into();
                        job_model.finished_at = ActiveValue::Set(Some(chrono::Utc::now().naive_utc()));
                        job_model.updated_at = ActiveValue::Set(chrono::Utc::now().naive_utc());
                        job_model.update(txn).await?;
                    }

                    // 删除claimed_execution记录
                    crate::entities::solid_queue_claimed_executions::Entity::delete_by_id(id)
                        .exec(txn)
                        .await?;

                    info!("Cancelled in-progress job ID: {}", id);
                } else {
                    return Err(DbErr::Custom(format!("In-progress job with ID {} not found", id)));
                }

                Ok(())
            })
        })
        .await
        .map(|_| {
            info!("Cancelled in-progress job ID: {}", id);
            (StatusCode::SEE_OTHER, [("Location", "/in-progress-jobs")])
        })
        .map_err(|e| {
            error!("Failed to cancel in-progress job {}: {}", id, e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/in-progress-jobs")])
        })
    }

    // 实现 blocked_jobs 方法，显示所有被阻塞的作业
    #[instrument(skip(state), fields(path = "/blocked-jobs"))]
    async fn blocked_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // 计算分页偏移
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // 获取被阻塞的作业
        let blocked_executions = crate::entities::solid_queue_blocked_executions::Entity::find()
            .offset(offset as u64)
            .limit(page_size as u64)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 创建一个存储被阻塞作业信息的向量
        let mut blocked_jobs: Vec<BlockedJobInfo> = Vec::with_capacity(blocked_executions.len());

        // 获取当前时间，用于计算等待时间
        let now = chrono::Utc::now().naive_utc();

        // 获取每个被阻塞执行对应的作业信息
        for execution in blocked_executions {
            if let Ok(Some(job)) = solid_queue_jobs::Entity::find_by_id(execution.job_id).one(db).await {
                // 计算等待时间
                let waiting_time = match now.signed_duration_since(execution.created_at) {
                    duration if duration.num_hours() >= 1 => {
                        format!("{}小时 {}分钟", duration.num_hours(), duration.num_minutes() % 60)
                    },
                    duration if duration.num_minutes() >= 1 => {
                        format!("{}分钟 {}秒", duration.num_minutes(), duration.num_seconds() % 60)
                    },
                    duration => {
                        format!("{}秒", duration.num_seconds())
                    },
                };

                blocked_jobs.push(BlockedJobInfo {
                    id: execution.id,
                    job_id: execution.job_id,
                    queue_name: execution.queue_name.clone(),
                    class_name: job.class_name.clone(),
                    concurrency_key: execution.concurrency_key.clone(),
                    created_at: execution.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                    waiting_time,
                    expires_at: execution.expires_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                });
            }
        }

        debug!("Fetched blocked jobs in {:?}", start.elapsed());
        info!("Found {} blocked jobs", blocked_jobs.len());

        // 获取被阻塞任务总数以计算分页
        let start = Instant::now();

        // 执行 SQL count 查询
        let total_count: i64 = crate::entities::solid_queue_blocked_executions::Entity::find()
            .select_only()
            .column_as(sea_orm::sea_query::Expr::expr(sea_orm::sea_query::Func::count(sea_orm::sea_query::Expr::asterisk())), "count")
            .into_tuple()
            .one(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .unwrap_or(0);

        info!("Total blocked jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        // 使用抽象方法获取所有队列名称和作业类
        let queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("blocked_jobs", &blocked_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "blocked-jobs");
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);

        let html = state.render_template("blocked-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // 实现解除单个被阻塞作业的方法
    #[instrument(skip(state), fields(path = "/blocked-jobs/:id/unblock"))]
    async fn unblock_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // 查找被阻塞的执行
                let blocked_execution = solid_queue_blocked_executions::Entity::find_by_id(id)
                    .one(txn)
                    .await?
                    .ok_or_else(|| DbErr::Custom(format!("Blocked execution with ID {} not found", id)))?;

                // 删除被阻塞的执行
                solid_queue_blocked_executions::Entity::delete_by_id(id)
                    .exec(txn)
                    .await?;

                info!("Unblocked job ID: {}", id);
                Ok(())
            })
        })
        .await
        .map(|_| {
            (StatusCode::SEE_OTHER, "/blocked-jobs".to_string())
        })
        .map_err(|e| {
            error!("Failed to unblock job {}: {}", id, e);
            match e.to_string() {
                s if s.contains("not found") => (StatusCode::NOT_FOUND, "/blocked-jobs".to_string()),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "/blocked-jobs".to_string())
            }
        })
    }

    // 实现取消单个被阻塞作业的方法
    #[instrument(skip(state), fields(path = "/blocked-jobs/:id/cancel"))]
    async fn cancel_blocked_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // 查找被阻塞的执行
                let blocked_execution = solid_queue_blocked_executions::Entity::find_by_id(id)
                    .one(txn)
                    .await?
                    .ok_or_else(|| DbErr::Custom(format!("Blocked execution with ID {} not found", id)))?;

                let job_id = blocked_execution.job_id;

                // 删除被阻塞的执行
                solid_queue_blocked_executions::Entity::delete_by_id(id)
                    .exec(txn)
                    .await?;

                // 删除相关的作业
                solid_queue_jobs::Entity::delete_by_id(job_id)
                    .exec(txn)
                    .await?;

                info!("Cancelled blocked job ID: {}, job ID: {}", id, job_id);
                Ok(())
            })
        })
        .await
        .map(|_| {
            (StatusCode::SEE_OTHER, "/blocked-jobs".to_string())
        })
        .map_err(|e| {
            error!("Failed to cancel blocked job {}: {}", id, e);
            match e.to_string() {
                s if s.contains("not found") => (StatusCode::NOT_FOUND, "/blocked-jobs".to_string()),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "/blocked-jobs".to_string())
            }
        })
    }

    // 实现解除所有被阻塞作业的方法
    #[instrument(skip(state), fields(path = "/blocked-jobs/all/unblock"))]
    async fn unblock_all_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // 删除所有被阻塞的执行
                let result = solid_queue_blocked_executions::Entity::delete_many()
                    .exec(txn)
                    .await?;

                let count = result.rows_affected;
                info!("Unblocked all {} blocked jobs", count);
                Ok(count)
            })
        })
        .await
        .map(|count| {
            (StatusCode::SEE_OTHER, "/blocked-jobs".to_string())
        })
        .map_err(|e| {
            error!("Failed to unblock all jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "/blocked-jobs".to_string())
        })
    }

    // 实现取消所有正在进行中作业的方法
    #[instrument(skip(state), fields(path = "/in-progress-jobs/all/cancel"))]
    async fn cancel_all_in_progress_jobs(
        State(state): State<Arc<ControlPlane>>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        db.transaction::<_, u64, DbErr>(|txn| {
            Box::pin(async move {
                // 获取所有正在进行中的作业
                let claimed_executions = crate::entities::solid_queue_claimed_executions::Entity::find()
                    .all(txn)
                    .await?;

                if claimed_executions.is_empty() {
                    return Ok(0);
                }

                let job_ids: Vec<i64> = claimed_executions.iter()
                    .map(|execution| execution.job_id)
                    .collect();

                // 更新所有相关作业为已完成状态
                let now = chrono::Utc::now().naive_utc();

                // 使用批量更新
                let update_sql = r#"
                    UPDATE solid_queue_jobs
                    SET finished_at = $1, updated_at = $1
                    WHERE id = ANY($2)
                "#;

                let stmt = sea_orm::Statement::from_sql_and_values(
                    sea_orm::DbBackend::Postgres,
                    update_sql,
                    [now.into(), job_ids.into()]
                );

                txn.execute(stmt).await?;

                // 删除所有claimed_execution记录
                let delete_result = crate::entities::solid_queue_claimed_executions::Entity::delete_many()
                    .exec(txn)
                    .await?;

                let count = delete_result.rows_affected;
                info!("Cancelled all {} in-progress jobs", count);
                Ok(count)
            })
        })
        .await
        .map(|count| {
            info!("Cancelled all {} in-progress jobs", count);
            (StatusCode::SEE_OTHER, [("Location", "/in-progress-jobs")])
        })
        .map_err(|e| {
            error!("Failed to cancel all in-progress jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, [("Location", "/in-progress-jobs")])
        })
    }

    #[instrument(skip(state), fields(path = "/queues/:name"))]
    async fn queue_details(
        State(state): State<Arc<ControlPlane>>,
        Path(queue_name): Path<String>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // 计算分页偏移
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // 获取队列状态信息
        let is_paused = solid_queue_pauses::Entity::find()
            .filter(solid_queue_pauses::Column::QueueName.eq(queue_name.clone()))
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            > 0;

        let status = if is_paused { "paused" } else { "active" };

        // 获取队列中未完成的作业总数
        let total_count = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::QueueName.eq(queue_name.clone()))
            .filter(solid_queue_jobs::Column::FinishedAt.is_null())
            .count(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 等待中的作业数（未完成且未被其他表关联的）
        let waiting_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_jobs j
                 WHERE j.queue_name = $1
                 AND j.finished_at IS NULL
                 AND NOT EXISTS (SELECT 1 FROM solid_queue_claimed_executions c WHERE c.job_id = j.id)
                 AND NOT EXISTS (SELECT 1 FROM solid_queue_failed_executions f WHERE f.job_id = j.id)
                 AND NOT EXISTS (SELECT 1 FROM solid_queue_scheduled_executions s WHERE s.job_id = j.id)",
                [Value::from(queue_name.clone())]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        // 查询正在处理中的作业数（使用claimed_executions表，且未完成）
        let processing_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_claimed_executions c
                 JOIN solid_queue_jobs j ON c.job_id = j.id
                 WHERE j.queue_name = $1
                 AND j.finished_at IS NULL",
                [Value::from(queue_name.clone())]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        // 获取队列中的作业，包括作业状态（只显示未完成的作业）
        let jobs_result = db
            .query_all(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT
                    j.id,
                    j.class_name,
                    j.created_at,
                    CASE
                        WHEN c.id IS NOT NULL THEN 'processing'
                        WHEN f.id IS NOT NULL THEN 'failed'
                        WHEN s.id IS NOT NULL THEN 'scheduled'
                        ELSE 'pending'
                    END AS status
                FROM solid_queue_jobs j
                LEFT JOIN solid_queue_claimed_executions c ON j.id = c.job_id
                LEFT JOIN solid_queue_failed_executions f ON j.id = f.job_id
                LEFT JOIN solid_queue_scheduled_executions s ON j.id = s.job_id
                WHERE j.queue_name = $1
                AND j.finished_at IS NULL
                ORDER BY j.created_at DESC
                LIMIT $2 OFFSET $3",
                [
                    Value::from(queue_name.clone()),
                    Value::from(page_size as i32),
                    Value::from(offset as i32)
                ]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut jobs = Vec::with_capacity(jobs_result.len());
        for row in jobs_result {
            let id: i64 = row.try_get("", "id").unwrap_or_default();
            let class_name: String = row.try_get("", "class_name").unwrap_or_default();
            let status: String = row.try_get("", "status").unwrap_or_default();

            // 获取时间并记录调试信息
            let created_at_debug = match row.try_get::<chrono::NaiveDateTime>("", "created_at") {
                Ok(dt) => format!("{:?}", dt),
                Err(_) => format!("无法解析时间")
            };
            debug!("Job {}, created_at value: {}", id, created_at_debug);

            // 尝试多种方式解析时间
            let created_at_str = match row.try_get::<chrono::NaiveDateTime>("", "created_at") {
                Ok(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                Err(e1) => {
                    debug!("Failed to parse as NaiveDateTime: {}", e1);
                    match row.try_get::<DateTime<chrono::Utc>>("", "created_at") {
                        Ok(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                        Err(e2) => {
                            debug!("Failed to parse as DateTime<Utc>: {}", e2);
                            match row.try_get::<String>("", "created_at") {
                                Ok(s) => s,
                                Err(e3) => {
                                    debug!("Failed to parse as String: {}", e3);
                                    "未知时间".to_string()
                                }
                            }
                        }
                    }
                }
            };

            jobs.push(QueueJobInfo {
                id,
                class_name,
                status,
                created_at: created_at_str,
                execution_id: None,
            });
        }

        debug!("Fetched queue details in {:?}", start.elapsed());

        // 计算分页 - 使用未完成的作业数
        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }

        // 使用SQL统计失败的作业数（未完成的）
        let failed_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_failed_executions f
                 JOIN solid_queue_jobs j ON f.job_id = j.id
                 WHERE j.queue_name = $1
                 AND j.finished_at IS NULL",
                [Value::from(queue_name.clone())]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        // 使用SQL统计计划中的作业数（未完成的）
        let scheduled_count = db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count
                 FROM solid_queue_scheduled_executions s
                 JOIN solid_queue_jobs j ON s.job_id = j.id
                 WHERE j.queue_name = $1
                 AND j.finished_at IS NULL",
                [Value::from(queue_name.clone())]
            ))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map(|row| row.try_get::<i64>("", "count").unwrap_or(0))
            .unwrap_or(0);

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("queue_name", &queue_name);
        context.insert("status", &status);
        context.insert("jobs", &jobs);
        context.insert("waiting_count", &(waiting_count as usize));
        context.insert("processing_count", &(processing_count as usize));
        context.insert("scheduled_count", &(scheduled_count as usize));
        context.insert("failed_count", &(failed_count as usize));
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("total_count", &(total_count as usize));
        context.insert("active_page", "queues");

        let html = state.render_template("queue-details.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // 实现 finished_jobs 方法，显示所有已完成的作业
    #[instrument(skip(state), fields(path = "/finished-jobs"))]
    async fn finished_jobs(
        State(state): State<Arc<ControlPlane>>,
        Query(pagination): Query<Pagination>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let start = Instant::now();

        // 计算分页偏移
        let page_size = state.page_size;
        let offset = (pagination.page - 1) * page_size;

        // 获取已完成的作业
        let finished_jobs_query = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .order_by_desc(solid_queue_jobs::Column::FinishedAt)
            .offset(offset as u64)
            .limit(page_size as u64);

        let finished_jobs_models = finished_jobs_query
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 创建一个存储已完成作业信息的向量
        let mut finished_jobs: Vec<FinishedJobInfo> = Vec::with_capacity(finished_jobs_models.len());

        // 获取每个已完成作业的信息
        for job in finished_jobs_models {
            if let Some(finished_at) = job.finished_at {
                // 计算运行时间
                let runtime = match finished_at.signed_duration_since(job.created_at) {
                    duration if duration.num_hours() >= 1 => {
                        format!("{}小时 {}分钟", duration.num_hours(), duration.num_minutes() % 60)
                    },
                    duration if duration.num_minutes() >= 1 => {
                        format!("{}分钟 {}秒", duration.num_minutes(), duration.num_seconds() % 60)
                    },
                    duration => {
                        format!("{}秒", duration.num_seconds())
                    },
                };

                finished_jobs.push(FinishedJobInfo {
                    id: job.id,
                    queue_name: job.queue_name,
                    class_name: job.class_name,
                    created_at: job.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                    finished_at: finished_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                    runtime,
                });
            }
        }

        debug!("Fetched finished jobs in {:?}", start.elapsed());
        info!("Found {} finished jobs", finished_jobs.len());

        // 获取已完成任务总数以计算分页
        let start = Instant::now();

        // 执行 SQL count 查询
        let total_count: i64 = solid_queue_jobs::Entity::find()
            .filter(solid_queue_jobs::Column::FinishedAt.is_not_null())
            .select_only()
            .column_as(sea_orm::sea_query::Expr::expr(sea_orm::sea_query::Func::count(sea_orm::sea_query::Expr::asterisk())), "count")
            .into_tuple()
            .one(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .unwrap_or(0);

        info!("Total finished jobs count: {}", total_count);

        let total_pages = ((total_count as f64) / (page_size as f64)).ceil() as usize;
        if total_pages > 0 && pagination.page > total_pages {
            return Err((StatusCode::NOT_FOUND, "Page not found".to_string()));
        }
        debug!("Fetched count in {:?}", start.elapsed());

        // 使用抽象方法获取所有队列名称和作业类
        let queue_names = state.get_queue_names(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let job_classes = state.get_job_classes(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let start = Instant::now();
        let mut context = tera::Context::new();
        context.insert("finished_jobs", &finished_jobs);
        context.insert("current_page_num", &pagination.page);
        context.insert("total_pages", &total_pages);
        context.insert("active_page", "finished-jobs");
        context.insert("queue_names", &queue_names);
        context.insert("job_classes", &job_classes);

        let html = state.render_template("finished-jobs.html", &mut context).await?;
        debug!("Template rendering completed in {:?}", start.elapsed());

        Ok(Html(html))
    }

    // 实现 stats 方法，专门用于返回统计数据
    async fn stats(
        State(state): State<Arc<ControlPlane>>,
        req: axum::http::Request<axum::body::Body>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        let mut context = tera::Context::new();

        // 使用 populate_nav_stats 方法填充统计信息
        state.populate_nav_stats(db, &mut context)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 直接返回 Turbo Stream 格式的响应
        let html = state.render_template("stats.html", &mut context).await?;

        // 设置正确的 Content-Type
        let response = axum::response::Response::builder()
            .header("Content-Type", "text/vnd.turbo-stream.html")
            .body(axum::body::Body::from(html))
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        Ok(response)
    }

    // 实现 workers 方法，用于显示所有 worker 进程
    #[instrument(skip(state), fields(path = "/workers"))]
    async fn workers(
        State(state): State<Arc<ControlPlane>>,
    ) -> Result<Html<String>, (StatusCode, String)> {
        let start = Instant::now();
        let db = state.ctx.get_db().await;
        let db = db.as_ref();
        debug!("Database connection obtained in {:?}", start.elapsed());

        // 查询所有 worker 进程
        let workers = solid_queue_processes::Entity::find()
            .order_by(solid_queue_processes::Column::LastHeartbeatAt, Order::Desc)
            .all(db)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // 为每个 worker 计算距离上次心跳的时间
        let workers_info: Vec<WorkerInfo> = workers.into_iter().map(|worker| {
            let last_heartbeat = worker.last_heartbeat_at;
            let now = chrono::Utc::now().naive_utc();
            // 计算时间差（秒）
            let duration = now.signed_duration_since(last_heartbeat);
            let seconds_since_heartbeat = duration.num_seconds();

            // 如果超过 30 秒没有心跳，认为 worker 已经挂掉
            let status = if seconds_since_heartbeat > 30 {
                "dead"
            } else {
                "alive"
            };

            WorkerInfo {
                id: worker.id,
                name: worker.name,
                kind: worker.kind,
                hostname: worker.hostname,
                pid: worker.pid,
                last_heartbeat_at: last_heartbeat.format("%Y-%m-%d %H:%M:%S").to_string(),
                seconds_since_heartbeat,
                status: status.to_string(),
            }
        }).collect();

        let mut context = tera::Context::new();
        context.insert("workers", &workers_info);
        context.insert("active_page", "workers");

        let html = state.render_template("workers.html", &mut context).await?;

        Ok(Html(html))
    }

    // 添加取消计划作业的控制器方法
    #[instrument(skip(state), fields(path = "/scheduled-jobs/:id/cancel"))]
    async fn cancel_scheduled_job(
        State(state): State<Arc<ControlPlane>>,
        Path(id): Path<i64>,
    ) -> impl IntoResponse {
        let db = state.ctx.get_db().await;
        let db = db.as_ref();

        // 使用事务进行操作
        let txn_result = db.transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                // 查找需要取消的计划执行记录
                let scheduled_execution = solid_queue_scheduled_executions::Entity::find_by_id(id)
                    .one(txn)
                    .await?;

                if let Some(execution) = scheduled_execution {
                    // 首先将作业标记为已完成
                    let job_result = solid_queue_jobs::Entity::find_by_id(execution.job_id)
                        .one(txn)
                        .await?;

                    if let Some(job) = job_result {
                        // 更新作业状态为已完成
                        let mut job_model: solid_queue_jobs::ActiveModel = job.into();
                        job_model.finished_at = ActiveValue::Set(Some(chrono::Utc::now().naive_utc()));
                        job_model.updated_at = ActiveValue::Set(chrono::Utc::now().naive_utc());
                        job_model.update(txn).await?;
                    }

                    // 删除scheduled_execution记录
                    solid_queue_scheduled_executions::Entity::delete_by_id(id)
                        .exec(txn)
                        .await?;

                    info!("Cancelled scheduled job ID: {}", id);
                } else {
                    return Err(DbErr::Custom(format!("Scheduled job with ID {} not found", id)));
                }

                Ok(())
            })
        }).await;

        match txn_result {
            Ok(_) => (StatusCode::SEE_OTHER, "/scheduled-jobs".to_string()),
            Err(e) => {
                error!("Failed to cancel scheduled job {}: {}", id, e);
                (StatusCode::INTERNAL_SERVER_ERROR, "/scheduled-jobs".to_string())
            }
        }
    }
}

pub trait ControlPlaneExt {
    fn start_control_plane(&self, addr: String) -> tokio::task::JoinHandle<()>;
}

impl ControlPlaneExt for Arc<AppContext> {
    fn start_control_plane(&self, addr: String) -> tokio::task::JoinHandle<()> {
        let ctx = self.clone();
        let addr = addr.clone();
        let app = ControlPlane::new(ctx.clone()).router();
        let graceful_shutdown = ctx.graceful_shutdown.clone();
        let force_quit = ctx.force_quit.clone();

        tokio::spawn(async move {
            info!("Starting control plane on http://{}", addr);

            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => {
                    debug!("Control plane successfully bound to {}", addr);
                    l
                },
                Err(e) => {
                    error!("Failed to bind control plane to {}: {}", addr, e);
                    return;
                }
            };

            let server = axum::serve(listener, app);

            let shutdown_future = async move {
                tokio::select! {
                    _ = graceful_shutdown.cancelled() => {
                        debug!("Control plane received graceful shutdown signal");
                        sleep(Duration::from_secs(1)).await;
                        debug!("Control plane graceful shutdown completed");
                    }
                    _ = force_quit.cancelled() => {
                        warn!("Control plane received force quit signal");
                    }
                }
            };

            match server.with_graceful_shutdown(shutdown_future).await {
                Ok(_) => debug!("Control plane server shut down successfully"),
                Err(e) => error!("Control plane server error during shutdown: {}", e),
            }
        })
    }
}
