use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::{
    Router,
    routing::{get, post},
};
use tera::Tera;
use tower_http::trace::{self, TraceLayer};
use tracing::{Level, Span, debug, info, error};

use crate::context::AppContext;

pub mod models;
pub mod handlers;
pub mod utils;
pub mod ext;
pub mod templates;

pub use ext::ControlPlaneExt;

pub struct ControlPlane {
    pub(crate) ctx: Arc<AppContext>,
    pub(crate) tera: RwLock<Tera>,
    pub(crate) template_path: String,
    pub(crate) page_size: u64,
}

impl ControlPlane {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        let start = Instant::now();

        // Initialize Tera
        let mut tera = Tera::default();

        // Add all templates
        for template_name in templates::list_templates() {
            if let Some(content) = templates::get_template_content(&template_name) {
                if let Err(e) = tera.add_raw_template(&template_name, &content) {
                    error!("Failed to add template {}: {}", template_name, e);
                }
            }
        }

        // Set automatic HTML escaping
        tera.autoescape_on(vec!["html"]);

        // Output final template list
        let template_list = tera.get_template_names().collect::<Vec<_>>();
        debug!("Available templates: {:?}", template_list);
        debug!("Tera template engine initialized in {:?}", start.elapsed());

        // Save template path for hot reload
        let template_path = "src/templates/**/*".to_string();

        Self {
            ctx,
            tera: RwLock::new(tera),
            template_path,
            page_size: 10,
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
            .route("/jobs/:id", get(Self::job_details))
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
}
