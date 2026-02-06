use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::{
    extract::Path,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use http_body_util::BodyExt;
use tera::Tera;
use tower::{Service, ServiceExt};
use tower_http::trace::{self, TraceLayer};
use tracing::{debug, error, info, Level, Span};

use crate::context::AppContext;

pub mod ext;
pub mod handlers;
pub mod models;
pub mod templates;
pub mod utils;

pub use ext::ControlPlaneExt;

pub struct ControlPlane {
    pub(crate) ctx: Arc<AppContext>,
    pub(crate) tera: RwLock<Tera>,
    #[cfg(debug_assertions)]
    pub(crate) template_path: String,
    pub(crate) page_size: u64,
    pub(crate) base_path: String,
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

        Self {
            ctx,
            tera: RwLock::new(tera),
            #[cfg(debug_assertions)]
            template_path: "src/templates/**/*".to_string(),
            page_size: 10,
            base_path: String::new(),
        }
    }

    pub fn with_base_path(mut self, base_path: String) -> Self {
        self.base_path = base_path;
        self
    }

    async fn static_file(
        Path(filename): Path<String>,
    ) -> Result<([(http::header::HeaderName, &'static str); 2], Vec<u8>), StatusCode> {
        debug!("static_file requested: {}", filename);

        // Reject path traversal and non-alphanumeric filenames
        if !filename
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'_')
        {
            return Err(StatusCode::BAD_REQUEST);
        }

        let content_type = if filename.ends_with(".js") {
            "application/javascript"
        } else if filename.ends_with(".css") {
            "text/css"
        } else {
            "application/octet-stream"
        };

        let data = {
            #[cfg(debug_assertions)]
            {
                let path = format!("src/control_plane/static/{}", filename);
                tokio::fs::read(&path)
                    .await
                    .map_err(|_| StatusCode::NOT_FOUND)?
            }

            #[cfg(not(debug_assertions))]
            {
                templates::StaticAssets::get(&filename)
                    .map(|asset| asset.data.to_vec())
                    .ok_or(StatusCode::NOT_FOUND)?
            }
        };

        Ok((
            [
                (http::header::CONTENT_TYPE, content_type),
                (
                    http::header::CACHE_CONTROL,
                    "public, max-age=31536000, immutable",
                ),
            ],
            data,
        ))
    }

    pub fn build_router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/static/:filename", get(Self::static_file))
            .route("/", get(Self::overview))
            .route("/queues", get(Self::queues))
            .route("/queues/:name", get(Self::queue_details))
            .route("/queues/:name/pause", post(Self::pause_queue))
            .route("/queues/:name/resume", post(Self::resume_queue))
            .route("/failed-jobs", get(Self::failed_jobs))
            .route("/failed-jobs/:id/retry", post(Self::retry_failed_job))
            .route("/failed-jobs/:id/delete", post(Self::delete_failed_job))
            .route("/failed-jobs/all/retry", post(Self::retry_all_failed_jobs))
            .route(
                "/failed-jobs/all/delete",
                post(Self::discard_all_failed_jobs),
            )
            .route("/in-progress-jobs", get(Self::in_progress_jobs))
            .route(
                "/in-progress-jobs/:id/cancel",
                post(Self::cancel_in_progress_job),
            )
            .route(
                "/in-progress-jobs/all/cancel",
                post(Self::cancel_all_in_progress_jobs),
            )
            .route("/blocked-jobs", get(Self::blocked_jobs))
            .route("/blocked-jobs/:id/unblock", post(Self::unblock_job))
            .route("/blocked-jobs/:id/cancel", post(Self::cancel_blocked_job))
            .route("/blocked-jobs/all/unblock", post(Self::unblock_all_jobs))
            .route("/scheduled-jobs", get(Self::scheduled_jobs))
            .route(
                "/scheduled-jobs/:id/cancel",
                post(Self::cancel_scheduled_job),
            )
            .route("/recurring-jobs", get(Self::recurring_jobs))
            .route(
                "/recurring-jobs/schedule",
                get(Self::recurring_jobs_schedule),
            )
            .route("/recurring-jobs/:id/run", post(Self::run_recurring_job_now))
            .route("/finished-jobs", get(Self::finished_jobs))
            .route("/jobs/:id", get(Self::job_details))
            .route("/stats", get(Self::stats))
            .route("/workers", get(Self::workers))
            .route("/health", get(Self::health))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                    .on_request(|request: &axum::http::Request<_>, _span: &Span| {
                        debug!("Started {} {}", request.method(), request.uri());
                    })
                    .on_response(
                        |response: &axum::http::Response<_>, latency: Duration, _span: &Span| {
                            info!(
                                "Finished in {:?} with status {}",
                                latency,
                                response.status()
                            );
                        },
                    )
                    .on_failure(trace::DefaultOnFailure::new().level(Level::ERROR)),
            )
            .with_state(self)
    }

    /// Handle a single HTTP request via the router (for ASGI bridge).
    pub async fn handle_request(
        router: &mut Router,
        method: &str,
        uri: &str,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
        body: Vec<u8>,
    ) -> (u16, Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>) {
        let mut builder = http::Request::builder()
            .method(http::Method::from_bytes(method.as_bytes()).unwrap_or(http::Method::GET))
            .uri(uri);

        for (name, value) in &headers {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::from_bytes(name),
                http::header::HeaderValue::from_bytes(value),
            ) {
                builder = builder.header(name, value);
            }
        }

        let request = builder
            .body(axum::body::Body::from(body))
            .unwrap_or_else(|_| http::Request::new(axum::body::Body::empty()));

        let mut service = router.as_service();
        let ready = match service.ready().await {
            Ok(svc) => svc,
            Err(err) => {
                error!("Router service not ready: {}", err);
                return (500, vec![], b"Internal Server Error".to_vec());
            }
        };
        let response = ready.call(request).await.unwrap_or_else(|err| {
            error!("Router call error: {}", err);
            http::Response::builder()
                .status(500)
                .body(axum::body::Body::from("Internal Server Error"))
                .unwrap()
        });

        let status = response.status().as_u16();
        let resp_headers: Vec<(Vec<u8>, Vec<u8>)> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().as_bytes().to_vec(), v.as_bytes().to_vec()))
            .collect();
        let resp_body = response
            .into_body()
            .collect()
            .await
            .map(|b| b.to_bytes().to_vec())
            .unwrap_or_default();

        (status, resp_headers, resp_body)
    }
}
