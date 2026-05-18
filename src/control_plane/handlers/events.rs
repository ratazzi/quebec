use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use futures::stream::Stream;
use std::sync::Arc;
use tracing::debug;

use crate::control_plane::ControlPlane;

impl ControlPlane {
    pub async fn events(
        State(state): State<Arc<ControlPlane>>,
    ) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
        let shutdown = state.ctx.graceful_shutdown.clone();
        let sse_interval = state.sse_interval;
        let stream = async_stream::stream! {
            let mut interval = tokio::time::interval(sse_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = shutdown.cancelled() => {
                        debug!("SSE stream closing due to shutdown");
                        break;
                    }
                }

                // Nav stats are added implicitly by render_template. Stream
                // extras (workers, ...) are this loop's explicit
                // responsibility: failure here skips a tick rather than
                // blowing up unrelated page renders elsewhere.
                let mut context = tera::Context::new();
                if let Err(e) = state.populate_stream_extras(&mut context).await {
                    debug!("SSE stream extras error: {}", e);
                    continue;
                }
                match state.render_template("stats.html", &mut context).await {
                    Ok(html) => {
                        yield Ok(Event::default().event("message").data(html));
                    }
                    Err(e) => {
                        debug!("SSE render error: {:?}", e);
                    }
                }
            }
        };

        Sse::new(stream).keep_alive(KeepAlive::default())
    }
}
