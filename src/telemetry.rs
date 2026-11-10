//! Logging setup and optional OpenTelemetry trace export.
//!
//! Traces are exported over OTLP/HTTP only when a standard endpoint variable
//! is set (`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or
//! `OTEL_EXPORTER_OTLP_ENDPOINT`) and `OTEL_SDK_DISABLED` is not `true`.
//! Without one, no exporter, tracer or extra layer is installed.
//!
//! When enabled, incoming W3C `traceparent` headers on the HTTP API become
//! the parent of the request span, so a caller's trace continues into tidx.
//! Sampling follows the caller's decision; requests without a parent are
//! sampled at [`DEFAULT_ROOT_SAMPLE_RATIO`] unless `OTEL_TRACES_SAMPLER`
//! (and `OTEL_TRACES_SAMPLER_ARG`) choose otherwise. `OTEL_SERVICE_NAME`
//! defaults to `tidx`.

use anyhow::Result;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing::Level;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Layer, Registry};

/// Share of parentless (root) requests traced when `OTEL_TRACES_SAMPLER` is unset.
pub const DEFAULT_ROOT_SAMPLE_RATIO: f64 = 0.01;

/// Flushes and shuts down the trace exporter when dropped.
#[must_use = "dropping the guard shuts down trace export"]
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    /// Whether spans are being exported.
    pub fn exporting(&self) -> bool {
        self.provider.is_some()
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("OpenTelemetry shutdown failed: {e}");
        }
    }
}

/// Whether the environment asks for OTLP trace export.
pub fn otlp_traces_configured() -> bool {
    otlp_traces_configured_from(|key| std::env::var(key).ok())
}

fn otlp_traces_configured_from(var: impl Fn(&str) -> Option<String>) -> bool {
    if var("OTEL_SDK_DISABLED").is_some_and(|v| v.trim().eq_ignore_ascii_case("true")) {
        return false;
    }
    [
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ]
    .iter()
    .any(|key| var(key).is_some_and(|v| !v.trim().is_empty()))
}

/// Install the global subscriber: the log formatter (`RUST_LOG`, and
/// `RUST_LOG_FORMAT=json` for JSON), plus the OpenTelemetry layer when
/// [`otlp_traces_configured`].
pub fn init() -> Result<TelemetryGuard> {
    let filter = EnvFilter::from_default_env().add_directive("tidx=info".parse()?);
    let fmt_layer = if std::env::var("RUST_LOG_FORMAT").as_deref() == Ok("json") {
        tracing_subscriber::fmt::layer()
            .json()
            .with_filter(filter)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer().with_filter(filter).boxed()
    };

    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = vec![fmt_layer];
    let provider = if otlp_traces_configured() {
        let provider = build_provider()?;
        let tracer = provider.tracer("tidx");
        // Spans and events from tidx at INFO and above: the API's request,
        // parse, route, execution and serialization spans.
        let targets = Targets::new().with_target("tidx", Level::INFO);
        layers.push(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(targets)
                .boxed(),
        );
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        opentelemetry::global::set_tracer_provider(provider.clone());
        Some(provider)
    } else {
        None
    };

    tracing_subscriber::registry().with(layers).init();
    if provider.is_some() {
        tracing::info!("OpenTelemetry trace export enabled (OTLP/HTTP)");
    }
    Ok(TelemetryGuard { provider })
}

fn build_provider() -> Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()?;
    let service_name = std::env::var("OTEL_SERVICE_NAME")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "tidx".to_string());
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name)
        .with_attribute(opentelemetry::KeyValue::new(
            "service.version",
            env!("CARGO_PKG_VERSION"),
        ))
        .build();
    let mut builder = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource);
    if std::env::var("OTEL_TRACES_SAMPLER").is_err() {
        builder = builder.with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            DEFAULT_ROOT_SAMPLE_RATIO,
        ))));
    }
    Ok(builder.build())
}

struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Continue the caller's trace: make the W3C `traceparent` (and
/// `tracestate`) in `headers` the parent of `span`. A no-op without
/// trace export or a valid header.
pub fn set_parent_from_headers(span: &tracing::Span, headers: &axum::http::HeaderMap) {
    if !headers.contains_key("traceparent") {
        return;
    }
    let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(headers))
    });
    // Fails only when no OpenTelemetry layer is installed.
    let _ = span.set_parent(cx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry::trace::TraceContextExt;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key| pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    }

    #[test]
    fn export_is_off_without_an_endpoint() {
        assert!(!otlp_traces_configured_from(env(&[])));
        assert!(!otlp_traces_configured_from(env(&[(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            " "
        )])));
    }

    #[test]
    fn export_is_on_with_either_endpoint_unless_disabled() {
        assert!(otlp_traces_configured_from(env(&[(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "http://collector:4318"
        )])));
        assert!(otlp_traces_configured_from(env(&[(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://collector:4318/v1/traces"
        )])));
        assert!(!otlp_traces_configured_from(env(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318"),
            ("OTEL_SDK_DISABLED", "TRUE"),
        ])));
    }

    #[test]
    fn traceparent_header_is_extracted() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-02ca98b468e6e271276c087be23dcf6c-00f067aa0ba902b7-01"
                .parse()
                .unwrap(),
        );
        let cx = TraceContextPropagator::new().extract(&HeaderExtractor(&headers));
        let span = cx.span();
        let sc = span.span_context();
        assert!(sc.is_valid());
        assert!(sc.is_remote());
        assert!(sc.is_sampled());
        assert_eq!(
            sc.trace_id().to_string(),
            "02ca98b468e6e271276c087be23dcf6c"
        );
    }
}
