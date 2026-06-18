// src/main.rs
//
// Wires together a controller loop and a validating admission webhook, running
// both concurrently on the same Tokio runtime.
//
// Controller — koprs::controller::ControllerBuilder
//   Reconciles ApiConfigSync CRs: polls the configured external HTTP API
//   (koprs-external) and syncs the response body into a ConfigMap in the
//   target namespace.
//
//   .health_port(8080)         — GET /healthz + GET /readyz for pod probes
//   .metrics_port(9090)        — GET /metrics — Prometheus reconcile counts/errors/durations
//   .graceful_shutdown()       — clean stop on SIGTERM / Ctrl+C
//   .leader_election(...)      — Kubernetes Lease-based HA; only one replica reconciles
//   .reconcile_timeout(300s)   — kills and requeues reconciles stuck longer than 5 minutes
//
// Admission webhook — koprs_admission::WebhookBuilder
//   Validates ApiConfigSync resources before they are persisted.  Denies
//   resources with invalid URLs, empty namespaces, or missing configKey.
//   Attaches non-blocking warnings for plain HTTP URLs or missing bearer tokens.
//
//   POST /validate/apiconfigsync — handled by ApiConfigSyncValidator
//   .port(8443)                — HTTPS (or plain HTTP in dev if no TLS certs found)
//   .health_port(8081)         — separate probe port so k8s can target the webhook pod
//   .graceful_shutdown()       — clean stop on SIGTERM / Ctrl+C
//
// TLS certificates for the webhook are loaded from the paths set by
// WEBHOOK_TLS_CERT / WEBHOOK_TLS_KEY (default: /tls/tls.crt, /tls/tls.key).
// When the files are absent the webhook falls back to plain HTTP, which is
// useful for local development and integration testing.

mod reconciler;
mod types;
mod validator;

use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, Client};
use tracing::info;

use koprs::controller::{Context, ControllerBuilder, watcher};
use koprs::owners::owner_label_mapper;
use koprs_admission::WebhookBuilder;

use crate::reconciler::ApiConfigSyncReconciler;
use crate::types::ApiConfigSync;
use crate::validator::ApiConfigSyncValidator;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    info!("starting apiconfigsync-operator");

    let client = Client::try_default().await?;

    // The operator namespace is injected via the downward API in production:
    //   env:
    //     - name: OPERATOR_NAMESPACE
    //       valueFrom:
    //         fieldRef:
    //           fieldPath: metadata.namespace
    let operator_ns =
        std::env::var("OPERATOR_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    // Drive the controller loop and webhook server concurrently.
    // If either task returns an error, the other is cancelled and the process exits.
    tokio::try_join!(
        run_controller(client, operator_ns),
        run_webhook(),
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

async fn run_controller(client: Client, operator_ns: String) -> anyhow::Result<()> {
    // Primary watched resource — all ApiConfigSync CRs across all namespaces.
    let acs_api: Api<ApiConfigSync> = Api::all(client.clone());

    // Secondary watch — managed ConfigMaps re-queue the owning CR on change.
    let cm_api: Api<ConfigMap> = Api::all(client.clone());

    let ctx = Context::new(client);

    let labels = "app.kubernetes.io/managed-by=apiconfigsync-operator";

    ControllerBuilder::new(acs_api)
        .watch(
            cm_api,
            watcher::Config::default().labels(labels),
            owner_label_mapper("apiconfigsync.example.io/owner"),
        )
        .health_port(8080)
        .metrics_port(9090)
        .graceful_shutdown()
        .leader_election(operator_ns, "apiconfigsync-operator-leader")
        .reconcile_timeout(Duration::from_secs(300))
        .run(ApiConfigSyncReconciler, ctx)
        .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Admission webhook
// ---------------------------------------------------------------------------

async fn run_webhook() -> anyhow::Result<()> {
    let cert_path =
        std::env::var("WEBHOOK_TLS_CERT").unwrap_or_else(|_| "/tls/tls.crt".to_string());
    let key_path =
        std::env::var("WEBHOOK_TLS_KEY").unwrap_or_else(|_| "/tls/tls.key".to_string());

    let mut builder = WebhookBuilder::new()
        .port(8443)
        .health_port(8081)
        .graceful_shutdown()
        .validate("/validate/apiconfigsync", ApiConfigSyncValidator);

    // Load TLS certificates when the cert-manager Secret volume is mounted.
    // Falls back to plain HTTP when the files are absent (local dev / CI).
    match (std::fs::read(&cert_path), std::fs::read(&key_path)) {
        (Ok(cert), Ok(key)) => {
            info!(cert = %cert_path, "admission webhook: TLS enabled");
            builder = builder.tls_from_pem(&cert, &key)?;
        }
        _ => {
            info!("admission webhook: TLS cert/key not found — serving plain HTTP (dev mode)");
        }
    }

    builder.run().await?;
    Ok(())
}
