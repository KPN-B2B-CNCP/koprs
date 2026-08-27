// src/reconciler.rs

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::ResourceExt;
use tokio::time::Duration;
use tracing::{error, info, warn};

use koprs::controller::{Action, Context, Reconciler};
use koprs::error::KubeGenericError;
use koprs::events::{EventType, record_event};
use koprs::finalizers::{add_finalizer_namespaced, remove_finalizers};
use koprs::gc::gc_resources;
use koprs::is_being_deleted;
use koprs::meta::ObjectMetaBuilder;
use koprs::resources::{EnsureOutcome, delete_resource, ensure_resource, patch_labels};
use koprs::scope::Namespaced;
use koprs::status::{make_condition, patch_status_namespaced, upsert_condition};

use koprs_external::http::HttpPoller;
use koprs_external::watcher::{ExternalEvent, ExternalSource};

use crate::types::{ApiConfigSync, ApiConfigSyncStatus};

const FINALIZER: &str = "apiconfigsync.example.io/cleanup";
const FIELD_MANAGER: &str = "apiconfigsync-operator";
const MANAGED_LABEL: &str = "app.kubernetes.io/managed-by=apiconfigsync-operator";

// ---------------------------------------------------------------------------
// Reconciler
// ---------------------------------------------------------------------------

pub struct ApiConfigSyncReconciler;

impl Reconciler<ApiConfigSync> for ApiConfigSyncReconciler {
    type Error = KubeGenericError;

    async fn reconcile(
        &self,
        cr: Arc<ApiConfigSync>,
        ctx: Arc<Context>,
    ) -> Result<Action, KubeGenericError> {
        let client = ctx.client.clone();
        let name = cr.name_any();
        let namespace = cr
            .namespace()
            .ok_or(KubeGenericError::MissingMetadata("namespace".into()))?;

        info!(cr = %name, ns = %namespace, "reconciling ApiConfigSync");

        // -------------------------------------------------------------------
        // Deletion path
        // -------------------------------------------------------------------
        if is_being_deleted(&*cr) {
            info!(cr = %name, "deletion timestamp set — running cleanup");

            let target_ns = &cr.spec.target_namespace;
            let cm_name = configmap_name(&name);

            match delete_resource::<ConfigMap, _>(client.clone(), Namespaced(target_ns), &cm_name)
                .await
            {
                Ok(true) => info!(cm = %cm_name, ns = %target_ns, "deleted synced ConfigMap"),
                Ok(false) => info!(cm = %cm_name, "ConfigMap was already gone"),
                Err(e) => {
                    error!(error = %e, "failed to delete ConfigMap during cleanup");
                    return Err(e.into());
                }
            }

            remove_finalizers::<ApiConfigSync, _>(client.clone(), Namespaced(&namespace), &name)
                .await?;
            info!(cr = %name, "finalizer removed — deletion complete");
            return Ok(Action::await_change());
        }

        // -------------------------------------------------------------------
        // Normal reconcile path
        // -------------------------------------------------------------------

        // 1. Ensure finalizer is present.
        add_finalizer_namespaced::<ApiConfigSync>(client.clone(), &cr, FINALIZER).await?;

        // 2. Poll the external API endpoint once to fetch current config.
        //
        //    HttpPoller is created fresh each reconcile.  Because `seen` starts
        //    as false it always emits Added on the first (and only) call here;
        //    subsequent reconciles do the same.  That is intentional: we always
        //    want the latest data from the endpoint.
        let target_ns = &cr.spec.target_namespace;
        let cm_name = configmap_name(&name);

        let mut poller = build_poller(&cr.spec.api_url, cr.spec.bearer_token.as_deref(), &name);

        let config_body = match poller.poll().await {
            Ok(events) => match events.into_iter().next() {
                Some(ExternalEvent::Added(r) | ExternalEvent::Modified(r)) => {
                    String::from_utf8_lossy(&r.body).into_owned()
                }
                Some(ExternalEvent::Removed(_)) | None => {
                    warn!(
                        cr = %name,
                        url = %cr.spec.api_url,
                        "API endpoint returned no content — ConfigMap will be empty"
                    );
                    String::new()
                }
            },
            Err(e) => {
                error!(cr = %name, error = %e, "failed to poll external API");
                return Err(KubeGenericError::Internal(e.to_string()));
            }
        };

        // 3. Build and apply the ConfigMap from the fetched body.
        let desired_cm = build_configmap(
            &cm_name,
            target_ns,
            &name,
            &cr.spec.config_key,
            &config_body,
        );

        let outcome = ensure_resource::<ConfigMap, _>(
            client.clone(),
            Namespaced(target_ns),
            &desired_cm,
            FIELD_MANAGER,
        )
        .await?;
        info!(cm = %cm_name, ns = %target_ns, "applied ConfigMap");

        if outcome.was_changed() {
            let (reason, note) = match &outcome {
                EnsureOutcome::Created(_) => (
                    "ConfigMapCreated",
                    format!("ConfigMap '{cm_name}' created in namespace '{target_ns}'"),
                ),
                EnsureOutcome::Updated(_) => (
                    "ConfigMapUpdated",
                    format!("ConfigMap '{cm_name}' updated in namespace '{target_ns}'"),
                ),
                EnsureOutcome::Unchanged(_) => unreachable!(),
            };
            record_event(
                client.clone(),
                &*cr,
                EventType::Normal,
                "Sync",
                reason,
                note,
                FIELD_MANAGER,
            )
            .await?;
        }

        // 4. Garbage-collect stale ConfigMaps previously owned by this CR.
        gc_resources::<ConfigMap, _>(client.clone(), Namespaced(target_ns), MANAGED_LABEL, |cm| {
            cm.name_any() == cm_name
        })
        .await?;

        // 5. Stamp the target namespace as a label on the CR.
        patch_labels::<ApiConfigSync, _>(
            client.clone(),
            Namespaced(&namespace),
            &name,
            &[("apiconfigsync.example.io/synced-to", target_ns)],
        )
        .await?;

        // 6. Write the full status in one SSA patch.
        let generation = cr.metadata.generation;
        let status_message = format!(
            "ConfigMap '{cm_name}' synced from '{}' to namespace '{target_ns}'",
            cr.spec.api_url
        );

        let mut conditions = cr
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();
        upsert_condition(
            &mut conditions,
            make_condition(
                "Ready",
                "True",
                "ApiConfigSynced",
                &status_message,
                generation,
            ),
        );

        patch_status_namespaced::<ApiConfigSync, ApiConfigSyncStatus>(
            client.clone(),
            &namespace,
            &name,
            ApiConfigSyncStatus {
                ready: true,
                message: status_message,
                conditions,
            },
            FIELD_MANAGER,
        )
        .await?;

        info!(cr = %name, "reconcile complete");
        Ok(Action::requeue(Duration::from_secs(300)))
    }

    fn error_policy(
        &self,
        cr: Arc<ApiConfigSync>,
        error: &KubeGenericError,
        _ctx: Arc<Context>,
    ) -> Action {
        error!(cr = %cr.name_any(), error = %error, "reconcile failed — retrying in 5s");
        Action::requeue(Duration::from_secs(5))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn configmap_name(cr_name: &str) -> String {
    format!("acs-{cr_name}")
}

fn build_poller(url: &str, bearer_token: Option<&str>, cr_name: &str) -> HttpPoller {
    let poller = HttpPoller::new(url).with_name(format!("apiconfigsync/{cr_name}"));
    match bearer_token {
        Some(token) => poller.with_bearer_token(token),
        None => poller,
    }
}

fn build_configmap(
    name: &str,
    namespace: &str,
    owner_cr: &str,
    config_key: &str,
    config_body: &str,
) -> ConfigMap {
    let mut data = BTreeMap::new();
    data.insert(config_key.to_string(), config_body.to_string());

    ConfigMap {
        metadata: ObjectMetaBuilder::new()
            .name(name)
            .namespace(namespace)
            .label("app.kubernetes.io/managed-by", "apiconfigsync-operator")
            .label("apiconfigsync.example.io/owner", owner_cr)
            .build(),
        data: Some(data),
        ..Default::default()
    }
}
