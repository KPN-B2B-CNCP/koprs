// src/types.rs
//
// Defines the `ApiConfigSync` CRD.  The operator watches for instances of
// this resource and ensures a ConfigMap whose content is fetched from an
// external HTTP API exists in the specified target namespace.

use koprs::status::KoprsCondition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Spec section of the ApiConfigSync CRD.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[kube(
    group = "example.io",
    version = "v1alpha1",
    kind = "ApiConfigSync",
    namespaced,
    status = "ApiConfigSyncStatus",
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.targetNamespace"}"#,
    printcolumn = r#"{"name":"ApiUrl","type":"string","jsonPath":".spec.apiUrl"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.ready"}"#
)]
pub struct ApiConfigSyncSpec {
    /// Namespace where the synced ConfigMap should be created/maintained.
    pub target_namespace: String,
    /// HTTP(S) endpoint to poll once per reconcile for configuration data.
    pub api_url: String,
    /// Optional Bearer token sent as `Authorization: Bearer <token>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<String>,
    /// Key name used in the resulting ConfigMap (default: "config").
    #[serde(default = "default_config_key")]
    pub config_key: String,
}

fn default_config_key() -> String {
    "config".to_string()
}

/// Status section written back by the operator after each reconcile.
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiConfigSyncStatus {
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub message: String,
    /// Standard Kubernetes conditions array.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<KoprsCondition>,
}
