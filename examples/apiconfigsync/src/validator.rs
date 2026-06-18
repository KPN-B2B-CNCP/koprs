// src/validator.rs
//
// Validating admission webhook for ApiConfigSync resources.
//
// Registered at POST /validate/apiconfigsync and invoked by Kubernetes before
// any ApiConfigSync is created or updated.  The validator enforces the rules
// below and attaches non-blocking warnings for advisory conditions.
//
// Hard denials (HTTP 403 to the user):
//   - spec.apiUrl is empty or does not begin with http:// or https://
//   - spec.targetNamespace is empty
//   - spec.configKey is empty
//
// Warnings (surfaced by kubectl, request still allowed):
//   - spec.apiUrl uses plain HTTP instead of HTTPS
//   - spec.bearerToken is absent (polling will be unauthenticated)

use koprs_admission::webhook::Validator;
use koprs_admission::{AdmissionRequest, ValidationResponse};

use crate::types::ApiConfigSync;

pub struct ApiConfigSyncValidator;

impl Validator<ApiConfigSync> for ApiConfigSyncValidator {
    type Error = std::convert::Infallible;

    async fn validate(
        &self,
        request: &AdmissionRequest<ApiConfigSync>,
    ) -> Result<ValidationResponse, Self::Error> {
        // DELETE requests carry no object — nothing to validate.
        let Some(cr) = &request.object else {
            return Ok(ValidationResponse::allow());
        };

        let spec = &cr.spec;

        // --- Hard validation rules ---

        if spec.api_url.is_empty() {
            return Ok(ValidationResponse::deny("spec.apiUrl must not be empty"));
        }
        if !spec.api_url.starts_with("http://") && !spec.api_url.starts_with("https://") {
            return Ok(ValidationResponse::deny(
                "spec.apiUrl must begin with http:// or https://",
            ));
        }
        if spec.target_namespace.is_empty() {
            return Ok(ValidationResponse::deny(
                "spec.targetNamespace must not be empty",
            ));
        }
        if spec.config_key.is_empty() {
            return Ok(ValidationResponse::deny("spec.configKey must not be empty"));
        }

        // --- Advisory warnings ---

        let mut warnings = Vec::new();

        if spec.api_url.starts_with("http://") {
            warnings.push(
                "spec.apiUrl uses plain HTTP; consider HTTPS for production deployments"
                    .to_string(),
            );
        }
        if spec.bearer_token.is_none() {
            warnings.push(
                "spec.bearerToken is not set; the external API will be polled unauthenticated"
                    .to_string(),
            );
        }

        if warnings.is_empty() {
            Ok(ValidationResponse::allow())
        } else {
            Ok(ValidationResponse::allow_with_warnings(warnings))
        }
    }
}
