use crate::validation::ApiError;
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};

pub struct HttpResponse {
    status: u16,
    content_type: String,
    body: Vec<u8>,
    retry_after_seconds: Option<u32>,
}

impl HttpResponse {
    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn json_body(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|_| json!({"raw": String::from_utf8_lossy(&self.body)}))
    }

    pub fn text_body(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json".to_string(),
            body: serde_json::to_vec(&value).unwrap(),
            retry_after_seconds: None,
        }
    }

    pub fn html(status: u16, value: String) -> Self {
        Self {
            status,
            content_type: "text/html; charset=utf-8".to_string(),
            body: value.into_bytes(),
            retry_after_seconds: None,
        }
    }

    pub fn text(status: u16, content_type: &str, value: String) -> Self {
        Self {
            status,
            content_type: content_type.to_string(),
            body: value.into_bytes(),
            retry_after_seconds: None,
        }
    }

    pub fn bytes(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: content_type.to_string(),
            body,
            retry_after_seconds: None,
        }
    }

    pub fn from_api_error(err: &ApiError) -> Self {
        let mut response = Self::json(err.status, err.body());
        response.retry_after_seconds = err.retry_after_seconds;
        response
    }

    pub fn with_retry_after(mut self, seconds: u32) -> Self {
        self.retry_after_seconds = Some(seconds);
        self
    }
}

impl IntoResponse for HttpResponse {
    fn into_response(self) -> Response {
        let mut builder = Response::builder()
            .status(self.status)
            .header(axum::http::header::CONTENT_TYPE, self.content_type);
        if let Some(seconds) = self.retry_after_seconds {
            builder = builder.header(axum::http::header::RETRY_AFTER, seconds.to_string());
        }
        builder
            .body(axum::body::Body::from(self.body))
            .expect("valid response headers")
    }
}
