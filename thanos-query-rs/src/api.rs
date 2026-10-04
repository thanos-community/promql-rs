//! The Prometheus HTTP API envelope, a port of Thanos's `pkg/api/api.go`:
//! `{"status":..,"data":..,"errorType":..,"error":..,"warnings":..}`,
//! the error types and their status codes, and the CORS headers.

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::value::RawValue;

/// `api.ErrorType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    Timeout,
    Canceled,
    Exec,
    BadData,
    Internal,
    NotFound,
}

impl ErrorType {
    /// The `errorType` field.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Canceled => "canceled",
            Self::Exec => "execution",
            Self::BadData => "bad_data",
            Self::Internal => "internal",
            Self::NotFound => "not_found",
        }
    }

    /// `RespondError`'s status per type.
    pub fn status_code(self) -> StatusCode {
        match self {
            Self::BadData => StatusCode::BAD_REQUEST,
            Self::Exec => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Canceled | Self::Timeout => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NotFound => StatusCode::NOT_FOUND,
        }
    }
}

/// `api.ApiError`: what a handler returns instead of data.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ApiError {
    pub typ: ErrorType,
    pub message: String,
}

impl ApiError {
    pub fn new(typ: ErrorType, message: impl Into<String>) -> Self {
        Self {
            typ,
            message: message.into(),
        }
    }

    pub fn bad_data(message: impl Into<String>) -> Self {
        Self::new(ErrorType::BadData, message)
    }

    pub fn exec(message: impl Into<String>) -> Self {
        Self::new(ErrorType::Exec, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorType::Internal, message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(ErrorType::Timeout, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorType::NotFound, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        respond_error(&self)
    }
}

/// `api.response`, in Go's field order.
#[derive(Serialize)]
struct Envelope<'a> {
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a RawValue>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    error_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    warnings: Option<&'a [String]>,
}

/// `Respond`: 200 with the success envelope, marked uncacheable when
/// there are warnings.
pub fn respond(data: &RawValue, warnings: &[String]) -> Response {
    let body = encode(&Envelope {
        status: "success",
        data: Some(data),
        error_type: None,
        error: None,
        warnings: (!warnings.is_empty()).then_some(warnings),
    });
    let mut response = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response();
    if !warnings.is_empty() {
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    response
}

/// `RespondError`: the error envelope with the type's status code.
pub fn respond_error(err: &ApiError) -> Response {
    let body = encode(&Envelope {
        status: "error",
        data: None,
        error_type: Some(err.typ.as_str()),
        error: Some(&err.message),
        warnings: None,
    });
    (
        err.typ.status_code(),
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

fn encode<T: Serialize>(value: &T) -> String {
    escape_like_go(&serde_json::to_string(value).expect("the envelope serializes"))
}

/// What Go's `encoding/json` (and jsoniter in compatible mode) does that
/// serde_json does not: `<`, `>`, `&` and the two Unicode line separators
/// become `\u` escapes. None of them can occur outside a string in valid
/// JSON, so rewriting the whole document is safe.
pub fn escape_like_go(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    for c in json.chars() {
        match c {
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            _ => out.push(c),
        }
    }
    out
}

/// Thanos's `corsHeaders`.
const CORS_HEADERS: [(header::HeaderName, &str); 4] = [
    (
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        "Accept, Accept-Encoding, Authorization, Content-Type, Origin",
    ),
    (header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS"),
    (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
    (header::ACCESS_CONTROL_EXPOSE_HEADERS, "Date"),
];

/// `SetCORS` plus the `OPTIONS` handler as one middleware: a preflight is
/// answered with 204, and every response carries the four CORS headers
/// unless `--web-disable-cors` turned them off.
pub async fn cors(State(enabled): State<bool>, request: Request, next: Next) -> Response {
    let mut response = if request.method() == Method::OPTIONS {
        StatusCode::NO_CONTENT.into_response()
    } else {
        next.run(request).await
    };
    if enabled {
        let headers = response.headers_mut();
        for (name, value) in CORS_HEADERS {
            headers.insert(name, HeaderValue::from_static(value));
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(response: Response) -> (StatusCode, Vec<(String, String)>, String) {
        let status = response.status();
        let headers = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect();
        let bytes = futures_body(response);
        (status, headers, String::from_utf8(bytes).unwrap())
    }

    fn futures_body(response: Response) -> Vec<u8> {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .to_vec()
            })
    }

    #[test]
    fn success_envelope() {
        let data = RawValue::from_string("[\"a\"]".into()).unwrap();
        let (status, headers, text) = body(respond(&data, &[]));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(text, r#"{"status":"success","data":["a"]}"#);
        assert!(!headers.iter().any(|(k, _)| k == "cache-control"));

        let (_, headers, text) = body(respond(&data, &["slow <store>".into()]));
        assert_eq!(
            text,
            r#"{"status":"success","data":["a"],"warnings":["slow \u003cstore\u003e"]}"#
        );
        assert!(headers.contains(&("cache-control".into(), "no-store".into())));
        assert!(headers.contains(&("content-type".into(), "application/json".into())));
    }

    #[test]
    fn error_envelope_and_codes() {
        let (status, headers, text) = body(ApiError::bad_data("bad \"x\"").into_response());
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            text,
            r#"{"status":"error","errorType":"bad_data","error":"bad \"x\""}"#
        );
        assert!(headers.contains(&("cache-control".into(), "no-store".into())));

        assert_eq!(
            ErrorType::Exec.status_code(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            ErrorType::Timeout.status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            ErrorType::Canceled.status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            ErrorType::Internal.status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(ErrorType::NotFound.as_str(), "not_found");
    }

    #[test]
    fn go_escapes() {
        assert_eq!(
            escape_like_go(r#"{"a":"<&>"}"#),
            r#"{"a":"\u003c\u0026\u003e"}"#
        );
        assert_eq!(escape_like_go("\"\u{2028}\""), "\"\\u2028\"");
    }
}
