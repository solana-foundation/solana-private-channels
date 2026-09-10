use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;
use tracing::{error, warn};

/// True when the database never answered: the pool or the connection to it is
/// the problem, not the query. These flip `/health` unhealthy and shed as 503,
/// since the client can retry them and we have no bug to report.
pub fn is_communication_error(e: &sqlx::Error) -> bool {
    matches!(
        e,
        sqlx::Error::Io(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed
    )
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    /// Client is over its per-IP or per-username budget on the credential routes.
    #[error("too many requests")]
    TooManyRequests,
    /// Password hashing is at its concurrency cap. Shedding here keeps the
    /// runtime responsive instead of queueing work without bound.
    #[error("service unavailable")]
    Unavailable,
    /// Wraps unexpected internal failures. The message is logged but never sent to the client.
    #[error("internal error")]
    Internal(#[from] anyhow::Error),
    /// DB errors are caught here and returned without leaking details: 503 when
    /// the database was unreachable, 500 when it answered and we asked wrong.
    #[error("database error")]
    Db(#[from] sqlx::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, self.to_string()),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            AppError::TooManyRequests => (StatusCode::TOO_MANY_REQUESTS, self.to_string()),
            AppError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, self.to_string()),
            AppError::Internal(e) => {
                error!("internal error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
            AppError::Db(e) if is_communication_error(e) => {
                warn!("database unavailable: {e}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "service unavailable".to_string(),
                )
            }
            AppError::Db(e) => {
                error!("database error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    fn status(err: AppError) -> StatusCode {
        err.into_response().status()
    }

    #[test]
    fn unauthorized_maps_to_401() {
        assert_eq!(status(AppError::Unauthorized), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn conflict_maps_to_409() {
        assert_eq!(
            status(AppError::Conflict("duplicate username".to_string())),
            StatusCode::CONFLICT,
        );
    }

    #[test]
    fn bad_request_maps_to_400() {
        assert_eq!(
            status(AppError::BadRequest("missing field".to_string())),
            StatusCode::BAD_REQUEST,
        );
    }

    #[test]
    fn internal_maps_to_500() {
        assert_eq!(
            status(AppError::Internal(anyhow::anyhow!("boom"))),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }

    #[test]
    fn an_unreachable_database_sheds_as_503() {
        assert_eq!(
            status(AppError::Db(sqlx::Error::PoolTimedOut)),
            StatusCode::SERVICE_UNAVAILABLE,
        );
    }

    #[test]
    fn a_query_the_database_rejected_maps_to_500() {
        // The database answered, so this is our bug, not a capacity shed.
        assert_eq!(
            status(AppError::Db(sqlx::Error::RowNotFound)),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }
}
