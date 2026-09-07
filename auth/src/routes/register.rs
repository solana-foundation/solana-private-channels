use axum::{extract::State, Json};
use tracing::info;

use crate::{
    db,
    error::{AppError, AppResult},
    models::{RegisterRequest, User},
    validation::{PASSWORD_MAX_LEN, PASSWORD_MIN_LEN, USERNAME_MAX_LEN, USERNAME_MIN_LEN},
    AppState,
};

pub async fn register(
    State(state): State<AppState>,
    Json(req): Json<RegisterRequest>,
) -> AppResult<Json<User>> {
    // Validate username length and characters.
    // ASCII only: Unicode alphanumerics include homoglyphs that render identically to
    // ASCII, and the signed wallet message names the account.
    if req.username.len() < USERNAME_MIN_LEN {
        return Err(AppError::BadRequest(format!(
            "username must be at least {USERNAME_MIN_LEN} characters"
        )));
    }
    if req.username.len() > USERNAME_MAX_LEN {
        return Err(AppError::BadRequest(format!(
            "username must not exceed {USERNAME_MAX_LEN} characters"
        )));
    }
    if !req
        .username
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(AppError::BadRequest(
            "username may only contain letters, numbers, underscores, and hyphens".into(),
        ));
    }

    // Validate password length before doing any hashing work.
    // chars().count() is used so limits are in characters, which is what users understand.
    let password_len = req.password.chars().count();
    if password_len < PASSWORD_MIN_LEN {
        return Err(AppError::BadRequest(format!(
            "password must be at least {PASSWORD_MIN_LEN} characters"
        )));
    }
    if password_len > PASSWORD_MAX_LEN {
        return Err(AppError::BadRequest(format!(
            "password must not exceed {PASSWORD_MAX_LEN} characters"
        )));
    }

    // Hash the password with Argon2 before storing.
    let hash = state.passwords.hash(req.password).await?;

    // Attempt the INSERT directly rather than doing a SELECT first.
    //
    // The old check-then-insert pattern (find_user_by_username → insert_user) has a TOCTOU
    // race: two concurrent requests for the same username can both pass the existence check,
    // then the second INSERT hits the UNIQUE constraint and returns a 500 instead of 409.
    //
    // The UNIQUE constraint on `username` is atomic at the database level — exactly one
    // concurrent INSERT will succeed. We rely on that guarantee and convert the constraint
    // violation into the correct 409 response here. This is one fewer round-trip and
    // correct under concurrency without any additional transaction isolation.
    //
    // The constraint name `users_username_key` is generated deterministically by Postgres
    // from the table and column name (inline UNIQUE), so it is stable as long as the
    // schema definition in db.rs does not change.
    let raw = db::insert_user(&state.pool, &req.username, &hash).await;
    state.pool_status.observe_app(&raw);
    let user = raw.map_err(|e| match e {
        AppError::Db(sqlx::Error::Database(ref db_err))
            if db_err.constraint() == Some("users_username_key") =>
        {
            // Charged on the outcome, and only when the name is already taken.
            // Charging before the INSERT let an attacker drain the budget of a
            // name nobody had registered, blocking the person who wanted it.
            if state
                .throttle
                .per_username
                .check_key(&req.username)
                .is_err()
            {
                AppError::TooManyRequests
            } else {
                AppError::Conflict("username already taken".into())
            }
        }
        other => other,
    })?;

    info!(username = %user.username, "new user registered");

    Ok(Json(user))
}
