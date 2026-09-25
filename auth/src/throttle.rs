use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{ConnectInfo, Request, State},
    middleware::Next,
    response::Response,
};
use governor::{clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota, RateLimiter};
use uuid::Uuid;

use crate::{error::AppError, serve::client_key, AppState};

/// `retain_recent` only reclaims replenished keys, so sweep often.
const PRUNE_INTERVAL: Duration = Duration::from_secs(10);

/// Wallet challenges one account may request per minute. Enough to retry a
/// rejected wallet prompt, well short of a write flood.
pub const CHALLENGES_PER_USER_PER_MINUTE: u32 = 5;

type IpRateLimiter = RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>;
type UsernameRateLimiter = RateLimiter<String, DefaultKeyedStateStore<String>, DefaultClock>;
type UserRateLimiter = RateLimiter<Uuid, DefaultKeyedStateStore<Uuid>, DefaultClock>;

/// Per-IP stops one host flooding the credential routes; per-username stops
/// guesses against one account spread across many hosts. Per-user caps wallet
/// challenges, which need a token but no signature.
pub struct AuthThrottle {
    pub per_ip: IpRateLimiter,
    pub per_username: UsernameRateLimiter,
    pub per_user: UserRateLimiter,
}

impl AuthThrottle {
    pub fn new(
        ip_per_second: NonZeroU32,
        ip_burst: NonZeroU32,
        username_per_minute: NonZeroU32,
    ) -> Self {
        Self {
            per_ip: RateLimiter::keyed(Quota::per_second(ip_per_second).allow_burst(ip_burst)),
            per_username: RateLimiter::keyed(Quota::per_minute(username_per_minute)),
            per_user: RateLimiter::keyed(Quota::per_minute(
                NonZeroU32::new(CHALLENGES_PER_USER_PER_MINUTE).unwrap(),
            )),
        }
    }
}

/// Keys are client-controlled, so without this sweep the maps grow unboundedly.
pub fn spawn_pruner(throttle: Arc<AuthThrottle>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(PRUNE_INTERVAL);
        loop {
            ticker.tick().await;
            throttle.per_ip.retain_recent();
            throttle.per_username.retain_recent();
            throttle.per_user.retain_recent();
        }
    });
}

/// Sheds an over-budget IP before the handler hits the database or Argon2.
pub async fn per_ip(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    // Keyed by the same masked value as the connection cap, so a client holding
    // a whole IPv6 /64 gets one budget rather than one per address in it.
    if state
        .throttle
        .per_ip
        .check_key(&client_key(addr.ip()))
        .is_err()
    {
        return Err(AppError::TooManyRequests);
    }
    Ok(next.run(request).await)
}
