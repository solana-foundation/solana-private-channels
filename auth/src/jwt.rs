use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::models::Role;

/// Issuer claim embedded in every JWT. Must match `JWT_ISSUER` in the gateway.
pub const JWT_ISSUER: &str = "private-channel-auth";

/// Audience claim embedded in every JWT. Must match `JWT_AUDIENCE` in the gateway.
pub const JWT_AUDIENCE: &str = "private-channel-gateway";

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: Uuid,
    pub role: Role,
    pub exp: usize,
    /// Issuer — always `"private-channel-auth"`.
    pub iss: String,
    /// Audience — always `"private-channel-gateway"`.
    pub aud: String,
}

pub struct JwtConfig {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
}

impl JwtConfig {
    pub fn new(secret: &str) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
        }
    }

    /// Sign a JWT for the given user. Tokens expire after 24 hours.
    pub fn sign(&self, user_id: Uuid, role: Role) -> Result<String, jsonwebtoken::errors::Error> {
        let exp = Utc::now()
            .checked_add_signed(Duration::hours(24))
            .unwrap()
            .timestamp() as usize;

        let claims = Claims {
            sub: user_id,
            role,
            exp,
            iss: JWT_ISSUER.to_string(),
            aud: JWT_AUDIENCE.to_string(),
        };

        encode(&Header::default(), &claims, &self.encoding_key)
    }

    /// Verify a JWT and return its claims. Returns an error if the token is invalid or expired.
    pub fn verify(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let mut validation = Validation::default();
        validation.set_issuer(&[JWT_ISSUER]);
        validation.set_audience(&[JWT_AUDIENCE]);
        let data = decode::<Claims>(token, &self.decoding_key, &validation)?;
        Ok(data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Role;
    use uuid::Uuid;

    const SECRET: &str = "test-secret";

    fn config() -> JwtConfig {
        JwtConfig::new(SECRET)
    }

    /// Sign a token with arbitrary claims using the test secret.
    /// Used to exercise rejection paths that `JwtConfig::sign` cannot produce
    /// (e.g. wrong issuer, wrong audience, past expiry).
    fn forge_raw(claims: &Claims) -> String {
        encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    }

    #[test]
    fn sign_and_verify_round_trip_user() {
        let id = Uuid::new_v4();
        let token = config().sign(id, Role::User).unwrap();
        let claims = config().verify(&token).unwrap();
        assert_eq!(claims.sub, id);
        assert_eq!(claims.role, Role::User);
        assert_eq!(claims.iss, JWT_ISSUER);
        assert_eq!(claims.aud, JWT_AUDIENCE);
    }

    #[test]
    fn sign_and_verify_round_trip_operator() {
        let id = Uuid::new_v4();
        let token = config().sign(id, Role::Operator).unwrap();
        let claims = config().verify(&token).unwrap();
        assert_eq!(claims.sub, id);
        assert_eq!(claims.role, Role::Operator);
    }

    /// Decodes the raw JWT payload and asserts the role claim is exactly
    /// "user" or "operator", the spelling the gateway accepts. Reading it as
    /// plain JSON instead of into `Role` is the point: a round trip through
    /// our own enum passes even if it writes "User".
    #[test]
    fn role_claim_is_lowercase_in_the_payload() {
        for (role, expected_claim) in [(Role::User, "user"), (Role::Operator, "operator")] {
            let token = config().sign(Uuid::new_v4(), role).unwrap();
            let mut validation = Validation::default();
            validation.set_issuer(&[JWT_ISSUER]);
            validation.set_audience(&[JWT_AUDIENCE]);
            let payload = decode::<serde_json::Value>(
                &token,
                &DecodingKey::from_secret(SECRET.as_bytes()),
                &validation,
            )
            .unwrap()
            .claims;
            assert_eq!(payload["role"], expected_claim);
        }
    }

    #[test]
    fn wrong_secret_is_rejected() {
        let token = config().sign(Uuid::new_v4(), Role::User).unwrap();
        assert!(JwtConfig::new("different-secret").verify(&token).is_err());
    }

    #[test]
    fn expired_token_is_rejected() {
        let claims = Claims {
            sub: Uuid::new_v4(),
            role: Role::User,
            exp: (Utc::now().timestamp() - 3600) as usize,
            iss: JWT_ISSUER.to_string(),
            aud: JWT_AUDIENCE.to_string(),
        };
        assert!(config().verify(&forge_raw(&claims)).is_err());
    }

    #[test]
    fn wrong_issuer_is_rejected() {
        let claims = Claims {
            sub: Uuid::new_v4(),
            role: Role::User,
            exp: (Utc::now().timestamp() + 3600) as usize,
            iss: "wrong-issuer".to_string(),
            aud: JWT_AUDIENCE.to_string(),
        };
        assert!(config().verify(&forge_raw(&claims)).is_err());
    }

    #[test]
    fn wrong_audience_is_rejected() {
        let claims = Claims {
            sub: Uuid::new_v4(),
            role: Role::User,
            exp: (Utc::now().timestamp() + 3600) as usize,
            iss: JWT_ISSUER.to_string(),
            aud: "wrong-audience".to_string(),
        };
        assert!(config().verify(&forge_raw(&claims)).is_err());
    }

    #[test]
    fn malformed_token_is_rejected() {
        assert!(config().verify("not.a.jwt").is_err());
        assert!(config().verify("").is_err());
        assert!(config().verify("a.b.c").is_err());
    }
}
