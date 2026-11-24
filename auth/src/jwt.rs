use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::models::Role;

/// Issuer claim embedded in every JWT. Must match `JWT_ISSUER` in the gateway.
pub const JWT_ISSUER: &str = "private-channel-auth";

/// Audience claim embedded in every JWT. Must match `JWT_AUDIENCE` in the gateway.
pub const JWT_AUDIENCE: &str = "private-channel-gateway";

/// PKCS#8 header of an Ed25519 private key (RFC 8410); the 32-byte seed follows it.
const PKCS8_ED25519_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

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
    /// Current key, then the previous one during a rotation.
    decoding_keys: Vec<DecodingKey>,
}

impl JwtConfig {
    /// Signs with `signing_key`, a base58 64-byte Ed25519 keypair whose public half is
    /// `public_key`. Verifies with `public_key`, then `previous` when given. Errors name
    /// the setting, never its value.
    pub fn new(
        signing_key: &str,
        public_key: &str,
        previous: Option<&str>,
    ) -> Result<Self, String> {
        let keypair = decode_keypair(signing_key)?;
        let public = decode_public_key("JWT_PUBLIC_KEY", public_key)?;
        if keypair[32..] != public {
            return Err("JWT_PUBLIC_KEY is not the public key of JWT_SIGNING_KEY".to_string());
        }
        let mut decoding_keys = vec![DecodingKey::from_ed_der(&public)];
        if let Some(previous) = previous {
            let previous = decode_public_key("JWT_PUBLIC_KEY_PREVIOUS", previous)?;
            if previous == public {
                return Err("JWT_PUBLIC_KEY_PREVIOUS must differ from JWT_PUBLIC_KEY".to_string());
            }
            decoding_keys.push(DecodingKey::from_ed_der(&previous));
        }
        let config = Self {
            encoding_key: encoding_key(signing_key)?,
            decoding_keys,
        };

        // The keypair's public half is not checked against its seed until something is signed.
        let probe = config
            .sign(Uuid::nil(), Role::User)
            .map_err(|_| "JWT_SIGNING_KEY is not a valid Ed25519 keypair".to_string())?;
        decode::<Claims>(&probe, &config.decoding_keys[0], &validation())
            .map_err(|_| "JWT_SIGNING_KEY's seed does not match its public key".to_string())?;
        Ok(config)
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

        encode(&Header::new(Algorithm::EdDSA), &claims, &self.encoding_key)
    }

    /// Verify a JWT and return its claims. Returns an error if the token is invalid or expired.
    pub fn verify(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let validation = validation();
        let mut result = Err(jsonwebtoken::errors::ErrorKind::InvalidSignature.into());
        for key in &self.decoding_keys {
            result = decode::<Claims>(token, key, &validation);
            if result.is_ok() {
                break;
            }
        }
        Ok(result?.claims)
    }
}

/// The Ed25519 key in `signing_key`, a base58 64-byte keypair.
pub fn encoding_key(signing_key: &str) -> Result<EncodingKey, String> {
    let keypair = decode_keypair(signing_key)?;
    let der = [PKCS8_ED25519_PREFIX.as_slice(), &keypair[..32]].concat();
    Ok(EncodingKey::from_ed_der(&der))
}

/// EdDSA only, so a token signed any other way is refused before a key is tried.
fn validation() -> Validation {
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.set_issuer(&[JWT_ISSUER]);
    validation.set_audience(&[JWT_AUDIENCE]);
    validation
}

fn decode_keypair(signing_key: &str) -> Result<[u8; 64], String> {
    bs58::decode(signing_key.trim())
        .into_vec()
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| "JWT_SIGNING_KEY must be a base58 64-byte Ed25519 keypair".to_string())
}

fn decode_public_key(name: &str, value: &str) -> Result<[u8; 32], String> {
    bs58::decode(value.trim())
        .into_vec()
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| format!("{name} must be a base58 32-byte Ed25519 public key"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Role;
    use jsonwebtoken::{decode_header, Algorithm};
    use solana_sdk::signature::{Keypair, Signer};
    use uuid::Uuid;

    /// A fresh keypair as (JWT_SIGNING_KEY, JWT_PUBLIC_KEY).
    fn new_keys() -> (String, String) {
        let keypair = Keypair::new();
        (keypair.to_base58_string(), keypair.pubkey().to_string())
    }

    /// Claims `JwtConfig::sign` cannot produce: any expiry, issuer and audience.
    fn claims(exp_offset_secs: i64, iss: &str, aud: &str) -> Claims {
        Claims {
            sub: Uuid::new_v4(),
            role: Role::User,
            exp: (Utc::now().timestamp() + exp_offset_secs) as usize,
            iss: iss.to_string(),
            aud: aud.to_string(),
        }
    }

    fn forge(claims: &Claims, signing_key: &str) -> String {
        encode(
            &Header::new(Algorithm::EdDSA),
            claims,
            &encoding_key(signing_key).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn signs_with_eddsa_and_verifies_both_roles() {
        let (signing, public) = new_keys();
        let config = JwtConfig::new(&signing, &public, None).unwrap();
        for role in [Role::User, Role::Operator] {
            let user_id = Uuid::new_v4();
            let token = config.sign(user_id, role.clone()).unwrap();
            assert_eq!(decode_header(&token).unwrap().alg, Algorithm::EdDSA);
            let claims = config.verify(&token).unwrap();
            assert_eq!(claims.sub, user_id);
            assert_eq!(claims.role, role);
            assert_eq!(claims.iss, JWT_ISSUER);
            assert_eq!(claims.aud, JWT_AUDIENCE);
        }
    }

    /// The gateway trims the same way, so a stray newline cannot start one and not the other.
    #[test]
    fn keys_are_trimmed() {
        let (signing, public) = new_keys();
        let (_, previous) = new_keys();
        JwtConfig::new(
            &format!(" {signing}\n"),
            &format!("{public}\n"),
            Some(&format!("\t{previous} ")),
        )
        .unwrap();
    }

    /// Decodes the raw JWT payload and asserts the role claim is exactly
    /// "user" or "operator", the spelling the gateway accepts. Reading it as
    /// plain JSON instead of into `Role` is the point: a round trip through
    /// our own enum passes even if it writes "User".
    #[test]
    fn role_claim_is_lowercase_in_the_payload() {
        let (signing, public) = new_keys();
        let config = JwtConfig::new(&signing, &public, None).unwrap();
        let public_key = DecodingKey::from_ed_der(&bs58::decode(&public).into_vec().unwrap());
        for (role, expected_claim) in [(Role::User, "user"), (Role::Operator, "operator")] {
            let token = config.sign(Uuid::new_v4(), role).unwrap();
            let payload = decode::<serde_json::Value>(&token, &public_key, &validation())
                .unwrap()
                .claims;
            assert_eq!(payload["role"], expected_claim);
        }
    }

    /// The gateway holds the public key, so it must not be able to mint a token by
    /// using that key as an HMAC secret.
    #[test]
    fn a_token_hmac_signed_with_the_public_key_is_rejected() {
        let (signing, public) = new_keys();
        let config = JwtConfig::new(&signing, &public, None).unwrap();
        let public_bytes = bs58::decode(&public).into_vec().unwrap();
        for secret in [public.as_bytes(), public_bytes.as_slice()] {
            let token = encode(
                &Header::new(Algorithm::HS256),
                &claims(3600, JWT_ISSUER, JWT_AUDIENCE),
                &EncodingKey::from_secret(secret),
            )
            .unwrap();
            assert!(config.verify(&token).is_err());
        }
    }

    #[test]
    fn a_token_from_another_keypair_is_rejected() {
        let (signing, public) = new_keys();
        let (other_signing, _) = new_keys();
        let token = forge(&claims(3600, JWT_ISSUER, JWT_AUDIENCE), &other_signing);
        assert!(JwtConfig::new(&signing, &public, None)
            .unwrap()
            .verify(&token)
            .is_err());
    }

    #[test]
    fn sign_uses_the_current_key_only() {
        let (signing, public) = new_keys();
        let (previous_signing, previous_public) = new_keys();
        let token = JwtConfig::new(&signing, &public, Some(&previous_public))
            .unwrap()
            .sign(Uuid::new_v4(), Role::User)
            .unwrap();
        assert!(JwtConfig::new(&signing, &public, None)
            .unwrap()
            .verify(&token)
            .is_ok());
        assert!(JwtConfig::new(&previous_signing, &previous_public, None)
            .unwrap()
            .verify(&token)
            .is_err());
    }

    #[test]
    fn verify_accepts_current_and_previous_and_rejects_a_third() {
        let (signing, public) = new_keys();
        let (previous_signing, previous_public) = new_keys();
        let (third_signing, _) = new_keys();
        let rotating = JwtConfig::new(&signing, &public, Some(&previous_public)).unwrap();
        for signer in [&signing, &previous_signing] {
            let token = forge(&claims(3600, JWT_ISSUER, JWT_AUDIENCE), signer);
            assert!(rotating.verify(&token).is_ok());
        }
        let third = forge(&claims(3600, JWT_ISSUER, JWT_AUDIENCE), &third_signing);
        assert!(rotating.verify(&third).is_err());
    }

    /// Expiry, issuer and audience apply to the previous key exactly as to the current.
    #[test]
    fn both_keys_check_expiry_issuer_and_audience() {
        let (signing, public) = new_keys();
        let (previous_signing, previous_public) = new_keys();
        let rotating = JwtConfig::new(&signing, &public, Some(&previous_public)).unwrap();
        for signer in [&signing, &previous_signing] {
            for bad in [
                claims(-3600, JWT_ISSUER, JWT_AUDIENCE),
                claims(3600, "wrong-issuer", JWT_AUDIENCE),
                claims(3600, JWT_ISSUER, "wrong-audience"),
            ] {
                assert!(rotating.verify(&forge(&bad, signer)).is_err());
            }
        }
    }

    #[test]
    fn malformed_token_is_rejected() {
        let (signing, public) = new_keys();
        let config = JwtConfig::new(&signing, &public, None).unwrap();
        assert!(config.verify("not.a.jwt").is_err());
        assert!(config.verify("").is_err());
        assert!(config.verify("a.b.c").is_err());
    }

    #[test]
    fn bad_keys_are_refused_without_echoing_them() {
        let (signing, public) = new_keys();
        let (other_signing, other_public) = new_keys();
        // Another seed under this public key: only signing reveals the mismatch.
        let mut swapped_seed = bs58::decode(&other_signing).into_vec().unwrap();
        swapped_seed[32..].copy_from_slice(&bs58::decode(&public).into_vec().unwrap());
        let swapped_seed = bs58::encode(swapped_seed).into_string();
        let not_base58 = "0OIl-not-base58";

        let cases = [
            (not_base58, public.as_str(), None),
            (public.as_str(), public.as_str(), None),
            (signing.as_str(), signing.as_str(), None),
            (signing.as_str(), other_public.as_str(), None),
            (swapped_seed.as_str(), public.as_str(), None),
            (signing.as_str(), public.as_str(), Some(public.as_str())),
            (signing.as_str(), public.as_str(), Some(not_base58)),
        ];
        for (signing_key, public_key, previous) in cases {
            let Err(err) = JwtConfig::new(signing_key, public_key, previous) else {
                panic!("must be refused: {signing_key:?} {public_key:?} {previous:?}");
            };
            for value in [signing_key, public_key].into_iter().chain(previous) {
                assert!(!err.contains(value), "value leaked: {err}");
            }
        }
    }
}
