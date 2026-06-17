use {
    bigdecimal::{BigDecimal, ToPrimitive},
    serde::{Deserialize, Serialize},
    sqlx::{
        encode::IsNull,
        error::BoxDynError,
        postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
        Decode, Encode, Postgres, Type,
    },
    std::fmt,
};

/// On-chain token amounts are `u64` end to end. The database column is
/// `NUMERIC(20,0)`, which losslessly holds the whole `u64` range (0..2^64-1).
/// This newtype is the single signed/unsigned conversion seam: it encodes as a NUMERIC via
/// `BigDecimal` and decodes with a checked `BigDecimal -> u64`, so a corrupt or
/// out-of-range row surfaces as a decode error at the read site instead of a
/// silent wrap. `#[serde(transparent)]` keeps the JSON shape a bare integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TokenAmount(pub u64);

impl TokenAmount {
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for TokenAmount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Convert a decoded NUMERIC into a `u64`, rejecting anything that cannot be the
/// exact non-negative integer an on-chain amount must be. A negative value
/// (e.g. a legacy wrapped `BIGINT` row), a fractional value, or a value above
/// `u64::MAX` all fail loudly rather than wrapping.
fn u64_from_big_decimal(value: &BigDecimal) -> Result<u64, BoxDynError> {
    // A raw token quantity is always a whole number; a fraction means a corrupt row, not a truncation.
    if !value.is_integer() {
        return Err(format!("token amount {value} is not an integer").into());
    }
    // to_u64 yields None for negatives and for values past u64::MAX; both are corruption, never valid.
    value
        .to_u64()
        .ok_or_else(|| format!("token amount {value} is out of range for u64").into())
}

/// Outcome of converting a reconciliation net (deposits - withdrawals) to a u64.
/// A real net equals an escrow ATA balance, itself a u64, so neither a negative
/// nor an over-u64 net can occur in a healthy system; both are corruption signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetBalance {
    /// The net is a valid non-negative u64.
    Exact(u64),
    /// Withdrawals exceeded deposits; the caller should treat the net as 0.
    Negative,
    /// The net exceeded u64::MAX; the caller should treat it as a mismatch signal.
    Overflow,
}

/// Convert a net (deposits - withdrawals) BigDecimal into a checked `NetBalance`
/// without ever wrapping. The caller decides how to log and what sentinel to use,
/// so the per-path warning context (mint id, reason) stays at the call site.
pub fn net_to_u64(net: &BigDecimal) -> NetBalance {
    use bigdecimal::Zero;
    if net < &BigDecimal::zero() {
        return NetBalance::Negative;
    }
    match net.to_u64() {
        Some(v) => NetBalance::Exact(v),
        None => NetBalance::Overflow,
    }
}

impl Type<Postgres> for TokenAmount {
    fn type_info() -> PgTypeInfo {
        <BigDecimal as Type<Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <BigDecimal as Type<Postgres>>::compatible(ty)
    }
}

impl Encode<'_, Postgres> for TokenAmount {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        let value = BigDecimal::from(self.0);
        <BigDecimal as Encode<Postgres>>::encode_by_ref(&value, buf)
    }
}

impl<'r> Decode<'r, Postgres> for TokenAmount {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let decoded = <BigDecimal as Decode<Postgres>>::decode(value)?;
        Ok(TokenAmount(u64_from_big_decimal(&decoded)?))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::str::FromStr};

    #[test]
    fn display_renders_bare_integer() {
        assert_eq!(
            TokenAmount(18_446_744_073_709_551_615).to_string(),
            "18446744073709551615"
        );
    }

    #[test]
    fn serde_round_trips_as_bare_integer() {
        let json = serde_json::to_string(&TokenAmount(u64::MAX)).unwrap();
        assert_eq!(json, "18446744073709551615");
        let back: TokenAmount = serde_json::from_str(&json).unwrap();
        assert_eq!(back, TokenAmount(u64::MAX));
    }

    #[test]
    fn decode_helper_round_trips_full_u64_range() {
        for v in [0u64, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            let bd = BigDecimal::from(v);
            assert_eq!(u64_from_big_decimal(&bd).unwrap(), v);
        }
    }

    #[test]
    fn decode_helper_rejects_negative() {
        let bd = BigDecimal::from(-1);
        let err = u64_from_big_decimal(&bd).unwrap_err().to_string();
        assert!(err.contains("out of range"), "got: {err}");
    }

    #[test]
    fn decode_helper_rejects_non_integer() {
        let bd = BigDecimal::from_str("1.5").unwrap();
        let err = u64_from_big_decimal(&bd).unwrap_err().to_string();
        assert!(err.contains("not an integer"), "got: {err}");
    }

    #[test]
    fn decode_helper_rejects_over_u64_max() {
        // u64::MAX + 1, which NUMERIC(20,0) can hold but u64 cannot.
        let bd = BigDecimal::from_str("18446744073709551616").unwrap();
        let err = u64_from_big_decimal(&bd).unwrap_err().to_string();
        assert!(err.contains("out of range"), "got: {err}");
    }
}
