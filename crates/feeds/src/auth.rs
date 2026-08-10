use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// Chainlink Data Streams API credentials (loaded from env, never hardcoded).
#[derive(Debug, Clone)]
pub struct Creds {
    pub api_key: String,
    pub api_secret: String,
}

/// The three auth header values for a request.
#[derive(Debug, Clone)]
pub struct SignedHeaders {
    pub api_key: String,
    pub timestamp_millis: i64,
    pub signature: String,
}

/// Lowercase-hex SHA-256 of a byte body.
pub fn body_hash(body: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(body);
    hex::encode(h.finalize())
}

/// Build the auth headers for a Data Streams request (REST or WS handshake).
/// String to sign: `METHOD FULL_PATH BODY_HASH API_KEY TIMESTAMP`.
pub fn sign(
    creds: &Creds,
    method: &str,
    full_path: &str,
    body: &[u8],
    timestamp_millis: i64,
) -> SignedHeaders {
    let bh = body_hash(body);
    let to_sign = format!(
        "{method} {full_path} {bh} {} {timestamp_millis}",
        creds.api_key
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(creds.api_secret.as_bytes())
        .expect("HMAC accepts any key length");
    mac.update(to_sign.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    SignedHeaders {
        api_key: creds.api_key.clone(),
        timestamp_millis,
        signature,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_hash_of_empty_string_is_known_sha256() {
        assert_eq!(
            body_hash(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn signature_is_deterministic_hmac_sha256_hex() {
        let creds = Creds {
            api_key: "key-123".into(),
            api_secret: "secret-xyz".into(),
        };
        let h = sign(
            &creds,
            "GET",
            "/api/v1/reports/latest?feedID=0xabc",
            b"",
            1_716_211_845_123,
        );
        assert_eq!(h.signature.len(), 64);
        let h2 = sign(
            &creds,
            "GET",
            "/api/v1/reports/latest?feedID=0xabc",
            b"",
            1_716_211_845_123,
        );
        assert_eq!(h.signature, h2.signature);
        assert_eq!(h.api_key, "key-123");
        assert_eq!(h.timestamp_millis, 1_716_211_845_123);
    }

    #[test]
    fn different_path_changes_signature() {
        let creds = Creds {
            api_key: "k".into(),
            api_secret: "s".into(),
        };
        let a = sign(&creds, "GET", "/a", b"", 1).signature;
        let b = sign(&creds, "GET", "/b", b"", 1).signature;
        assert_ne!(a, b);
    }
}
