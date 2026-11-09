//! Field-level AES-256-GCM encryption of vehicle location (contract §5).
//!
//! Wire format: `{"ciphertext": b64(ct || tag), "nonce": b64(12 bytes), "key_id": "..."}`.
//! AAD binds each ciphertext to its record: `telemetry.location.v1|<vin>|<event_id>`.

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::fmt;

pub const KEY_ENV: &str = "LOCATION_ENC_KEY";
pub const KEY_ID_ENV: &str = "LOCATION_ENC_KEY_ID";
pub const DEFAULT_KEY_ID: &str = "local-dev-1";
pub const AAD_PREFIX: &str = "telemetry.location.v1";
pub const NONCE_LEN: usize = 12;
pub const TAG_LEN: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocationEnc {
    pub ciphertext: String,
    pub nonce: String,
    pub key_id: String,
}

/// Exact decimal coordinates as JSON number literals (no binary-float rounding).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub lat: Number,
    pub lon: Number,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CryptoError {
    MissingKey,
    InvalidKeyEncoding,
    InvalidKeyLength(usize),
    KeyIdMismatch,
    InvalidNonce,
    InvalidCiphertextEncoding,
    AuthenticationFailed,
    InvalidPlaintext,
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CryptoError::MissingKey => write!(f, "{KEY_ENV} is not set"),
            CryptoError::InvalidKeyEncoding => write!(f, "{KEY_ENV} is not valid standard base64"),
            CryptoError::InvalidKeyLength(n) => {
                write!(f, "{KEY_ENV} must decode to 32 bytes (got {n})")
            }
            CryptoError::KeyIdMismatch => {
                write!(f, "location_enc.key_id does not match the configured key")
            }
            CryptoError::InvalidNonce => write!(f, "nonce is not base64 of 12 bytes"),
            CryptoError::InvalidCiphertextEncoding => {
                write!(f, "ciphertext is not valid base64 or too short")
            }
            CryptoError::AuthenticationFailed => write!(f, "AES-GCM authentication failed"),
            CryptoError::InvalidPlaintext => {
                write!(f, "decrypted plaintext is not a {{lat,lon}} object")
            }
        }
    }
}

impl std::error::Error for CryptoError {}

pub struct LocationCipher {
    cipher: Aes256Gcm,
    key_id: String,
}

impl fmt::Debug for LocationCipher {
    // Never print key material.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocationCipher")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl LocationCipher {
    pub fn from_bytes(key: &[u8], key_id: &str) -> Result<Self, CryptoError> {
        if key.len() != 32 {
            return Err(CryptoError::InvalidKeyLength(key.len()));
        }
        let cipher =
            Aes256Gcm::new_from_slice(key).map_err(|_| CryptoError::InvalidKeyLength(key.len()))?;
        Ok(Self {
            cipher,
            key_id: key_id.to_string(),
        })
    }

    pub fn from_base64(key_b64: &str, key_id: &str) -> Result<Self, CryptoError> {
        let trimmed = key_b64.trim();
        if trimmed.is_empty() {
            return Err(CryptoError::MissingKey);
        }
        let raw = B64
            .decode(trimmed)
            .map_err(|_| CryptoError::InvalidKeyEncoding)?;
        Self::from_bytes(&raw, key_id)
    }

    /// Reads `LOCATION_ENC_KEY` / `LOCATION_ENC_KEY_ID`. Callers must treat any error as fatal at startup.
    pub fn from_env() -> Result<Self, CryptoError> {
        let key = std::env::var(KEY_ENV).map_err(|_| CryptoError::MissingKey)?;
        let key_id = std::env::var(KEY_ID_ENV).unwrap_or_else(|_| DEFAULT_KEY_ID.to_string());
        Self::from_base64(
            &key,
            if key_id.trim().is_empty() {
                DEFAULT_KEY_ID
            } else {
                key_id.trim()
            },
        )
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }
}

pub fn aad(vin: &str, event_id: &str) -> Vec<u8> {
    format!("{AAD_PREFIX}|{vin}|{event_id}").into_bytes()
}

fn plaintext(loc: &Location) -> Vec<u8> {
    let mut m = serde_json::Map::new();
    m.insert("lat".into(), Value::Number(loc.lat.clone()));
    m.insert("lon".into(), Value::Number(loc.lon.clone()));
    serde_json::to_vec(&Value::Object(m)).expect("serializing a two-number object cannot fail")
}

/// Encrypts with a fresh random 96-bit nonce from the OS CSPRNG.
pub fn encrypt_location(
    cipher: &LocationCipher,
    vin: &str,
    event_id: &str,
    loc: &Location,
) -> Result<LocationEnc, CryptoError> {
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let pt = plaintext(loc);
    let aad = aad(vin, event_id);
    let ct = cipher
        .cipher
        .encrypt(
            &nonce,
            Payload {
                msg: &pt,
                aad: &aad,
            },
        )
        .map_err(|_| CryptoError::AuthenticationFailed)?;
    Ok(LocationEnc {
        ciphertext: B64.encode(ct),
        nonce: B64.encode(nonce),
        key_id: cipher.key_id.clone(),
    })
}

pub fn decrypt_location(
    cipher: &LocationCipher,
    vin: &str,
    event_id: &str,
    enc: &LocationEnc,
) -> Result<Location, CryptoError> {
    if enc.key_id != cipher.key_id {
        return Err(CryptoError::KeyIdMismatch);
    }
    let nonce = B64
        .decode(&enc.nonce)
        .map_err(|_| CryptoError::InvalidNonce)?;
    if nonce.len() != NONCE_LEN {
        return Err(CryptoError::InvalidNonce);
    }
    let ct = B64
        .decode(&enc.ciphertext)
        .map_err(|_| CryptoError::InvalidCiphertextEncoding)?;
    if ct.len() < TAG_LEN {
        return Err(CryptoError::InvalidCiphertextEncoding);
    }
    let aad = aad(vin, event_id);
    let pt = cipher
        .cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &ct,
                aad: &aad,
            },
        )
        .map_err(|_| CryptoError::AuthenticationFailed)?;
    let v: Value = serde_json::from_slice(&pt).map_err(|_| CryptoError::InvalidPlaintext)?;
    match (v.get("lat"), v.get("lon")) {
        (Some(Value::Number(lat)), Some(Value::Number(lon))) => Ok(Location {
            lat: lat.clone(),
            lon: lon.clone(),
        }),
        _ => Err(CryptoError::InvalidPlaintext),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn key() -> [u8; 32] {
        let mut k = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut k);
        k
    }

    fn cipher() -> LocationCipher {
        LocationCipher::from_bytes(&key(), "test-key").unwrap()
    }

    fn loc() -> Location {
        Location {
            lat: Number::from_str("10.12345").unwrap(),
            lon: Number::from_str("-29.54321").unwrap(),
        }
    }

    #[test]
    fn round_trip_recovers_exact_decimals() {
        let c = cipher();
        let enc = encrypt_location(&c, "SYN-VIN-0001", "evt-1", &loc()).unwrap();
        let back = decrypt_location(&c, "SYN-VIN-0001", "evt-1", &enc).unwrap();
        assert_eq!(back, loc());
        assert_eq!(back.lat.to_string(), "10.12345");
    }

    #[test]
    fn wire_format_lengths() {
        let c = cipher();
        let enc = encrypt_location(&c, "v", "e", &loc()).unwrap();
        assert_eq!(B64.decode(&enc.nonce).unwrap().len(), 12);
        let pt_len = plaintext(&loc()).len();
        assert_eq!(B64.decode(&enc.ciphertext).unwrap().len(), pt_len + TAG_LEN);
        assert_eq!(enc.key_id, "test-key");
        let ct = B64.decode(&enc.ciphertext).unwrap();
        assert!(!String::from_utf8_lossy(&ct).contains("10.12345"));
        assert!(!enc.ciphertext.contains("10.12345"));
    }

    #[test]
    fn nonce_is_fresh_per_event_and_ciphertexts_differ() {
        let c = cipher();
        let mut nonces = std::collections::HashSet::new();
        let mut cts = std::collections::HashSet::new();
        for _ in 0..1000 {
            let enc = encrypt_location(&c, "v", "e", &loc()).unwrap();
            assert!(nonces.insert(enc.nonce.clone()), "nonce reused");
            cts.insert(enc.ciphertext);
        }
        assert_eq!(cts.len(), 1000);
    }

    #[test]
    fn tampered_ciphertext_is_rejected() {
        let c = cipher();
        let enc = encrypt_location(&c, "v", "e", &loc()).unwrap();
        let mut ct = B64.decode(&enc.ciphertext).unwrap();
        for i in [0, ct.len() / 2, ct.len() - 1] {
            ct[i] ^= 0x01;
            let bad = LocationEnc {
                ciphertext: B64.encode(&ct),
                ..enc.clone()
            };
            assert_eq!(
                decrypt_location(&c, "v", "e", &bad),
                Err(CryptoError::AuthenticationFailed)
            );
            ct[i] ^= 0x01;
        }
    }

    #[test]
    fn tampered_nonce_is_rejected() {
        let c = cipher();
        let enc = encrypt_location(&c, "v", "e", &loc()).unwrap();
        let mut n = B64.decode(&enc.nonce).unwrap();
        n[0] ^= 0x80;
        let bad = LocationEnc {
            nonce: B64.encode(&n),
            ..enc
        };
        assert_eq!(
            decrypt_location(&c, "v", "e", &bad),
            Err(CryptoError::AuthenticationFailed)
        );
    }

    #[test]
    fn truncated_ciphertext_and_bad_nonce_length_are_rejected() {
        let c = cipher();
        let enc = encrypt_location(&c, "v", "e", &loc()).unwrap();
        let short = LocationEnc {
            ciphertext: B64.encode([0u8; 8]),
            ..enc.clone()
        };
        assert_eq!(
            decrypt_location(&c, "v", "e", &short),
            Err(CryptoError::InvalidCiphertextEncoding)
        );
        let n8 = LocationEnc {
            nonce: B64.encode([0u8; 8]),
            ..enc.clone()
        };
        assert_eq!(
            decrypt_location(&c, "v", "e", &n8),
            Err(CryptoError::InvalidNonce)
        );
        let notb64 = LocationEnc {
            ciphertext: "***".into(),
            ..enc
        };
        assert_eq!(
            decrypt_location(&c, "v", "e", &notb64),
            Err(CryptoError::InvalidCiphertextEncoding)
        );
    }

    #[test]
    fn ciphertext_bound_to_record_via_aad() {
        let c = cipher();
        let enc = encrypt_location(&c, "SYN-VIN-0001", "evt-1", &loc()).unwrap();
        assert_eq!(
            decrypt_location(&c, "SYN-VIN-0002", "evt-1", &enc),
            Err(CryptoError::AuthenticationFailed)
        );
        assert_eq!(
            decrypt_location(&c, "SYN-VIN-0001", "evt-2", &enc),
            Err(CryptoError::AuthenticationFailed)
        );
    }

    #[test]
    fn wrong_key_is_rejected() {
        let a = cipher();
        let b = cipher();
        let enc = encrypt_location(&a, "v", "e", &loc()).unwrap();
        assert_eq!(
            decrypt_location(&b, "v", "e", &enc),
            Err(CryptoError::AuthenticationFailed)
        );
    }

    #[test]
    fn key_id_mismatch_is_rejected() {
        let k = key();
        let a = LocationCipher::from_bytes(&k, "k1").unwrap();
        let b = LocationCipher::from_bytes(&k, "k2").unwrap();
        let enc = encrypt_location(&a, "v", "e", &loc()).unwrap();
        assert_eq!(
            decrypt_location(&b, "v", "e", &enc),
            Err(CryptoError::KeyIdMismatch)
        );
    }

    #[test]
    fn key_parsing_hard_fails() {
        assert_eq!(
            LocationCipher::from_base64("", "k").unwrap_err(),
            CryptoError::MissingKey
        );
        assert_eq!(
            LocationCipher::from_base64("   ", "k").unwrap_err(),
            CryptoError::MissingKey
        );
        assert_eq!(
            LocationCipher::from_base64("not base64!!", "k").unwrap_err(),
            CryptoError::InvalidKeyEncoding
        );
        assert_eq!(
            LocationCipher::from_base64(&B64.encode([1u8; 16]), "k").unwrap_err(),
            CryptoError::InvalidKeyLength(16)
        );
        assert_eq!(
            LocationCipher::from_base64(&B64.encode([1u8; 33]), "k").unwrap_err(),
            CryptoError::InvalidKeyLength(33)
        );
        assert!(LocationCipher::from_base64(&B64.encode([1u8; 32]), "k").is_ok());
    }

    #[test]
    fn debug_output_never_contains_key_material() {
        let k = [0x41u8; 32];
        let c = LocationCipher::from_bytes(&k, "k").unwrap();
        let dbg = format!("{c:?}");
        assert!(!dbg.contains(&B64.encode(k)));
        assert!(!dbg.contains("AAAA"));
    }
}
