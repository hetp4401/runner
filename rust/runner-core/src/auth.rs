use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

pub fn sha256(data: impl AsRef<[u8]>) -> String {
    hex::encode(Sha256::digest(data.as_ref()))
}

pub fn equal_secret(a: &str, b: &str) -> bool {
    !b.is_empty() && bool::from(Sha256::digest(a.as_bytes()).ct_eq(&Sha256::digest(b.as_bytes())))
}

pub fn sign(key: &[u8], data: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    hex::encode(mac.finalize().into_bytes())
}

pub fn verify(key: &[u8], data: &[u8], signature: &str) -> bool {
    let Ok(signature) = hex::decode(signature) else {
        return false;
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.verify_slice(&signature).is_ok()
}

pub fn password_hash(password: &str, salt: &str) -> anyhow::Result<String> {
    let salt = hex::decode(salt)?;
    let mut out = [0; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, 10_000, &mut out);
    Ok(hex::encode(out))
}
