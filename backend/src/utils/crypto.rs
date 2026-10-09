//! All cryptography in one place.
//!
//! * Every file gets a random 256-bit key.
//! * The file is cut into chunks; each chunk is encrypted independently with
//!   AES-256-GCM (random 96-bit nonce, AAD = file_id || chunk_index).
//!   Independent chunks => chunks can be encrypted/decrypted IN PARALLEL, and the
//!   AAD stops a malicious storage node from swapping/reordering chunks.
//! * The file key is wrapped with the recipient's RSA public key (OAEP-SHA256).
//! * Chunk id = SHA-256(ciphertext) => content-addressed, self-verifying storage.
//! * User RSA private keys are sealed with AES-256-GCM under the cluster MASTER_KEY.

use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Key, Nonce,
};
use rand::{rngs::OsRng, RngCore};
use rsa::{Oaep, RsaPrivateKey, RsaPublicKey};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::HttpError;

pub const NONCE_LEN: usize = 12;

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

pub fn random_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);
    key
}

fn chunk_aad(file_id: &Uuid, idx: u32) -> [u8; 20] {
    let mut aad = [0u8; 20];
    aad[..16].copy_from_slice(file_id.as_bytes());
    aad[16..].copy_from_slice(&idx.to_be_bytes());
    aad
}

pub fn encrypt_chunk(
    key: &[u8; 32],
    file_id: &Uuid,
    idx: u32,
    plain: &[u8],
) -> Result<(Vec<u8>, [u8; NONCE_LEN]), HttpError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    let aad = chunk_aad(file_id, idx);
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: plain, aad: &aad })
        .map_err(|_| HttpError::server_error("chunk encryption failed"))?;
    Ok((ct, nonce))
}

pub fn decrypt_chunk(
    key: &[u8; 32],
    file_id: &Uuid,
    idx: u32,
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, HttpError> {
    if nonce.len() != NONCE_LEN {
        return Err(HttpError::server_error("corrupt chunk metadata (bad nonce)"));
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let aad = chunk_aad(file_id, idx);
    cipher
        .decrypt(Nonce::from_slice(nonce), Payload { msg: ciphertext, aad: &aad })
        .map_err(|_| HttpError::server_error("chunk failed authentication (corrupted or tampered)"))
}

pub fn wrap_file_key(public_key: &RsaPublicKey, key: &[u8; 32]) -> Result<Vec<u8>, HttpError> {
    public_key
        .encrypt(&mut OsRng, Oaep::new::<Sha256>(), key)
        .map_err(|e| HttpError::server_error(e.to_string()))
}

pub fn unwrap_file_key(private_key: &RsaPrivateKey, wrapped: &[u8]) -> Result<[u8; 32], HttpError> {
    let raw = private_key
        .decrypt(Oaep::new::<Sha256>(), wrapped)
        .map_err(|e| HttpError::server_error(e.to_string()))?;
    raw.try_into()
        .map_err(|_| HttpError::server_error("unwrapped file key has wrong length"))
}

/// Seal a secret with the master key. Output = nonce || ciphertext.
pub fn seal_secret(master: &[u8; 32], aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, HttpError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(master));
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: plaintext, aad })
        .map_err(|_| HttpError::server_error("failed to seal secret"))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

pub fn open_secret(master: &[u8; 32], aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>, HttpError> {
    if sealed.len() <= NONCE_LEN {
        return Err(HttpError::server_error("sealed secret is too short"));
    }
    let (nonce, ct) = sealed.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(master));
    cipher
        .decrypt(Nonce::from_slice(nonce), Payload { msg: ct, aad })
        .map_err(|_| HttpError::server_error("cannot open sealed secret (wrong MASTER_KEY?)"))
}
