use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine};
use rand::rngs::OsRng;
use rsa::{
    pkcs1::{EncodeRsaPrivateKey, EncodeRsaPublicKey},
    RsaPrivateKey, RsaPublicKey,
};

use crate::{error::HttpError, models::User, utils::crypto, AppState};

/// Generate an RSA key pair for a new user.
///
/// * public key  -> `users.public_key` (base64 of PEM, same format as the original app)
/// * private key -> `users.private_key_enc`, sealed with the cluster MASTER_KEY, so
///   EVERY API node can decrypt on behalf of the user (the original app kept it in
///   a local .pem file, which only works with a single server).
///
/// RSA key generation is CPU-heavy, so it runs on the blocking thread pool instead
/// of stalling the async runtime.
pub async fn generate_key(app_state: Arc<AppState>, user: User) -> Result<(), HttpError> {
    let master_key = app_state.env.master_key;
    let user_id = user.id;

    let (public_b64, sealed_private) = tokio::task::spawn_blocking(
        move || -> Result<(String, Vec<u8>), HttpError> {
            let mut rng = OsRng;
            let private_key = RsaPrivateKey::new(&mut rng, 2048)
                .map_err(|e| HttpError::server_error(e.to_string()))?;
            let public_key = RsaPublicKey::from(&private_key);

            let private_pem = private_key
                .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
                .map_err(|e| HttpError::server_error(e.to_string()))?;
            let public_pem = public_key
                .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
                .map_err(|e| HttpError::server_error(e.to_string()))?;

            let sealed = crypto::seal_secret(&master_key, user_id.as_bytes(), private_pem.as_bytes())?;
            Ok((STANDARD.encode(public_pem.as_bytes()), sealed))
        },
    )
    .await
    .map_err(|e| HttpError::server_error(e.to_string()))??;

    app_state
        .db_client
        .save_user_key(user_id, public_b64, sealed_private)
        .await
        .map_err(|e| HttpError::server_error(e.to_string()))?;

    Ok(())
}
