use argon2::{
    password_hash::{
        rand_core::OsRng,
        PasswordHash,
        PasswordHasher,
        PasswordVerifier,
        SaltString
    },
    Argon2
};

use crate::error::ErrorMessage;

const MAX_PASSWORD_LENGTH: usize = 64;

pub fn hash(password: impl Into<String>) -> Result<String, ErrorMessage> {
    let password = password.into();

    if password.is_empty() {
        return Err(ErrorMessage::EmptyPassword);
    }

    if password.len() > MAX_PASSWORD_LENGTH {
        return Err(ErrorMessage::ExceededMaxPasswordLength(MAX_PASSWORD_LENGTH));
    }

    let salt = SaltString::generate(&mut OsRng);
    let hash_password = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|_| ErrorMessage::HashingError)?
        .to_string();

    Ok(hash_password)
}

pub fn compare(password: &str, hashed_password: &str) -> Result<bool, ErrorMessage> {
    if password.is_empty() {
        return Err(ErrorMessage::EmptyPassword);
    }

    if password.len() > MAX_PASSWORD_LENGTH {
        return Err(ErrorMessage::ExceededMaxPasswordLength(MAX_PASSWORD_LENGTH));
    }

    let parsed_hash = PasswordHash::new(hashed_password)
    .map_err(|_| ErrorMessage::InvalidHashFormat)?;

    let password_matched = Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .map_or(false, |_| true);

    Ok(password_matched)
}

/// Argon2 is deliberately slow (tens of ms of pure CPU). Running it directly inside an
/// async handler would block a runtime worker thread, so these wrappers move it to the
/// blocking pool. Under load this is what lets many logins run in parallel.
pub async fn hash_async(password: String) -> Result<String, ErrorMessage> {
    tokio::task::spawn_blocking(move || hash(password))
        .await
        .map_err(|_| ErrorMessage::HashingError)?
}

pub async fn compare_async(password: String, hashed: String) -> Result<bool, ErrorMessage> {
    tokio::task::spawn_blocking(move || compare(&password, &hashed))
        .await
        .map_err(|_| ErrorMessage::HashingError)?
}
