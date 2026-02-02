//! CLI command for generating and managing API keys

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use rand::Rng;
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file (default: config.toml)
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,
}

/// Generate a cryptographically secure API key (32 bytes, base64url encoded)
fn generate_api_key() -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 32] = rng.random();
    base64_url_encode(&bytes)
}

/// Base64url encode without padding (RFC 4648)
fn base64_url_encode(bytes: &[u8]) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn run(args: Args) -> Result<()> {
    let config_path = &args.config;
    
    if !config_path.exists() {
        anyhow::bail!("Config file not found: {}", config_path.display());
    }

    let content = std::fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config: {}", config_path.display()))?;

    let api_key = generate_api_key();

    // Parse existing config to check current state
    let mut doc = content.parse::<toml_edit::DocumentMut>()
        .with_context(|| "Failed to parse config as TOML")?;

    // Ensure [http] section exists
    if doc.get("http").is_none() {
        doc["http"] = toml_edit::Item::Table(toml_edit::Table::new());
    }

    // Get or create api_keys array
    let http = doc["http"].as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("[http] is not a table"))?;

    if http.get("api_keys").is_none() {
        http["api_keys"] = toml_edit::value(toml_edit::Array::new());
    }

    let api_keys = http["api_keys"].as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("api_keys is not an array"))?;

    api_keys.push(&api_key);

    // Write back to file
    std::fs::write(config_path, doc.to_string())
        .with_context(|| format!("Failed to write config: {}", config_path.display()))?;

    println!("{}", api_key);
    eprintln!("Added to {} (hot-reload will apply within 500ms)", config_path.display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_api_key_length() {
        let key = generate_api_key();
        // 32 bytes base64url encoded = 43 characters (no padding)
        assert_eq!(key.len(), 43);
    }

    #[test]
    fn test_generate_api_key_unique() {
        let key1 = generate_api_key();
        let key2 = generate_api_key();
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_api_key_is_url_safe() {
        let key = generate_api_key();
        assert!(key.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }
}
