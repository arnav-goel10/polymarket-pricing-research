use crate::auth::Creds;

/// Chainlink Data Streams endpoint set, config-driven for testnet/mainnet.
#[derive(Debug, Clone)]
pub struct Endpoints {
    pub rest_base: String,
    pub ws_base: String,
}

impl Endpoints {
    pub fn mainnet() -> Self {
        Endpoints {
            rest_base: "https://api.dataengine.chain.link".into(),
            ws_base: "wss://ws.dataengine.chain.link".into(),
        }
    }
    pub fn testnet() -> Self {
        Endpoints {
            rest_base: "https://api.testnet-dataengine.chain.link".into(),
            ws_base: "wss://ws.testnet-dataengine.chain.link".into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required env var: {0}")]
    MissingEnv(String),
}

/// Load credentials from the named environment variables, failing fast.
pub fn load_creds_from(key_var: &str, secret_var: &str) -> Result<Creds, ConfigError> {
    let api_key = std::env::var(key_var).map_err(|_| ConfigError::MissingEnv(key_var.into()))?;
    let api_secret =
        std::env::var(secret_var).map_err(|_| ConfigError::MissingEnv(secret_var.into()))?;
    Ok(Creds {
        api_key,
        api_secret,
    })
}

/// Standard accessor: CHAINLINK_STREAMS_API_KEY / CHAINLINK_STREAMS_API_SECRET.
pub fn load_creds() -> Result<Creds, ConfigError> {
    load_creds_from("CHAINLINK_STREAMS_API_KEY", "CHAINLINK_STREAMS_API_SECRET")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_endpoints_are_wellformed() {
        let e = Endpoints::mainnet();
        assert!(e.rest_base.starts_with("https://"));
        assert!(e.ws_base.starts_with("wss://"));
    }

    #[test]
    fn missing_secret_is_an_error() {
        assert!(load_creds_from("DEFINITELY_UNSET_KEY_X", "DEFINITELY_UNSET_SECRET_X").is_err());
    }
}
